//! 将端点的业务入口与工厂初始化结果绑定；共享端点仍由 Application 一次监听。
use super::CallContext;
use super::{CallTask, EventTask, Handler};
use std::sync::{Arc, OnceLock};
use tok_core::{Extensions, service::Identity};
use tok_execution::Request;
use tok_lifecycle::{Handle, Init};
use tok_proto::internal::v1::Payload;
use tonic::Status;

struct Installed {
    handler: Arc<dyn Handler>,
    service: Handle,
    extensions: Extensions,
}
/// 端点与服务工厂之间的一次性绑定槽；只在初始化中安装，不负责驱动服务。
#[derive(Clone)]
pub struct Binding {
    identity: Identity,
    installed: Arc<OnceLock<Installed>>,
}
impl Binding {
    pub(crate) fn new(identity: Identity) -> Self {
        Self {
            identity,
            installed: Arc::new(OnceLock::new()),
        }
    }
    /// 查询一次性处理者是否安装；应用在初始化结束后据此检查入口就绪。
    pub fn is_installed(&self) -> bool {
        self.installed.get().is_some()
    }
    /// 在所属服务工厂中安装实际 handler、生命周期与能力；拒绝错误身份和重复安装。
    pub fn install(&self, init: &Init, handler: Arc<dyn Handler>) -> Result<(), super::Error> {
        if init.identity != self.identity {
            return Err(super::Error::Configuration(
                "RPC 绑定与初始化服务身份不匹配".into(),
            ));
        }
        let mut extensions = init.extensions.clone();
        // 初始化装配能力不能存回自身，避免 Binding → Installed → Extensions 的引用环。
        drop(extensions.remove::<Self>());
        self.installed
            .set(Installed {
                handler,
                service: init.service.clone(),
                extensions,
            })
            .map_err(|_| super::Error::Configuration("RPC 服务处理者已安装".into()))
    }
    fn prepare<T>(&self, request: &mut Request<T>) -> Result<&Installed, Status> {
        if request.context.service() != &self.identity {
            return Err(Status::failed_precondition("RPC 执行服务与绑定不匹配"));
        }
        let installed = self
            .installed
            .get()
            .ok_or_else(|| Status::unavailable("RPC 服务尚未初始化"))?;
        request
            .context
            .extensions_mut()
            .extend(&installed.extensions);
        request
            .context
            .extensions_mut()
            .insert(installed.service.clone());
        Ok(installed)
    }
}
impl Handler for Binding {
    fn call(&self, mut request: Request<(CallContext, Payload)>) -> CallTask<'_> {
        Box::pin(async move {
            let installed = self.prepare(&mut request)?;
            request
                .context
                .extensions_mut()
                .insert(request.input.0.clone());
            let stop = installed.service.cancellation();
            let cancellation = request.context.cancellation();
            tokio::select! { biased;
                ()=stop.cancelled()=> {cancellation.cancel();Err(Status::cancelled("服务正在退出"))},
                result=installed.handler.call(request)=>result,
            }
        })
    }
    fn event(
        &self,
        mut request: Request<(CallContext, Arc<tok_events::Message>)>,
    ) -> EventTask<'_> {
        Box::pin(async move {
            let installed = self.prepare(&mut request)?;
            request
                .context
                .extensions_mut()
                .insert(request.input.0.clone());
            let stop = installed.service.cancellation();
            let cancellation = request.context.cancellation();
            tokio::select! { biased;
                ()=stop.cancelled()=> {cancellation.cancel();Err(Status::cancelled("服务正在退出"))},
                result=installed.handler.event(request)=>result,
            }
        })
    }
}
