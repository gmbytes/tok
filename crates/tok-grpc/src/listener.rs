use std::{
    collections::BTreeMap,
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use tok_core::BoxFuture;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{
    body::Body,
    codegen::http::{Request, Response},
    server::NamedService,
    service::interceptor::InterceptedService,
    transport::{Server, server::Router},
};
use tower::Service;

use crate::Error;

/// 各业务服务的就绪开关。未就绪的调用返回 Unavailable。
#[derive(Clone, Default)]
pub struct Readiness(BTreeMap<String, Arc<AtomicBool>>);

impl Readiness {
    /// 发布就绪前应已完成对应业务服务初始化；更新成功返回 true，未绑定的名称返回 false。
    /// 应用遍历多个端点时可以忽略未命中结果；直接指定服务时应检查返回值。
    pub fn set(&self, service_name: &str, ready: bool) -> bool {
        let Some(flag) = self.0.get(service_name) else {
            return false;
        };
        flag.store(ready, Ordering::Release);
        true
    }

    /// 退出或初始化失败时先停止接收所有绑定服务的新调用。
    pub fn clear(&self) {
        for flag in self.0.values() {
            flag.store(false, Ordering::Release);
        }
    }
}

/// 一个共享监听端点及其显式业务绑定。
///
/// 同一 protobuf 服务只能绑定一次，避免 tonic 路由相互覆盖。
/// 不同 protobuf 服务可以共享一个端点，支持用户自己生成的服务与 Tower 包装。
pub struct Listener {
    server: Server,
    router: Option<Router>,
    bindings: BTreeMap<String, &'static str>,
    readiness: Readiness,
}

impl Default for Listener {
    fn default() -> Self {
        Self::from_server(Server::builder())
    }
}

impl Listener {
    /// 创建没有业务绑定、尚未监听的端点。
    pub fn new() -> Self {
        Self::default()
    }

    /// 使用已配置的 Tonic Server 创建尚未绑定业务的端点，保留 TLS 与 HTTP/2 等传输参数。
    ///
    /// TLS 需在应用依赖中启用 Tonic 对应的 TLS feature。
    /// 本入口接受未添加 Tower Layer 的 Server；业务中间件可在 add_service 时包装具体服务。
    /// 服务绑定、就绪控制和监听 Future 的监督约定与 new 相同。
    pub fn from_server(server: Server) -> Self {
        Self {
            server,
            router: None,
            bindings: BTreeMap::new(),
            readiness: Readiness::default(),
        }
    }

    /// 绑定业务名到 NamedService；默认未就绪，由运行时开启入口。
    pub fn add_service<S>(
        mut self,
        service_name: impl Into<String>,
        service: S,
    ) -> Result<Self, Error>
    where
        S: Service<Request<Body>, Response = Response<Body>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        let service_name = service_name.into();
        if service_name.trim().is_empty() {
            return Err(Error::Configuration(
                "RPC 绑定的业务服务名称不能为空".into(),
            ));
        }
        if self.bindings.contains_key(&service_name) {
            return Err(Error::Configuration(format!(
                "RPC 业务服务重复绑定：{service_name}"
            )));
        }
        if self.bindings.values().any(|name| *name == S::NAME) {
            return Err(Error::Configuration(format!(
                "RPC protobuf 服务重复绑定：{}",
                S::NAME
            )));
        }
        let ready = Arc::new(AtomicBool::new(false));
        self.readiness
            .0
            .insert(service_name.clone(), Arc::clone(&ready));
        self.bindings.insert(service_name, S::NAME);
        let service = InterceptedService::new(service, move |request: tonic::Request<()>| {
            if !ready.load(Ordering::Acquire) {
                return Err(tonic::Status::unavailable("业务服务尚未就绪或正在退出"));
            }
            Ok(request)
        });
        self.router = Some(match self.router {
            Some(router) => router.add_service(service),
            None => self.server.add_service(service),
        });
        Ok(self)
    }

    /// 遍历已绑定的业务服务名，供应用核对端点配置。
    pub fn service_names(&self) -> impl Iterator<Item = &str> {
        self.bindings.keys().map(String::as_str)
    }

    /// 获取共享就绪开关；通常由 Application 在初始化和退出时管理。
    pub fn readiness(&self) -> Readiness {
        self.readiness.clone()
    }

    /// 先绑定监听，返回待驱动的监听 Future。调用方必须驱动、监督并设置退出期限。
    pub async fn bind(
        self,
        address: std::net::SocketAddr,
        shutdown: CancellationToken,
    ) -> Result<BoxFuture<'static, Result<(), Error>>, Error> {
        let listener = TcpListener::bind(address).await?;
        self.serve(listener, shutdown)
    }

    /// 接管已绑定的监听，可用于需要动态端口或自定义监听准备的组装。
    /// 返回待驱动的 Future，不创建任务；调用方必须驱动、监督并设置退出期限。
    pub fn serve(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<BoxFuture<'static, Result<(), Error>>, Error> {
        let router = self
            .router
            .ok_or_else(|| Error::Configuration("RPC 端点至少需要绑定一个服务".into()))?;
        Ok(Box::pin(async move {
            router
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    shutdown.cancelled_owned(),
                )
                .await?;
            Ok(())
        }))
    }
}
