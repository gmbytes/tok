//! 接入组件共用的监听、容量控制、后台任务监督和退出管理。

use crate::Handle;
use crate::{ErrorCode, Result, transport::GatewayTransport};
use std::{future::Future, pin::Pin, sync::Arc};
use tok_core::{Extensions, service::Identity};
use tok_service::{lifecycle::Init, task::Supervisor};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// 受网关监督的连接或后台任务，不能自行遗留脱管任务。
pub type GatewayTask = Pin<Box<dyn Future<Output = std::io::Result<()>> + Send>>;

/// 与 Player、Scene 语义无关的运行容量。
#[derive(Debug, Clone, Copy)]
pub struct GatewayConfig {
    /// 同时处理的接入连接数；具体连接类型由传输适配器决定。
    pub max_connections: usize,
    /// 待启动与正在运行的后台任务总上限。
    pub max_tasks: usize,
}
impl GatewayConfig {
    /// 验证容量为 1 至 65536。
    pub fn validate(&self) -> Result<()> {
        if [self.max_connections, self.max_tasks]
            .iter()
            .any(|value| *value == 0 || *value > 65_536)
        {
            return Err(ErrorCode::InvalidRequest.into());
        }
        Ok(())
    }
}

/// 连接处理与业务生命周期的组装契约，不在运行时判断 Player 或 Scene。
pub trait GatewayIngress: Send + Sync + 'static {
    /// 此组件接受的连接对象，由相匹配的传输适配器提供。
    type Connection: Send + 'static;
    /// 完成组装后执行一次同步注册；不启动连接任务，失败时撤销本组件并关闭范围。
    fn initialize(&self) -> Result<()> {
        Ok(())
    }
    /// 处理一个连接；返回错误仅结束该连接，panic 则使网关整体失败。
    fn connection(&self, connection: Self::Connection, shutdown: CancellationToken) -> GatewayTask;
    /// 同步停止新业务入口；运行 future 被丢弃时也会调用，须可重复调用。
    fn stop(&self) {}
    /// 后台任务收束后的异步清理；整体期限由外层 Application 或调用方控制。
    fn finish(&self) -> GatewayTask {
        Box::pin(async { Ok(()) })
    }
}

/// 统一网关运行框架，连接语义由传输适配器与接入组件组装。
pub struct Gateway<I: GatewayIngress> {
    ingress: Arc<I>,
    supervisor: Supervisor,
    handle: Handle,
    _owner: Owner<I>,
}
struct Owner<I: GatewayIngress> {
    ingress: Option<Arc<I>>,
    handle: Handle,
}
impl<I: GatewayIngress> Drop for Owner<I> {
    fn drop(&mut self) {
        self.handle.request_stop();
        if let Some(ingress) = &self.ingress {
            ingress.stop();
        }
        self.handle.close_owner();
    }
}
impl<I: GatewayIngress> Gateway<I> {
    /// 创建运行框架并注入组件所需的任务入口，不启动监听或后台任务。
    pub fn new(
        identity: Identity,
        config: GatewayConfig,
        build: impl FnOnce(Handle) -> Result<I>,
    ) -> Result<Self> {
        config.validate()?;
        let supervisor = Supervisor::new(config.max_tasks, CancellationToken::new())
            .map_err(|_| ErrorCode::InvalidRequest)?;
        Self::build(identity, Extensions::default(), config, supervisor, build)
    }
    /// 在所属服务内创建网关；共享父取消关系，父结束等待覆盖网关内部任务的实际回收。
    pub fn new_in(
        config: GatewayConfig,
        init: &Init,
        build: impl FnOnce(Handle) -> Result<I>,
    ) -> Result<Self> {
        config.validate()?;
        let supervisor = Supervisor::child(config.max_tasks, &init.service.tasks())
            .map_err(|_| ErrorCode::InvalidRequest)?;
        let mut extensions = init.extensions.clone();
        extensions.insert(init.service.clone());
        Self::build(init.identity.clone(), extensions, config, supervisor, build)
    }
    fn build(
        identity: Identity,
        extensions: Extensions,
        config: GatewayConfig,
        supervisor: Supervisor,
        build: impl FnOnce(Handle) -> Result<I>,
    ) -> Result<Self> {
        if identity.node_id.trim().is_empty() || identity.service_name.trim().is_empty() {
            return Err(ErrorCode::InvalidRequest.into());
        }
        let handle = Handle::new(identity, config, extensions, supervisor.handle());
        let mut owner = Owner {
            ingress: None,
            handle: handle.clone(),
        };
        let ingress = Arc::new(build(handle.clone())?);
        owner.ingress = Some(Arc::clone(&ingress));
        ingress.initialize()?;
        Ok(Self {
            ingress,
            supervisor,
            handle,
            _owner: owner,
        })
    }
    /// 访问已组装的接入组件，如取得 Ingress 的 session 查询句柄。
    pub fn ingress(&self) -> &I {
        &self.ingress
    }

    /// 获取网关阶段、容量、能力与完整退出观察句柄。
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }

    /// 获取通用任务监督入口；游戏组装层可预留容量并安装事件等后台任务。
    pub fn tasks(&self) -> tok_service::task::Handle {
        self.supervisor.handle()
    }

    /// 接管监听并监督连接与后台任务；退出时停止入口、收束任务并清理组件。
    pub async fn serve<T>(
        mut self,
        mut transport: T,
        shutdown: CancellationToken,
    ) -> std::io::Result<()>
    where
        T: GatewayTransport<Connection = I::Connection>,
    {
        let signal = self.supervisor.handle().cancellation();
        self.handle.serving();
        let capacity = self.handle.connections();
        let mut connections = JoinSet::new();
        let mut outcome = loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break Ok(()),
                _ = signal.cancelled() => break Ok(()),
                completion = self.supervisor.next() => {
                    if let Err(error) = completion.result { break Err(std::io::Error::other(error)); }
                }
                result = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = result { break Err(std::io::Error::other(format!("网关连接任务崩溃：{error}"))); }
                }
                accepted = transport.accept() => {
                    let connection = match accepted { Ok(connection) => connection, Err(error) => break Err(error) };
                    let permit = match Arc::clone(&capacity).try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => { metrics::counter!("tok_gateway_connections_rejected_total").increment(1); drop(connection); continue; }
                    };
                    let ingress = Arc::clone(&self.ingress);
                    let signal = signal.child_token();
                    connections.spawn(self.supervisor.track(async move {
                        let _permit = permit;
                        if let Err(error) = ingress.connection(connection, signal).await {
                            tracing::debug!(error = %error, "网关连接已结束");
                        }
                    }));
                }
            }
        };
        drop(transport);
        self.supervisor.handle().request_stop();
        self.ingress.stop();
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        // 专用连接退出后，后台任务沿共用监督器收束；整体期限仍由外层控制。
        if let Err(error) = self.supervisor.shutdown(None).await {
            outcome = outcome.and(Err(std::io::Error::other(error)));
        }
        if let Err(error) = self.ingress.finish().await {
            outcome = outcome.and(Err(error));
        }
        outcome
    }
}
