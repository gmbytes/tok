//! 接入组件共用的监听、容量控制、后台任务监督和退出管理。

use crate::{ErrorCode, Result, transport::GatewayTransport};
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinSet,
};
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

/// 接入组件向统一运行时提交后台任务的入口。
#[derive(Clone)]
pub struct GatewayContext {
    shutdown: CancellationToken,
    launches: mpsc::Sender<GatewayTask>,
    capacity: Arc<Semaphore>,
}

/// 已预留的后台任务容量；丢弃时归还，可用于改变所有权前检查资源。
pub struct GatewayTaskReservation {
    context: GatewayContext,
    permit: tokio::sync::OwnedSemaphorePermit,
}
impl GatewayTaskReservation {
    /// 使用已预留容量提交受监督任务。
    pub async fn launch(self, task: GatewayTask) -> std::io::Result<()> {
        let permit = self.permit;
        let task: GatewayTask = Box::pin(async move {
            let _permit = permit;
            task.await
        });
        tokio::select! {
            biased;
            _ = self.context.shutdown.cancelled() => Err(std::io::Error::other("网关正在退出")),
            result = self.context.launches.send(task) => result.map_err(|_| std::io::Error::other("网关任务入口已关闭")),
        }
    }
}
impl GatewayContext {
    /// 预留一个后台任务名额；满时立即失败，不启动任务。
    pub fn reserve(&self) -> std::io::Result<GatewayTaskReservation> {
        let permit = Arc::clone(&self.capacity)
            .try_acquire_owned()
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::WouldBlock, "网关后台任务容量已满")
            })?;
        Ok(GatewayTaskReservation {
            context: self.clone(),
            permit,
        })
    }
    /// 网关关闭时取消；组件应派生子 token 管理自己的会话。
    pub fn cancellation(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// 提交后台任务；容量满立即失败，提交成功不表示任务已开始执行。
    pub async fn launch(&self, task: GatewayTask) -> std::io::Result<()> {
        self.reserve()?.launch(task).await
    }
}

/// 连接处理与业务生命周期的组装契约，不在运行时判断 Player 或 Scene。
pub trait GatewayIngress: Send + Sync + 'static {
    /// 此组件接受的连接对象，由相匹配的传输适配器提供。
    type Connection: Send + 'static;
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
    config: GatewayConfig,
    ingress: Arc<I>,
    context: GatewayContext,
    launches: mpsc::Receiver<GatewayTask>,
}
struct Stop<I: GatewayIngress>(Arc<I>, CancellationToken);
impl<I: GatewayIngress> Drop for Stop<I> {
    fn drop(&mut self) {
        self.1.cancel();
        self.0.stop();
    }
}
impl<I: GatewayIngress> Gateway<I> {
    /// 创建运行框架并注入组件所需的任务入口，不启动监听或后台任务。
    pub fn new(
        config: GatewayConfig,
        build: impl FnOnce(GatewayContext) -> Result<I>,
    ) -> Result<Self> {
        config.validate()?;
        let (sender, launches) = mpsc::channel(config.max_tasks);
        let context = GatewayContext {
            shutdown: CancellationToken::new(),
            launches: sender,
            capacity: Arc::new(Semaphore::new(config.max_tasks)),
        };
        let ingress = Arc::new(build(context.clone())?);
        Ok(Self {
            config,
            ingress,
            context,
            launches,
        })
    }
    /// 访问已组装的接入组件，如取得 Ingress 的 session 查询句柄。
    pub fn ingress(&self) -> &I {
        &self.ingress
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
        let _stop = Stop(Arc::clone(&self.ingress), self.context.shutdown.clone());
        let capacity = Arc::new(Semaphore::new(self.config.max_connections));
        let mut connections = JoinSet::new();
        let mut tasks = JoinSet::new();
        let mut outcome = loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break Ok(()),
                _ = self.context.shutdown.cancelled() => break Ok(()),
                Some(task) = self.launches.recv() => { tasks.spawn(task); }
                result = tasks.join_next(), if !tasks.is_empty() => {
                    match result {
                        Some(Err(error)) => break Err(std::io::Error::other(format!("网关后台任务崩溃：{error}"))),
                        Some(Ok(Err(error))) => break Err(error),
                        _ => {}
                    }
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
                    let signal = self.context.shutdown.child_token();
                    connections.spawn(async move {
                        let _permit = permit;
                        if let Err(error) = ingress.connection(connection, signal).await {
                            tracing::debug!(error = %error, "网关连接已结束");
                        }
                    });
                }
            }
        };
        drop(transport);
        self.context.shutdown.cancel();
        self.ingress.stop();
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        // 将尚未启动的任务也纳入监督；组件收到取消后只能清理，不能开启新业务。
        while let Ok(task) = self.launches.try_recv() {
            tasks.spawn(task);
        }
        while let Some(result) = tasks.join_next().await {
            let error = match result {
                Err(error) => Some(std::io::Error::other(format!(
                    "网关后台清理任务崩溃：{error}"
                ))),
                Ok(Err(error)) => Some(error),
                _ => None,
            };
            if let Some(error) = error {
                outcome = outcome.and(Err(error));
            }
        }
        if let Err(error) = self.ingress.finish().await {
            outcome = outcome.and(Err(error));
        }
        outcome
    }
}
