//! 网关的只读快照、任务入口与完整退出观察。
use crate::GatewayConfig;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tok_core::{Extensions, service::Identity};
use tok_service::task;
use tokio::sync::{Semaphore, watch};
use tokio_util::sync::CancellationToken;

/// 网关运行阶段；Stopped 包含组件 finish 与被跟踪任务的释放。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// 已组装，尚未驱动监听。
    Prepared,
    /// 正在接受连接。
    Serving,
    /// 已请求停止，正在清理。
    Stopping,
    /// 运行 future 已释放且任务范围已结束。
    Stopped,
}
/// 单次观察所得的网关信息；后续操作仍会重新检查准入状态。
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// 当前节点和服务身份。
    pub identity: Identity,
    /// 当前运行阶段。
    pub phase: Phase,
    /// 正在占用连接名额的数量。
    pub connections: usize,
    /// 连接容量上限。
    pub max_connections: usize,
    /// 已预留、排队和执行中的后台任务名额。
    pub background_tasks: usize,
    /// 后台任务容量上限。
    pub max_tasks: usize,
}
struct GatewayState {
    identity: Identity,
    config: GatewayConfig,
    extensions: Extensions,
    serving: AtomicBool,
    closed: watch::Sender<bool>,
    connections: Arc<Semaphore>,
}
/// 可克隆的网关操作句柄，不拥有监听循环。
#[derive(Clone)]
pub struct Handle {
    state: Arc<GatewayState>,
    tasks: task::Handle,
}
impl Handle {
    pub(crate) fn new(
        identity: Identity,
        config: GatewayConfig,
        extensions: Extensions,
        tasks: task::Handle,
    ) -> Self {
        let (closed, _) = watch::channel(false);
        Self {
            state: Arc::new(GatewayState {
                identity,
                config,
                extensions,
                serving: AtomicBool::new(false),
                closed,
                connections: Arc::new(Semaphore::new(config.max_connections)),
            }),
            tasks,
        }
    }
    /// 网关所属服务身份。
    pub fn identity(&self) -> &Identity {
        &self.state.identity
    }
    /// 工厂安装的网关作用域能力，不跨进程传播。
    pub fn extensions(&self) -> &Extensions {
        &self.state.extensions
    }
    /// 取得受监督后台任务入口。
    pub fn tasks(&self) -> task::Handle {
        self.tasks.clone()
    }
    /// 查询运行阶段，不代表后续操作必然成功。
    pub fn phase(&self) -> Phase {
        if *self.state.closed.borrow() && self.tasks.state() == task::State::Stopped {
            Phase::Stopped
        } else if self.tasks.state() != task::State::Open || *self.state.closed.borrow() {
            Phase::Stopping
        } else if self.state.serving.load(Ordering::Acquire) {
            Phase::Serving
        } else {
            Phase::Prepared
        }
    }
    /// 查询有界容量及阶段快照。
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            identity: self.identity().clone(),
            phase: self.phase(),
            connections: self.state.config.max_connections
                - self.state.connections.available_permits(),
            max_connections: self.state.config.max_connections,
            background_tasks: self.tasks.used_capacity(),
            max_tasks: self.state.config.max_tasks,
        }
    }
    /// 请求停止新接入及所属任务，不等待清理。
    pub fn request_stop(&self) {
        self.tasks.request_stop();
    }
    /// 获取网关范围的取消信号。
    pub fn cancellation(&self) -> CancellationToken {
        self.tasks.cancellation()
    }
    /// 等待监听、组件清理以及全部被跟踪工作释放；不得在自身任务内等待。
    pub async fn wait_finished(&self) {
        let mut closed = self.state.closed.subscribe();
        while !*closed.borrow_and_update() {
            if closed.changed().await.is_err() {
                break;
            }
        }
        self.tasks.wait_finished().await;
    }
    pub(crate) fn serving(&self) {
        self.state.serving.store(true, Ordering::Release);
    }
    pub(crate) fn close_owner(&self) {
        self.request_stop();
        self.state.closed.send_replace(true);
    }
    pub(crate) fn connections(&self) -> Arc<Semaphore> {
        Arc::clone(&self.state.connections)
    }
}
