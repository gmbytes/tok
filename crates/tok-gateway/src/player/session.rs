//! 玩家逻辑会话：句柄、生命周期协调与受监督执行，内部机制按职责组织。

pub(super) mod channel;
pub(in crate::player) mod events;
pub mod extension;
mod handle;
mod handoff;
mod manager;
mod replay;
mod state;
mod subscriptions;
mod timer;
mod topics;
mod worker;

pub(super) use handle::BoundSession;
pub(super) use handle::SessionHandle;
pub use handle::{PendingResponse, SessionHandle as Handle};
pub(crate) use handoff::HandoffAction;
pub use manager::SessionManager as Manager;
pub(super) use manager::SessionManager;
pub use replay::ReplayBatch;

#[cfg(test)]
mod tests;

/// 逻辑会话阶段；快照是观察值，准入仍需重验。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// 已取得身份，尚未完成登录绑定。
    Opening,
    /// 当前连接在线。
    Online,
    /// 连接断开，仍处于可恢复期。
    Detached,
    /// 正在恢复连接或补发。
    Resuming,
    /// 已暂停新业务，正在交接。
    Handoff,
    /// 已取消，正在收束。
    Stopping,
    /// 执行任务已释放。
    Stopped,
}
/// 当前会话的通信、主题和容量快照；身份通过 SessionHandle::identity 获取。
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// 当前阶段。
    pub phase: Phase,
    /// 当前连接代次，初次绑定从一开始。
    pub connection_generation: u64,
    /// 是否保留连接绑定。
    pub connected: bool,
    /// 已加入的本地主题。
    pub topics: Vec<String>,
    /// 已准入的最大客户端请求序号。
    pub last_request_id: u64,
    /// 最后分配的下行序号。
    pub last_server_seq: u64,
    /// 当前排队工作数量，不含执行中的工作。
    pub queue_len: usize,
    /// 会话 FIFO 容量上限。
    pub queue_capacity: usize,
}

/// 玩家接入范围的生命周期阶段。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManagerPhase {
    /// 接受新的认证会话协调。
    Open,
    /// 已停止准入，尚有会话或协调 future。
    Stopping,
    /// 已准入的会话及协调 future 已释放。
    Stopped,
}
/// 玩家管理器的容量与生命周期快照。
#[derive(Clone, Debug)]
pub struct ManagerSnapshot {
    /// 当前网关所有者身份。
    pub owner: tok_core::Identity,
    /// 当前玩家接入阶段。
    pub phase: ManagerPhase,
    /// 尚未失效的逻辑会话数量。
    pub sessions: usize,
    /// 同时保留的会话槽位上限。
    pub max_sessions: usize,
    /// 每个会话的 FIFO 容量。
    pub queue_capacity: usize,
}
