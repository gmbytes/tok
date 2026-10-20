//! 玩家逻辑会话：句柄、生命周期协调与受监督执行，内部机制按职责组织。

pub(super) mod channel;
pub(in crate::player) mod events;
mod handle;
mod handoff;
mod manager;
mod replay;
mod state;
mod subscriptions;
mod timer;
mod worker;

pub(super) use handle::BoundSession;
pub use handle::{PendingResponse, SessionHandle};
pub(crate) use handoff::HandoffAction;
pub use manager::SessionManager;
pub use replay::ReplayBatch;

#[cfg(test)]
mod tests;
