//! 玩家运行契约，供 RPC、网关与所有权后端共同使用。
//!
//! 不包含会话调度实现、网络协议或具体游戏业务；通用服务设施不依赖本 crate。
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod credentials;
mod error;
mod message;
pub mod ownership;
mod session;
mod timer;

pub use error::{Error, Result};
pub use message::{LogicalMessage, MessageKind};
pub use session::SessionIdentity;
pub use timer::{TimerSpec, TimerUpdate};
