//! 玩家运行契约，供 RPC、网关与所有权后端共同使用。
//!
//! [`SessionIdentity`] 标识逻辑会话；[`LogicalMessage`] 表达客户端业务消息；
//! [`TimerSpec`]、[`TimerUpdate`] 与 [`TimerTrigger`] 描述服务器声明和驱动的计时。
//! [`credentials::ResumeTokenCodec`] 只负责本地签发、验签及有效期校验；
//! [`ownership::Store`] 协调会话驱动权与最新登录标记，持有者复用 [`tok_core::Identity`]。
//! 凭证有效不代表持有驱动权；两个领域分别使用自己的 Error / Result。
//!
//! 不包含会话调度实现、网络协议或具体游戏业务；通用服务设施不依赖本 crate。
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod credentials;
mod message;
pub mod ownership;
mod session;
mod timer;

pub use message::{LogicalMessage, MessageKind};
pub use session::SessionIdentity;
pub use timer::{TimerSpec, TimerTrigger, TimerUpdate};
