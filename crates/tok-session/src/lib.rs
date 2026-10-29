//! 客户端逻辑会话契约与协议适配，供 RPC、网关与所有权后端共同使用。
//!
//! [`SessionIdentity`] 标识逻辑会话；[`LogicalMessage`] 表达客户端业务消息；
//! [`TimerSpec`]、[`TimerUpdate`] 与 [`TimerTrigger`] 描述服务器声明和驱动的计时。
//! [`credentials::ResumeTokenCodec`] 只负责本地签发、验签及有效期校验；
//! [`ownership::Store`] 协调会话驱动权与最新登录标记，持有者复用 [`tok_core::Identity`]。
//! 凭证有效不代表持有驱动权；两个领域分别使用自己的 Error / Result。
//! [`notification`] 编码客户端通知，[`logic::rpc::timer`] 转换逻辑会话计时协议。
//!
//! 不包含网关会话调度、网络监听或具体游戏业务；尚不定义场景会话契约。
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod credentials;
/// 由逻辑会话驱动的业务入口。
pub mod logic;
mod message;
pub mod ownership;
mod session;
mod timer;

pub use message::{LogicalMessage, MessageKind, NotificationError, notification};
pub use session::SessionIdentity;
pub use timer::{TimerSpec, TimerTrigger, TimerUpdate};
