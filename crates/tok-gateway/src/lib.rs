//! 可注入认证、路由与所有权后端的玩家 TCP 接入和串行 session。
//! 场景网关、timer、短线恢复和安全适配按后续阶段实施。

pub mod error;
pub use error::{ErrorCode, GatewayError, Result};
/// 客户端逻辑协议，正文由具体游戏定义。
pub use tok_rpc::protocol::client::v1 as protocol;

pub mod codec;
pub mod player;
pub mod scene;
pub mod security;
pub mod transport;
