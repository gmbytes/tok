//! 统一 Gateway 运行框架，按组件组装 Player 接入及后续 Scene 接入。
//! 当前提供 TCP、注入认证、路由、所有权、timer 调度、下行 ACK 缓存、同网关恢复及显式开启的跨网关交接；场景组件后续实施。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod error;
pub use error::{ErrorCode, GatewayError, Result};
/// 客户端逻辑协议，正文由具体游戏定义。
pub use tok_rpc::protocol::client::v1 as protocol;

pub mod codec;
pub mod player;
pub mod rpc;
pub mod scene;
pub mod security;
mod server;
pub mod transport;
pub use server::{Gateway, GatewayConfig, GatewayIngress, GatewayTask};
