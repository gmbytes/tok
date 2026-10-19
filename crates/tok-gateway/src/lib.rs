//! 统一 Gateway 运行框架，按组件组装 Player 接入及后续 Scene 接入。
//! 当前提供 TCP、注入认证、路由和所有权后端；timer、恢复及场景组件后续实施。

pub mod error;
pub use error::{ErrorCode, GatewayError, Result};
/// 客户端逻辑协议，正文由具体游戏定义。
pub use tok_rpc::protocol::client::v1 as protocol;

pub mod codec;
pub mod player;
pub mod runtime;
pub mod scene;
pub mod security;
pub mod transport;
pub use runtime::{Gateway, GatewayConfig, GatewayContext, GatewayIngress, GatewayTask};
