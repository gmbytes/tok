//! PlayerIngress：统一 Gateway 中的玩家接入和逻辑会话组件。

pub mod auth;
pub mod config;
mod connection;
pub mod executor;
pub mod gateway;
pub mod ingress;
pub mod outbound;
pub mod resume;
pub mod session;
pub mod timer;

pub use auth::{AuthenticatedPlayer, AuthenticationTask, Authenticator};
pub use config::PlayerConfig;
pub use executor::{PlayerExecutor, PlayerReply, PlayerTask, RpcErrorMapper, RpcPlayerExecutor};
pub use gateway::PlayerGateway;
pub use ingress::PlayerIngress;
pub use session::{SessionHandle, SessionManager};
