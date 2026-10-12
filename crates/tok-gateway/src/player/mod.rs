//! PlayerGateway：玩家服务接入和逻辑会话管理。

pub mod auth;
pub mod config;
pub mod executor;
pub mod gateway;
pub mod outbound;
pub mod resume;
pub mod session;
pub mod timer;

pub use auth::{AuthenticatedPlayer, AuthenticationTask, Authenticator};
pub use config::GatewayConfig;
pub use executor::{PlayerExecutor, PlayerTask, RpcErrorMapper, RpcPlayerExecutor};
pub use gateway::PlayerGateway;
pub use session::{SessionHandle, SessionManager};
