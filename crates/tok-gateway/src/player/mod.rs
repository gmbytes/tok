//! Ingress：统一 Gateway 中的玩家接入和逻辑会话组件。

pub mod auth;
mod config;
mod connection;
mod executor;
mod ingress;
pub mod resume_token;
mod session;

pub use auth::{AuthenticatedIdentity, AuthenticationTask, Authenticator};
pub use config::Config;
pub use executor::{DefaultRpcErrorMapper, Executor, Reply, RpcErrorMapper, RpcExecutor, Task};
pub use ingress::{Ingress, MAX_CREDENTIAL_BYTES};
pub(crate) use session::HandoffAction;
pub use session::{PendingResponse, ReplayBatch, SessionHandle, SessionManager};
