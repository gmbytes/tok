//! Ingress：统一 Gateway 中的玩家接入和逻辑会话组件。

pub mod auth;
mod config;
mod connection;
pub mod events;
mod executor;
mod pipeline;
pub use pipeline::Pipeline;
mod ingress;
pub mod resume_token;
pub mod session;

pub use auth::{AuthenticatedIdentity, AuthenticationTask, Authenticator};
pub use config::Config;
pub use executor::{
    ClientRequest, DefaultRpcErrorMapper, EventInput, EventTask, Executor, Reply, RequestInput,
    RpcErrorMapper, RpcExecutor, Task, TimerInput,
};
pub use ingress::{Ingress, MAX_CREDENTIAL_BYTES};
pub(crate) use session::HandoffAction;
pub use session::{
    Handle as SessionHandle, Manager as SessionManager, PendingResponse, ReplayBatch,
};
