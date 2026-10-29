//! 普通编号消息的 gRPC 协议适配。
mod binding;
pub mod business;
mod call;
mod context;
mod handler;
pub mod lock;
mod service;

pub use binding::Binding;
pub use context::CallContext;
pub use handler::{CallTask, Handler, TowerHandler};
pub use service::Service;
pub use tok_grpc::Error;
pub use tok_proto as protocol;
/// gRPC 客户端、服务端和状态类型。
pub use tonic;
/// Service、Layer 与中间件组装工具。
pub use tower;
