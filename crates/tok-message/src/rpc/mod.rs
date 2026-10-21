//! 编号消息的 gRPC 协议适配：会话、事件和计时入口。
mod binding;
pub mod business;
mod call;
mod context;
mod handler;
pub mod lock;
pub mod player;
mod service;
pub mod timer;

pub use binding::Binding;
pub use context::CallContext;
pub use handler::{CallTask, EventTask, Handler, HandlerRegistry, HandlerReply, TowerHandler};
pub use service::Service;
pub use tok_grpc::Error;

pub use tok_proto as protocol;
/// gRPC 客户端、服务端和状态类型；详细 API 见 tonic 上游文档。
pub use tonic;
/// Service、Layer 与中间件组装工具；详细 API 见 Tower 上游文档。
pub use tower;
