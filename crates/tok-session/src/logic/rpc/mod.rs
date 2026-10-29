//! 主体 RPC 协议适配：会话、事件和计时入口。
mod binding;
mod call;
mod client;
mod context;
mod handler;
mod input;
mod registry_service;
mod service;
/// 主体计时协议转换。
pub mod timer;

pub use binding::Binding;
pub use client::Client;
pub use context::CallContext;
pub use handler::{HandlerRegistry, HandlerReply};
pub use input::{Input, Output};
pub use registry_service::RegistryService;
pub use service::Service;
pub use tok_grpc::Error;

pub use tok_proto as protocol;
/// gRPC 客户端、服务端和状态类型；详细 API 见 tonic 上游文档。
pub use tonic;
/// Service、Layer 与中间件组装工具；详细 API 见 Tower 上游文档。
pub use tower;

type Request = tok_execution::Request<Input>;
type Response = tok_execution::Response<Output>;
type Chain = tower::util::BoxCloneSyncService<Request, Response, tonic::Status>;
