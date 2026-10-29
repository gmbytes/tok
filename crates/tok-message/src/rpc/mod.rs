//! 普通编号消息的 gRPC 协议适配。
mod binding;
mod call;
mod client;
mod context;
mod handler;
pub mod lock;
mod registry_handler;
mod service;

pub use binding::Binding;
pub use client::Client;
pub use context::CallContext;
pub use registry_handler::RegistryHandler;
pub use service::Service;
/// RPC 适配构造及配置错误；调用失败使用具体的 gRPC 状态。
pub use tok_grpc::Error;
pub use tok_proto as protocol;
/// gRPC 客户端、服务端和状态类型。
pub use tonic;
/// Service、Layer 与中间件组装工具。
pub use tower;

type Input = tok_execution::Request<(CallContext, tok_proto::internal::v1::Payload)>;
type Output = tok_execution::Response<tok_proto::internal::v1::CallResponse>;
type Chain = tower::util::BoxCloneSyncService<Input, Output, tonic::Status>;
