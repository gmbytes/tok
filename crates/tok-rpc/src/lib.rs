//! 内部 RPC：共享端点、连接复用、强类型分发及调用上下文。
//!
//! [`RpcEndpoint`] 显式绑定业务服务名和 protobuf 服务，监听由应用生命周期管理。
//! [`RpcClients`] 复用有界 Channel 缓存，每次调用有超时且不添加重试层。
//! [`HandlerRegistry`] 将游戏 protobuf 消息注册为强类型业务处理函数。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod client;
mod endpoint;
mod handler;
mod propagation;
pub mod timer;

pub use client::RpcClients;
pub use endpoint::{RpcEndpoint, RpcReadiness, RpcServerTask};
pub use handler::{CallHandler, CallTask, HandlerRegistry, HandlerReply, RpcService, TowerHandler};

pub use tok_proto as protocol;
/// gRPC 客户端、服务端和状态类型；详细 API 见 tonic 上游文档。
pub use tonic;
/// Service、Layer 与中间件组装工具；详细 API 见 Tower 上游文档。
pub use tower;

/// 编码游戏通知；下行序号由网关 session 分配。
pub fn notification<M: prost::Message>(
    message_id: u32,
    message: &M,
) -> Result<tok_core::LogicalMessage, Error> {
    if message_id == 0 {
        return Err(Error::Configuration("通知消息 ID 必须非零".into()));
    }
    Ok(tok_core::LogicalMessage {
        kind: tok_core::MessageKind::Notify,
        message_id,
        request_id: 0,
        server_seq: None,
        error_code: 0,
        body: message.encode_to_vec(),
    })
}

/// RPC 组装和传输错误；业务错误仍由协议中的 RpcError 表达。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 端点、消息注册或客户端参数无效，附带原因。
    #[error("RPC 配置错误：{0}")]
    Configuration(String),
    /// tonic 服务端或客户端传输错误。
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    /// 监听绑定等底层 I/O 操作失败。
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
