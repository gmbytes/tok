//! 游戏 RPC 协议适配：消息分发、玩家身份、服务器事件和 timer。
//!
//! 通用连接、端点与调用执行由 tok-rpc 提供；本 crate 定义游戏协议语义。
//!
//! [`RpcEndpoint`] 显式绑定业务服务名和 protobuf 服务，监听由应用生命周期管理。
//! [`RpcClients`] 管理共享 Channel 缓存和传输配置；具体协议由
//! [`player::Client`]、[`business::Client`] 等适配器调用，有超时且不添加重试。
//! [`HandlerRegistry`] 将游戏 protobuf 消息注册为强类型业务处理函数。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod binding;
pub mod business;
mod call;
mod context;
mod handler;
pub mod lock;
pub mod player;
pub mod timer;

pub use binding::Binding;
pub use context::CallContext;
pub use handler::{
    CallTask, EventTask, Handler, HandlerRegistry, HandlerReply, RpcService, TowerHandler,
};
pub use tok_rpc::{Error, RpcClients, RpcEndpoint, RpcReadiness, RpcServerTask};

pub use tok_proto as protocol;
/// gRPC 客户端、服务端和状态类型；详细 API 见 tonic 上游文档。
pub use tonic;
/// Service、Layer 与中间件组装工具；详细 API 见 Tower 上游文档。
pub use tower;

/// 编码游戏通知；下行序号由网关 session 分配。
pub fn notification<M: prost::Message>(
    message_id: u32,
    message: &M,
) -> Result<tok_player::LogicalMessage, Error> {
    if message_id == 0 {
        return Err(Error::Configuration("通知消息 ID 必须非零".into()));
    }
    Ok(tok_player::LogicalMessage {
        kind: tok_player::MessageKind::Notify,
        message_id,
        request_id: 0,
        server_seq: None,
        error_code: 0,
        body: message.encode_to_vec(),
    })
}
