//! 编号业务消息的本地处理与传输适配。
//!
//! [`Registry`] 在 protobuf 编解码和通用执行链之间施加非零消息编号约束，
//! 保留调用方的状态、附加结果与业务错误类型，可直接作为 Tower Service 组合公共 Layer；
//! 本地调用不需要 RPC 信封。
//! [`rpc`] 提供 gRPC 客户端、服务端以及玩家会话、事件和计时协议适配。
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod registry;
pub mod rpc;

pub use registry::{Error, Registry};
pub use tok_protobuf::Reply;
/// 已编码的编号业务消息；编号零保留为无消息。
pub type Message = tok_protobuf::Message<u32>;

/// 编码游戏通知；下行序号由网关 session 分配。
pub fn notification<M: prost::Message>(
    message_id: u32,
    message: &M,
) -> Result<tok_player::LogicalMessage, Error> {
    if message_id == 0 {
        return Err(Error::Invalid("通知消息 ID 必须非零".into()));
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
