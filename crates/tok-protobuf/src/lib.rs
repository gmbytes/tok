//! 使用 prost 实现框架正文编解码契约。

/// protobuf 消息约束，供需要声明类型边界的组装代码使用。
pub use prost::Message;

/// 事件正文的 protobuf 便捷适配。
#[cfg(feature = "events")]
pub mod events;
/// 消息正文的 protobuf 便捷适配。
#[cfg(feature = "message")]
pub mod message;
/// NATS 发布的 protobuf 便捷适配。
#[cfg(feature = "nats")]
pub mod nats;
/// 逻辑会话的 protobuf 便捷注册。
#[cfg(feature = "session")]
pub mod session;

/// 无状态 protobuf 编解码器。
#[derive(Clone, Copy, Default, Debug)]
pub struct Codec;
impl<T: prost::Message> tok_core::codec::Encode<T> for Codec {
    type Error = std::convert::Infallible;
    fn encode(&self, value: &T) -> Result<Vec<u8>, Self::Error> {
        Ok(value.encode_to_vec())
    }
}
impl<T: prost::Message + Default> tok_core::codec::Decode<T> for Codec {
    type Error = prost::DecodeError;
    fn decode(&self, body: &[u8]) -> Result<T, Self::Error> {
        T::decode(body)
    }
}
