#![allow(dead_code)]
/// 测试协议正文使用的本地 codec，不依赖框架的 protobuf 适配层。
#[derive(Clone)]
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
pub fn message<E: tok_events::Event + prost::Message>(
    event: &tok_events::Envelope<E>,
) -> Result<tok_events::Message, tok_events::EncodeError<std::convert::Infallible>> {
    tok_events::Message::from_codec(event, &Codec)
}
