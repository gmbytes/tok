//! 使用 prost 的测试正文 codec；测试中不依赖 tok-protobuf 适配 crate。
#[derive(Clone, Copy)]
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
