#![allow(dead_code)]

use tok_events::{Envelope, Event, Message};

/// 测试专用的正文格式，与框架的信封格式无关。
pub trait Body: Sized {
    fn encode_body(&self) -> Vec<u8>;
    fn decode_body(body: &[u8]) -> Result<Self, std::io::Error>;
}

pub struct Codec;
impl<T: Body> tok_core::codec::Encode<T> for Codec {
    type Error = std::convert::Infallible;
    fn encode(&self, value: &T) -> Result<Vec<u8>, Self::Error> {
        Ok(value.encode_body())
    }
}
impl<T: Body> tok_core::codec::Decode<T> for Codec {
    type Error = std::io::Error;
    fn decode(&self, body: &[u8]) -> Result<T, Self::Error> {
        T::decode_body(body)
    }
}
pub fn message<E: Event + Body>(
    event: &Envelope<E>,
) -> Result<Message, tok_events::EncodeError<std::convert::Infallible>> {
    Message::from_codec(event, &Codec)
}
