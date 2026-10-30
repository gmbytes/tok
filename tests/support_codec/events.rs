//! 事件正文的 protobuf 便捷适配。
#![allow(dead_code)]

use tok_events::{DeliveryPolicy, EncodeError, Envelope, Error, Event, Message, Publication};

/// 编码事件正文并保留事件契约；投递策略由发布后端处理。
pub fn encode<E: Event + prost::Message>(event: &Envelope<E>) -> Result<Message, Error> {
    if event.payload.encoded_len() > 1_048_576 {
        return Err(Error::InvalidInput("事件正文超出上限"));
    }
    match Message::from_codec(event, &crate::test_codec::Codec) {
        Ok(message) => Ok(message),
        Err(EncodeError::Event(error)) => Err(error),
        Err(EncodeError::Body(never)) => match never {},
    }
}

/// 编码即时事件，拒绝持久投递要求。
pub fn immediate<E: Event + prost::Message>(event: &Envelope<E>) -> Result<Message, Error> {
    if E::DELIVERY != DeliveryPolicy::Immediate {
        return Err(Error::Unsupported("Core NATS 不支持持久化"));
    }
    encode(event)
}

/// 编码事件和业务指定的投递要求。
pub fn publication<E: Event + prost::Message>(event: &Envelope<E>) -> Result<Publication, Error> {
    let mut message = encode(event)?;
    message.trace_context = event.trace_context.clone();
    Ok(Publication {
        message,
        delivery: E::DELIVERY,
    })
}

/// 为事件注册表安装 protobuf 解码器。
pub trait RegistryExt {
    /// 为已注册类型安装 protobuf 解码器。
    fn enable_protobuf<E: Event + prost::Message + Default>(&mut self) -> Result<(), Error>;
}
impl<C: Send, O, F> RegistryExt for tok_events::Registry<C, O, F> {
    fn enable_protobuf<E: Event + prost::Message + Default>(&mut self) -> Result<(), Error> {
        self.enable_codec::<E, _>(crate::test_codec::Codec)
    }
}
