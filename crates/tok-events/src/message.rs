use crate::{Envelope, Error, Event, EventId, TraceContext, validate_name};
use prost::Message as _;
use tok_proto::internal::v1::EventEnvelope;

/// 服务器事件的正文与来源，携带通用 topic，不解释客户端请求或玩家归属。
#[derive(Clone, Debug)]
pub struct Message {
    /// 业务路由主题，不等同于正文中的目标身份。
    pub topic: crate::Topic,
    /// 同一次发布的稳定 ID，不隐含框架去重。
    pub event_id: EventId,
    /// 显式事件名称。
    pub event_type: String,
    /// 由业务选择编码方式的正文。
    pub body: Vec<u8>,
    /// 标准追踪头载体。
    pub trace_context: TraceContext,
}
/// 构造编码事件时的契约或具体正文编码错误。
#[derive(Debug, thiserror::Error)]
pub enum EncodeError<E> {
    /// 事件契约无效。
    #[error(transparent)]
    Event(#[from] Error),
    /// 正文编码失败。
    #[error("事件正文编码失败：{0}")]
    Body(E),
}
impl Message {
    /// 校验事件名称、正文上限和追踪载体。
    pub fn validate(&self, max_bytes: usize) -> Result<(), Error> {
        validate_name(&self.event_type)?;
        if self.body.len() > max_bytes {
            return Err(Error::InvalidInput("事件正文超出上限"));
        }
        self.trace_context.validate()
    }

    /// 使用指定编码器构造消息；保留具体编码错误，不选择投递后端。
    pub fn from_codec<E: Event, C: tok_core::codec::Encode<E>>(
        envelope: &Envelope<E>,
        codec: &C,
    ) -> Result<Self, EncodeError<C::Error>> {
        envelope.validate()?;
        let trace_context = if envelope.trace_context == TraceContext::default() {
            TraceContext::capture()
        } else {
            envelope.trace_context.clone()
        };
        let result = Self {
            topic: envelope.topic.clone(),
            event_id: envelope.event_id.clone(),
            event_type: E::NAME.into(),
            body: codec.encode(&envelope.payload).map_err(EncodeError::Body)?,
            trace_context,
        };
        result.validate(1_048_576)?;
        Ok(result)
    }
    /// 检查完整长度后编码；追踪上下文由 NATS headers 承载。
    pub fn encode(&self, max_bytes: usize) -> Result<Vec<u8>, Error> {
        self.validate(max_bytes)?;
        let wire = EventEnvelope {
            version: 1,
            topic: self.topic.as_str().map(str::to_owned),
            event_id: self.event_id.to_string(),
            event_type: self.event_type.clone(),
            body: self.body.clone(),
        };
        if wire.encoded_len() > max_bytes {
            return Err(Error::InvalidInput("事件线协议超出上限"));
        }
        Ok(wire.encode_to_vec())
    }
    /// 在解码前检查总字节数和协议版本。
    pub fn decode(bytes: &[u8], max_bytes: usize) -> Result<Self, Error> {
        if bytes.len() > max_bytes {
            return Err(Error::InvalidInput("事件线协议超出上限"));
        }
        let wire = EventEnvelope::decode(bytes).map_err(|_| Error::InvalidInput("事件编码无效"))?;
        if wire.version != 1 {
            return Err(Error::InvalidInput("事件版本无效"));
        }
        let result = Self {
            topic: crate::Topic::from_wire(wire.topic)?,
            event_id: EventId::new(wire.event_id)?,
            event_type: wire.event_type,
            body: wire.body,
            trace_context: TraceContext::default(),
        };
        result.validate(max_bytes)?;
        Ok(result)
    }
}
