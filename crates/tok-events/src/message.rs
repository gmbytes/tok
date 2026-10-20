use crate::{DeliveryPolicy, Envelope, Error, Event, EventId, TraceContext, validate_name};
use prost::Message as _;
use tok_proto::internal::v1::EventEnvelope;

/// 服务器事件的正文与来源，不携带客户端请求或网关路由信息。
#[derive(Clone, Debug)]
pub struct Message {
    /// 同一次发布的稳定 ID，不隐含框架去重。
    pub event_id: EventId,
    /// 显式事件名称。
    pub event_type: String,
    /// 业务 protobuf 编码正文。
    pub body: Vec<u8>,
    /// 标准追踪头载体。
    pub trace_context: TraceContext,
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

    /// 编码强类型即时事件；不支持的持久化策略直接拒绝。
    pub fn new<E: Event + prost::Message>(envelope: Envelope<E>) -> Result<Self, Error> {
        envelope.validate()?;
        if E::DELIVERY != DeliveryPolicy::Immediate {
            return Err(Error::Unsupported("Core NATS 不支持持久化"));
        }
        if envelope.payload.encoded_len() > 1_048_576 {
            return Err(Error::InvalidInput("事件正文超出上限"));
        }
        let trace_context = if envelope.trace_context == TraceContext::default() {
            TraceContext::capture()
        } else {
            envelope.trace_context
        };
        let result = Self {
            event_id: envelope.event_id,
            event_type: E::NAME.into(),
            body: envelope.payload.encode_to_vec(),
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
            event_id: EventId::new(wire.event_id)?,
            event_type: wire.event_type,
            body: wire.body,
            trace_context: TraceContext::default(),
        };
        result.validate(max_bytes)?;
        Ok(result)
    }
}
