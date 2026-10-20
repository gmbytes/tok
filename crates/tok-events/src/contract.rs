use crate::{DeliveryPolicy, Error};

/// 业务显式声明的事件类型；本地分发只使用类型及名称，不应用跨节点投递选项。
pub trait Event: Send + Sync + 'static {
    /// 稳定事件名，使用 ASCII 字母、数字、点、短横线和下划线，最长 128 字节。
    const NAME: &'static str;
    /// 事件自身决定的路由主题；默认位于当前 namespace 的全局作用域。
    /// 可覆盖为固定主题，或根据 channel_id / realm_id 等字段计算；不决定消费组。
    /// 必须是无 I/O、无副作用的确定性计算，构造、编码和解码校验可重复调用。
    fn topic(&self) -> Result<crate::Topic, Error> {
        Ok(crate::Topic::default())
    }
    /// 跨节点投递策略；Core NATS 当前只接受 Immediate。
    const DELIVERY: DeliveryPolicy = DeliveryPolicy::Immediate;
}

/// 检查稳定事件名称，不从 Rust 类型名自动推导。
pub fn validate_name(name: &str) -> Result<(), Error> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
        || name.split('.').any(str::is_empty)
    {
        return Err(Error::InvalidInput("事件名称无效"));
    }
    Ok(())
}

/// 跨节点同一次事件的稳定 ID；重复发布必须复用，不承担业务幂等保证。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventId(String);
impl EventId {
    /// 从已有 ID 恢复；允许 1 到 128 个可打印 ASCII 非空白字节。
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        if value.is_empty() || value.len() > 128 || !value.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(Error::InvalidInput("事件 ID 无效"));
        }
        Ok(Self(value))
    }
    /// 为一次新事件生成 UUID v4；重复发布时保留生成的 ID。
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
    /// 读取稳定 ID 的字符串表示。
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// 有界追踪头载体；capture / extract 使用当前配置的标准 OpenTelemetry 传播器。
///
/// Envelope 本身不采集 span；Message 编码强类型事件时默认采集，NATS 使用标准 headers。
/// 本地分发直接沿用当前调用栈。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TraceContext {
    /// 可选 traceparent 原始值，最多 512 字节；不允许空白控制字符。
    pub traceparent: Option<String>,
    /// 可选 tracestate 原始值，最多 512 字节；只能使用可打印 ASCII。
    pub tracestate: Option<String>,
}
impl TraceContext {
    /// 使用当前配置的标准传播器采集 tracing span 的追踪头。
    pub fn capture() -> Self {
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        let mut result = Self::default();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&tracing::Span::current().context(), &mut result);
        });
        result
    }
    /// 校验头载体后交给标准传播器解析；无效格式由传播器按其规则忽略。
    pub fn extract(&self) -> Result<opentelemetry::Context, Error> {
        self.validate()?;
        Ok(opentelemetry::global::get_text_map_propagator(
            |propagator| propagator.extract(self),
        ))
    }

    /// 检查长度、ASCII 和头字段配套关系，不宣称验证完整 W3C 格式。
    pub fn validate(&self) -> Result<(), Error> {
        for value in [&self.traceparent, &self.tracestate].into_iter().flatten() {
            if value.is_empty()
                || value.len() > 512
                || !value.bytes().all(|b| b.is_ascii_graphic() || b == b' ')
            {
                return Err(Error::InvalidInput("事件追踪头无效"));
            }
        }
        if self
            .traceparent
            .as_ref()
            .is_some_and(|value| value.bytes().any(|b| b == b' '))
            || (self.tracestate.is_some() && self.traceparent.is_none())
        {
            return Err(Error::InvalidInput("事件追踪头配套关系无效"));
        }
        Ok(())
    }
}

/// 本地执行与跨节点接收共用的强类型事件封装，不隐含去重。
/// 只有传输适配需要通过 Message 编码；本地 dispatch 直接借用正文。
///
/// 业务目标由正文表达，订阅者决定处理范围；通用事件层不认识玩家或网关。
/// 修改公开字段后须重新 validate；发布适配在编码和发送前重新校验。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope<E> {
    /// 发布与订阅共同选择的路由主题，默认作用域不代表所有主题。
    pub topic: crate::Topic,
    /// 同一次事件的稳定 ID；重复发送时保持不变。
    pub event_id: EventId,
    /// 强类型业务载荷；投递策略由 E 声明。
    pub payload: E,
    /// 显式传入的追踪头，默认不携带。
    pub trace_context: TraceContext,
}
impl<E: Event> Envelope<E> {
    /// 使用既有 ID 创建事件，检查事件名；按事件契约取得 topic，默认不携带追踪头。
    pub fn new(event_id: EventId, payload: E) -> Result<Self, Error> {
        let envelope = Self {
            topic: payload.topic()?,
            event_id,
            payload,
            trace_context: TraceContext::default(),
        };
        envelope.validate()?;
        Ok(envelope)
    }
    /// 校验名称和有界追踪头；载荷编码及线协议大小限制由 Message 和传输适配处理。
    pub fn validate(&self) -> Result<(), Error> {
        validate_name(E::NAME)?;
        if self.topic != self.payload.topic()? {
            return Err(Error::InvalidInput("事件 topic 与正文路由契约不匹配"));
        }
        self.trace_context.validate()
    }
}

impl opentelemetry::propagation::Injector for TraceContext {
    fn set(&mut self, key: &str, value: String) {
        match key {
            "traceparent" => self.traceparent = Some(value),
            "tracestate" => self.tracestate = Some(value),
            _ => {}
        }
    }
}
impl opentelemetry::propagation::Extractor for TraceContext {
    fn get(&self, key: &str) -> Option<&str> {
        match key {
            "traceparent" => self.traceparent.as_deref(),
            "tracestate" => self.tracestate.as_deref(),
            _ => None,
        }
    }
    fn keys(&self) -> Vec<&str> {
        let mut keys = Vec::new();
        if self.traceparent.is_some() {
            keys.push("traceparent");
        }
        if self.tracestate.is_some() {
            keys.push("tracestate");
        }
        keys
    }
}
