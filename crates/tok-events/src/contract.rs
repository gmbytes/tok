use crate::{DeliveryPolicy, Error, EventTarget};

/// 业务显式声明的事件类型；本地分发只使用类型及名称，不应用跨节点投递选项。
pub trait Event: Send + Sync + 'static {
    /// 稳定事件名，使用 ASCII 字母、数字、点、短横线和下划线，最长 128 字节。
    const NAME: &'static str;
    /// 跨节点投递策略；声明不代表当前已有 NATS 发布能力。
    const DELIVERY: DeliveryPolicy = DeliveryPolicy::Immediate;
    /// 跨节点转发时是否允许加载离线玩家，默认不加载；本地分发不使用。
    const LOAD_OFFLINE: bool = false;
}

pub(crate) fn validate_name(name: &str) -> Result<(), Error> {
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

/// 追踪头的数据载体；不解析 W3C 格式，也不自动采集当前 span。
///
/// 后续传输适配负责标准格式校验与上下文注入提取；本地分发直接沿用当前调用栈。
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TraceContext {
    /// 可选 traceparent 原始值，最多 512 字节；不允许空白控制字符。
    pub traceparent: Option<String>,
    /// 可选 tracestate 原始值，最多 512 字节；只能使用可打印 ASCII。
    pub tracestate: Option<String>,
}
impl TraceContext {
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

/// 跨节点事件的数据契约；尚不提供编码、发布、消费或去重。
///
/// 目标 ID 类型由游戏决定，其身份格式由未来传输及目标适配校验。
/// 修改公开字段后须重新 validate；未来发布适配必须在编码和发送前校验。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope<E, Id = String> {
    /// 同一次事件的稳定 ID；重复发送时保持不变。
    pub event_id: EventId,
    /// 目标玩家、频道或当前投递范围内所有玩家。
    pub target: EventTarget<Id>,
    /// 强类型业务载荷；投递策略由 E 声明。
    pub payload: E,
    /// 显式传入的追踪头，默认不携带。
    pub trace_context: TraceContext,
}
impl<E: Event, Id> Envelope<E, Id> {
    /// 使用既有 ID 创建事件，检查事件名与目标结构；默认不携带追踪头。
    pub fn new(event_id: EventId, target: EventTarget<Id>, payload: E) -> Result<Self, Error> {
        let envelope = Self {
            event_id,
            target,
            payload,
            trace_context: TraceContext::default(),
        };
        envelope.validate()?;
        Ok(envelope)
    }
    /// 校验名称、目标结构和有界追踪头；载荷编码及线协议大小限制由后续适配处理。
    pub fn validate(&self) -> Result<(), Error> {
        validate_name(E::NAME)?;
        match &self.target {
            EventTarget::Channel(channel) if channel.trim().is_empty() || channel.len() > 256 => {
                return Err(Error::InvalidInput("事件频道无效"));
            }
            EventTarget::Players(players) if players.is_empty() || players.len() > 1024 => {
                return Err(Error::InvalidInput("事件玩家目标数量必须在 1 到 1024 之间"));
            }
            _ => {}
        }
        self.trace_context.validate()
    }
}
