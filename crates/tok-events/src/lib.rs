//! 事件类型与投递策略。NATS 后端及同步本地分发器尚未实现。

pub mod local;
pub mod nats;

/// 投递策略独立于是否允许加载离线 player。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryPolicy {
    /// Core NATS 即时投递，不积压离线消息。
    Immediate,
    /// JetStream 持久化投递，消费端需要处理重复。
    Persistent,
}

/// player ID 类型由具体游戏决定。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventTarget<Id> {
    /// 当前投递范围内的所有目标玩家。
    All,
    /// 已加入指定频道的玩家，携带频道 ID。
    Channel(String),
    /// 显式指定的玩家业务 ID 集合。
    Players(Vec<Id>),
}
