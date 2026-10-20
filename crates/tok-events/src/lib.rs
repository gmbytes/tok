//! 强类型事件契约与同步本地分发；NATS 传输、订阅和消费尚未实现。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod contract;
pub mod local;
pub mod nats;

pub use contract::{Envelope, Event, EventId, TraceContext};

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

/// 事件契约和分发器的组装错误；订阅者的业务错误由分发器原样返回。
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    /// 名称、ID、目标或追踪载体不符合事件契约，附带原因。
    #[error("事件参数无效：{0}")]
    InvalidInput(&'static str),
    /// 分发器的注册容量必须大于零。
    #[error("本地事件注册容量必须大于零")]
    InvalidCapacity,
    /// 订阅者数量达到初始化时设定的上限。
    #[error("本地事件订阅容量已满")]
    CapacityExceeded,
}
