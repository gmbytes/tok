//! 统一强类型事件注册和异步执行，本地借用与远程解码共用 Tower 处理链。
//! Core NATS 即时投递；JetStream 与离线加载尚未实现。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod contract;
mod message;
pub mod nats;
mod registry;
mod subscriber;

pub use contract::{Envelope, Event, EventId, TraceContext, validate_name};
pub use message::Message;
pub use registry::{Callback, DispatchError, Handler, Input, Outcome, Registry, Task, handler};
pub use subscriber::Subscriber;

/// 通用服务器事件的投递策略，与业务目标和处理方式无关。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryPolicy {
    /// Core NATS 即时投递，不积压离线消息。
    Immediate,
    /// JetStream 持久化投递，消费端需要处理重复。
    Persistent,
}

/// 事件契约、分发组装和传输错误；本地订阅者的业务错误仍原样返回。
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    /// 执行范围已取消或期限耗尽。
    #[error(transparent)]
    Stopped(tok_core::execution::StopReason),
    /// 后端连接、订阅或发布失败；不包含凭据和消息正文。
    #[error("事件后端不可用：{0}")]
    Unavailable(&'static str),
    /// 请求了当前后端不支持的投递语义。
    #[error("事件能力尚未支持：{0}")]
    Unsupported(&'static str),
    /// 名称、ID、目标或追踪载体不符合事件契约，附带原因。
    #[error("事件参数无效：{0}")]
    InvalidInput(&'static str),
    /// 分发器的注册容量必须大于零。
    #[error("事件注册容量必须大于零")]
    InvalidCapacity,
    /// 订阅者数量达到初始化时设定的上限。
    #[error("事件订阅或处理容量已满")]
    CapacityExceeded,
}
