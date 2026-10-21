//! 与传输和玩家运行时无关的通用执行上下文与来源。

mod context;

pub use context::{Context, Extensions, Source, StopReason};

/// 调度器产生的一次计时触发来源，不绑定玩家会话或业务消息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerTrigger {
    /// 调度范围内的稳定 timer 标识。
    pub timer_id: String,
    /// 本次触发原定的 UTC Unix 毫秒时刻。
    pub scheduled_at_ms: i64,
    /// 实际开始驱动业务的 UTC Unix 毫秒时刻。
    pub fired_at_ms: i64,
}
