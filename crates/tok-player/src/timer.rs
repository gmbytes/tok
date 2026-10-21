//! 玩家计时需求与更新；实际调度由会话驱动。

/// 玩家业务处理者声明的服务器计时需求，实际调度由 session 负责，客户端不能直接提交。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerSpec {
    /// player 范围内稳定的 timer 标识。
    pub timer_id: String,
    /// 到期时调用的游戏消息类型。
    pub message_id: u32,
    /// 到期调用携带的游戏业务参数。
    pub body: Vec<u8>,
    /// UTC Unix 时间戳，单位毫秒。
    pub due_at_ms: i64,
    /// 周期毫秒数；None 表示一次性，Some 的值必须大于零。
    pub interval_ms: Option<u64>,
}

/// 没有返回此类型表示保留已有 timer 集合。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerUpdate {
    /// 全量替换，空集合表示清空。
    ReplaceAll(Vec<TimerSpec>),
    /// 同 ID 替换；取消不存在的 timer 为无操作。
    Patch {
        /// 按 timer_id 新增或替换的计时需求。
        upsert: Vec<TimerSpec>,
        /// 需要取消的 timer_id。
        cancel: Vec<String>,
    },
}

/// 玩家会话调度器产生的一次计时触发；业务调用与计时需求分别表达。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerTrigger {
    /// 调度范围内的稳定 timer 标识。
    pub timer_id: String,
    /// 本次触发原定的 UTC Unix 毫秒时刻。
    pub scheduled_at_ms: i64,
    /// 实际开始驱动业务的 UTC Unix 毫秒时刻。
    pub fired_at_ms: i64,
}
