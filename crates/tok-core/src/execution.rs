//! 与传输及游戏 protobuf 无关的逻辑调用类型。

mod context;

pub use context::{Context, Extensions, Source, StopReason};

/// 可恢复的逻辑会话身份；epoch 是所有权代次，不是消息序号。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionIdentity {
    /// 认证扩展点确认的玩家业务 ID。
    pub player_id: String,
    /// 跨连接保持的逻辑会话 ID。
    pub session_id: String,
    /// 当前所有权代次，零表示尚未取得所有权。
    pub epoch: u64,
}

/// RPC 业务调用上下文；服务身份与业务实体 ID 分别管理。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallContext {
    /// 目标部署节点。
    pub node_id: String,
    /// 目标业务服务名，显式绑定到对应的 protobuf 服务。
    pub service_name: String,
    /// 目标业务 ID；管理类调用可以没有目标实体。
    pub bid: Option<String>,
    /// 关联请求与回应，不承担幂等或自动重试语义。
    pub request_id: u64,
    /// player 调用的会话身份；其他服务可以没有会话。
    pub session: Option<SessionIdentity>,
    /// 网关调度产生的内部计时来源；客户端业务请求为 None。
    pub timer: Option<TimerTrigger>,
}

/// 一次内部计时触发的来源信息，不由客户端业务正文决定。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerTrigger {
    /// player 范围内的稳定 timer 标识。
    pub timer_id: String,
    /// 本次触发原定的 UTC Unix 毫秒时刻。
    pub scheduled_at_ms: i64,
    /// 实际开始驱动业务的 UTC Unix 毫秒时刻。
    pub fired_at_ms: i64,
}

/// 逻辑消息类型；握手、确认及恢复等控制消息由接入协议另行定义。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    /// 客户端发起、需要对应回应的业务请求。
    Request,
    /// 通过 request_id 关联到请求的业务回应。
    Response,
    /// 不与请求配对的主动通知。
    Notify,
}

/// 加密和传输封装之前的客户端逻辑消息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalMessage {
    /// 请求、回应或主动通知。
    pub kind: MessageKind,
    /// 游戏维护的消息类型 ID。
    pub message_id: u32,
    /// 请求与回应的关联序号；主动通知不与请求配对。
    pub request_id: u64,
    /// 下行消息序号；请求尚无服务端序号。
    pub server_seq: Option<u64>,
    /// 具体游戏定义的错误码，零表示成功。
    pub error_code: u32,
    /// 游戏 protobuf 编码后的消息正文。
    pub body: Vec<u8>,
}

/// 玩家提供的计时需求，实际调度由 session 负责。
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
