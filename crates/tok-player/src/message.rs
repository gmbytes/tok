//! 玩家客户端的逻辑业务消息，不包含传输封装。

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
    /// 请求与回应的关联序号；主动通知为零，不与请求配对。
    pub request_id: u64,
    /// 下行发布序号，由会话发布时分配；请求及尚未发布的回应、通知均为 None。
    pub server_seq: Option<u64>,
    /// 框架或游戏定义的错误码，零表示成功；两者的编号分配由接入协议约定。
    pub error_code: u32,
    /// 业务编码后的消息正文，不包含传输封装。
    pub body: Vec<u8>,
}
