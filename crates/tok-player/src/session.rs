//! 跨连接保持的玩家逻辑会话身份。

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
