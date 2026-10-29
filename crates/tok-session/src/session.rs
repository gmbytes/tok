//! 跨连接保持的主体逻辑会话身份。

/// 可恢复的逻辑会话身份；epoch 是所有权代次，不是消息序号。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionIdentity {
    /// 认证扩展点确认的主体业务 ID。
    pub subject_id: String,
    /// 跨连接保持的逻辑会话 ID。
    pub session_id: String,
    /// 当前所有权代次，零表示尚未取得所有权。
    pub epoch: u64,
}
