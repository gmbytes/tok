//! 恢复凭证的本地签发和验证契约，不访问网络或凭证存储。

use crate::{Result, SessionIdentity};
use std::time::Duration;

/// 签名验证通过的恢复身份，不代表已取得会话执行权。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResumeClaims {
    /// 可信玩家标识。
    pub player_id: String,
    /// 凭证签发时的逻辑会话，恢复时仍须匹配原会话。
    pub session_id: String,
}

/// 仅使用本地密钥签发和验证，不执行网络 I/O。
pub trait ResumeTokenCodec: Send + Sync {
    /// 签发具有有限有效期的身份凭证，不创建服务端凭证记录。
    fn issue(&self, identity: &SessionIdentity, ttl: Duration) -> Result<String>;
    /// 本地验证消息认证、身份和有效期；非法或过期返回 None，不回源取密钥。
    fn verify(&self, token: &str) -> Result<Option<ResumeClaims>>;
}
