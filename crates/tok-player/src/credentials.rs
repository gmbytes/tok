//! 恢复凭证的本地签发和验证契约，不访问网络或凭证存储。

use crate::SessionIdentity;
use std::time::Duration;

/// 恢复凭证的配置、签发或本地验证故障。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 签发身份、有效期或正文大小无效。
    #[error("恢复凭证参数无效：{0}")]
    InvalidInput(String),
    /// 密钥格式或时钟容差配置无效，不包含密钥本身。
    #[error("恢复凭证配置错误：{0}")]
    InvalidConfig(String),
    /// 本地时钟、随机源或其他凭证组件不可用；仅携带安全的静态原因。
    #[error("本地凭证组件不可用：{0}")]
    Unavailable(&'static str),
}

/// 恢复凭证操作结果；验证不通过时返回 Ok(None)。
pub type Result<T> = std::result::Result<T, Error>;

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
    /// Err 表示本地组件故障，不表示客户端凭证无效；验证通过不授予会话所有权。
    fn verify(&self, token: &str) -> Result<Option<ResumeClaims>>;
}
