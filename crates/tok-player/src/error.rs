//! 玩家运行契约的错误；通用参数、配置及后端错误保留原类型。

/// 玩家身份、会话所有权或恢复凭证操作失败。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 通用参数、配置或协调后端错误。
    #[error(transparent)]
    Core(#[from] tok_core::Error),
    /// 所有权已过期或与当前 epoch、会话、所有者不匹配。
    #[error("session 所有权已变化或过期")]
    OwnershipConflict,
    /// 本地凭证密钥或系统时钟不可用，不包含密钥及 token。
    #[error("本地凭证组件不可用")]
    CredentialUnavailable,
}

/// 玩家运行契约的操作结果。
pub type Result<T> = std::result::Result<T, Error>;
