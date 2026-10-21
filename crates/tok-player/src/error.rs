//! 玩家运行契约的参数、配置、所有权与凭证错误。

/// 玩家身份、会话所有权或恢复凭证操作失败。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 玩家身份、租约或凭证参数无效。
    #[error("玩家参数无效：{0}")]
    InvalidInput(String),
    /// 玩家所有权或凭证配置无效，不包含密钥。
    #[error("玩家配置错误：{0}")]
    InvalidConfig(String),
    /// 当前玩家能力不支持指定的可选操作。
    #[error("玩家能力不支持此操作：{0}")]
    Unsupported(&'static str),
    /// 所有权后端或本地状态不可用，附带不含凭据的原因。
    #[error("会话所有权不可用：{0}")]
    OwnershipUnavailable(String),
    /// 所有权已过期或与当前 epoch、会话、所有者不匹配。
    #[error("session 所有权已变化或过期")]
    OwnershipConflict,
    /// 本地凭证密钥或系统时钟不可用，不包含密钥及 token。
    #[error("本地凭证组件不可用")]
    CredentialUnavailable,
}

/// 玩家运行契约的操作结果。
pub type Result<T> = std::result::Result<T, Error>;
