//! 客户端可识别的有限框架错误码，不包含认证凭证或协议正文。

/// 框架错误码；具体业务的非零错误码由游戏自行维护。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum ErrorCode {
    /// 操作成功。
    Ok = 0,
    /// 帧、消息正文、消息 ID 或请求序号无效。
    InvalidRequest = 1,
    /// 尚未建立认证会话。
    Unauthenticated = 2,
    /// 游戏认证扩展点拒绝凭证。
    AuthenticationFailed = 3,
    /// 有界业务队列或 RPC 资源已满。
    QueueFull = 4,
    /// 会话关闭或租约已失效。
    SessionClosed = 5,
    /// 目标服务或会话所有者不可用。
    Unavailable = 6,
    /// 认证、帧读写或业务调用超时，未知结果不自动重试。
    Timeout = 7,
    /// 非预期系统错误。
    Internal = 8,
    /// 当前连接状态不允许此控制操作。
    InvalidState = 9,
    /// 帧正文超过配置限制。
    FrameTooLarge = 10,
    /// 游戏消息未注册。
    UnknownMessage = 11,
    /// 会话已被其他登录替换。
    SessionReplaced = 12,
    /// session 目录容量不足。
    CapacityExceeded = 13,
    /// 凭证有效，但原会话已过期、已丢失或缓存不完整，须快速重登。
    ResumeUnavailable = 14,
}

/// 网关控制错误，只携带有限错误码，避免泄漏凭证。
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("网关错误：{code:?}")]
pub struct GatewayError {
    /// 可转换到客户端信封的错误码。
    pub code: ErrorCode,
}

impl From<ErrorCode> for GatewayError {
    fn from(code: ErrorCode) -> Self {
        Self { code }
    }
}

/// 使用网关错误码的操作结果。
pub type Result<T> = std::result::Result<T, GatewayError>;
