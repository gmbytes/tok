//! 通用基础契约的错误类别与结果别名，不收集具体后端、RPC 或游戏业务的专用错误。
//!
/// 通用基础契约的失败类别；调用侧按变体判断，错误文本用于诊断。
///
/// 后端将自身故障映射到契约定义的类别；同一操作只返回与自身语义相关的类别。
/// 未知的底层故障归入现有不可用类别，不因增加一种 Redis/网络错误就扩充枚举。
///
/// | 契约 | 主要错误类别 |
/// | --- | --- |
/// | 输入或配置校验 | InvalidInput、InvalidConfig |
/// | 请求锁获取 | InvalidInput、LockWaitTimeout、LockCapacityExceeded、CoordinationUnavailable |
/// | 请求锁释放 | CoordinationUnavailable |
/// | 发现注册与撤销 | InvalidInput、RegistrationConflict、RegistrationCapacityExceeded、DirectoryStateUnavailable、DiscoveryUnavailable |
/// | 目录快照与变更通知 | DirectoryStateUnavailable、DiscoveryUnavailable；不支持订阅时为 Unsupported |
/// | 业务 ID 选点 | InvalidInput、NoAvailableService，以及目录向上传递的错误 |
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 调用参数不符合组件契约，附带原因。
    #[error("调用参数无效：{0}")]
    InvalidInput(String),
    /// 指定服务没有可供路由的节点，附带服务名。
    #[error("服务没有可用节点：{0}")]
    NoAvailableService(String),
    /// 协调后端无法完成操作，附带原因。
    #[error("协调后端不可用：{0}")]
    CoordinationUnavailable(String),
    /// 等待请求锁超过竞争期限；不表示锁后端 I/O 操作超时。
    #[error("等待请求锁超时")]
    LockWaitTimeout,
    /// 锁记录或并发获取操作的容量已耗尽。
    #[error("请求锁资源容量已耗尽")]
    LockCapacityExceeded,
    /// 配置校验失败，附带原因。
    #[error("配置错误：{0}")]
    InvalidConfig(String),
    /// 服务或节点注册与已有持有者冲突，附带不包含凭据的原因。
    #[error("服务注册冲突：{0}")]
    RegistrationConflict(String),
    /// 服务注册资源容量已耗尽；释放已有注册后可再次尝试。
    #[error("服务注册资源容量已耗尽")]
    RegistrationCapacityExceeded,
    /// 服务目录的本地状态不可用，例如同步锁已损坏。
    #[error("服务目录本地状态不可用")]
    DirectoryStateUnavailable,
    /// 发现连接、注册租约或快照同步不可用，也包括本地缓存超过有效期；不包含凭据。
    #[error("服务发现不可用")]
    DiscoveryUnavailable,
    /// 当前组件不支持请求的可选能力，附带能力名称。
    #[error("当前组件不支持此能力：{0}")]
    Unsupported(&'static str),
}

/// 使用框架共享错误类型的操作结果。
pub type Result<T> = std::result::Result<T, Error>;
