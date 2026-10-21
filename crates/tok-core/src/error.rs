//! 框架组件共享的错误类型与结果别名。

/// 框架内部错误；客户端 ErrorCode 映射由网关协议层提供。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 调用参数不符合组件契约，附带原因。
    #[error("调用参数无效：{0}")]
    InvalidInput(String),
    /// 目标业务服务没有可用节点，附带服务名。
    #[error("没有可用的服务节点：{0}")]
    NoAvailableService(String),
    /// 协调后端无法完成操作，附带原因。
    #[error("协调后端不可用：{0}")]
    CoordinationUnavailable(String),
    /// 请求级锁在等待期限内未能获取。
    #[error("请求级锁获取超时")]
    LockTimeout,
    /// 锁后端的本地容量或并发操作容量已满。
    #[error("请求级锁容量已满")]
    LockCapacity,
    /// 配置校验失败，附带原因。
    #[error("配置错误：{0}")]
    InvalidConfig(String),
    /// 同一节点服务重复注册，附带服务身份。
    #[error("节点服务已注册：{0}")]
    DuplicateService(String),
    /// 内存服务目录的同步锁不可用。
    #[error("内存服务目录锁不可用")]
    DirectoryUnavailable,
    /// 发现后端连接、租约或快照同步失败；不包含连接凭据。
    #[error("服务发现后端不可用")]
    DiscoveryUnavailable,
    /// 请求的组件能力尚未实现，附带能力名称。
    #[error("当前工程骨架尚未实现：{0}")]
    NotImplemented(&'static str),
}

/// 使用框架共享错误类型的操作结果。
pub type Result<T> = std::result::Result<T, Error>;
