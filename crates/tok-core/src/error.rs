/// 框架内部错误；客户端 ErrorCode 映射由网关协议层提供。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("配置错误：{0}")]
    InvalidConfig(String),
    #[error("节点服务已注册：{0}")]
    DuplicateService(String),
    #[error("内存服务目录锁不可用")]
    DirectoryUnavailable,
    #[error("当前工程骨架尚未实现：{0}")]
    NotImplemented(&'static str),
}

pub type Result<T> = std::result::Result<T, Error>;
