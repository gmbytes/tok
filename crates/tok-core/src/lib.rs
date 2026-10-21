//! 通用服务基础契约，不依赖玩家运行时、具体游戏业务或基础设施实现。
//!
//! [`discovery`]、[`lock`]、[`routing`]、[`service`] 提供功能命名空间。
//! 常用服务身份、发现与路由契约同时从根导出，便于使用者导入；双路径指向同一定义。
//! [`Error`] / [`Result`]、[`Extensions`]、[`TimerTrigger`] 和 [`BoxFuture`] 直接作为根入口，
//! 对应实现文件不构成额外的公开命名空间。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod discovery;
mod error;
mod extensions;
pub mod lock;
pub mod routing;
pub mod service;
mod timer;

pub use discovery::{DirectoryWatch, ServiceDirectory, ServiceRegistry};
pub use error::{Error, Result};
pub use extensions::Extensions;
pub use routing::ServiceRouter;
pub use service::{Identity, ServiceDescriptor};
pub use timer::TimerTrigger;

/// 可跨线程传递的类型擦除 Future；T 是完整输出类型，不隐含 Result。
/// 本别名不创建或监督任务，也不要求静态生命周期。
pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;
