//! 通用服务基础契约，不依赖玩家运行时、具体游戏业务或基础设施实现。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod coordination;
pub mod discovery;
pub mod error;
pub mod execution;
pub mod routing;
pub mod service;

pub use discovery::{DirectoryWatch, ServiceDirectory, ServiceRegistry};
pub use error::{Error, Result};
pub use execution::TimerTrigger;
pub use routing::ServiceRouter;
pub use service::ServiceDescriptor;
