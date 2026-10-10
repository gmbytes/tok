//! 框架共享类型，不依赖具体游戏业务或基础设施实现。

pub mod discovery;
pub mod error;
pub mod service;

pub use discovery::ServiceDirectory;
pub use error::{Error, Result};
pub use service::{ServiceDescriptor, ServiceKind};
