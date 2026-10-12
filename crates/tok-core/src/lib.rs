//! 框架共享类型，不依赖具体游戏业务或基础设施实现。

pub mod coordination;
pub mod discovery;
pub mod error;
pub mod execution;
pub mod routing;
pub mod service;

pub use discovery::{DirectoryWatch, ServiceDirectory};
pub use error::{Error, Result};
pub use execution::{
    CallContext, LogicalMessage, MessageKind, SessionIdentity, TimerSpec, TimerUpdate,
};
pub use routing::ServiceRouter;
pub use service::ServiceDescriptor;
