//! 游戏通过此入口组合框架组件，最终二进制由游戏项目提供。

pub mod config;
pub mod runtime;

pub use config::{AppConfig, DiscoveryBackend, RoleConfig};
pub use tok_core as core;
pub use tok_events as events;
pub use tok_gateway as gateway;
pub use tok_infra as infra;
pub use tok_observability as observability;
pub use tok_rpc as rpc;
