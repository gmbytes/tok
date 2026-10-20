#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]
#![doc = include_str!("../docs/guide.md")]

pub mod config;
pub mod runtime;

pub use config::{AppConfig, DiscoveryBackend, EndpointConfig, OwnershipBackend, ServiceConfig};
pub use tok_core as core;
pub use tok_events as events;
pub use tok_gateway as gateway;
pub use tok_infra as infra;
pub use tok_observability as observability;
pub use tok_rpc as rpc;

/// 通用服务执行与处理链原型；既有运行时及网关尚未迁移。
pub use tok_service as service;
