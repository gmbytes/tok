#![doc = include_str!("../docs/guide.md")]

pub mod config;
pub mod runtime;

pub use config::{AppConfig, DiscoveryBackend, EndpointConfig, ServiceConfig};
pub use tok_core as core;
pub use tok_events as events;
pub use tok_gateway as gateway;
pub use tok_infra as infra;
pub use tok_observability as observability;
pub use tok_rpc as rpc;
