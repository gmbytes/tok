#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]
#![doc = include_str!("../docs/guide.md")]

pub mod application;
pub mod config;

pub use config::{AppConfig, DiscoveryBackend, EndpointConfig, OwnershipBackend, ServiceConfig};
pub use tok_core as core;
pub use tok_events as events;
/// 游戏消息与玩家执行协议的 RPC 适配。
pub use tok_game_rpc as game_rpc;
pub use tok_gateway as gateway;
pub use tok_infra as infra;
pub use tok_observability as observability;
/// 玩家会话、逻辑消息、计时需求、所有权与恢复凭证契约。
pub use tok_player as player;
pub use tok_rpc as rpc;

/// 通用服务执行、工厂与生命周期设施；RPC、事件及网关扩展复用共同处理协议。
pub use tok_service as service;
