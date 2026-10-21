#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]
#![doc = include_str!("../docs/guide.md")]

pub mod application;
pub mod config;

pub use config::{AppConfig, DiscoveryBackend, EndpointConfig, OwnershipBackend, ServiceConfig};
pub use tok_core as core;
pub use tok_events as events;
pub use tok_gateway as gateway;
/// 基于 Tonic 的 gRPC 连接、监听与调用执行设施。
pub use tok_grpc as grpc;
pub use tok_infra as infra;
/// 编号业务消息注册与协议适配。
pub use tok_message as message;
pub use tok_observability as observability;
/// 玩家会话、逻辑消息、计时需求、所有权与恢复凭证契约。
pub use tok_player as player;
/// 通用 protobuf 正文编解码与执行链适配。
pub use tok_protobuf as protobuf;

/// 类型化调用上下文、请求与处理链执行设施。
pub use tok_execution as execution;
/// 服务构建、生命周期与有界任务监督。
pub use tok_lifecycle as lifecycle;
