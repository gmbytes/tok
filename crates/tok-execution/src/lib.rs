//! 类型化调用执行：上下文、请求、Tower 处理链及期限、取消和追踪约束。
//!
//! 主要入口为 Context、Request、Layer 和同步 handler 适配；不管理服务启动或后台任务。
//! 服务构建、生命周期和任务监督由独立的 tok-lifecycle 提供。
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]
mod context;
mod execution;
mod handler;
mod request;
pub use context::{Context, StopReason};
pub use execution::{Call, Error, Layer, Service};
pub use handler::{SyncHandler, sync_handler};
pub use request::Request;
