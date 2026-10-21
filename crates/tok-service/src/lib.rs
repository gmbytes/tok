//! 通用服务设施：类型化执行共用 Tower Layer，工厂与任务监督管理构建和生命周期。
//!
//! task 管理通用任务范围，lifecycle 编排服务构建与阶段；Application 和 Gateway 负责接入适配。
//! 根入口提供 Context、Request 和调用 Layer；Error 只表示调用执行错误。
//! 任务提交与执行错误见 task，服务初始化与退出错误见 lifecycle。
//! 借用的请求可以在当前任务中执行，不要求静态生命周期，不隐式启动后台任务。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod context;
mod execution;
mod handler;
pub mod lifecycle;
mod request;
pub mod task;

pub use context::{Context, Source, StopReason};
pub use execution::{Call, Error, Layer, Service};
pub use handler::{SyncHandler, sync_handler};
pub use request::Request;
