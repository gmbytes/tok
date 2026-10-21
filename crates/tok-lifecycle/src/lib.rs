//! 服务构建与生命周期编排：Builder 初始化为 Prepared，启动为 Running，再驱动退出。
//!
//! Factory 负责创建状态和入口，task::Supervisor 负责有界任务范围。
//! 主要生命周期类型在根导出；任务监督保留独立的 task 功能命名空间。
//! 本 crate 不依赖调用执行层或 Tower，也不绑定 RPC、事件或网关协议。
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]
mod factory;
mod service;
pub mod task;
pub use factory::{Factory, Init, InitFuture};
pub use service::{Builder, CreateError, Error, Handle, Phase, Prepared, Running};
