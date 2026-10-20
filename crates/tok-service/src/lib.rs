//! 通用服务设施：类型化执行共用 Tower Layer，工厂与任务监督管理构建和生命周期。
//!
//! task 管理通用任务范围，lifecycle 编排服务构建与阶段；Application 和 Gateway 负责接入适配。
//! 借用的请求可以在当前任务中执行，不要求静态生命周期，不隐式启动后台任务。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod execution;
pub mod lifecycle;
pub mod task;

pub use execution::{Call, Error, Layer, Service};

/// 执行上下文与类型化输入；正文可以是具体消息，也可以包含当前状态的可变借用。
#[derive(Debug)]
pub struct Request<T> {
    /// 由可信入口建立的上下文；不从客户端正文自动恢复。
    pub context: tok_core::execution::Context,
    /// 当前处理者明确接受的输入，协议解码由入口适配承担。
    pub input: T,
}
impl<T> Request<T> {
    /// 组合上下文和输入；不入队、不编码、不启动任务。
    pub fn new(context: tok_core::execution::Context, input: T) -> Self {
        Self { context, input }
    }
}
