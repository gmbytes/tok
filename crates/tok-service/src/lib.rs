//! 通用执行原型：类型化请求共用 Tower Layer，就绪等待、执行与取消受同一预算控制。
//!
//! 本 crate 当前只提供 P1 执行设施，不替代已有 Application、Gateway 或路由注册表。
//! 借用的请求可以在当前任务中执行，不要求静态生命周期，不隐式启动后台任务。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod execution;

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
