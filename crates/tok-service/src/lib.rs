//! 通用服务设施：类型化执行共用 Tower Layer，工厂与任务监督管理构建和生命周期。
//!
//! task 管理通用任务范围，lifecycle 编排服务构建与阶段；Application 和 Gateway 负责接入适配。
//! 借用的请求可以在当前任务中执行，不要求静态生命周期，不隐式启动后台任务。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod context;
mod execution;
pub mod lifecycle;
pub mod task;

pub use context::{Context, Source, StopReason};
pub use execution::{Call, Error, Layer, Service};

/// 执行上下文与类型化输入；正文可以是具体消息，也可以包含当前状态的可变借用。
#[derive(Debug)]
pub struct Request<T> {
    /// 由可信入口建立的上下文；不从客户端正文自动恢复。
    pub context: Context,
    /// 当前处理者明确接受的输入，协议解码由入口适配承担。
    pub input: T,
}
impl<T> Request<T> {
    /// 组合上下文和输入；不入队、不编码、不启动任务。
    pub fn new(context: Context, input: T) -> Self {
        Self { context, input }
    }
}

/// 将同步处理函数适配为相同的 Tower Service，不创建线程或任务。
pub fn sync_handler<F>(handler: F) -> SyncHandler<F> {
    SyncHandler(std::sync::Arc::new(handler))
}

/// 同步回调的 Tower 适配；输入可以包含当前调用的借用。
pub struct SyncHandler<F>(std::sync::Arc<F>);
impl<F> Clone for SyncHandler<F> {
    fn clone(&self) -> Self {
        Self(std::sync::Arc::clone(&self.0))
    }
}
impl<T, O, E, F> tower::Service<Request<T>> for SyncHandler<F>
where
    F: Fn(Request<T>) -> Result<O, E>,
{
    type Response = O;
    type Error = E;
    type Future = std::future::Ready<Result<O, E>>;
    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), E>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<T>) -> Self::Future {
        std::future::ready((self.0)(request))
    }
}
