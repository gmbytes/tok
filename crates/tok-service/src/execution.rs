use crate::Request;
use crate::{Context as ExecutionContext, StopReason};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::time::Timeout;
use tokio_util::sync::WaitForCancellationFutureOwned;
use tower::{ServiceExt, util::Oneshot};
use tracing::{Instrument, instrument::Instrumented};

type Work<S, T> = Instrumented<Timeout<Oneshot<S, Request<T>>>>;

/// 执行停止或内层原始错误；不依赖 RPC Status，不吞掉游戏自定义错误。
#[derive(Debug, thiserror::Error)]
pub enum Error<E> {
    /// 原调用范围已取消或预算耗尽；已发生的副作用不回滚。
    #[error(transparent)]
    Stopped(#[from] StopReason),
    /// 内层 Service 就绪或处理失败，保留其具体错误类型。
    #[error("处理失败：{0}")]
    Inner(E),
}

/// 为任意类型化调用增加原期限、取消及追踪约束，不增加业务重试或后台队列。
///
/// 每次调用克隆内层 Service；需要跨克隆共享的并发限额由内层 Layer 自行共享。
/// 内层就绪等待放在调用 future 内，计入上下文期限；外层始终就绪不代表内层就绪。
#[derive(Debug, Clone, Copy, Default)]
pub struct Layer;
impl<S> tower::Layer<S> for Layer {
    type Service = Service<S>;
    fn layer(&self, inner: S) -> Self::Service {
        Service { inner }
    }
}

/// Layer 包装后的调用入口；可处理包含非静态借用的输入。
#[derive(Debug, Clone)]
pub struct Service<S> {
    inner: S,
}
impl<S, T> tower::Service<Request<T>> for Service<S>
where
    S: tower::Service<Request<T>> + Clone,
{
    type Response = S::Response;
    type Error = Error<S::Error>;
    type Future = Call<S, T>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<T>) -> Self::Future {
        let context = request.context.clone();
        let cancellation = Box::pin(context.cancellation().cancelled_owned());
        let work = Box::pin(
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(context.deadline()),
                self.inner.clone().oneshot(request),
            )
            .instrument(context.span().clone()),
        );
        Call {
            context,
            work,
            cancellation,
        }
    }
}

/// 一次受预算和取消保护的调用；内部 future 保留输入的真实生命周期。
///
/// 不要求静态生命周期，因此不能把借用调用交给要求静态任务的 Buffer 或 spawn。
/// 丢弃本 future 会丢弃就绪/业务 future，但不承诺回滚已经发生的外部副作用。
pub struct Call<S, T>
where
    S: tower::Service<Request<T>>,
{
    context: ExecutionContext,
    work: Pin<Box<Work<S, T>>>,
    cancellation: Pin<Box<WaitForCancellationFutureOwned>>,
}
impl<S, T> Future for Call<S, T>
where
    S: tower::Service<Request<T>>,
{
    type Output = Result<S::Response, Error<S::Error>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Err(reason) = this.context.check() {
            return Poll::Ready(Err(reason.into()));
        }
        if this.cancellation.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(StopReason::Cancelled.into()));
        }
        match this.work.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                // 非协作式处理者可能一次 poll 超过预算，不能把迟到成功当成按时完成。
                if let Err(reason) = this.context.check() {
                    return Poll::Ready(Err(reason.into()));
                }
                Poll::Ready(match result {
                    Ok(result) => result.map_err(Error::Inner),
                    Err(_) => Err(StopReason::DeadlineExceeded.into()),
                })
            }
        }
    }
}
