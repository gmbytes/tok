use super::{Call, Client};
use crate::PreparedCall;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tok_execution::Request;
use tonic::Status;
use tower::Service as TowerService;

/// 为协议发送器增加连接获取、期限、追踪与指标的标准 Tower Layer。
///
/// 外层输入为 `Request<Call<T>>`，内层为 `Request<PreparedCall<T>>`，保留原执行范围，不重试；
/// 每次调用克隆内层 Service，不要求输入或 Future 为 static，支持借用调用。
/// 原 Context 的截止点覆盖已发生的等待，客户端与 metadata 上限只能收紧预算。
/// 外层等待期间的及时停止仍由外层执行保护负责；本层约束准备、内层就绪与发送。
/// 需要跨调用共享的限流或熔断状态由内层策略显式共享。
#[derive(Clone)]
pub struct Layer {
    client: Client,
}
impl Layer {
    /// 克隆共享客户端配置和缓存，不访问网络。
    pub fn new(client: &Client) -> Self {
        Self {
            client: client.clone(),
        }
    }
}
impl<S> tower::Layer<S> for Layer {
    type Service = Service<S>;
    fn layer(&self, inner: S) -> Self::Service {
        Service {
            client: self.client.clone(),
            inner,
        }
    }
}

/// 执行客户端处理链；外部就绪不预占内部资源，内部就绪计入调用预算。
#[derive(Clone)]
pub struct Service<S> {
    client: Client,
    inner: S,
}
impl<S> Service<S> {
    // 便捷方法消费单次处理者，允许保留 FnOnce 和非 Clone Service，而不增加另一套执行循环。
    pub(super) fn call_once<T>(self, call: Request<Call<T>>) -> ResponseFuture<S, T>
    where
        S: TowerService<Request<PreparedCall<T>>, Error = Status>,
    {
        ResponseFuture::new(&self.client, self.inner, call)
    }
}
impl<S, T> TowerService<Request<Call<T>>> for Service<S>
where
    S: TowerService<Request<PreparedCall<T>>, Error = Status> + Clone,
{
    type Response = S::Response;
    type Error = Status;
    type Future = ResponseFuture<S, T>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, call: Request<Call<T>>) -> Self::Future {
        ResponseFuture::new(&self.client, self.inner.clone(), call)
    }
}

enum State<S: TowerService<Request<PreparedCall<T>>, Error = Status>, T> {
    Failed(Option<Status>),
    Running(Pin<Box<tok_execution::Call<S, PreparedCall<T>>>>),
    Done,
}

/// 一次客户端执行的 Future，包含内部就绪等待，保留输入和处理者的真实生命周期。
///
/// 丢弃后不继续驱动内层 Future，不保证回滚远端副作用；不启动后台任务或自动重试。
pub struct ResponseFuture<S: TowerService<Request<PreparedCall<T>>, Error = Status>, T> {
    state: State<S, T>,
    metrics: Option<Metrics>,
    context: tok_execution::Context,
}
impl<S, T> ResponseFuture<S, T>
where
    S: TowerService<Request<PreparedCall<T>>, Error = Status>,
{
    fn new(client: &Client, service: S, request: Request<Call<T>>) -> Self {
        let (mut context, mut call, extensions) = request.into_parts();
        context = context.with_budget_limit(client.timeout());
        let metrics = Metrics {
            target: call.target.clone(),
            started: std::time::Instant::now(),
        };
        let state = match (|| {
            context.check().map_err(stopped)?;
            let budget =
                crate::timeout::request_budget(call.request.metadata(), context.remaining())?;
            context = context.clone().with_budget_limit(budget);
            let channel = client.channel(&call.target)?;
            crate::propagation::inject(call.request.metadata_mut(), context.span());
            let remaining = context.remaining();
            if remaining.is_zero() {
                return Err(expired());
            }
            call.request.set_timeout(remaining);
            let prepared = PreparedCall::new(call.target, channel, call.request);
            Ok(tok_execution::Call::new(
                service,
                Request::from_parts(context.clone(), prepared, extensions),
            ))
        })() {
            Ok(work) => State::Running(Box::pin(work)),
            Err(error) => State::Failed(Some(error)),
        };
        Self {
            state,
            metrics: Some(metrics),
            context,
        }
    }
}
impl<S, T> Future for ResponseFuture<S, T>
where
    S: TowerService<Request<PreparedCall<T>>, Error = Status>,
{
    type Output = Result<S::Response, Status>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = match &mut this.state {
            State::Failed(error) => Err(error.take().unwrap_or_else(completed)),
            State::Running(work) => match work.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result.map_err(|error| match error {
                    tok_execution::Error::Inner(error) => error,
                    tok_execution::Error::Stopped(reason) => stopped(reason),
                }),
            },
            State::Done => return Poll::Ready(Err(completed())),
        };
        let result = this.context.check().map_err(stopped).and(result);
        this.state = State::Done;
        if let Some(metrics) = this.metrics.take() {
            this.context.span().in_scope(|| metrics.record(&result));
        }
        Poll::Ready(result)
    }
}

fn stopped(reason: tok_execution::StopReason) -> Status {
    match reason {
        tok_execution::StopReason::Cancelled => Status::cancelled("调用已取消"),
        tok_execution::StopReason::DeadlineExceeded => expired(),
    }
}
fn expired() -> Status {
    Status::deadline_exceeded("内部 RPC 超时，未自动重试")
}
fn completed() -> Status {
    Status::failed_precondition("出站调用已经完成")
}
struct Metrics {
    target: tok_core::ServiceDescriptor,
    started: std::time::Instant,
}
impl Metrics {
    fn record<T>(&self, result: &Result<T, Status>) {
        metrics::histogram!("tok_grpc_client_duration_seconds", "service_name" => self.target.service_name.clone()).record(self.started.elapsed().as_secs_f64());
        metrics::counter!("tok_grpc_client_calls_total", "service_name" => self.target.service_name.clone(), "result" => if result.is_ok() { "ok" } else { "error" }).increment(1);
        if let Err(status) = result {
            tracing::error!(node_id = %self.target.node_id, service_name = %self.target.service_name, code = ?status.code(), "内部 RPC 失败，未自动重试");
        }
    }
}
