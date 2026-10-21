use super::{Call, Client};
use crate::Outgoing;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tok_execution::Request;
use tokio::time::{Instant, Timeout};
use tokio_util::sync::WaitForCancellationFutureOwned;
use tonic::Status;
use tower::Service as TowerService;
use tracing::{Instrument, instrument::Instrumented};

/// 为协议发送器增加连接获取、期限、追踪与指标的标准 Tower Layer。
///
/// 外层输入为 `Request<Call<T>>`，内层为 `Request<Outgoing<T>>`，保留原执行范围，不重试；
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
        S: TowerService<Request<Outgoing<T>>, Error = Status>,
    {
        ResponseFuture::new(&self.client, self.inner, call)
    }
}
impl<S, T> TowerService<Request<Call<T>>> for Service<S>
where
    S: TowerService<Request<Outgoing<T>>, Error = Status> + Clone,
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

type Work<S, T> = Instrumented<Timeout<Dispatch<S, T>>>;
enum State<S: TowerService<Request<Outgoing<T>>, Error = Status>, T> {
    Failed(Option<Status>),
    Running {
        deadline: Instant,
        work: Pin<Box<Work<S, T>>>,
    },
    Done,
}

/// 一次客户端执行的 Future，包含内部就绪等待，保留输入和处理者的真实生命周期。
///
/// 丢弃后不继续驱动内层 Future，不保证回滚远端副作用；不启动后台任务或自动重试。
pub struct ResponseFuture<S: TowerService<Request<Outgoing<T>>, Error = Status>, T> {
    state: State<S, T>,
    metrics: Option<Metrics>,
    context: tok_execution::Context,
    cancellation: Pin<Box<WaitForCancellationFutureOwned>>,
}
impl<S, T> ResponseFuture<S, T>
where
    S: TowerService<Request<Outgoing<T>>, Error = Status>,
{
    fn new(client: &Client, service: S, request: Request<Call<T>>) -> Self {
        let Request {
            mut context,
            input: mut call,
        } = request;
        context = context.with_budget_limit(client.timeout());
        let metrics = Metrics {
            target: call.target.clone(),
            request_id: call.request_id,
            started: std::time::Instant::now(),
        };
        let state = match (|| {
            context.check().map_err(stopped)?;
            let budget =
                crate::timeout::request_budget(call.request.metadata(), context.remaining())?;
            context = context.clone().with_budget_limit(budget);
            let deadline = Instant::from_std(context.deadline());
            let channel = client.channel(&call.target)?;
            crate::propagation::inject(call.request.metadata_mut(), context.span());
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(expired());
            }
            call.request.set_timeout(remaining);
            let outgoing = Outgoing::new(call.target, call.request_id, channel, call.request);
            let work = tokio::time::timeout_at(
                deadline,
                Dispatch::new(service, Request::new(context.clone(), outgoing)),
            )
            .instrument(context.span().clone());
            Ok((deadline, work))
        })() {
            Ok((deadline, work)) => State::Running {
                deadline,
                work: Box::pin(work),
            },
            Err(error) => State::Failed(Some(error)),
        };
        Self {
            state,
            metrics: Some(metrics),
            cancellation: Box::pin(context.cancellation().cancelled_owned()),
            context,
        }
    }
}
impl<S, T> Future for ResponseFuture<S, T>
where
    S: TowerService<Request<Outgoing<T>>, Error = Status>,
{
    type Output = Result<S::Response, Status>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let stop_error = this.context.check().err().map(stopped).or_else(|| {
            this.cancellation
                .as_mut()
                .poll(cx)
                .is_ready()
                .then(|| Status::cancelled("调用已取消"))
        });
        let result = if let Some(error) = stop_error {
            Err(error)
        } else {
            match &mut this.state {
                State::Failed(error) => Err(error.take().unwrap_or_else(completed)),
                State::Running { deadline, work } => {
                    if Instant::now() >= *deadline {
                        Err(expired())
                    } else {
                        let result = match work.as_mut().poll(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(result) => result,
                        };
                        if Instant::now() >= *deadline {
                            Err(expired())
                        } else {
                            result.unwrap_or_else(|_| Err(expired()))
                        }
                    }
                }
                State::Done => return Poll::Ready(Err(completed())),
            }
        };
        let result = this.context.check().map_err(stopped).and(result);
        this.state = State::Done;
        if let Some(metrics) = this.metrics.take() {
            metrics.record(&result);
        }
        Poll::Ready(result)
    }
}

// 就绪后的分发必须再次检查截止点，避免单次阻塞 poll_ready 在超时后发出请求。
struct Dispatch<S: TowerService<Request<Outgoing<T>>>, T> {
    ready: Option<Box<(S, Request<Outgoing<T>>)>>,
    running: Option<Pin<Box<S::Future>>>,
}
impl<S, T> Dispatch<S, T>
where
    S: TowerService<Request<Outgoing<T>>>,
{
    fn new(service: S, call: Request<Outgoing<T>>) -> Self {
        Self {
            ready: Some(Box::new((service, call))),
            running: None,
        }
    }
}
impl<S, T> Future for Dispatch<S, T>
where
    S: TowerService<Request<Outgoing<T>>, Error = Status>,
{
    type Output = Result<S::Response, Status>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(ready) = this.ready.as_mut() {
            match ready.0.poll_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    this.ready = None;
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Ok(())) => {}
            }
        }
        if let Some(ready) = this.ready.take() {
            let (mut service, call) = *ready;
            if let Err(error) = call.context.check().map_err(stopped) {
                return Poll::Ready(Err(error));
            }
            this.running = Some(Box::pin(service.call(call)));
        }
        match this.running.as_mut() {
            Some(future) => future.as_mut().poll(cx),
            None => Poll::Ready(Err(completed())),
        }
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
    request_id: u64,
    started: std::time::Instant,
}
impl Metrics {
    fn record<T>(&self, result: &Result<T, Status>) {
        metrics::histogram!("tok_grpc_client_duration_seconds", "service_name" => self.target.service_name.clone()).record(self.started.elapsed().as_secs_f64());
        metrics::counter!("tok_grpc_client_calls_total", "service_name" => self.target.service_name.clone(), "result" => if result.is_ok() { "ok" } else { "error" }).increment(1);
        if let Err(status) = result {
            tracing::error!(node_id = %self.target.node_id, service_name = %self.target.service_name, request_id = self.request_id, code = ?status.code(), "内部 RPC 失败，未自动重试");
        }
    }
}
