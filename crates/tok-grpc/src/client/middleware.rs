use super::{Call, Client};
use crate::Outgoing;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::time::{Instant, Timeout};
use tonic::Status;
use tower::Service as TowerService;
use tracing::{Instrument, instrument::Instrumented};

/// 为协议发送器增加连接获取、期限、追踪与指标的标准 Tower Layer。
///
/// 外层输入为 Call，内层输入为 Outgoing。内层就绪和发送共用本地期限，不重试；
/// 每次调用克隆内层 Service，不要求输入或 Future 为 static，支持借用调用。
/// 预算从本层 call 开始，覆盖连接准备、内层就绪与发送；外层等待不在此预算内。
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
    pub(super) fn call_once<T>(self, call: Call<T>) -> ResponseFuture<S, T>
    where
        S: TowerService<Outgoing<T>, Error = Status>,
    {
        ResponseFuture::new(&self.client, self.inner, call)
    }
}
impl<S, T> TowerService<Call<T>> for Service<S>
where
    S: TowerService<Outgoing<T>, Error = Status> + Clone,
{
    type Response = S::Response;
    type Error = Status;
    type Future = ResponseFuture<S, T>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, call: Call<T>) -> Self::Future {
        ResponseFuture::new(&self.client, self.inner.clone(), call)
    }
}

type Work<S, T> = Instrumented<Timeout<Dispatch<S, T>>>;
enum State<S: TowerService<Outgoing<T>, Error = Status>, T> {
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
pub struct ResponseFuture<S: TowerService<Outgoing<T>, Error = Status>, T> {
    state: State<S, T>,
    metrics: Option<Metrics>,
}
impl<S, T> ResponseFuture<S, T>
where
    S: TowerService<Outgoing<T>, Error = Status>,
{
    fn new(client: &Client, service: S, mut call: Call<T>) -> Self {
        let metrics = Metrics {
            target: call.target.clone(),
            request_id: call.request_id,
            started: std::time::Instant::now(),
        };
        let state = match (|| {
            let budget = crate::timeout::request_budget(call.request.metadata(), client.timeout())?;
            let deadline = Instant::now() + budget;
            let channel = client.channel(&call.target)?;
            crate::propagation::inject(call.request.metadata_mut(), &call.span);
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(expired());
            }
            call.request.set_timeout(remaining);
            let outgoing = Outgoing::new(
                call.target,
                call.request_id,
                channel,
                call.request,
                deadline,
            );
            let work = tokio::time::timeout_at(deadline, Dispatch::new(service, outgoing))
                .instrument(call.span);
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
        }
    }
}
impl<S, T> Future for ResponseFuture<S, T>
where
    S: TowerService<Outgoing<T>, Error = Status>,
{
    type Output = Result<S::Response, Status>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = match &mut this.state {
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
        };
        this.state = State::Done;
        if let Some(metrics) = this.metrics.take() {
            metrics.record(&result);
        }
        Poll::Ready(result)
    }
}

// 就绪后的分发必须再次检查截止点，避免单次阻塞 poll_ready 在超时后发出请求。
struct Dispatch<S: TowerService<Outgoing<T>>, T> {
    ready: Option<Box<(S, Outgoing<T>)>>,
    running: Option<Pin<Box<S::Future>>>,
}
impl<S, T> Dispatch<S, T>
where
    S: TowerService<Outgoing<T>>,
{
    fn new(service: S, call: Outgoing<T>) -> Self {
        Self {
            ready: Some(Box::new((service, call))),
            running: None,
        }
    }
}
impl<S, T> Future for Dispatch<S, T>
where
    S: TowerService<Outgoing<T>, Error = Status>,
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
            if let Err(error) = call.check() {
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
