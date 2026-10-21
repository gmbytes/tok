use super::Server;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tok_core::Identity;
use tok_execution::{Request, StopReason};
use tonic::Status;
use tower::Service as TowerService;

/// 将类型化服务接入 gRPC 执行范围，校验服务身份并映射停止原因与记录指标。
///
/// metadata 转换仍由 Server::context 完成；本 Layer 不解码、不认证、不重新分配预算。
#[derive(Clone)]
pub struct Layer {
    identity: Identity,
}
impl Layer {
    /// 复用已校验的入口身份，不访问网络或建立新的调用上下文。
    pub fn new(server: &Server) -> Self {
        Self {
            identity: server.identity().clone(),
        }
    }
}
impl<S> tower::Layer<S> for Layer {
    type Service = Service<S>;
    fn layer(&self, inner: S) -> Self::Service {
        Service {
            identity: self.identity.clone(),
            inner: tok_execution::Layer.layer(inner),
        }
    }
}

/// 共同执行链的 gRPC 边界适配；就绪等待、取消与期限由 tok-execution 统一约束。
#[derive(Clone)]
pub struct Service<S> {
    identity: Identity,
    inner: tok_execution::Service<S>,
}
impl<S, T> TowerService<Request<T>> for Service<S>
where
    S: TowerService<Request<T>, Error = Status> + Clone,
{
    type Response = S::Response;
    type Error = Status;
    type Future = ResponseFuture<tok_execution::Call<S, T>>;
    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<T>) -> Self::Future {
        let metrics = Metrics {
            identity: self.identity.clone(),
            started: std::time::Instant::now(),
        };
        if request.context.service() != &self.identity {
            return ResponseFuture {
                work: None,
                error: Some(Status::failed_precondition("RPC 执行服务与入口不匹配")),
                metrics: Some(metrics),
            };
        }
        ResponseFuture {
            work: Some(Box::pin(self.inner.call(request))),
            error: None,
            metrics: Some(metrics),
        }
    }
}

/// 服务端边界的执行 Future，保留内层真实生命周期，不创建后台任务或重试。
pub struct ResponseFuture<F> {
    work: Option<Pin<Box<F>>>,
    error: Option<Status>,
    metrics: Option<Metrics>,
}
impl<F, T> Future for ResponseFuture<F>
where
    F: Future<Output = Result<T, tok_execution::Error<Status>>>,
{
    type Output = Result<T, Status>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = match this.work.as_mut() {
            Some(work) => match work.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result.map_err(|error| match error {
                    tok_execution::Error::Inner(error) => error,
                    tok_execution::Error::Stopped(StopReason::Cancelled) => {
                        Status::cancelled("调用已取消")
                    }
                    tok_execution::Error::Stopped(StopReason::DeadlineExceeded) => {
                        Status::deadline_exceeded("调用预算已耗尽")
                    }
                }),
            },
            None => Err(this
                .error
                .take()
                .unwrap_or_else(|| Status::failed_precondition("服务端调用已经完成"))),
        };
        this.work = None;
        if let Some(metrics) = this.metrics.take() {
            metrics.record(&result);
        }
        Poll::Ready(result)
    }
}
struct Metrics {
    identity: Identity,
    started: std::time::Instant,
}
impl Metrics {
    fn record<T>(&self, result: &Result<T, Status>) {
        metrics::histogram!("tok_grpc_server_duration_seconds", "service_name" => self.identity.service_name.clone()).record(self.started.elapsed().as_secs_f64());
        metrics::counter!("tok_grpc_server_calls_total", "service_name" => self.identity.service_name.clone(), "result" => if result.is_ok() { "ok" } else { "error" }).increment(1);
    }
}
