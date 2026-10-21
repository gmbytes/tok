//! 普通业务 RPC 的请求锁适配；取键、回应判断和 Status 映射留在协议边界。
//! 时间预算与持锁执行由 tok_execution::lock 提供，不自动安装到 player 或 GatewayRpc。
use super::{CallContext, CallTask, Error};
use std::{
    sync::Arc,
    task::{Context, Poll},
};
use tok_core::lock::{Backend, Key};
use tok_execution::{Request, Response, lock};
use tok_proto::internal::v1::{CallResponse, Payload};
use tonic::Status;

type Input = Request<(CallContext, Payload)>;
type KeyFn = Box<dyn Fn(&Input) -> Result<Key, Status> + Send + Sync>;
type SuccessFn = fn(&Response<CallResponse>) -> bool;

/// 为已校验的业务 RPC 注入显式请求锁策略；克隆共享后端和策略。
#[derive(Clone)]
pub struct Layer {
    inner: lock::Layer<KeyFn, SuccessFn>,
}
impl Layer {
    /// 指定逻辑业务服务名和通用时间预算；不访问后端，不包含节点名。
    pub fn new(
        backend: Arc<dyn Backend>,
        namespace: impl Into<String>,
        config: lock::Config,
    ) -> Result<Self, Error> {
        let namespace = namespace.into();
        if namespace.trim().is_empty() || namespace.len() > 256 {
            return Err(Error::Configuration("请求锁命名空间无效".into()));
        }
        let key: KeyFn = Box::new(move |request| {
            let call = &request.input.0;
            if call.service_name != namespace {
                return Err(Status::failed_precondition(
                    "请求锁命名空间与目标服务不匹配",
                ));
            }
            let bid = call
                .bid
                .as_ref()
                .filter(|bid| !bid.trim().is_empty() && bid.len() <= 1024)
                .ok_or_else(|| Status::invalid_argument("请求锁需要有效 bid"))?;
            Ok(Key {
                namespace: namespace.clone(),
                bid: bid.clone(),
            })
        });
        let success: SuccessFn = |response| {
            response
                .output
                .error
                .as_ref()
                .is_none_or(|error| error.code == 0)
        };
        let inner = lock::Layer::new(backend, config, key, success)
            .map_err(|error| Error::Configuration(error.to_string()))?;
        Ok(Self { inner })
    }
}
impl<S> tower::Layer<S> for Layer {
    type Service = Service<S>;
    fn layer(&self, inner: S) -> Self::Service {
        Service {
            inner: self.inner.layer(inner),
        }
    }
}
/// 只将通用锁错误映射为 gRPC Status，持锁执行逻辑不在此重复实现。
#[derive(Clone)]
pub struct Service<S> {
    inner: lock::Service<S, KeyFn, SuccessFn>,
}
impl<S> tower::Service<Input> for Service<S>
where
    S: tower::Service<Input, Response = Response<CallResponse>, Error = Status>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
{
    type Response = Response<CallResponse>;
    type Error = Status;
    type Future = CallTask<'static>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Status>> {
        self.inner.poll_ready(cx).map_err(status)
    }
    fn call(&mut self, request: Input) -> Self::Future {
        let future = self.inner.call(request);
        Box::pin(async move { future.await.map_err(status) })
    }
}
fn status(error: lock::Error<Status>) -> Status {
    match error {
        lock::Error::Key(error) | lock::Error::Inner(error) => error,
        lock::Error::Acquire(error) => match error {
            tok_core::lock::Error::InvalidInput(_) => Status::invalid_argument("请求锁参数无效"),
            tok_core::lock::Error::WaitTimeout => Status::deadline_exceeded("请求锁获取超时"),
            tok_core::lock::Error::CapacityExceeded => Status::resource_exhausted("请求锁容量已满"),
            tok_core::lock::Error::InvalidConfig(_) => {
                Status::failed_precondition("请求锁配置无效")
            }
            tok_core::lock::Error::Unavailable(_) => Status::unavailable("请求锁后端不可用"),
        },
        lock::Error::AcquireTimeout => Status::deadline_exceeded("请求锁获取超时"),
        lock::Error::InsufficientLease => Status::deadline_exceeded("请求锁剩余租期不足"),
        lock::Error::ExecutionTimeout => Status::deadline_exceeded("持锁业务执行超时"),
        lock::Error::Release(_) | lock::Error::ReleaseTimeout => {
            Status::unavailable("请求锁释放结果不确定")
        }
        lock::Error::Stopped(tok_execution::StopReason::Cancelled) => {
            Status::cancelled("调用已取消")
        }
        lock::Error::Stopped(tok_execution::StopReason::DeadlineExceeded) => {
            Status::deadline_exceeded("调用预算已耗尽")
        }
    }
}
