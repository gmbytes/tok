use super::CallContext;
use std::{future::Future, pin::Pin};
use tok_execution::{Request, Response, StopReason};
use tok_proto::internal::v1::{CallResponse, Payload};
use tonic::Status;
use tower::{Service, ServiceExt};

/// RPC 边界处理结果，保留输出扩展。
pub type CallTask<'a> =
    Pin<Box<dyn Future<Output = Result<Response<CallResponse>, Status>> + Send + 'a>>;
/// 普通业务 RPC 的调用处理入口。
pub trait Handler: Send + Sync {
    /// 处理已校验调用，保留执行范围。
    fn call(&self, request: Request<(CallContext, Payload)>) -> CallTask<'_>;
}
/// 将 Tower 处理链接入普通业务 RPC。
pub struct TowerHandler<S>(
    /// 克隆应共享需要共享的中间件状态。
    pub S,
);
impl<S> Handler for TowerHandler<S>
where
    S: Service<Request<(CallContext, Payload)>, Response = Response<CallResponse>, Error = Status>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
{
    fn call(&self, request: Request<(CallContext, Payload)>) -> CallTask<'_> {
        Box::pin(self.0.clone().oneshot(request))
    }
}
fn execution_status(error: tok_execution::Error<Status>) -> Status {
    match error {
        tok_execution::Error::Inner(error) => error,
        tok_execution::Error::Stopped(StopReason::Cancelled) => Status::cancelled("调用已取消"),
        tok_execution::Error::Stopped(StopReason::DeadlineExceeded) => {
            Status::deadline_exceeded("调用预算已耗尽")
        }
    }
}
pub(super) fn dispatch_status<E>(
    error: tok_execution::Error<E>,
    map_error: impl FnOnce(E) -> Status,
) -> Status {
    match error {
        tok_execution::Error::Inner(error) => map_error(error),
        tok_execution::Error::Stopped(reason) => {
            execution_status(tok_execution::Error::Stopped(reason))
        }
    }
}
pub(super) fn table_status<E>(
    error: tok_execution::registry::DispatchError<u32, crate::protobuf::Error<E>>,
    map_error: impl FnOnce(E) -> Status,
) -> Status {
    use tok_execution::registry::DispatchError;
    match error {
        DispatchError::Unknown(_) => Status::unimplemented("游戏消息尚未注册"),
        DispatchError::Handler(crate::protobuf::Error::Decode(_)) => {
            Status::invalid_argument("游戏消息解码失败")
        }
        DispatchError::Handler(crate::protobuf::Error::Handler(error)) => map_error(error),
    }
}
