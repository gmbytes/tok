use tok_execution::StopReason;
use tonic::Status;
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
