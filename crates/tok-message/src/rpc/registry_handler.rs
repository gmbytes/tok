//! 将编号消息注册器适配为普通业务 RPC 处理入口。
use super::CallContext;
use std::{
    sync::Arc,
    task::{Context, Poll},
};
use tok_proto::internal::v1::{CallResponse, Payload};
use tonic::Status;
use tower::ServiceExt as _;

type ContextFactory<C> = dyn Fn(&CallContext) -> Result<C, Status> + Send + Sync;

/// 将传输无关的消息分发表适配为普通业务 RPC；只在此边界构造线协议回应和 Status。
///
/// 上下文转换器负责提取业务状态；默认构造映射原始业务错误，自定义构造映射注册器派发错误。
/// 此适配器不接受 timer 或服务器事件，也不添加锁、队列或重试。
pub struct RegistryHandler<
    C,
    E,
    G: tok_execution::Registry + ?Sized = crate::codec::Table<u32, C, (), E>,
> {
    registry: Arc<crate::Registry<C, (), E, G>>,
    context: Arc<ContextFactory<C>>,
    error: Arc<dyn Fn(G::DispatchError) -> Status + Send + Sync>,
}

impl<C, E, G: tok_execution::Registry + ?Sized> Clone for RegistryHandler<C, E, G> {
    fn clone(&self) -> Self {
        Self {
            registry: Arc::clone(&self.registry),
            context: Arc::clone(&self.context),
            error: Arc::clone(&self.error),
        }
    }
}

impl<C, E> RegistryHandler<C, E>
where
    C: Send + 'static,
    E: Send + 'static,
{
    /// 使用默认 Table 的状态映射，只需提供原始业务错误转换。
    pub fn new(
        registry: Arc<crate::Registry<C, (), E>>,
        context: impl Fn(&CallContext) -> Result<C, Status> + Send + Sync + 'static,
        error: impl Fn(E) -> Status + Send + Sync + 'static,
    ) -> Self {
        Self::from_registry(registry, context, move |failure| {
            super::handler::table_status(failure, &error)
        })
    }
}
impl<C, E, G: tok_execution::Registry + ?Sized> RegistryHandler<C, E, G> {
    /// 注入自定义注册器，并显式映射其完整派发错误；执行停止由共同边界映射。
    pub fn from_registry(
        registry: Arc<crate::Registry<C, (), E, G>>,
        context: impl Fn(&CallContext) -> Result<C, Status> + Send + Sync + 'static,
        error: impl Fn(G::DispatchError) -> Status + Send + Sync + 'static,
    ) -> Self {
        Self {
            registry,
            context: Arc::new(context),
            error: Arc::new(error),
        }
    }
}

impl<C, E, G: ?Sized> tower::Service<tok_execution::Request<(CallContext, Payload)>>
    for RegistryHandler<C, E, G>
where
    C: Send + 'static,
    E: Send + 'static,
    G: tok_execution::Registry<
            Key = u32,
            Input = (C, Vec<u8>),
            Output = crate::codec::Reply<crate::codec::Message<u32>>,
            HandlerError = crate::codec::Error<E>,
        > + 'static,
    G::DispatchError: Send + 'static,
{
    type Response = tok_execution::Response<CallResponse>;
    type Error = Status;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Status>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: tok_execution::Request<(CallContext, Payload)>) -> Self::Future {
        let handler = self.clone();
        Box::pin(async move {
            let (context, (call, payload), mut extensions) = request.into_parts();
            if payload.message_id == 0 {
                return Err(Status::invalid_argument("业务请求消息 ID 必须非零"));
            }
            let state = (handler.context)(&call)?;
            let id = call.request_id;
            extensions.insert(call);
            let reply = handler
                .registry
                .as_ref()
                .clone()
                .oneshot(tok_execution::Request::from_parts(
                    context,
                    (
                        state,
                        crate::codec::Message {
                            key: payload.message_id,
                            body: payload.body,
                        },
                    ),
                    extensions,
                ))
                .await
                .map_err(|error| super::handler::dispatch_status(error, &*handler.error))?;
            if reply.output.message.key == 0 {
                return Err(Status::failed_precondition("业务回应消息 ID 必须非零"));
            }
            Ok(reply.map(|reply| CallResponse {
                request_id: id,
                payload: Some(Payload {
                    message_id: reply.message.key,
                    body: reply.message.body,
                }),
                error: None,
                timers: None,
            }))
        })
    }
}
