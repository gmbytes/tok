//! 普通 BusinessRpc 客户端适配；不要求会话，不自动加锁。
use super::CallContext;
use super::call;
use std::sync::Arc;
use tok_core::ServiceDescriptor;
use tok_proto::internal::v1::{self as proto, CallResponse, Payload};
use tonic::Status;
use tower::ServiceExt as _;

/// 普通业务协议客户端；与其他适配器共享连接而不共享业务语义。
#[derive(Clone)]
pub struct Client {
    connections: Arc<tok_grpc::Client>,
}
impl Client {
    /// 注入共享连接管理器，不建立连接。
    pub fn new(connections: Arc<tok_grpc::Client>) -> Self {
        Self { connections }
    }
    /// 调用普通业务入口；拒绝内部 timer，不附加锁或重试。
    pub async fn call(
        &self,
        target: &ServiceDescriptor,
        request: tok_execution::Request<(CallContext, Payload)>,
    ) -> Result<tok_execution::Response<CallResponse>, Status> {
        if request.input.0.timer.is_some() {
            return Err(Status::invalid_argument("计时触发只能调用 player 入口"));
        }
        call::call(
            &self.connections,
            target,
            request,
            |channel, request| async move {
                proto::business_rpc_client::BusinessRpcClient::new(channel)
                    .call(request)
                    .await
            },
        )
        .await
    }
}

type ContextFactory<C> = dyn Fn(&CallContext) -> Result<C, Status> + Send + Sync;

/// 将传输无关的消息分发表适配为普通业务 RPC；只在此边界构造线协议回应和 Status。
///
/// 上下文转换器负责提取业务状态；默认构造映射原始业务错误，自定义构造映射注册器派发错误。
/// 此适配器不接受 timer 或服务器事件，也不添加锁、队列或重试。
pub struct Handler<C, E, G: tok_execution::Registry + ?Sized = tok_protobuf::Table<u32, C, (), E>> {
    registry: Arc<crate::Registry<C, (), E, G>>,
    context: Box<ContextFactory<C>>,
    error: Box<dyn Fn(G::DispatchError) -> Status + Send + Sync>,
}

impl<C, E> Handler<C, E>
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
impl<C, E, G: tok_execution::Registry + ?Sized> Handler<C, E, G> {
    /// 注入自定义注册器，并显式映射其完整派发错误；执行停止由共同边界映射。
    pub fn from_registry(
        registry: Arc<crate::Registry<C, (), E, G>>,
        context: impl Fn(&CallContext) -> Result<C, Status> + Send + Sync + 'static,
        error: impl Fn(G::DispatchError) -> Status + Send + Sync + 'static,
    ) -> Self {
        Self {
            registry,
            context: Box::new(context),
            error: Box::new(error),
        }
    }
}

impl<C, E, G: ?Sized> super::Handler for Handler<C, E, G>
where
    C: Send + 'static,
    E: Send + 'static,
    G: tok_execution::Registry<
            Key = u32,
            Input = (C, Vec<u8>),
            Output = tok_protobuf::Reply<tok_protobuf::Message<u32>>,
            HandlerError = tok_protobuf::Error<E>,
        > + 'static,
    G::DispatchError: Send + 'static,
{
    fn call(&self, request: tok_execution::Request<(CallContext, Payload)>) -> super::CallTask<'_> {
        Box::pin(async move {
            let (context, (call, payload), mut extensions) = request.into_parts();
            if call.timer.is_some() {
                return Err(Status::invalid_argument("普通业务入口不能接受计时触发"));
            }
            if payload.message_id == 0 {
                return Err(Status::invalid_argument("业务请求消息 ID 必须非零"));
            }
            let state = (self.context)(&call)?;
            let id = call.request_id;
            extensions.insert(call);
            drop(extensions.remove::<tok_player::TimerTrigger>());
            let reply = self
                .registry
                .as_ref()
                .clone()
                .oneshot(tok_execution::Request::from_parts(
                    context,
                    (
                        state,
                        tok_protobuf::Message {
                            key: payload.message_id,
                            body: payload.body,
                        },
                    ),
                    extensions,
                ))
                .await
                .map_err(|error| super::handler::dispatch_status(error, &self.error))?;
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
