//! 普通 BusinessRpc 客户端适配；不要求会话，不自动加锁。
use super::CallContext;
use super::call;
use std::sync::Arc;
use tok_core::ServiceDescriptor;
use tok_proto::internal::v1::{self as proto, CallResponse, Payload};
use tonic::Status;

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
        context: CallContext,
        payload: Payload,
    ) -> Result<CallResponse, Status> {
        if context.timer.is_some() {
            return Err(Status::invalid_argument("计时触发只能调用 player 入口"));
        }
        call::call(
            &self.connections,
            target,
            context,
            payload,
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
/// 上下文转换器负责提取业务状态，错误转换器仅映射原始业务错误。
/// 此适配器不接受 timer 或服务器事件，也不添加锁、队列或重试。
pub struct Handler<C, E> {
    registry: Arc<crate::Registry<C, (), E>>,
    context: Box<ContextFactory<C>>,
    error: Box<dyn Fn(E) -> Status + Send + Sync>,
}

impl<C, E> Handler<C, E> {
    /// 注入可与本地调用共享的分发表，并显式提供业务上下文和业务错误转换。
    pub fn new(
        registry: Arc<crate::Registry<C, (), E>>,
        context: impl Fn(&CallContext) -> Result<C, Status> + Send + Sync + 'static,
        error: impl Fn(E) -> Status + Send + Sync + 'static,
    ) -> Self {
        Self {
            registry,
            context: Box::new(context),
            error: Box::new(error),
        }
    }
}

impl<C, E> super::Handler for Handler<C, E>
where
    C: Send + 'static,
    E: Send + 'static,
{
    fn call(&self, request: tok_execution::Request<(CallContext, Payload)>) -> super::CallTask<'_> {
        Box::pin(async move {
            let (call, payload) = request.input;
            if call.timer.is_some() {
                return Err(Status::invalid_argument("普通业务入口不能接受计时触发"));
            }
            if payload.message_id == 0 {
                return Err(Status::invalid_argument("业务请求消息 ID 必须非零"));
            }
            let state = (self.context)(&call)?;
            let mut context = request.context;
            let id = call.request_id;
            context.extensions_mut().insert(call);
            drop(
                context
                    .extensions_mut()
                    .remove::<tok_player::TimerTrigger>(),
            );
            let reply = self
                .registry
                .dispatch(tok_execution::Request::new(
                    context,
                    (
                        state,
                        tok_protobuf::Message {
                            key: payload.message_id,
                            body: payload.body,
                        },
                    ),
                ))
                .await
                .map_err(|error| super::handler::dispatch_status(error, &self.error))?;
            if reply.message.key == 0 {
                return Err(Status::failed_precondition("业务回应消息 ID 必须非零"));
            }
            Ok(CallResponse {
                request_id: id,
                payload: Some(Payload {
                    message_id: reply.message.key,
                    body: reply.message.body,
                }),
                error: None,
                timers: None,
            })
        })
    }
}
