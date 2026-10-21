//! 会话驱动的 PlayerRpc 客户端适配；与通用连接管理分离。
use super::CallContext;
use super::call;
use std::sync::Arc;
use tok_core::ServiceDescriptor;
use tok_proto::internal::v1::{self as proto, CallResponse, Payload};
use tonic::{Request, Status};

/// 玩家协议客户端；多个适配器可以共享同一个 tok_grpc::Client 连接缓存。
#[derive(Clone)]
pub struct Client {
    connections: Arc<tok_grpc::Client>,
}
impl Client {
    /// 注入共享连接管理器，不建立连接。
    pub fn new(connections: Arc<tok_grpc::Client>) -> Self {
        Self { connections }
    }
    /// 调用玩家请求或内部 timer 入口；会话字段校验不代替所有权与调用者认证。
    pub async fn call(
        &self,
        target: &ServiceDescriptor,
        request: tok_execution::Request<(CallContext, Payload)>,
    ) -> Result<CallResponse, Status> {
        call::call(
            &self.connections,
            target,
            request,
            |channel, request| async move {
                proto::player_rpc_client::PlayerRpcClient::new(channel)
                    .call(request)
                    .await
            },
        )
        .await
    }
    /// 等待玩家内部事件执行；不重试，不进入客户端请求或 timer 分发表。
    pub async fn handle_event(
        &self,
        target: &ServiceDescriptor,
        request: tok_execution::Request<(CallContext, std::sync::Arc<tok_events::Message>)>,
    ) -> Result<proto::EventResponse, Status> {
        let tok_execution::Request {
            context: execution,
            input: (context, event),
        } = request;
        call::validate_target(target, &context)?;
        if context.timer.is_some() || context.request_id == 0 {
            return Err(Status::invalid_argument("内部事件上下文无效"));
        }
        event
            .validate(1_048_576)
            .map_err(|_| Status::invalid_argument("事件参数无效"))?;
        let request_id = context.request_id;
        let request = Request::new(proto::EventRequest {
            topic: event.topic.as_str().map(str::to_owned),
            context: Some(call::encode_context(context)),
            event_id: event.event_id.to_string(),
            event_type: event.event_type.clone(),
            body: event.body.clone(),
        });
        let span = tracing::info_span!(parent: execution.span(), "rpc_event_client", node_id = %target.node_id, service_name = %target.service_name, event_id = %event.event_id);
        let response = self
            .connections
            .execute(
                target,
                request_id,
                tok_execution::Request::new(execution.with_span(span), request),
                |channel, request| async move {
                    proto::player_rpc_client::PlayerRpcClient::new(channel)
                        .handle_event(request)
                        .await
                },
            )
            .await?;
        if response.request_id != request_id {
            return Err(Status::data_loss("内部事件回应无效"));
        }
        Ok(response)
    }
}
