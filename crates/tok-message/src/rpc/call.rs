//! 游戏协议的目标校验与正文编码；追踪、期限和指标委托通用 Client。
use super::CallContext;
use std::future::Future;
use tok_core::ServiceDescriptor;
use tok_grpc::Client;
use tok_proto::internal::v1::{self as proto, CallRequest, CallResponse, Payload};
use tonic::{Request, Response, Status, transport::Channel};

pub(crate) fn validate_target(
    target: &ServiceDescriptor,
    context: &CallContext,
) -> Result<(), Status> {
    if context.node_id != target.node_id || context.service_name != target.service_name {
        return Err(Status::invalid_argument("调用上下文与目标服务不匹配"));
    }
    Ok(())
}
pub(crate) fn encode_context(context: CallContext) -> proto::CallContext {
    proto::CallContext {
        node_id: context.node_id,
        service_name: context.service_name,
        bid: context.bid,
        request_id: context.request_id,
        session: context.session.map(|identity| proto::SessionIdentity {
            player_id: identity.player_id,
            session_id: identity.session_id,
            epoch: identity.epoch,
        }),
    }
}
pub(crate) async fn call<F, Fut>(
    client: &Client,
    target: &ServiceDescriptor,
    request: tok_execution::Request<(CallContext, Payload)>,
    send: F,
) -> Result<tok_execution::Response<CallResponse>, Status>
where
    F: FnOnce(Channel, Request<CallRequest>) -> Fut,
    Fut: Future<Output = Result<Response<CallResponse>, Status>>,
{
    let (execution, (mut context, payload), extensions) = request.into_parts();
    validate_target(target, &context)?;
    let request_id = context.request_id;
    let timer = context.timer.take().map(|timer| proto::TimerTrigger {
        timer_id: timer.timer_id,
        scheduled_at_ms: timer.scheduled_at_ms,
        fired_at_ms: timer.fired_at_ms,
    });
    let request = Request::new(CallRequest {
        timer,
        context: Some(encode_context(context)),
        payload: Some(payload),
    });
    let span = tracing::info_span!(parent: execution.span(), "rpc_client", node_id = %target.node_id, service_name = %target.service_name, request_id);
    let response = client
        .execute(
            target,
            tok_execution::Request::from_parts(execution.with_span(span), request, extensions),
            send,
        )
        .await?;
    if response.output.request_id != request_id {
        return Err(Status::data_loss("RPC 回应序号不匹配"));
    }
    Ok(response)
}
