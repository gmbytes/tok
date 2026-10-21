//! 协议适配器共用的上下文编码、追踪、期限和指标，不持有连接缓存。
use crate::CallContext;
use crate::RpcClients;
use std::future::Future;
use tok_core::ServiceDescriptor;
use tok_proto::internal::v1::{self as proto, CallRequest, CallResponse, Payload};
use tonic::{Request, Response, Status, transport::Channel};
use tracing::Instrument;

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
pub(crate) async fn execute<Req, Res, F, Fut>(
    clients: &RpcClients,
    target: &ServiceDescriptor,
    request_id: u64,
    mut request: Request<Req>,
    span: tracing::Span,
    send: F,
) -> Result<Res, Status>
where
    F: FnOnce(Channel, Request<Req>) -> Fut,
    Fut: Future<Output = Result<Response<Res>, Status>>,
{
    let channel = clients.channel(target)?;
    request.set_timeout(clients.timeout());
    crate::propagation::inject(request.metadata_mut(), &span);
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(clients.timeout(), send(channel, request).instrument(span))
        .await
        .unwrap_or_else(|_| Err(Status::deadline_exceeded("内部 RPC 超时，未自动重试")));
    metrics::histogram!("tok_rpc_client_duration_seconds", "service_name" => target.service_name.clone()).record(started.elapsed().as_secs_f64());
    metrics::counter!("tok_rpc_client_calls_total", "service_name" => target.service_name.clone(), "result" => if result.is_ok() { "ok" } else { "error" }).increment(1);
    if let Err(status) = &result {
        tracing::error!(node_id = %target.node_id, service_name = %target.service_name, request_id, code = ?status.code(), "内部 RPC 失败，未自动重试");
    }
    Ok(result?.into_inner())
}
pub(crate) async fn call<F, Fut>(
    clients: &RpcClients,
    target: &ServiceDescriptor,
    mut context: CallContext,
    payload: Payload,
    send: F,
) -> Result<CallResponse, Status>
where
    F: FnOnce(Channel, Request<CallRequest>) -> Fut,
    Fut: Future<Output = Result<Response<CallResponse>, Status>>,
{
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
    let span = tracing::info_span!("rpc_client", node_id = %target.node_id, service_name = %target.service_name, request_id);
    let response = execute(clients, target, request_id, request, span, send).await?;
    if response.request_id != request_id {
        return Err(Status::data_loss("RPC 回应序号不匹配"));
    }
    Ok(response)
}
