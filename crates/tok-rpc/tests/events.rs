//! 独立事件 RPC：来源隔离、身份校验、稳定 ID 与 timer 更新。
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::{CallContext, ServiceDescriptor, SessionIdentity, TimerUpdate};
use tok_events::{Event, EventId};
use tok_rpc::{
    HandlerRegistry, RpcClients, RpcEndpoint, RpcService, protocol::internal::v1::Payload,
};
use tokio_util::sync::CancellationToken;
#[derive(Clone, PartialEq, prost::Message)]
struct Update {
    #[prost(int64, tag = "1")]
    value: i64,
}
impl Event for Update {
    const NAME: &'static str = "test.update.v1";
}
#[tokio::test]
async fn event_handlers_are_inaccessible_to_requests_and_timers_and_require_session_identity() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let mut registry = HandlerRegistry::new();
    registry
        .register_event::<Update, _, _>(move |context, id, event| {
            let seen = Arc::clone(&seen);
            async move {
                assert_eq!(context.session.unwrap().player_id, "42");
                assert_eq!(id.as_str(), "operation-1");
                assert_eq!(event.value, 7);
                assert!(context.timer.is_none());
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(Some(TimerUpdate::ReplaceAll(vec![])))
            }
        })
        .unwrap();
    assert!(
        registry
            .register_event::<Update, _, _>(|_, _, _| async { Ok(None) })
            .is_err()
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = RpcEndpoint::new()
        .add_service(
            "player",
            RpcService::new("node", "player", Arc::new(registry))
                .unwrap()
                .into_player(),
        )
        .unwrap();
    endpoint.readiness().set("player", true);
    let stop = CancellationToken::new();
    let server = tokio::spawn(endpoint.serve(listener, stop.clone()).unwrap());
    let clients = Arc::new(RpcClients::new(Duration::from_secs(2), 2).unwrap());
    let player_client = tok_rpc::player::Client::new(Arc::clone(&clients));
    let target = ServiceDescriptor {
        node_id: "node".into(),
        service_name: "player".into(),
        protocol: "grpc".into(),
        address,
    };
    let context = CallContext {
        node_id: "node".into(),
        service_name: "player".into(),
        bid: Some("42".into()),
        request_id: 1,
        session: Some(SessionIdentity {
            player_id: "42".into(),
            session_id: "s".into(),
            epoch: 1,
        }),
        timer: None,
    };
    use prost::Message;
    let event = tok_events::Message {
        event_id: EventId::new("operation-1").unwrap(),
        event_type: Update::NAME.into(),
        body: Update { value: 7 }.encode_to_vec(),
        trace_context: Default::default(),
    };
    assert_eq!(
        player_client
            .call(
                &target,
                context.clone(),
                Payload {
                    message_id: 1,
                    body: event.body.clone()
                }
            )
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unimplemented
    );
    let mut timer = context.clone();
    timer.timer = Some(tok_core::TimerTrigger {
        timer_id: "t".into(),
        scheduled_at_ms: 1,
        fired_at_ms: 1,
    });
    assert_eq!(
        player_client
            .call(
                &target,
                timer,
                Payload {
                    message_id: 1,
                    body: event.body.clone()
                }
            )
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unimplemented
    );
    let mut missing = context.clone();
    missing.session = None;
    assert_eq!(
        player_client
            .handle_event(&target, missing, &event)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let mut invalid = event.clone();
    invalid.body = vec![255];
    assert_eq!(
        player_client
            .handle_event(&target, context.clone(), &invalid)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    let mut unknown = event.clone();
    unknown.event_type = "test.unknown".into();
    assert_eq!(
        player_client
            .handle_event(&target, context.clone(), &unknown)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unimplemented
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let response = player_client
        .handle_event(&target, context, &event)
        .await
        .unwrap();
    assert_eq!(response.request_id, 1);
    assert!(
        matches!(tok_rpc::timer::decode_update(response.timers.unwrap()).unwrap(),TimerUpdate::ReplaceAll(timers) if timers.is_empty())
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    stop.cancel();
    server.await.unwrap().unwrap();
}
