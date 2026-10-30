//! 计时更新、调用来源与强类型分发的真实 RPC 验证。

#[path = "../../../tests/support_codec/codec.rs"]
mod test_codec;
#[path = "../../../tests/support_codec/session.rs"]
mod test_session;
use prost::Message;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use test_session::RegistryExt as _;
use tok_core::ServiceDescriptor;
use tok_grpc::{Client, Listener};
use tok_session::TimerTrigger;
use tok_session::logic::rpc::CallContext;
use tok_session::logic::rpc::timer::{decode_update, encode_update};
use tok_session::logic::rpc::{
    HandlerRegistry, HandlerReply, Service as RpcService,
    protocol::internal::v1::{self as proto, Payload},
};
use tok_session::{SessionIdentity, TimerSpec, TimerUpdate};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::Code;

#[derive(Clone, PartialEq, Message)]
struct Data {
    #[prost(uint64, tag = "1")]
    value: u64,
}
struct NumberDecoder;
impl tok_core::codec::Decode<u64> for NumberDecoder {
    type Error = std::io::Error;
    fn decode(&self, body: &[u8]) -> Result<u64, Self::Error> {
        std::str::from_utf8(body)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| std::io::Error::other("无效数字"))
    }
}
fn spec() -> TimerSpec {
    TimerSpec {
        timer_id: "income".into(),
        message_id: 3,
        body: Data { value: 7 }.encode_to_vec(),
        due_at_ms: 1000,
        interval_ms: Some(1000),
    }
}

#[test]
fn timer_updates_round_trip_and_missing_operation_is_rejected() {
    for update in [
        TimerUpdate::ReplaceAll(vec![spec()]),
        TimerUpdate::ReplaceAll(vec![]),
        TimerUpdate::Patch {
            upsert: vec![spec()],
            cancel: vec!["old".into()],
        },
    ] {
        assert_eq!(
            decode_update(encode_update(update.clone())).unwrap(),
            update
        );
    }
    assert_eq!(
        decode_update(proto::TimerUpdate { update: None })
            .unwrap_err()
            .code(),
        Code::Internal
    );
}

#[tokio::test]
async fn internal_trigger_is_propagated_and_cannot_be_selected_by_client_message_id() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = HandlerRegistry::new();
    registry
        .register_timer_service_with_codec(
            4,
            NumberDecoder,
            tower::service_fn(
                |request: tok_execution::Request<(CallContext, u64)>| async move {
                    assert_eq!(request.input.1, 42);
                    Ok::<_, tonic::Status>(tok_execution::Response::new(None))
                },
            ),
        )
        .unwrap();
    registry
        .register_with_timers(
            1,
            2,
            |execution_request: tok_execution::Request<(
                tok_session::logic::rpc::CallContext,
                Data,
            )>| async move {
                let (_context, request) = execution_request.input;
                Ok(tok_execution::Response::new(HandlerReply {
                    message: request,
                    timers: Some(TimerUpdate::ReplaceAll(vec![spec()])),
                }))
            },
        )
        .unwrap();
    let seen = Arc::clone(&calls);
    registry
        .register_timer(
            3,
            move |execution_request: tok_execution::Request<(
                tok_session::logic::rpc::CallContext,
                Data,
            )>| {
                let (context, request) = execution_request.input;
                let seen = Arc::clone(&seen);
                async move {
                    let trigger = context.timer.unwrap();
                    assert_eq!(trigger.timer_id, "income");
                    assert_eq!(trigger.scheduled_at_ms, 1000);
                    assert_eq!(trigger.fired_at_ms, 1500);
                    assert_eq!(request.value, 7);
                    seen.fetch_add(1, Ordering::SeqCst);
                    Ok(tok_execution::Response::new(Some(TimerUpdate::Patch {
                        upsert: vec![],
                        cancel: vec!["income".into()],
                    })))
                }
            },
        )
        .unwrap();
    assert!(
        registry
            .register_timer(
                3,
                |execution_request: tok_execution::Request<(
                    tok_session::logic::rpc::CallContext,
                    Data
                )>| async move {
                    let (_ctx, _) = execution_request.input;
                    Ok(tok_execution::Response::new(None))
                }
            )
            .is_err()
    );
    let endpoint = Listener::new()
        .add_service(
            "player",
            RpcService::new(
                "node",
                "player",
                tok_session::logic::rpc::RegistryService::new(Arc::new(registry)),
            )
            .unwrap()
            .into_logic(),
        )
        .unwrap();
    endpoint.readiness().set("player", true);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = ServiceDescriptor {
        node_id: "node".into(),
        service_name: "player".into(),
        protocol: "grpc".into(),
        address: listener.local_addr().unwrap(),
    };
    let cancel = CancellationToken::new();
    let server = tokio::spawn(endpoint.serve(listener, cancel.clone()).unwrap());
    let clients = Arc::new(Client::new(Duration::from_secs(2), 2).unwrap());
    let player_client = tok_session::logic::rpc::Client::new(Arc::clone(&clients));
    let mut context = CallContext {
        node_id: "node".into(),
        service_name: "player".into(),
        bid: Some("42".into()),
        request_id: 1,
        session: Some(SessionIdentity {
            subject_id: "42".into(),
            session_id: "session".into(),
            epoch: 1,
        }),
        timer: None,
    };
    let reply = player_client
        .call(
            &target,
            root(
                (
                    context.clone(),
                    Payload {
                        message_id: 1,
                        body: Data { value: 7 }.encode_to_vec(),
                    },
                ),
                tracing::Span::current(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        Data::decode(reply.output.payload.unwrap().body.as_slice())
            .unwrap()
            .value,
        7
    );
    assert_eq!(
        decode_update(reply.output.timers.unwrap()).unwrap(),
        TimerUpdate::ReplaceAll(vec![spec()])
    );
    let body = Payload {
        message_id: 3,
        body: Data { value: 7 }.encode_to_vec(),
    };
    assert_eq!(
        player_client
            .call(
                &target,
                root((context.clone(), body.clone()), tracing::Span::current())
            )
            .await
            .unwrap_err()
            .code(),
        Code::Unimplemented
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    context.timer = Some(TimerTrigger {
        timer_id: "income".into(),
        scheduled_at_ms: 1000,
        fired_at_ms: 1500,
    });
    let reply = player_client
        .call(
            &target,
            root((context.clone(), body.clone()), tracing::Span::current()),
        )
        .await
        .unwrap();
    assert!(reply.output.payload.is_none());
    assert_eq!(
        decode_update(reply.output.timers.unwrap()).unwrap(),
        TimerUpdate::Patch {
            upsert: vec![],
            cancel: vec!["income".into()]
        }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let custom = Payload {
        message_id: 4,
        body: b"42".to_vec(),
    };
    assert!(
        player_client
            .call(
                &target,
                root((context.clone(), custom), tracing::Span::current())
            )
            .await
            .unwrap()
            .output
            .payload
            .is_none()
    );
    let invalid = Payload {
        message_id: 4,
        body: b"invalid".to_vec(),
    };
    assert_eq!(
        player_client
            .call(
                &target,
                root((context.clone(), invalid), tracing::Span::current())
            )
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    context.timer.as_mut().unwrap().fired_at_ms = 500;
    assert_eq!(
        player_client
            .call(&target, root((context, body), tracing::Span::current()))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    cancel.cancel();
    server.await.unwrap().unwrap();
}

fn root<T>(input: T, span: tracing::Span) -> tok_execution::Request<T> {
    tok_execution::Request::new(
        tok_execution::Context::new(
            tok_core::Identity {
                node_id: "local".into(),
                service_name: "test-caller".into(),
            },
            "test-call",
            std::time::Duration::from_secs(30),
            tokio_util::sync::CancellationToken::new(),
        )
        .unwrap()
        .with_span(span),
        input,
    )
}
use test_session::TimerRegistryExt as _;
