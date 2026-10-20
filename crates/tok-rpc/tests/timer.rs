//! 计时更新、调用来源与强类型分发的真实 RPC 验证。

use prost::Message;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::{
    CallContext, ServiceDescriptor, SessionIdentity, TimerSpec, TimerTrigger, TimerUpdate,
};
use tok_rpc::{
    HandlerRegistry, HandlerReply, RpcClients, RpcEndpoint, RpcService,
    protocol::internal::v1::{self as proto, Payload},
    timer::{decode_update, encode_update},
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::Code;

#[derive(Clone, PartialEq, Message)]
struct Data {
    #[prost(uint64, tag = "1")]
    value: u64,
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
        .register_with_timers(1, 2, |_context, request: Data| async move {
            Ok(HandlerReply {
                message: request,
                timers: Some(TimerUpdate::ReplaceAll(vec![spec()])),
            })
        })
        .unwrap();
    let seen = Arc::clone(&calls);
    registry
        .register_timer(3, move |context, request: Data| {
            let seen = Arc::clone(&seen);
            async move {
                let trigger = context.timer.unwrap();
                assert_eq!(trigger.timer_id, "income");
                assert_eq!(trigger.scheduled_at_ms, 1000);
                assert_eq!(trigger.fired_at_ms, 1500);
                assert_eq!(request.value, 7);
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(Some(TimerUpdate::Patch {
                    upsert: vec![],
                    cancel: vec!["income".into()],
                }))
            }
        })
        .unwrap();
    assert!(
        registry
            .register_timer(3, |_ctx, _: Data| async { Ok(None) })
            .is_err()
    );
    let endpoint = RpcEndpoint::new()
        .add_service(
            "player",
            RpcService::new("node", "player", Arc::new(registry))
                .unwrap()
                .into_player(),
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
    let clients = RpcClients::new(Duration::from_secs(2), 2).unwrap();
    let mut context = CallContext {
        node_id: "node".into(),
        service_name: "player".into(),
        bid: Some("42".into()),
        request_id: 1,
        session: Some(SessionIdentity {
            player_id: "42".into(),
            session_id: "session".into(),
            epoch: 1,
        }),
        timer: None,
    };
    let reply = clients
        .player_call(
            &target,
            context.clone(),
            Payload {
                message_id: 1,
                body: Data { value: 7 }.encode_to_vec(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        Data::decode(reply.payload.unwrap().body.as_slice())
            .unwrap()
            .value,
        7
    );
    assert_eq!(
        decode_update(reply.timers.unwrap()).unwrap(),
        TimerUpdate::ReplaceAll(vec![spec()])
    );
    let body = Payload {
        message_id: 3,
        body: Data { value: 7 }.encode_to_vec(),
    };
    assert_eq!(
        clients
            .player_call(&target, context.clone(), body.clone())
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
    let reply = clients
        .player_call(&target, context.clone(), body.clone())
        .await
        .unwrap();
    assert!(reply.payload.is_none());
    assert_eq!(
        decode_update(reply.timers.unwrap()).unwrap(),
        TimerUpdate::Patch {
            upsert: vec![],
            cancel: vec!["income".into()]
        }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        clients
            .business_call(&target, context.clone(), body.clone())
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    context.timer.as_mut().unwrap().fired_at_ms = 500;
    assert_eq!(
        clients
            .player_call(&target, context, body)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    cancel.cancel();
    server.await.unwrap().unwrap();
}
