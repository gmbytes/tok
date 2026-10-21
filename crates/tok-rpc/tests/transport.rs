//! RPC 分发、就绪检查、用户中间件与超时不重试的集成测试。

use prost::Message;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::ServiceDescriptor;
use tok_player::SessionIdentity;
use tok_proto::internal::v1::{
    CallRequest, CallResponse, Payload, player_rpc_client::PlayerRpcClient,
};
use tok_rpc::CallContext;
use tok_rpc::{CallHandler, CallTask, HandlerRegistry, RpcClients, RpcEndpoint, RpcService};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::Code;

#[derive(Clone, PartialEq, Message)]
struct Data {
    #[prost(uint64, tag = "1")]
    value: u64,
}

fn target(address: std::net::SocketAddr, service: &str) -> ServiceDescriptor {
    ServiceDescriptor {
        node_id: "local".into(),
        service_name: service.into(),
        protocol: "grpc".into(),
        address,
    }
}
fn context(target: &ServiceDescriptor) -> CallContext {
    CallContext {
        timer: None,
        node_id: target.node_id.clone(),
        service_name: target.service_name.clone(),
        bid: Some("42".into()),
        request_id: 99,
        session: Some(SessionIdentity {
            player_id: "42".into(),
            session_id: "session".into(),
            epoch: 1,
        }),
    }
}
fn payload(value: u64) -> Payload {
    Payload {
        message_id: 1,
        body: Data { value }.encode_to_vec(),
    }
}
fn handler() -> HandlerRegistry {
    let mut registry = HandlerRegistry::new();
    registry
        .register(1, 2, |_: CallContext, data: Data| async move {
            Ok(Data {
                value: data.value + 1,
            })
        })
        .unwrap();
    registry
}

#[tokio::test]
async fn shared_endpoint_dispatches_and_rejects_unready_malformed_and_wrong_targets() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = RpcEndpoint::new()
        .add_service(
            "player",
            RpcService::new("local", "player", Arc::new(handler()))
                .unwrap()
                .into_player(),
        )
        .unwrap()
        .add_service(
            "guild",
            RpcService::new("local", "guild", Arc::new(handler()))
                .unwrap()
                .into_business(),
        )
        .unwrap();
    let ready = endpoint.readiness();
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(endpoint.serve(listener, shutdown.clone()).unwrap());
    let clients = Arc::new(RpcClients::new(Duration::from_secs(2), 1).unwrap());
    let player_client = tok_rpc::player::Client::new(Arc::clone(&clients));
    let business_client = tok_rpc::business::Client::new(Arc::clone(&clients));
    let player = target(address, "player");
    let guild = target(address, "guild");
    assert_eq!(
        player_client
            .call(&player, context(&player), payload(1))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    ready.set("player", true);
    ready.set("guild", true);
    for (entry, is_player) in [(&player, true), (&guild, false)] {
        let response = if is_player {
            player_client.call(entry, context(entry), payload(41)).await
        } else {
            business_client
                .call(entry, context(entry), payload(41))
                .await
        }
        .unwrap();
        assert_eq!(response.request_id, 99);
        let body = response.payload.unwrap();
        assert_eq!(body.message_id, 2);
        assert_eq!(Data::decode(body.body.as_slice()).unwrap().value, 42);
    }
    let mut missing_identity = context(&player);
    missing_identity.session = None;
    assert_eq!(
        player_client
            .call(&player, missing_identity, payload(1))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let mut unknown = payload(1);
    unknown.message_id = 123;
    assert_eq!(
        business_client
            .call(&guild, context(&guild), unknown)
            .await
            .unwrap_err()
            .code(),
        Code::Unimplemented
    );
    let bad_payload = Payload {
        message_id: 1,
        body: vec![0xff],
    };
    assert_eq!(
        business_client
            .call(&guild, context(&guild), bad_payload)
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let mut raw = PlayerRpcClient::new(clients.channel(&player).unwrap());
    let mismatch = CallRequest {
        timer: None,
        context: Some(tok_proto::internal::v1::CallContext {
            node_id: "wrong-node".into(),
            service_name: "player".into(),
            bid: Some("42".into()),
            request_id: 1,
            session: None,
        }),
        payload: Some(payload(1)),
    };
    assert_eq!(
        raw.call(mismatch).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    ready.clear();
    assert_eq!(
        business_client
            .call(&guild, context(&guild), payload(1))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // 同一地址可以重新绑定，监听没有脱离任务监督。
    TcpListener::bind(address).await.unwrap();
}

struct Slow {
    calls: Arc<AtomicUsize>,
}
impl CallHandler for Slow {
    fn call(&self, request: tok_service::Request<(CallContext, Payload)>) -> CallTask<'_> {
        let context = request.input.0;
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(CallResponse {
                request_id: context.request_id,
                payload: None,
                error: None,
                timers: None,
            })
        })
    }
}

#[tokio::test]
async fn timeout_does_not_retry_business_execution() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let endpoint = RpcEndpoint::new()
        .add_service(
            "guild",
            RpcService::new(
                "local",
                "guild",
                Arc::new(Slow {
                    calls: Arc::clone(&calls),
                }),
            )
            .unwrap()
            .into_business(),
        )
        .unwrap();
    endpoint.readiness().set("guild", true);
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(endpoint.serve(listener, shutdown.clone()).unwrap());
    let target = target(address, "guild");
    let clients = Arc::new(RpcClients::new(Duration::from_millis(150), 1).unwrap());
    let business_client = tok_rpc::business::Client::new(Arc::clone(&clients));
    let error = business_client
        .call(&target, context(&target), payload(1))
        .await
        .unwrap_err();
    assert!(matches!(
        error.code(),
        Code::DeadlineExceeded | Code::Cancelled
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    shutdown.cancel();
    server.abort();
    #[expect(
        clippy::let_underscore_must_use,
        reason = "测试清理已取消或中止的服务任务，不限定两者的完成先后。"
    )]
    let _ = server.await;
}

#[test]
fn duplicate_message_and_proto_service_bindings_are_rejected() {
    let mut handlers = handler();
    assert!(
        handlers
            .register(1, 3, |_: CallContext, data: Data| async move { Ok(data) })
            .is_err()
    );
    assert!(
        handlers
            .register(0, 3, |_: CallContext, data: Data| async move { Ok(data) })
            .is_err()
    );
    let service = RpcService::new("local", "guild", Arc::new(handler())).unwrap();
    assert!(
        RpcEndpoint::new()
            .add_service("guild", service.clone().into_business())
            .unwrap()
            .add_service("mail", service.into_business())
            .is_err()
    );
}

#[tokio::test]
async fn user_tower_concurrency_limit_is_preserved_across_handler_clones() {
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let tracked_peak = Arc::clone(&peak);
    let service = tok_rpc::tower::ServiceBuilder::new()
        .concurrency_limit(1)
        .service(tok_rpc::tower::service_fn(
            move |request: tok_service::Request<(CallContext, Payload)>| {
                let (context, payload) = request.input;
                let active = Arc::clone(&active);
                let peak = Arc::clone(&tracked_peak);
                async move {
                    let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(count, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok::<_, tonic::Status>(CallResponse {
                        request_id: context.request_id,
                        payload: Some(payload),
                        error: None,
                        timers: None,
                    })
                }
            },
        ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = RpcEndpoint::new()
        .add_service(
            "guild",
            RpcService::new("local", "guild", Arc::new(tok_rpc::TowerHandler(service)))
                .unwrap()
                .into_business(),
        )
        .unwrap();
    endpoint.readiness().set("guild", true);
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(endpoint.serve(listener, shutdown.clone()).unwrap());
    let clients = Arc::new(RpcClients::new(Duration::from_secs(2), 1).unwrap());
    let business_client = tok_rpc::business::Client::new(Arc::clone(&clients));
    let target = target(address, "guild");
    let (first, second) = tokio::join!(
        business_client.call(&target, context(&target), payload(1)),
        business_client.call(&target, context(&target), payload(2))
    );
    assert_eq!(first.unwrap().payload.unwrap().body, payload(1).body);
    assert_eq!(second.unwrap().payload.unwrap().body, payload(2).body);
    assert_eq!(peak.load(Ordering::SeqCst), 1);
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
