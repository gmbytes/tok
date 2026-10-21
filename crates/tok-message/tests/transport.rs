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
use tok_grpc::{Client, Listener};
use tok_message::rpc::CallContext;
use tok_message::rpc::{CallTask, Handler, HandlerRegistry, Service as RpcService};
use tok_player::SessionIdentity;
use tok_proto::internal::v1::{
    CallRequest, CallResponse, Payload, player_rpc_client::PlayerRpcClient,
};
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
        .register(1, 2, |execution_request: tok_execution::Request<(tok_message::rpc::CallContext, Data)>| async move { let (_, data) = execution_request.input;
            Ok(tok_execution::Response::new(Data {
                value: data.value + 1,
            }))
        })
        .unwrap();
    registry
}

#[tokio::test]
async fn shared_endpoint_dispatches_and_rejects_unready_malformed_and_wrong_targets() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = Listener::new()
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
    let clients = Arc::new(Client::new(Duration::from_secs(2), 1).unwrap());
    let player_client = tok_message::rpc::player::Client::new(Arc::clone(&clients));
    let business_client = tok_message::rpc::business::Client::new(Arc::clone(&clients));
    let player = target(address, "player");
    let guild = target(address, "guild");
    assert_eq!(
        player_client
            .call(
                &player,
                root((context(&player), payload(1)), tracing::Span::current())
            )
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    assert!(!ready.set("unknown", true));
    assert!(ready.set("player", true));
    assert!(ready.set("guild", true));
    for (entry, is_player) in [(&player, true), (&guild, false)] {
        let response = if is_player {
            player_client
                .call(
                    entry,
                    root((context(entry), payload(41)), tracing::Span::current()),
                )
                .await
        } else {
            business_client
                .call(
                    entry,
                    root((context(entry), payload(41)), tracing::Span::current()),
                )
                .await
        }
        .unwrap();
        assert_eq!(response.output.request_id, 99);
        let body = response.output.payload.unwrap();
        assert_eq!(body.message_id, 2);
        assert_eq!(Data::decode(body.body.as_slice()).unwrap().value, 42);
    }
    let mut missing_identity = context(&player);
    missing_identity.session = None;
    assert_eq!(
        player_client
            .call(
                &player,
                root((missing_identity, payload(1)), tracing::Span::current())
            )
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let mut unknown = payload(1);
    unknown.message_id = 123;
    assert_eq!(
        business_client
            .call(
                &guild,
                root((context(&guild), unknown), tracing::Span::current())
            )
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
            .call(
                &guild,
                root((context(&guild), bad_payload), tracing::Span::current())
            )
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
            .call(
                &guild,
                root((context(&guild), payload(1)), tracing::Span::current())
            )
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
impl Handler for Slow {
    fn call(&self, request: tok_execution::Request<(CallContext, Payload)>) -> CallTask<'_> {
        let context = request.input.0;
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(tok_execution::Response::new(CallResponse {
                request_id: context.request_id,
                payload: None,
                error: None,
                timers: None,
            }))
        })
    }
}

#[tokio::test]
async fn timeout_does_not_retry_business_execution() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let endpoint = Listener::new()
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
    let clients = Arc::new(Client::new(Duration::from_millis(150), 1).unwrap());
    let business_client = tok_message::rpc::business::Client::new(Arc::clone(&clients));
    let error = business_client
        .call(
            &target,
            root((context(&target), payload(1)), tracing::Span::current()),
        )
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
            .register(
                1,
                3,
                |execution_request: tok_execution::Request<(
                    tok_message::rpc::CallContext,
                    Data
                )>| async move {
                    let (_, data) = execution_request.input;
                    Ok(tok_execution::Response::new(data))
                }
            )
            .is_err()
    );
    assert!(
        handlers
            .register(
                0,
                3,
                |execution_request: tok_execution::Request<(
                    tok_message::rpc::CallContext,
                    Data
                )>| async move {
                    let (_, data) = execution_request.input;
                    Ok(tok_execution::Response::new(data))
                }
            )
            .is_err()
    );
    let service = RpcService::new("local", "guild", Arc::new(handler())).unwrap();
    assert!(
        Listener::new()
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
    let service = tok_message::rpc::tower::ServiceBuilder::new()
        .concurrency_limit(1)
        .service(tok_message::rpc::tower::service_fn(
            move |request: tok_execution::Request<(CallContext, Payload)>| {
                let (context, payload) = request.input;
                let active = Arc::clone(&active);
                let peak = Arc::clone(&tracked_peak);
                async move {
                    let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(count, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok::<_, tonic::Status>(tok_execution::Response::new(CallResponse {
                        request_id: context.request_id,
                        payload: Some(payload),
                        error: None,
                        timers: None,
                    }))
                }
            },
        ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = Listener::new()
        .add_service(
            "guild",
            RpcService::new(
                "local",
                "guild",
                Arc::new(tok_message::rpc::TowerHandler(service)),
            )
            .unwrap()
            .into_business(),
        )
        .unwrap();
    endpoint.readiness().set("guild", true);
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(endpoint.serve(listener, shutdown.clone()).unwrap());
    let clients = Arc::new(Client::new(Duration::from_secs(2), 1).unwrap());
    let business_client = tok_message::rpc::business::Client::new(Arc::clone(&clients));
    let target = target(address, "guild");
    let (first, second) = tokio::join!(
        business_client.call(
            &target,
            root((context(&target), payload(1)), tracing::Span::current())
        ),
        business_client.call(
            &target,
            root((context(&target), payload(2)), tracing::Span::current())
        )
    );
    assert_eq!(first.unwrap().output.payload.unwrap().body, payload(1).body);
    assert_eq!(
        second.unwrap().output.payload.unwrap().body,
        payload(2).body
    );
    assert_eq!(peak.load(Ordering::SeqCst), 1);
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[test]
fn endpoint_binding_errors_identify_the_invalid_configuration() {
    let service = RpcService::new("local", "guild", Arc::new(handler())).unwrap();
    let error = Listener::new()
        .add_service(" ", service.clone().into_business())
        .err()
        .unwrap();
    assert!(error.to_string().contains("业务服务名称不能为空"));
    let error = Listener::new()
        .add_service("guild", service.clone().into_business())
        .unwrap()
        .add_service("guild", service.clone().into_player())
        .err()
        .unwrap();
    assert!(error.to_string().contains("业务服务重复绑定：guild"));
    let error = Listener::new()
        .add_service("guild", service.clone().into_business())
        .unwrap()
        .add_service("mail", service.into_business())
        .err()
        .unwrap();
    assert!(error.to_string().contains("protobuf 服务重复绑定"));
}

#[tokio::test]
async fn custom_client_uri_and_server_transport_timeout_are_applied() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let server = tonic::transport::Server::builder().timeout(Duration::from_millis(30));
    let endpoint = Listener::from_server(server)
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
    assert!(endpoint.readiness().set("guild", true));
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(endpoint.serve(listener, shutdown.clone()).unwrap());
    let client = Client::new(Duration::from_secs(3), 1)
        .unwrap()
        .with_endpoint_factory(move |_| {
            // 发现地址在本测试中不可达，工厂将其映射到实际监听，以验证 URI 没有被覆盖。
            Ok(tonic::transport::Endpoint::from_shared(format!(
                "http://{address}"
            ))?)
        });
    let client = tok_message::rpc::business::Client::new(Arc::new(client));
    let target = target("127.0.0.1:1".parse().unwrap(), "guild");
    // 服务端传输期限应先于客户端三秒预算生效。
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        client.call(
            &target,
            root((context(&target), payload(1)), tracing::Span::current()),
        ),
    )
    .await
    .expect("configured server timeout was not applied")
    .unwrap_err();
    assert!(matches!(
        error.code(),
        Code::Cancelled | Code::DeadlineExceeded
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn listener_tower_layers_cover_shared_routes_and_preserve_readiness() {
    use tonic::codegen::http;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let layers = tower::ServiceBuilder::new()
        .map_request(move |request: http::Request<tonic::body::Body>| {
            seen.fetch_add(1, Ordering::SeqCst);
            request
        })
        .map_response(|mut response: http::Response<tonic::body::Body>| {
            response
                .headers_mut()
                .insert("x-listener-layer", "applied".parse().unwrap());
            response
        });
    let endpoint = Listener::from_server(tonic::transport::Server::builder().layer(layers))
        .add_service(
            "guild",
            RpcService::new("local", "guild", Arc::new(handler()))
                .unwrap()
                .into_business(),
        )
        .unwrap()
        .add_service(
            "player",
            RpcService::new("local", "player", Arc::new(handler()))
                .unwrap()
                .into_player(),
        )
        .unwrap();
    let ready = endpoint.readiness();
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(endpoint.serve(listener, shutdown.clone()).unwrap());
    let client = Client::new(Duration::from_secs(2), 1).unwrap();
    let guild = target(address, "guild");
    let request = || {
        tonic::Request::new(CallRequest {
            context: Some(tok_proto::internal::v1::CallContext {
                node_id: "local".into(),
                service_name: "guild".into(),
                bid: Some("7".into()),
                request_id: 99,
                session: None,
            }),
            timer: None,
            payload: Some(payload(41)),
        })
    };
    let sender = || {
        tower::service_fn(
            |call: tok_execution::Request<tok_grpc::Outgoing<CallRequest>>| async move {
                let (channel, request) = call.input.into_parts(&call.context)?;
                let response =
                    tok_proto::internal::v1::business_rpc_client::BusinessRpcClient::new(channel)
                        .call(request)
                        .await?;
                assert_eq!(
                    response.metadata().get("x-listener-layer").unwrap(),
                    "applied"
                );
                Ok(tok_execution::Response::new(response))
            },
        )
    };
    assert_eq!(
        client
            .execute_service(&guild, 99, root(request(), tracing::Span::none()), sender())
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    assert!(ready.set("guild", true));
    assert!(ready.set("player", true));
    let response = client
        .execute_service(&guild, 99, root(request(), tracing::Span::none()), sender())
        .await
        .unwrap();
    assert_eq!(
        Data::decode(response.output.payload.unwrap().body.as_slice())
            .unwrap()
            .value,
        42
    );
    let player = target(address, "player");
    let player_client = tok_message::rpc::player::Client::new(Arc::new(client));
    assert_eq!(
        player_client
            .call(
                &player,
                root((context(&player), payload(41)), tracing::Span::current())
            )
            .await
            .unwrap()
            .output
            .request_id,
        99
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    ready.clear();
    assert_eq!(
        player_client
            .call(
                &player,
                root((context(&player), payload(41)), tracing::Span::current())
            )
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    shutdown.cancel();
    task.await.unwrap().unwrap();
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
