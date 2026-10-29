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
use tok_proto::internal::v1::{
    CallRequest, CallResponse, Payload, logic_rpc_client::LogicRpcClient,
};
use tok_session::SessionIdentity;
use tok_session::logic::rpc::CallContext;
use tok_session::logic::rpc::{HandlerRegistry, Input as LogicInput, Output as LogicOutput};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::Code;
use tower::ServiceExt;

// 本测试覆盖同一监听端点的不同协议，测试处理器仅用于观察共享传输行为。
#[derive(Clone)]
struct BusinessBridge(
    tower::util::BoxCloneSyncService<
        tok_execution::Request<LogicInput>,
        tok_execution::Response<LogicOutput>,
        tonic::Status,
    >,
);
impl tower::Service<tok_execution::Request<(tok_message::rpc::CallContext, Payload)>>
    for BusinessBridge
{
    type Response = tok_execution::Response<CallResponse>;
    type Error = tonic::Status;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(
        &mut self,
        request: tok_execution::Request<(tok_message::rpc::CallContext, Payload)>,
    ) -> Self::Future {
        let handler = self.0.clone();
        let request = request.map(|(context, payload)| {
            LogicInput::Message(
                CallContext {
                    node_id: context.node_id,
                    service_name: context.service_name,
                    bid: context.bid,
                    request_id: context.request_id,
                    session: None,
                    timer: None,
                },
                payload,
            )
        });
        Box::pin(async move {
            let response = handler.oneshot(request).await?;
            response.try_map(|output| match output {
                LogicOutput::Message(output) => Ok(output),
                _ => Err(tonic::Status::internal("结果类型不匹配")),
            })
        })
    }
}
#[derive(Clone)]
struct RpcService {
    player: tok_session::logic::rpc::Service,
    business: tok_message::rpc::Service,
}
impl RpcService {
    fn new<S>(node: &str, service: &str, handler: S) -> Result<Self, tok_grpc::Error>
    where
        S: tower::Service<
                tok_execution::Request<LogicInput>,
                Response = tok_execution::Response<LogicOutput>,
                Error = tonic::Status,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        let handler = tower::util::BoxCloneSyncService::new(handler);
        Ok(Self {
            player: tok_session::logic::rpc::Service::new(node, service, handler.clone())?,
            business: tok_message::rpc::Service::new(node, service, BusinessBridge(handler))?,
        })
    }
    fn into_logic(
        self,
    ) -> tok_proto::internal::v1::logic_rpc_server::LogicRpcServer<tok_session::logic::rpc::Service>
    {
        self.player.into_logic()
    }
    fn into_business(
        self,
    ) -> tok_proto::internal::v1::business_rpc_server::BusinessRpcServer<tok_message::rpc::Service>
    {
        self.business.into_business()
    }
}
struct BusinessClient(tok_message::rpc::Client);
impl BusinessClient {
    fn new(client: Arc<Client>) -> Self {
        Self(tok_message::rpc::Client::new(client))
    }
    async fn call(
        &self,
        target: &ServiceDescriptor,
        request: tok_execution::Request<(CallContext, Payload)>,
    ) -> Result<tok_execution::Response<CallResponse>, tonic::Status> {
        if request.input.0.timer.is_some() {
            return Err(tonic::Status::invalid_argument(
                "普通业务入口不能接受计时触发",
            ));
        }
        self.0
            .call(
                target,
                request.map(|(context, payload)| {
                    (
                        tok_message::rpc::CallContext {
                            node_id: context.node_id,
                            service_name: context.service_name,
                            bid: context.bid,
                            request_id: context.request_id,
                        },
                        payload,
                    )
                }),
            )
            .await
    }
}

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
            subject_id: "42".into(),
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
        .register(
            1,
            2,
            |execution_request: tok_execution::Request<(
                tok_session::logic::rpc::CallContext,
                Data,
            )>| async move {
                let (_, data) = execution_request.input;
                Ok(tok_execution::Response::new(Data {
                    value: data.value + 1,
                }))
            },
        )
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
            RpcService::new("local", "player", handler().into_service())
                .unwrap()
                .into_logic(),
        )
        .unwrap()
        .add_service(
            "guild",
            RpcService::new("local", "guild", handler().into_service())
                .unwrap()
                .into_business(),
        )
        .unwrap();
    let ready = endpoint.readiness();
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(endpoint.serve(listener, shutdown.clone()).unwrap());
    let clients = Arc::new(Client::new(Duration::from_secs(2), 1).unwrap());
    let player_client = tok_session::logic::rpc::Client::new(Arc::clone(&clients));
    let business_client = BusinessClient::new(Arc::clone(&clients));
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
    let mut raw = LogicRpcClient::new(clients.channel(&player).unwrap());
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

#[derive(Clone)]
struct Slow {
    calls: Arc<AtomicUsize>,
}
impl tower::Service<tok_execution::Request<LogicInput>> for Slow {
    type Response = tok_execution::Response<LogicOutput>;
    type Error = tonic::Status;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: tok_execution::Request<LogicInput>) -> Self::Future {
        let context = match request.input {
            LogicInput::Message(context, _)
            | LogicInput::Timer(context, _)
            | LogicInput::Event(context, _) => context,
        };
        let calls = Arc::clone(&self.calls);
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(tok_execution::Response::new(LogicOutput::Message(
                CallResponse {
                    request_id: context.request_id,
                    payload: None,
                    error: None,
                    timers: None,
                },
            )))
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
                Slow {
                    calls: Arc::clone(&calls),
                },
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
    let business_client = BusinessClient::new(Arc::clone(&clients));
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
                    tok_session::logic::rpc::CallContext,
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
                    tok_session::logic::rpc::CallContext,
                    Data
                )>| async move {
                    let (_, data) = execution_request.input;
                    Ok(tok_execution::Response::new(data))
                }
            )
            .is_err()
    );
    let service = RpcService::new("local", "guild", handler().into_service()).unwrap();
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
    let service = tok_session::logic::rpc::tower::ServiceBuilder::new()
        .concurrency_limit(1)
        .service(tok_session::logic::rpc::tower::service_fn(
            move |request: tok_execution::Request<LogicInput>| {
                let (context, payload) = match request.input {
                    LogicInput::Message(context, payload) => (context, payload),
                    _ => panic!("此测试只发送消息"),
                };
                let active = Arc::clone(&active);
                let peak = Arc::clone(&tracked_peak);
                async move {
                    let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(count, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok::<_, tonic::Status>(tok_execution::Response::new(LogicOutput::Message(
                        CallResponse {
                            request_id: context.request_id,
                            payload: Some(payload),
                            error: None,
                            timers: None,
                        },
                    )))
                }
            },
        ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = Listener::new()
        .add_service(
            "guild",
            RpcService::new("local", "guild", service)
                .unwrap()
                .into_business(),
        )
        .unwrap();
    endpoint.readiness().set("guild", true);
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(endpoint.serve(listener, shutdown.clone()).unwrap());
    let clients = Arc::new(Client::new(Duration::from_secs(2), 1).unwrap());
    let business_client = BusinessClient::new(Arc::clone(&clients));
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
    let service = RpcService::new("local", "guild", handler().into_service()).unwrap();
    let error = Listener::new()
        .add_service(" ", service.clone().into_business())
        .err()
        .unwrap();
    assert!(error.to_string().contains("业务服务名称不能为空"));
    let error = Listener::new()
        .add_service("guild", service.clone().into_business())
        .unwrap()
        .add_service("guild", service.clone().into_logic())
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
                Slow {
                    calls: Arc::clone(&calls),
                },
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
    let client = BusinessClient::new(Arc::new(client));
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
            RpcService::new("local", "guild", handler().into_service())
                .unwrap()
                .into_business(),
        )
        .unwrap()
        .add_service(
            "player",
            RpcService::new("local", "player", handler().into_service())
                .unwrap()
                .into_logic(),
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
            |call: tok_execution::Request<tok_grpc::PreparedCall<CallRequest>>| async move {
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
            .execute_service(&guild, root(request(), tracing::Span::none()), sender())
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );
    assert!(ready.set("guild", true));
    assert!(ready.set("player", true));
    let response = client
        .execute_service(&guild, root(request(), tracing::Span::none()), sender())
        .await
        .unwrap();
    assert_eq!(
        Data::decode(response.output.payload.unwrap().body.as_slice())
            .unwrap()
            .value,
        42
    );
    let player = target(address, "player");
    let player_client = tok_session::logic::rpc::Client::new(Arc::new(client));
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
