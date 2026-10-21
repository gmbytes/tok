//! 同一业务分发表由本地和真实 RPC 调用，业务错误仅在线协议边界转换。
use prost::Message as _;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::{Extensions, Identity, ServiceDescriptor};
use tok_execution::registry::DispatchError;
use tok_execution::{Context, Request};
use tok_grpc::{Client, Listener};
use tok_message::rpc::{CallContext, Handler as _, Service as RpcService, business};
use tok_message::{Message, Registry, Reply};
use tok_proto::internal::v1::Payload;
use tok_protobuf as protobuf;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Status};

#[derive(Clone, PartialEq, prost::Message)]
struct Value {
    #[prost(uint32, tag = "1")]
    value: u32,
}
#[derive(Debug, PartialEq, Eq)]
struct Denied;

fn context() -> Context {
    Context::new(
        Identity {
            node_id: "node".into(),
            service_name: "custom".into(),
        },
        "local",
        Duration::from_secs(1),
        CancellationToken::new(),
    )
    .unwrap()
}
fn call() -> CallContext {
    CallContext {
        node_id: "node".into(),
        service_name: "custom".into(),
        bid: Some("5".into()),
        request_id: 7,
        session: None,
        timer: None,
    }
}

#[tokio::test]
async fn local_and_rpc_share_one_chain_and_map_errors_only_at_rpc_boundary() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let mut registry = Registry::<u32, (), Denied>::new();
    registry
        .register_service(
            1,
            2,
            tower::service_fn(move |request: Request<(u32, Value)>| {
                let observed = Arc::clone(&observed);
                async move {
                    assert_eq!(request.extensions().get::<u64>(), Some(&42));
                    observed.fetch_add(1, Ordering::SeqCst);
                    if request.input.1.value == 0 {
                        return Err(Denied);
                    }
                    Ok(tok_execution::Response::new(Reply {
                        message: Value {
                            value: request.input.0 + request.input.1.value,
                        },
                        output: (),
                    }))
                }
            }),
        )
        .unwrap();
    let registry = Arc::new(registry);
    let body = Value { value: 3 }.encode_to_vec();
    let local = registry
        .dispatch(request((
            5,
            Message {
                key: 1,
                body: body.clone(),
            },
        )))
        .await
        .unwrap();
    let denied = registry
        .dispatch(request((
            5,
            Message {
                key: 1,
                body: vec![],
            },
        )))
        .await
        .unwrap_err();
    assert!(matches!(
        denied,
        DispatchError::Handler(protobuf::Error::Handler(Denied))
    ));
    let adapter = Arc::new(business::Handler::new(
        Arc::clone(&registry),
        |call| {
            call.bid
                .as_deref()
                .and_then(|value| value.parse::<u32>().ok())
                .ok_or_else(|| Status::invalid_argument("目标 ID 无效"))
        },
        |_: Denied| Status::permission_denied("业务拒绝"),
    ));
    let mut timer = call();
    timer.timer = Some(tok_player::TimerTrigger {
        timer_id: "internal".into(),
        scheduled_at_ms: 1,
        fired_at_ms: 2,
    });
    assert_eq!(
        adapter
            .call(request((
                timer,
                Payload {
                    message_id: 1,
                    body: body.clone()
                }
            )))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let mut extensions = Extensions::default();
    extensions.insert(42u64);
    let rpc = RpcService::new("node", "custom", adapter)
        .unwrap()
        .with_execution(Duration::from_secs(1), CancellationToken::new(), extensions)
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = Listener::new()
        .add_service("custom", rpc.into_business())
        .unwrap();
    assert!(endpoint.readiness().set("custom", true));
    let stop = CancellationToken::new();
    let task = tokio::spawn(endpoint.serve(listener, stop.clone()).unwrap());
    let client = business::Client::new(Arc::new(Client::new(Duration::from_secs(1), 1).unwrap()));
    let target = ServiceDescriptor {
        node_id: "node".into(),
        service_name: "custom".into(),
        protocol: "grpc".into(),
        address,
    };
    let result = client
        .call(
            &target,
            root(
                (
                    call(),
                    Payload {
                        message_id: 1,
                        body,
                    },
                ),
                tracing::Span::current(),
            ),
        )
        .await
        .unwrap();
    let payload = result.output.payload.unwrap();
    assert_eq!(result.output.request_id, 7);
    assert_eq!(payload.message_id, local.output.message.key);
    assert_eq!(payload.body, local.output.message.body);
    for (id, body, code) in [
        (0, vec![], Code::InvalidArgument),
        (1, vec![], Code::PermissionDenied),
        (99, vec![], Code::Unimplemented),
        (1, vec![0xff], Code::InvalidArgument),
    ] {
        assert_eq!(
            client
                .call(
                    &target,
                    root(
                        (
                            call(),
                            Payload {
                                message_id: id,
                                body
                            }
                        ),
                        tracing::Span::current()
                    )
                )
                .await
                .unwrap_err()
                .code(),
            code
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[test]
fn numbered_registry_rejects_zero_and_duplicate_registration() {
    let mut registry = Registry::<(), (), Denied>::new();
    for (request, response) in [(0, 1), (1, 0)] {
        assert!(matches!(
            registry.register(
                request,
                response,
                |execution_request: tok_execution::Request<((), Value)>| async move {
                    let (_, value) = execution_request.input;
                    Ok(tok_execution::Response::new(Reply {
                        message: value,
                        output: (),
                    }))
                }
            ),
            Err(tok_message::Error::Invalid(_))
        ));
    }
    registry
        .register(
            1,
            2,
            |execution_request: tok_execution::Request<((), Value)>| async move {
                let (_, value) = execution_request.input;
                Ok(tok_execution::Response::new(Reply {
                    message: value,
                    output: (),
                }))
            },
        )
        .unwrap();
    assert!(matches!(
        registry.register(
            1,
            3,
            |execution_request: tok_execution::Request<((), Value)>| async move {
                let (_, value) = execution_request.input;
                Ok(tok_execution::Response::new(Reply {
                    message: value,
                    output: (),
                }))
            }
        ),
        Err(tok_message::Error::Registration(_))
    ));
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

#[tokio::test]
async fn numbered_registry_is_a_tower_service_with_common_policy_and_original_business_error() {
    use tower::ServiceExt as _;
    let mut registry = Registry::<(), (), Denied>::new();
    registry
        .register(1, 2, |request: Request<((), Value)>| async move {
            assert_eq!(request.extensions().get::<u64>(), Some(&99));
            if request.input.1.value == 0 {
                return Err(Denied);
            }
            Ok(tok_execution::Response::new(Reply {
                message: request.input.1,
                output: (),
            }))
        })
        .unwrap();
    let service = tower::ServiceBuilder::new()
        .map_request(|mut request: Request<((), Message)>| {
            request.extensions_mut().insert(99u64);
            request
        })
        .service(registry);
    let input = |value| {
        request((
            (),
            Message {
                key: 1,
                body: Value { value }.encode_to_vec(),
            },
        ))
    };
    assert_eq!(
        service
            .clone()
            .oneshot(input(7))
            .await
            .unwrap()
            .output
            .message
            .key,
        2
    );
    assert!(matches!(
        service.oneshot(input(0)).await,
        Err(DispatchError::Handler(protobuf::Error::Handler(Denied)))
    ));
}

fn request<T>(input: T) -> Request<T> {
    let mut request = Request::new(context(), input);
    request.extensions_mut().insert(42u64);
    request
}
