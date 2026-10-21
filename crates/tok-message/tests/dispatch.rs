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
        tok_execution::Error::Inner(DispatchError::Handler(protobuf::Error::Handler(Denied)))
    ));
    let adapter = Arc::new(business::Handler::new(
        Arc::clone(&registry),
        |call| {
            call.bid
                .as_deref()
                .and_then(|value| value.parse::<u32>().ok())
                .ok_or_else(|| Status::invalid_argument("目标 ID 无效"))
        },
        |_| Status::permission_denied("业务拒绝"),
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
        Err(tok_execution::Error::Inner(DispatchError::Handler(
            protobuf::Error::Handler(Denied)
        )))
    ));
}

fn request<T>(input: T) -> Request<T> {
    let mut request = Request::new(context(), input);
    request.extensions_mut().insert(42u64);
    request
}

// 自定义后端共享动态路由状态；协议适配器仍独立完成编解码与非零编号检查。
#[derive(Debug, thiserror::Error)]
#[error("自定义注册拒绝：{0}")]
struct Refused(u32);
#[derive(Debug, thiserror::Error)]
enum RegistryFailure<E> {
    #[error("自定义派发后端失败：{0}")]
    Backend(Arc<String>),
    #[error(transparent)]
    Table(DispatchError<u32, protobuf::Error<E>>),
}
type SharedRoutes<C, O, E> = Arc<std::sync::Mutex<Arc<protobuf::Table<u32, C, O, E>>>>;
struct Redirect<C, O, E> {
    routes: SharedRoutes<C, O, E>,
    calls: Arc<AtomicUsize>,
    failure: Arc<String>,
}
impl<C, O, E> Default for Redirect<C, O, E> {
    fn default() -> Self {
        Self {
            routes: Arc::new(std::sync::Mutex::new(Arc::new(protobuf::Table::new()))),
            calls: Arc::new(AtomicUsize::new(0)),
            failure: Arc::new(String::from("offline")),
        }
    }
}
impl<C, O, E> Clone for Redirect<C, O, E> {
    fn clone(&self) -> Self {
        Self {
            routes: Arc::clone(&self.routes),
            calls: Arc::clone(&self.calls),
            failure: Arc::clone(&self.failure),
        }
    }
}
impl<C: Send + 'static, O: Send + 'static, E: Send + 'static> tok_execution::Registry
    for Redirect<C, O, E>
{
    type Key = u32;
    type Input = (C, Vec<u8>);
    type Output = Reply<Message, O>;
    type HandlerError = protobuf::Error<E>;
    type DispatchError = RegistryFailure<E>;
    type RegistrationError = Refused;
    fn register_handler(
        &mut self,
        key: u32,
        handler: tok_execution::registry::Handler<Self::Input, Self::Output, Self::HandlerError>,
    ) -> Result<(), Refused> {
        if key == 9 {
            return Err(Refused(key));
        }
        let mut routes = self.routes.lock().unwrap();
        Arc::make_mut(&mut routes)
            .register_handler(key, handler)
            .map_err(|_| Refused(key))
    }
    fn dispatch<'a>(
        &'a self,
        key: &'a u32,
        request: Request<Self::Input>,
    ) -> tok_core::BoxFuture<
        'a,
        Result<tok_execution::Response<Self::Output>, tok_execution::Error<Self::DispatchError>>,
    > {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if *key == 11 {
                return Err(tok_execution::Error::Inner(RegistryFailure::Backend(
                    Arc::clone(&self.failure),
                )));
            }
            let key = if *key == 7 { 1 } else { *key };
            let routes = Arc::clone(&self.routes.lock().unwrap());
            routes
                .dispatch(&key, request)
                .await
                .map_err(|error| error.map_inner(RegistryFailure::Table))
        })
    }
}

#[tokio::test]
async fn custom_registry_flows_through_message_codec_and_business_rpc_with_original_errors() {
    let backend = Redirect::<u32, (), Denied>::default();
    let mut registry = Registry::from_registry(backend.clone());
    registry
        .register(1, 2, |request: Request<(u32, Value)>| async move {
            assert_eq!(request.extensions().get::<u64>(), Some(&42));
            let mut response = tok_execution::Response::new(Reply {
                message: Value {
                    value: request.input.0 + request.input.1.value,
                },
                output: (),
            });
            response.extensions_mut().insert(true);
            Ok(response)
        })
        .unwrap();
    let old = registry.clone();
    registry
        .register(3, 4, |request: Request<(u32, Value)>| async move {
            Ok(tok_execution::Response::new(Reply {
                message: request.input.1,
                output: (),
            }))
        })
        .unwrap();
    assert_eq!(
        old.dispatch(request((
            5,
            Message {
                key: 3,
                body: Value { value: 7 }.encode_to_vec()
            }
        )))
        .await
        .unwrap()
        .output
        .message
        .key,
        4
    );
    assert!(matches!(
        registry.register(9, 10, |request: Request<(u32, Value)>| async move {
            Ok(tok_execution::Response::new(Reply {
                message: request.input.1,
                output: (),
            }))
        }),
        Err(tok_message::Error::Registration(Refused(9)))
    ));
    type Erased = dyn tok_execution::Registry<
            Key = u32,
            Input = (u32, Vec<u8>),
            Output = Reply<Message>,
            HandlerError = protobuf::Error<Denied>,
            DispatchError = RegistryFailure<Denied>,
            RegistrationError = Refused,
        >;
    let failure = registry
        .dispatch(request((
            5,
            Message {
                key: 11,
                body: vec![],
            },
        )))
        .await
        .unwrap_err();
    assert!(
        matches!(failure, tok_execution::Error::Inner(RegistryFailure::Backend(error)) if Arc::ptr_eq(&error, &backend.failure))
    );
    let erased: Arc<Erased> = Arc::new(backend.clone());
    let adapter = business::Handler::from_registry(
        Arc::new(Registry::from_shared_registry(erased)),
        |_| Ok(5),
        |error| match error {
            RegistryFailure::Backend(_) => Status::unavailable("自定义路由不可用"),
            RegistryFailure::Table(_) => Status::permission_denied("业务拒绝"),
        },
    );
    let response = adapter
        .call(request((
            call(),
            Payload {
                message_id: 7,
                body: Value { value: 7 }.encode_to_vec(),
            },
        )))
        .await
        .unwrap();
    assert_eq!(response.extensions().get::<bool>(), Some(&true));
    let payload = response.output.payload.unwrap();
    assert_eq!(payload.message_id, 2);
    assert_eq!(Value::decode(payload.body.as_slice()).unwrap().value, 12);
    let failure = adapter
        .call(request((
            call(),
            Payload {
                message_id: 11,
                body: vec![],
            },
        )))
        .await
        .unwrap_err();
    assert_eq!(failure.code(), Code::Unavailable);
    let before = backend.calls.load(Ordering::SeqCst);
    let stopped = request((
        call(),
        Payload {
            message_id: 7,
            body: vec![],
        },
    ));
    stopped.context.cancellation().cancel();
    assert_eq!(
        adapter.call(stopped).await.unwrap_err().code(),
        Code::Cancelled
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), before);
    let invalid = adapter
        .call(request((
            call(),
            Payload {
                message_id: 0,
                body: vec![],
            },
        )))
        .await
        .unwrap_err();
    assert_eq!(invalid.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn player_adapter_can_replace_message_registry_without_routing_timers_through_it() {
    let backend = Redirect::<CallContext, Option<tok_player::TimerUpdate>, Status>::default();
    let mut registry =
        tok_message::rpc::HandlerRegistry::from_registry(backend.clone(), |error| match error {
            RegistryFailure::Backend(_) => Status::unavailable("自定义路由不可用"),
            RegistryFailure::Table(error) => Status::internal(error.to_string()),
        });
    registry
        .register(1, 2, |request: Request<(CallContext, Value)>| async move {
            assert_eq!(request.extensions().get::<u64>(), Some(&42));
            Ok(tok_execution::Response::new(request.input.1))
        })
        .unwrap();
    let response = registry
        .call(request((
            call(),
            Payload {
                message_id: 7,
                body: Value { value: 3 }.encode_to_vec(),
            },
        )))
        .await
        .unwrap();
    assert_eq!(response.output.payload.unwrap().message_id, 2);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    let failure = registry
        .call(request((
            call(),
            Payload {
                message_id: 11,
                body: vec![],
            },
        )))
        .await
        .unwrap_err();
    assert_eq!(failure.code(), Code::Unavailable);
    let before_timer = backend.calls.load(Ordering::SeqCst);
    registry
        .register_timer(7, |_: Request<(CallContext, Value)>| async {
            Ok(tok_execution::Response::new(None))
        })
        .unwrap();
    let mut timer = call();
    timer.timer = Some(tok_player::TimerTrigger {
        timer_id: "tick".into(),
        scheduled_at_ms: 1,
        fired_at_ms: 2,
    });
    assert!(
        registry
            .call(request((
                timer,
                Payload {
                    message_id: 7,
                    body: vec![]
                }
            )))
            .await
            .unwrap()
            .output
            .payload
            .is_none()
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), before_timer);
}
