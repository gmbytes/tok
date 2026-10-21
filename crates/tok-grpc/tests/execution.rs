//! 通用设施可接入任意输入、结果及借用，不依赖游戏协议。
use std::time::Duration;
use tok_core::{Extensions, Identity, ServiceDescriptor};
use tok_execution::Request;
use tok_grpc::{Client, Server};
use tokio_util::sync::CancellationToken;
use tonic::{Code, metadata::MetadataMap};

fn server(stop: CancellationToken) -> Server {
    Server::new(Identity {
        node_id: "node".into(),
        service_name: "custom".into(),
    })
    .unwrap()
    .with_execution(Duration::from_secs(1), stop, Extensions::default())
    .unwrap()
}
#[tokio::test]
async fn arbitrary_borrowed_input_and_result_use_the_common_execution_chain() {
    let server = server(CancellationToken::new());
    let mut value = String::from("custom");
    let context = server.context(&MetadataMap::new(), "append").unwrap();
    let service = tok_execution::sync_handler(|request: Request<&mut String>| {
        request.input.push_str(" protocol");
        Ok::<_, tonic::Status>(request.input.len())
    });
    assert_eq!(
        server
            .execute(Request::new(context, &mut value), service)
            .await
            .unwrap(),
        15
    );
    assert_eq!(value, "custom protocol");
}
#[tokio::test]
async fn remote_budget_only_shortens_local_budget_and_stop_prevents_execution() {
    let stop = CancellationToken::new();
    let server = server(stop.clone());
    let mut metadata = MetadataMap::new();
    metadata.insert("grpc-timeout", "1H".parse().unwrap());
    let context = server.context(&metadata, "test").unwrap();
    assert!(context.remaining() <= Duration::from_secs(1));
    metadata.insert("grpc-timeout", "1m".parse().unwrap());
    assert!(server.context(&metadata, "test").unwrap().remaining() <= Duration::from_millis(1));
    metadata.insert("grpc-timeout", "invalid".parse().unwrap());
    assert_eq!(
        server.context(&metadata, "test").unwrap_err().code(),
        Code::InvalidArgument
    );
    stop.cancel();
    let result = server
        .execute(
            Request::new(context, ()),
            tok_execution::sync_handler(|_: Request<()>| -> Result<(), tonic::Status> {
                panic!("cancelled handler was polled")
            }),
        )
        .await;
    assert_eq!(result.unwrap_err().code(), Code::Cancelled);
}
#[tokio::test]
async fn arbitrary_client_protocol_gets_a_deadline_without_business_types() {
    let clients = Client::new(Duration::from_secs(1), 1).unwrap();
    let target = ServiceDescriptor {
        node_id: "node".into(),
        service_name: "custom".into(),
        protocol: "grpc".into(),
        address: "127.0.0.1:12345".parse().unwrap(),
    };
    let result = clients
        .execute(
            &target,
            root(tonic::Request::new("custom"), tracing::Span::none()),
            |_, request| async move {
                assert!(request.metadata().contains_key("grpc-timeout"));
                Ok(tonic::Response::new(request.into_inner().len()))
            },
        )
        .await
        .unwrap();
    assert_eq!(result.output, 6);
}

fn target() -> ServiceDescriptor {
    ServiceDescriptor {
        node_id: "node".into(),
        service_name: "custom".into(),
        protocol: "grpc".into(),
        address: "127.0.0.1:12345".parse().unwrap(),
    }
}

#[test]
fn client_rejects_invalid_limits_without_panicking() {
    for timeout in [Duration::ZERO, Duration::MAX, Duration::from_secs(86_401)] {
        assert!(matches!(
            Client::new(timeout, 1),
            Err(tok_grpc::Error::Configuration(_))
        ));
    }
    assert!(Client::new(Duration::from_secs(1), 0).is_err());
    assert!(Client::new(Duration::from_secs(86_400), 1).is_ok());
}

#[tokio::test]
async fn client_preserves_shorter_request_budget_and_rejects_invalid_or_expired_requests() {
    let client = Client::new(Duration::from_secs(1), 1).unwrap();
    let mut request = tonic::Request::new(());
    request.set_timeout(Duration::from_millis(100));
    client
        .execute(
            &target(),
            root(request, tracing::Span::none()),
            |_, request| async move {
                // 使用服务端同一解析入口核对发送预算，避免依赖 tonic 选择的编码单位。
                let server = server(CancellationToken::new());
                let context = server.context(request.metadata(), "test").unwrap();
                assert!(context.remaining() <= Duration::from_millis(100));
                Ok(tonic::Response::new(()))
            },
        )
        .await
        .unwrap();
    for (value, code) in [
        ("invalid", Code::InvalidArgument),
        ("0n", Code::DeadlineExceeded),
    ] {
        let mut request = tonic::Request::new(());
        request
            .metadata_mut()
            .insert("grpc-timeout", value.parse().unwrap());
        let error = client
            .execute(
                &target(),
                root(request, tracing::Span::none()),
                |_, _| -> std::future::Ready<Result<tonic::Response<()>, tonic::Status>> {
                    panic!("invalid or expired request must not be sent")
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), code);
    }
}

#[tokio::test]
async fn client_rejects_late_results_from_non_cooperative_work() {
    let client = Client::new(Duration::from_millis(20), 1).unwrap();
    for block_in_send in [false, true] {
        let error = client
            .execute(
                &target(),
                root(tonic::Request::new(()), tracing::Span::none()),
                |_, _| {
                    // 同步构造和单次 poll 内阻塞两种情况均不得延长预算。
                    if block_in_send {
                        std::thread::sleep(Duration::from_millis(60));
                    }
                    async move {
                        if !block_in_send {
                            std::thread::sleep(Duration::from_millis(60));
                        }
                        Ok(tonic::Response::new(()))
                    }
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::DeadlineExceeded);
    }
    let error = client
        .execute(
            &target(),
            root(tonic::Request::new(()), tracing::Span::none()),
            |_, _| async {
                std::future::pending::<Result<tonic::Response<()>, tonic::Status>>().await
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);
}

#[test]
fn rpc_error_text_retains_its_source() {
    use std::error::Error as _;
    let error = tok_grpc::Error::from(std::io::Error::new(
        std::io::ErrorKind::AddrInUse,
        "occupied",
    ));
    assert!(error.to_string().contains("RPC 监听 I/O 操作失败"));
    assert_eq!(error.source().unwrap().to_string(), "occupied");
}

#[tokio::test]
async fn custom_endpoint_factory_uses_cache_and_can_be_replaced() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let client = Client::new(Duration::from_secs(1), 1)
        .unwrap()
        .with_endpoint_factory(move |target| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(
                tonic::transport::Endpoint::from_shared(format!("http://{}", target.address))?
                    .user_agent("custom-transport")?,
            )
        });
    client.channel(&target()).unwrap();
    let mut other_service = target();
    other_service.service_name = "another-service".into();
    client.channel(&other_service).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    client.invalidate("node").unwrap();
    client.channel(&target()).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let client = client
        .with_endpoint_factory(|_| Err(tok_grpc::Error::Configuration("测试拒绝连接配置".into())));
    let error = client.channel(&target()).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("测试拒绝连接配置"));
}

async fn borrowed_sender(
    call: tok_execution::Request<tok_grpc::PreparedCall<&mut String>>,
) -> Result<tok_execution::Response<tonic::Response<usize>>, tonic::Status> {
    let (_, request) = call.input.into_parts(&call.context)?;
    assert_eq!(request.metadata().get("x-policy").unwrap(), "checked");
    let value = request.into_inner();
    tokio::task::yield_now().await;
    value.push_str(" sender");
    let mut response = tok_execution::Response::new(tonic::Response::new(value.len()));
    response
        .extensions_mut()
        .insert(String::from("sender-result"));
    Ok(response)
}

#[tokio::test]
async fn prepared_call_tower_chain_receives_target_and_can_mutate_borrowed_input() {
    use tok_grpc::PreparedCall;
    let client = Client::new(Duration::from_secs(1), 1).unwrap();
    let mut value = String::from("source");
    let service = tower::ServiceBuilder::new()
        .map_request(
            |mut call: tok_execution::Request<PreparedCall<&mut String>>| {
                assert_eq!(call.input.target().service_name, "custom");
                assert!(call.input.request.metadata().contains_key("grpc-timeout"));
                call.input
                    .request
                    .metadata_mut()
                    .insert("x-policy", "checked".parse().unwrap());
                call.input.request.get_mut().push_str(" middleware");
                call
            },
        )
        .service(tower::service_fn(borrowed_sender));
    let result = client
        .execute_service(
            &target(),
            root(tonic::Request::new(&mut value), tracing::Span::none()),
            service,
        )
        .await
        .unwrap();
    assert_eq!(value, "source middleware sender");
    assert_eq!(
        result.extensions().get::<String>().unwrap(),
        "sender-result"
    );
    assert_eq!(result.output, value.len());
}

#[derive(Clone)]
struct ReadinessProbe {
    mode: u8,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl tower::Service<tok_execution::Request<tok_grpc::PreparedCall<()>>> for ReadinessProbe {
    type Response = tok_execution::Response<tonic::Response<()>>;
    type Error = tonic::Status;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        match self.mode {
            0 => std::task::Poll::Pending,
            1 => std::task::Poll::Ready(Err(tonic::Status::permission_denied("策略拒绝"))),
            _ => {
                std::thread::sleep(Duration::from_millis(60));
                std::task::Poll::Ready(Ok(()))
            }
        }
    }
    fn call(&mut self, _: tok_execution::Request<tok_grpc::PreparedCall<()>>) -> Self::Future {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        std::future::ready(Ok(tok_execution::Response::new(tonic::Response::new(()))))
    }
}

#[tokio::test]
async fn prepared_call_readiness_is_bounded_and_late_ready_or_rejection_never_dispatches() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let client = Client::new(Duration::from_secs(1), 1).unwrap();
    for mode in 0..3 {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut request = tonic::Request::new(());
        request.set_timeout(Duration::from_millis(20));
        let error = client
            .execute_service(
                &target(),
                root(request, tracing::Span::none()),
                ReadinessProbe {
                    mode,
                    calls: Arc::clone(&calls),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.code(),
            if mode == 1 {
                Code::PermissionDenied
            } else {
                Code::DeadlineExceeded
            }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn prepared_call_sender_shortens_remote_budget_after_middleware_wait_and_keeps_shorter_metadata()
 {
    let client = Client::new(Duration::from_secs(1), 1).unwrap();
    let service = tower::service_fn(
        |call: tok_execution::Request<tok_grpc::PreparedCall<()>>| async move {
            let before = server(CancellationToken::new())
                .context(call.input.request.metadata(), "before")
                .unwrap()
                .remaining();
            tokio::time::sleep(Duration::from_millis(30)).await;
            let (_, request) = call.input.into_parts(&call.context)?;
            let after = server(CancellationToken::new())
                .context(request.metadata(), "after")
                .unwrap()
                .remaining();
            assert!(after + Duration::from_millis(20) < before);
            Ok::<_, tonic::Status>(tok_execution::Response::new(tonic::Response::new(after)))
        },
    );
    let mut request = tonic::Request::new(());
    request.set_timeout(Duration::from_millis(200));
    assert!(
        client
            .execute_service(&target(), root(request, tracing::Span::none()), service)
            .await
            .unwrap()
            .output
            < Duration::from_millis(180)
    );
    // 中间件主动缩短的远端期限不被发送器放大。
    let service = tower::service_fn(
        |mut call: tok_execution::Request<tok_grpc::PreparedCall<()>>| async move {
            call.input.request.set_timeout(Duration::from_millis(5));
            let (_, request) = call.input.into_parts(&call.context)?;
            let remaining = server(CancellationToken::new())
                .context(request.metadata(), "shorter")
                .unwrap()
                .remaining();
            assert!(remaining <= Duration::from_millis(5));
            Ok::<_, tonic::Status>(tok_execution::Response::new(tonic::Response::new(())))
        },
    );
    client
        .execute_service(
            &target(),
            root(tonic::Request::new(()), tracing::Span::none()),
            service,
        )
        .await
        .unwrap();
    let service = tower::service_fn(
        |mut call: tok_execution::Request<tok_grpc::PreparedCall<()>>| async move {
            call.context = call.context.with_budget_limit(Duration::from_millis(5));
            let (_, request) = call.input.into_parts(&call.context)?;
            let remaining = server(CancellationToken::new())
                .context(request.metadata(), "context-limit")
                .unwrap()
                .remaining();
            assert!(remaining <= Duration::from_millis(5));
            Ok::<_, tonic::Status>(tok_execution::Response::new(tonic::Response::new(())))
        },
    );
    client
        .execute_service(
            &target(),
            root(tonic::Request::new(()), tracing::Span::none()),
            service,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn prepared_call_work_timeout_is_not_retried_and_invalid_budget_skips_readiness() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let client = Client::new(Duration::from_millis(20), 1).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let service = tower::service_fn(
        move |_: tok_execution::Request<tok_grpc::PreparedCall<()>>| {
            observed.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<
                Result<tok_execution::Response<tonic::Response<()>>, tonic::Status>,
            >()
        },
    );
    let error = client
        .execute_service(
            &target(),
            root(tonic::Request::new(()), tracing::Span::none()),
            service,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let mut request = tonic::Request::new(());
    request
        .metadata_mut()
        .insert("grpc-timeout", "invalid".parse().unwrap());
    let error = client
        .execute_service(
            &target(),
            root(request, tracing::Span::none()),
            ReadinessProbe {
                mode: 2,
                calls: Arc::clone(&calls),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn client_layer_preserves_borrowed_input_and_response_metadata() {
    use tok_grpc::{PreparedCall, client::Call};
    use tower::ServiceExt as _;
    let client = Client::new(Duration::from_secs(1), 1).unwrap();
    let service = tower::ServiceBuilder::new()
        .map_request(|mut call: tok_execution::Request<Call<&mut String>>| {
            // 连接准备之前的策略可修改目标。
            call.input.target.service_name = "selected".into();
            call
        })
        .layer(client.layer())
        .map_request(
            |mut call: tok_execution::Request<PreparedCall<&mut String>>| {
                assert_eq!(call.input.target().service_name, "selected");
                call.input
                    .request
                    .metadata_mut()
                    .insert("x-policy", "checked".parse().unwrap());
                call
            },
        )
        .map_response(
            |mut response: tok_execution::Response<tonic::Response<usize>>| {
                response
                    .output
                    .metadata_mut()
                    .insert("x-response", "retained".parse().unwrap());
                response
            },
        )
        .service(tower::service_fn(borrowed_sender));
    let mut value = String::from("direct");
    let response = service
        .oneshot(root(
            Call {
                target: target(),
                request: tonic::Request::new(&mut value),
            },
            tracing::Span::none(),
        ))
        .await
        .unwrap();
    assert_eq!(
        response.output.metadata().get("x-response").unwrap(),
        "retained"
    );
    assert_eq!(*response.output.get_ref(), value.len());
    assert_eq!(value, "direct sender");
}

#[tokio::test]
async fn outer_client_policy_can_reject_before_connection_preparation() {
    use tok_grpc::{PreparedCall, client::Call};
    use tower::ServiceExt as _;
    let client = Client::new(Duration::from_secs(1), 1)
        .unwrap()
        .with_endpoint_factory(|_| panic!("rejected call must not prepare a connection"));
    let service = tower::ServiceBuilder::new()
        .layer(client.layer())
        .service(tower::service_fn(
            |_: tok_execution::Request<PreparedCall<()>>| async {
                panic!("rejected call must not dispatch");
                #[allow(unreachable_code)]
                Ok::<_, tonic::Status>(tok_execution::Response::new(tonic::Response::new(())))
            },
        ));
    let service = tower::service_fn(move |call: tok_execution::Request<Call<()>>| {
        let inner = service.clone();
        async move {
            if call.input.target.service_name == "custom" {
                return Err(tonic::Status::permission_denied("策略拒绝"));
            }
            inner.oneshot(call).await
        }
    });
    let error = service
        .oneshot(root(
            Call {
                target: target(),
                request: tonic::Request::new(()),
            },
            tracing::Span::none(),
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn cloned_client_layer_shares_cache_and_bounds_inner_readiness() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tok_grpc::client::Call;
    use tower::ServiceExt as _;
    let count = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&count);
    let client = Client::new(Duration::from_secs(1), 2)
        .unwrap()
        .with_endpoint_factory(move |target| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(tonic::transport::Endpoint::from_shared(format!(
                "http://{}",
                target.address
            ))?)
        });
    let cloned = client.clone();
    let permits = Arc::new(tokio::sync::Semaphore::new(0));
    let held = Arc::clone(&permits);
    let service = tower::ServiceBuilder::new()
        .layer(client.layer())
        .layer(tower::limit::GlobalConcurrencyLimitLayer::with_semaphore(
            held,
        ))
        .service(tower::service_fn(
            |_: tok_execution::Request<tok_grpc::PreparedCall<()>>| async {
                panic!("no permit available");
                #[allow(unreachable_code)]
                Ok::<_, tonic::Status>(tok_execution::Response::new(tonic::Response::new(())))
            },
        ));
    for service in [service.clone(), service] {
        let mut request = tonic::Request::new(());
        request.set_timeout(Duration::from_millis(20));
        let error = service
            .oneshot(root(
                Call {
                    target: target(),
                    request,
                },
                tracing::Span::none(),
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::DeadlineExceeded);
        assert_eq!(permits.available_permits(), 0);
    }
    cloned.channel(&target()).unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    cloned.invalidate("node").unwrap();
    client.channel(&target()).unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 2);
    let detached =
        cloned.with_endpoint_factory(|_| Err(tok_grpc::Error::Configuration("独立配置".into())));
    assert!(detached.channel(&target()).is_err());
    assert!(client.channel(&target()).is_ok());
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn server_layer_accepts_borrowed_input_and_rejects_wrong_identity_or_cancelled_context() {
    use tower::ServiceExt as _;
    let stop = CancellationToken::new();
    let server = server(stop.clone());
    let service =
        tower::ServiceBuilder::new()
            .layer(server.layer())
            .service(tok_execution::sync_handler(
                |request: Request<&mut String>| {
                    request.input.push_str(" layer");
                    Ok::<_, tonic::Status>(request.input.len())
                },
            ));
    let mut value = String::from("server");
    let context = server.context(&MetadataMap::new(), "append").unwrap();
    let length = service
        .clone()
        .oneshot(Request::new(context, &mut value))
        .await
        .unwrap();
    assert_eq!(length, value.len());
    let other = Server::new(Identity {
        node_id: "other".into(),
        service_name: "custom".into(),
    })
    .unwrap();
    let context = other.context(&MetadataMap::new(), "append").unwrap();
    let error = service
        .clone()
        .oneshot(Request::new(context, &mut value))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    let context = server.context(&MetadataMap::new(), "append").unwrap();
    stop.cancel();
    let error = service
        .oneshot(Request::new(context, &mut value))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Cancelled);
    assert_eq!(value, "server layer");
}

#[tokio::test]
async fn convenience_callback_can_consume_non_clone_capture_and_borrowed_input() {
    let client = Client::new(Duration::from_secs(1), 1).unwrap();
    let owned = std::sync::Mutex::new(String::from(" once"));
    let mut value = String::from("borrowed");
    let size = client
        .execute(
            &target(),
            root(tonic::Request::new(&mut value), tracing::Span::none()),
            move |_, request| async move {
                let suffix = owned.into_inner().unwrap();
                let value = request.into_inner();
                tokio::task::yield_now().await;
                value.push_str(&suffix);
                Ok(tonic::Response::new(value.len()))
            },
        )
        .await
        .unwrap();
    assert_eq!(value, "borrowed once");
    assert_eq!(size.output, value.len());
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
async fn prepared_call_context_keeps_local_identity_extensions_and_elapsed_upstream_budget() {
    let client = Client::new(Duration::from_secs(2), 1).unwrap();
    let mut request = root(tonic::Request::new(()), tracing::Span::none());
    request.context = request
        .context
        .with_budget_limit(Duration::from_millis(300));
    request
        .extensions_mut()
        .insert(String::from("outer-policy"));
    let original = request.context.deadline();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let service = tower::service_fn(
        move |call: tok_execution::Request<tok_grpc::PreparedCall<()>>| async move {
            assert_eq!(call.context.service().service_name, "test-caller");
            assert_eq!(call.extensions().get::<String>().unwrap(), "outer-policy");
            assert!(call.context.deadline() <= original);
            assert_eq!(call.input.target().service_name, "custom");
            let (_, request) = call.input.into_parts(&call.context)?;
            let remote = server(CancellationToken::new())
                .context(request.metadata(), "remote")
                .unwrap();
            assert!(remote.remaining() < Duration::from_millis(280));
            Ok::<_, tonic::Status>(tok_execution::Response::new(tonic::Response::new(())))
        },
    );
    client
        .execute_service(&target(), request, service)
        .await
        .unwrap();
}

#[tokio::test]
async fn upstream_stop_prevents_preparation_and_interrupts_inner_readiness() {
    use tower::{Layer as _, ServiceExt as _};
    let client = Client::new(Duration::from_secs(2), 1).unwrap();
    let mut request = root(tonic::Request::new(()), tracing::Span::none());
    request.context = request.context.with_budget_limit(Duration::ZERO);
    let guarded = client
        .clone()
        .with_endpoint_factory(|_| panic!("expired upstream must not prepare"));
    let error = guarded
        .execute(&target(), request, |_, _| async {
            Ok(tonic::Response::new(()))
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::DeadlineExceeded);
    let request = root(tonic::Request::new(()), tracing::Span::none());
    let cancellation = request.context.cancellation();
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let probe = ReadinessProbe {
        mode: 0,
        calls: std::sync::Arc::clone(&calls),
    };
    let work = client
        .layer()
        .layer(probe)
        .oneshot(request.map(|request| tok_grpc::client::Call {
            target: target(),
            request,
        }));
    let stopped = async {
        tokio::task::yield_now().await;
        cancellation.cancel();
    };
    let (result, ()) = tokio::join!(work, stopped);
    assert_eq!(result.unwrap_err().code(), Code::Cancelled);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn channel_cache_evicts_cold_keys_and_refreshes_recency_across_client_clones() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let builds = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&builds);
    let client = Client::new(Duration::from_secs(1), 2)
        .unwrap()
        .with_endpoint_factory(move |target| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(tonic::transport::Endpoint::from_shared(format!(
                "http://{}",
                target.address
            ))?)
        });
    let target = |node: &str| ServiceDescriptor {
        node_id: node.into(),
        ..target()
    };
    let shared = client.clone();
    for node in ["z", "a", "b"] {
        drop(client.channel(&target(node)).unwrap());
    }
    for _ in 0..10 {
        drop(client.channel(&target("a")).unwrap());
        drop(shared.channel(&target("b")).unwrap());
    }
    assert_eq!(builds.load(Ordering::SeqCst), 3);
    drop(client.channel(&target("z")).unwrap());
    assert_eq!(builds.load(Ordering::SeqCst), 4);
    // a 比 b 更久未访问，所以插入 z 后 b 仍命中。
    drop(shared.channel(&target("b")).unwrap());
    assert_eq!(builds.load(Ordering::SeqCst), 4);
    drop(client.channel(&target("a")).unwrap());
    assert_eq!(builds.load(Ordering::SeqCst), 5);
}

#[tokio::test]
async fn channel_invalidation_removes_all_node_addresses_without_dropping_other_nodes() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let builds = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&builds);
    let client = Client::new(Duration::from_secs(1), 3)
        .unwrap()
        .with_endpoint_factory(move |target| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(tonic::transport::Endpoint::from_shared(format!(
                "http://{}",
                target.address
            ))?)
        });
    let first = target();
    let second = ServiceDescriptor {
        address: "127.0.0.1:12346".parse().unwrap(),
        ..first.clone()
    };
    let other = ServiceDescriptor {
        node_id: "other".into(),
        ..first.clone()
    };
    for target in [&first, &second, &other] {
        drop(client.channel(target).unwrap());
    }
    let shared = client.clone();
    shared.invalidate(&first.node_id).unwrap();
    drop(client.channel(&other).unwrap());
    assert_eq!(builds.load(Ordering::SeqCst), 3);
    drop(client.channel(&first).unwrap());
    drop(shared.channel(&second).unwrap());
    assert_eq!(builds.load(Ordering::SeqCst), 5);
    drop(client.channel(&other).unwrap());
    assert_eq!(builds.load(Ordering::SeqCst), 5);
}
