//! 通用设施可接入任意输入、结果及借用，不依赖游戏协议。
use std::time::Duration;
use tok_core::{Extensions, Identity, ServiceDescriptor};
use tok_execution::Request;
use tok_rpc::{Client, Server};
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
            7,
            tonic::Request::new("custom"),
            tracing::Span::none(),
            |_, request| async move {
                assert!(request.metadata().contains_key("grpc-timeout"));
                Ok(tonic::Response::new(request.into_inner().len()))
            },
        )
        .await
        .unwrap();
    assert_eq!(result, 6);
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
            Err(tok_rpc::Error::Configuration(_))
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
            1,
            request,
            tracing::Span::none(),
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
                1,
                request,
                tracing::Span::none(),
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
                1,
                tonic::Request::new(()),
                tracing::Span::none(),
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
            1,
            tonic::Request::new(()),
            tracing::Span::none(),
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
    let error = tok_rpc::Error::from(std::io::Error::new(
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
        .with_endpoint_factory(|_| Err(tok_rpc::Error::Configuration("测试拒绝连接配置".into())));
    let error = client.channel(&target()).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("测试拒绝连接配置"));
}
