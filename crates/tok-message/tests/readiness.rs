//! 普通 RPC 在真实传输入口内约束 Tower 就绪等待。
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tok_core::{Identity, ServiceDescriptor};
use tok_execution::{Request, Response};
use tok_message::rpc::{
    self,
    protocol::internal::v1::{CallResponse, Payload},
};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tower::Service;

#[derive(Clone)]
struct Pending(Arc<AtomicUsize>);

impl Service<Request<(rpc::CallContext, Payload)>> for Pending {
    type Response = Response<CallResponse>;
    type Error = Status;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Pending
    }

    fn call(&mut self, _: Request<(rpc::CallContext, Payload)>) -> Self::Future {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(Response::new(CallResponse::default())))
    }
}

#[tokio::test]
async fn server_deadline_bounds_service_readiness_without_calling_it() {
    let calls = Arc::new(AtomicUsize::new(0));
    let service = rpc::Service::new("node", "guild", Pending(Arc::clone(&calls)))
        .unwrap()
        .with_execution(
            Duration::from_millis(60),
            CancellationToken::new(),
            Default::default(),
        )
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let endpoint = tok_grpc::Listener::new()
        .add_service("guild", service.into_business())
        .unwrap();
    assert!(endpoint.readiness().set("guild", true));
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(endpoint.serve(listener, shutdown.clone()).unwrap());
    let target = ServiceDescriptor {
        node_id: "node".into(),
        service_name: "guild".into(),
        protocol: "grpc".into(),
        address,
    };
    let context = tok_execution::Context::new(
        Identity {
            node_id: "caller".into(),
            service_name: "caller".into(),
        },
        "test",
        Duration::from_secs(2),
        CancellationToken::new(),
    )
    .unwrap();
    let client = rpc::Client::new(Arc::new(
        tok_grpc::Client::new(Duration::from_secs(2), 4).unwrap(),
    ));
    let result = client
        .call(
            &target,
            Request::new(
                context,
                (
                    rpc::CallContext {
                        node_id: "node".into(),
                        service_name: "guild".into(),
                        bid: None,
                        request_id: 1,
                    },
                    Payload {
                        message_id: 1,
                        body: vec![],
                    },
                ),
            ),
        )
        .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[derive(Clone)]
struct Waiting {
    polled: Arc<tokio::sync::Notify>,
    calls: Arc<AtomicUsize>,
}

impl Service<Request<(rpc::CallContext, Payload)>> for Waiting {
    type Response = Response<CallResponse>;
    type Error = Status;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Status>> {
        self.polled.notify_one();
        Poll::Pending
    }

    fn call(&mut self, _: Request<(rpc::CallContext, Payload)>) -> Self::Future {
        self.calls.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(Response::new(CallResponse::default())))
    }
}

#[tokio::test]
async fn binding_stop_interrupts_readiness_and_does_not_start_business() {
    use rpc::protocol::internal::v1::{CallContext, CallRequest, business_rpc_server::BusinessRpc};
    let (rpc, binding) = rpc::Service::deferred("node", "guild").unwrap();
    let identity = Identity {
        node_id: "node".into(),
        service_name: "guild".into(),
    };
    let builder =
        tok_lifecycle::Builder::new(identity.clone(), 1, CancellationToken::new()).unwrap();
    let init = tok_lifecycle::Init {
        identity,
        service: builder.handle(),
        extensions: Default::default(),
    };
    let polled = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    binding
        .install(
            &init,
            Waiting {
                polled: Arc::clone(&polled),
                calls: Arc::clone(&calls),
            },
        )
        .unwrap();
    let call = tokio::spawn(async move {
        rpc.call(tonic::Request::new(CallRequest {
            context: Some(CallContext {
                node_id: "node".into(),
                service_name: "guild".into(),
                bid: None,
                request_id: 1,
                session: None,
            }),
            payload: Some(Payload {
                message_id: 1,
                body: vec![],
            }),
            timer: None,
        }))
        .await
    });
    tokio::time::timeout(Duration::from_secs(2), polled.notified())
        .await
        .unwrap();
    init.service.request_stop();
    let result = tokio::time::timeout(Duration::from_secs(2), call)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().code(), tonic::Code::Cancelled);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
