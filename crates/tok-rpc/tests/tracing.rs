//! 内部 RPC 跨网络传播追踪上下文的集成测试。

use opentelemetry::{
    Context, global,
    trace::{TraceContextExt, Tracer, TracerProvider},
};
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace::SdkTracerProvider};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tok_core::{CallContext, ServiceDescriptor};
use tok_proto::internal::v1::{CallResponse, Payload};
use tok_rpc::{CallHandler, CallTask, RpcClients, RpcEndpoint, RpcService};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;

struct Capture(Arc<Mutex<Option<opentelemetry::trace::TraceId>>>);
impl CallHandler for Capture {
    fn call(&self, context: CallContext, payload: Payload) -> CallTask<'_> {
        Box::pin(async move {
            *self.0.lock().unwrap() = Some(
                tracing::Span::current()
                    .context()
                    .span()
                    .span_context()
                    .trace_id(),
            );
            Ok(CallResponse {
                request_id: context.request_id,
                payload: Some(payload),
                error: None,
                timers: None,
            })
        })
    }
}

#[tokio::test]
async fn trace_context_is_propagated_across_the_network_call() {
    global::set_text_map_propagator(TraceContextPropagator::new());
    let provider = SdkTracerProvider::builder().build();
    let tracer = provider.tracer("rpc-test");
    let root = tracer.start("root");
    let context = Context::current_with_span(root);
    let trace_id = context.span().span_context().trace_id();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer)),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let observed = Arc::new(Mutex::new(None));
    let endpoint = RpcEndpoint::new()
        .add_service(
            "guild",
            RpcService::new("local", "guild", Arc::new(Capture(observed.clone())))
                .unwrap()
                .into_business(),
        )
        .unwrap();
    endpoint.readiness().set("guild", true);
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(endpoint.serve(listener, shutdown.clone()).unwrap());
    async {
        let root = tracing::info_span!("client_root");
        root.set_parent(context).unwrap();
        let clients = RpcClients::new(Duration::from_secs(2), 1).unwrap();
        let target = ServiceDescriptor {
            node_id: "local".into(),
            service_name: "guild".into(),
            protocol: "grpc".into(),
            address,
        };
        let call = CallContext {
            node_id: "local".into(),
            service_name: "guild".into(),
            bid: Some("7".into()),
            request_id: 1,
            session: None,
        };
        use tracing::Instrument;
        clients
            .business_call(
                &target,
                call,
                Payload {
                    message_id: 1,
                    body: vec![],
                },
            )
            .instrument(root)
            .await
            .unwrap();
    }
    .await;
    assert_eq!(*observed.lock().unwrap(), Some(trace_id));
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    provider.shutdown().unwrap();
}
