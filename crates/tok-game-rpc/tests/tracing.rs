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
use tok_core::ServiceDescriptor;
use tok_game_rpc::CallContext;
use tok_game_rpc::{CallTask, Client, Handler, Listener, RpcService};
use tok_proto::internal::v1::{CallResponse, Payload};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;

struct Capture(Arc<Mutex<Option<opentelemetry::trace::TraceId>>>);
impl Handler for Capture {
    fn event(
        &self,
        request: tok_execution::Request<(CallContext, Arc<tok_events::Message>)>,
    ) -> tok_game_rpc::EventTask<'_> {
        let context = request.input.0;
        Box::pin(async move {
            *self.0.lock().unwrap() = Some(
                tracing::Span::current()
                    .context()
                    .span()
                    .span_context()
                    .trace_id(),
            );
            Ok(tok_proto::internal::v1::EventResponse {
                request_id: context.request_id,
                ..Default::default()
            })
        })
    }

    fn call(&self, request: tok_execution::Request<(CallContext, Payload)>) -> CallTask<'_> {
        let (context, payload) = request.input;
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
    let endpoint = Listener::new()
        .add_service(
            "guild",
            RpcService::new("local", "guild", Arc::new(Capture(Arc::clone(&observed))))
                .unwrap()
                .into_business(),
        )
        .unwrap()
        .add_service(
            "player",
            RpcService::new("local", "player", Arc::new(Capture(Arc::clone(&observed))))
                .unwrap()
                .into_player(),
        )
        .unwrap();
    endpoint.readiness().set("player", true);
    endpoint.readiness().set("guild", true);
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(endpoint.serve(listener, shutdown.clone()).unwrap());
    async {
        let root = tracing::info_span!("client_root");
        root.set_parent(context.clone()).unwrap();
        let clients = Arc::new(Client::new(Duration::from_secs(2), 1).unwrap());
        let business_client = tok_game_rpc::business::Client::new(Arc::clone(&clients));
        let target = ServiceDescriptor {
            node_id: "local".into(),
            service_name: "guild".into(),
            protocol: "grpc".into(),
            address,
        };
        let call = CallContext {
            timer: None,
            node_id: "local".into(),
            service_name: "guild".into(),
            bid: Some("7".into()),
            request_id: 1,
            session: None,
        };
        use tracing::Instrument;
        business_client
            .call(
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
    *observed.lock().unwrap() = None;
    let root = tracing::info_span!("event_root");
    root.set_parent(context).unwrap();
    let target = ServiceDescriptor {
        node_id: "local".into(),
        service_name: "player".into(),
        protocol: "grpc".into(),
        address,
    };
    let call = CallContext {
        node_id: "local".into(),
        service_name: "player".into(),
        bid: Some("42".into()),
        request_id: 1,
        session: Some(tok_player::SessionIdentity {
            player_id: "42".into(),
            session_id: "s".into(),
            epoch: 1,
        }),
        timer: None,
    };
    use tracing::Instrument;
    let clients = Arc::new(Client::new(Duration::from_secs(2), 1).unwrap());
    let player_client = tok_game_rpc::player::Client::new(Arc::clone(&clients));
    player_client
        .handle_event(
            &target,
            call,
            &tok_events::Message {
                topic: Default::default(),
                event_id: tok_events::EventId::generate(),
                event_type: "test.trace".into(),
                body: vec![],
                trace_context: Default::default(),
            },
        )
        .instrument(root)
        .await
        .unwrap();
    assert_eq!(*observed.lock().unwrap(), Some(trace_id));

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    provider.shutdown().unwrap();
}
