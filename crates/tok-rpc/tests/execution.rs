//! RPC、本地/远程事件与 timer 共用中间件及入口预算。
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tok_core::{
    CallContext,
    execution::{Context, Source},
    service::Identity,
};
use tok_events::{Envelope, Event, EventId, Input};
use tok_rpc::{
    CallHandler, HandlerRegistry, HandlerReply, RpcService,
    protocol::internal::v1::{self as proto, Payload},
};
use tok_service::Request;
use tokio_util::sync::CancellationToken;
use tower::{Layer, Service};
#[derive(Clone, PartialEq, prost::Message)]
struct Value {
    #[prost(uint32, tag = "1")]
    value: u32,
}
impl Event for Value {
    const NAME: &'static str = "test.value";
}
#[derive(Clone)]
struct Audit(Arc<AtomicUsize>);
#[derive(Clone)]
struct Audited<S> {
    inner: S,
    calls: Arc<AtomicUsize>,
}
impl<S> Layer<S> for Audit {
    type Service = Audited<S>;
    fn layer(&self, inner: S) -> Self::Service {
        Audited {
            inner,
            calls: Arc::clone(&self.0),
        }
    }
}
impl<T, S: Service<Request<T>>> Service<Request<T>> for Audited<S> {
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;
    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: Request<T>) -> Self::Future {
        assert_eq!(request.context.service().service_name, "test");
        assert_eq!(request.context.extensions().get::<u32>(), Some(&42));
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.call(request)
    }
}
fn call() -> CallContext {
    CallContext {
        node_id: "node".into(),
        service_name: "test".into(),
        bid: None,
        request_id: 1,
        session: None,
        timer: None,
    }
}
fn context(source: Source) -> Context {
    let mut context = Context::new(
        Identity {
            node_id: "node".into(),
            service_name: "test".into(),
        },
        "test",
        source,
        Duration::from_secs(5),
        CancellationToken::new(),
    )
    .unwrap();
    context.extensions_mut().insert(42u32);
    context
}
#[tokio::test]
async fn one_layer_and_one_event_registration_serve_rpc_timer_local_and_encoded_events() {
    use prost::Message as _;
    let calls = Arc::new(AtomicUsize::new(0));
    let audit = Audit(Arc::clone(&calls));
    let mut registry = HandlerRegistry::new();
    registry
        .register_service(
            1,
            2,
            audit.layer(tower::service_fn(
                |request: Request<(CallContext, Value)>| async move {
                    assert_eq!(request.context.operation(), "rpc/1");
                    Ok(HandlerReply {
                        message: request.input.1,
                        timers: None,
                    })
                },
            )),
        )
        .unwrap();
    registry
        .register_timer_service(
            1,
            audit.layer(tower::service_fn(
                |request: Request<(CallContext, Value)>| async move {
                    assert_eq!(request.context.operation(), "timer/1");
                    assert!(matches!(request.context.source(), Source::Timer(_)));
                    Ok(None)
                },
            )),
        )
        .unwrap();
    registry
        .events_mut()
        .register_service::<Value, _>(audit.layer(tok_events::handler(
            |request: Request<Input<'_, CallContext, Value>>| {
                Box::pin(async move {
                    tokio::task::yield_now().await;
                    assert_eq!(request.context.operation(), "event/test.value");
                    assert_eq!(request.input.event.event_id.as_str(), "same-event");
                    assert_eq!(request.input.event.payload.value, 7);
                    Ok(None)
                })
            },
        )))
        .unwrap();
    registry.events_mut().enable_protobuf::<Value>().unwrap();
    let payload = Payload {
        message_id: 1,
        body: Value { value: 7 }.encode_to_vec(),
    };
    let response = registry
        .call(Request::new(
            context(Source::Rpc { request_id: 1 }),
            (call(), payload.clone()),
        ))
        .await
        .unwrap();
    assert_eq!(response.payload.unwrap().message_id, 2);
    let timer = tok_core::TimerTrigger {
        timer_id: "tick".into(),
        scheduled_at_ms: 1,
        fired_at_ms: 2,
    };
    let mut timer_call = call();
    timer_call.timer = Some(timer.clone());
    let response = registry
        .call(Request::new(
            context(Source::Timer(timer)),
            (timer_call, payload),
        ))
        .await
        .unwrap();
    assert!(response.payload.is_none());
    let envelope = Envelope::new(EventId::new("same-event").unwrap(), Value { value: 7 }).unwrap();
    let result = registry
        .events()
        .dispatch(
            &mut call(),
            Request::new(context(Source::Rpc { request_id: 1 }), &envelope),
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    let event = Arc::new(tok_events::Message::new(&envelope).unwrap());
    let response = registry
        .event(Request::new(
            context(Source::Event {
                event_id: "same-event".into(),
            }),
            (call(), event),
        ))
        .await
        .unwrap();
    assert!(response.timers.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}
fn wire_request(timeout: Option<&str>) -> tonic::Request<proto::CallRequest> {
    use prost::Message as _;
    let mut request = tonic::Request::new(proto::CallRequest {
        context: Some(proto::CallContext {
            node_id: "node".into(),
            service_name: "test".into(),
            bid: None,
            request_id: 1,
            session: None,
        }),
        payload: Some(Payload {
            message_id: 1,
            body: Value { value: 7 }.encode_to_vec(),
        }),
        timer: None,
    });
    if let Some(value) = timeout {
        request
            .metadata_mut()
            .insert("grpc-timeout", value.parse().unwrap());
    }
    request
}
#[tokio::test]
async fn rpc_budget_and_malformed_timeout_are_enforced_before_accepting_results() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let mut registry = HandlerRegistry::new();
    registry
        .register(1, 2, move |_, value: Value| {
            let seen = Arc::clone(&seen);
            async move {
                seen.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(value)
            }
        })
        .unwrap();
    let rpc = RpcService::new("node", "test", Arc::new(registry)).unwrap();
    for value in ["", "123456789S", "1X", "-1S"] {
        assert_eq!(
            proto::business_rpc_server::BusinessRpc::call(&rpc, wire_request(Some(value)))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        proto::business_rpc_server::BusinessRpc::call(&rpc, wire_request(Some("0S")))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::DeadlineExceeded
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        proto::business_rpc_server::BusinessRpc::call(&rpc, wire_request(Some("1m")))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::DeadlineExceeded
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn deferred_rpc_is_initialized_once_by_its_own_factory_and_stops_with_service() {
    use tok_service::lifecycle::{Builder, Init, InitTask};
    let (rpc, binding) = RpcService::deferred("node", "test").unwrap();
    assert_eq!(
        proto::business_rpc_server::BusinessRpc::call(&rpc, wire_request(None))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    let wrong = Builder::new(
        Identity {
            node_id: "node".into(),
            service_name: "other".into(),
        },
        1,
        CancellationToken::new(),
    )
    .unwrap();
    let mut extensions = tok_core::execution::Extensions::default();
    extensions.insert(42u32);
    let identity = Identity {
        node_id: "node".into(),
        service_name: "test".into(),
    };
    let builder = Builder::new(identity, 1, CancellationToken::new()).unwrap();
    let service = builder.handle();
    let factory = move |init: Init| -> InitTask {
        let binding = binding.clone();
        let wrong = Init {
            identity: Identity {
                node_id: "node".into(),
                service_name: "other".into(),
            },
            service: wrong.handle(),
            extensions: Default::default(),
        };
        Box::pin(async move {
            let mut registry = HandlerRegistry::new();
            registry.register(1, 2, |_, v: Value| async move { Ok(v) })?;
            let handler: Arc<dyn CallHandler> = Arc::new(registry);
            assert!(binding.install(&wrong, Arc::clone(&handler)).is_err());
            binding.install(&init, Arc::clone(&handler))?;
            assert!(binding.install(&init, handler).is_err());
            Ok(Box::pin(async move {
                init.service.cancellation().cancelled().await;
                Ok(())
            }) as tok_service::task::Task)
        })
    };
    let prepared = builder
        .build(Arc::new(factory), extensions, Duration::from_secs(1))
        .await
        .unwrap();
    let running = tokio::spawn(prepared.start().run(Duration::from_secs(1)));
    assert!(
        proto::business_rpc_server::BusinessRpc::call(&rpc, wire_request(None))
            .await
            .is_ok()
    );
    service.request_stop();
    running.await.unwrap().unwrap();
    assert_eq!(
        proto::business_rpc_server::BusinessRpc::call(&rpc, wire_request(None))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Cancelled
    );
}

#[test]
fn initialization_binding_is_not_retained_in_its_own_execution_extensions() {
    let (rpc, binding) = RpcService::deferred("node", "test").unwrap();
    let identity = Identity {
        node_id: "node".into(),
        service_name: "test".into(),
    };
    let builder =
        tok_service::lifecycle::Builder::new(identity.clone(), 1, CancellationToken::new())
            .unwrap();
    let marker = Arc::new(AtomicUsize::new(0));
    let weak = Arc::downgrade(&marker);
    let mut extensions = tok_core::execution::Extensions::default();
    extensions.insert(marker);
    extensions.insert(binding.clone());
    let init = tok_service::lifecycle::Init {
        identity,
        service: builder.handle(),
        extensions,
    };
    binding
        .install(&init, Arc::new(HandlerRegistry::new()))
        .unwrap();
    assert!(binding.is_installed());
    drop(init);
    drop(builder);
    drop(binding);
    drop(rpc);
    assert!(
        weak.upgrade().is_none(),
        "初始化绑定不能通过自身扩展形成引用环"
    );
}
