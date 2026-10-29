//! 自定义注册器使用不可克隆的键，派发保留执行保护和双向扩展。
use prost::Message as _;
use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::{BoxFuture, Identity};
use tok_execution::{
    Context, Registry as _, Request, Response, StopReason,
    registry::{self, DispatchError},
};
use tok_message::protobuf::{self, Message, Reply};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt as _;

#[derive(Debug, PartialEq, Eq)]
struct Key(u32);
#[derive(Clone, PartialEq, prost::Message)]
struct Value {
    #[prost(uint32, tag = "1")]
    value: u32,
}
type CodecError = protobuf::Error<Infallible>;
type DispatchFailure = DispatchError<u32, CodecError>;
type Table = tok_execution::Table<u32, ((), Vec<u8>), Reply<Message<Key>>, CodecError>;

// 后端本身也不实现 Clone；它用内部 u32 表按借用键派发。
struct Backend {
    table: Table,
    calls: Arc<AtomicUsize>,
}
impl Backend {
    fn new(calls: Arc<AtomicUsize>) -> Self {
        let mut table = Table::new();
        table
            .register(1, |request| async move {
                assert_eq!(request.context.operation(), "custom");
                assert_eq!(request.extensions().get::<u64>(), Some(&42));
                let value =
                    Value::decode(request.input.1.as_slice()).map_err(CodecError::Decode)?;
                let mut response = Response::new(Reply {
                    message: Message {
                        key: Key(2),
                        body: Value {
                            value: value.value + 1,
                        }
                        .encode_to_vec(),
                    },
                    output: (),
                });
                response.extensions_mut().insert(true);
                Ok(response)
            })
            .unwrap();
        Self { table, calls }
    }
}
impl tok_execution::Registry for Backend {
    type Key = Key;
    type Input = ((), Vec<u8>);
    type Output = Reply<Message<Key>>;
    type HandlerError = CodecError;
    type DispatchError = DispatchFailure;
    type RegistrationError = registry::Error<u32>;
    fn register_handler(
        &mut self,
        key: Key,
        handler: registry::Handler<Self::Input, Self::Output, Self::HandlerError>,
    ) -> Result<(), Self::RegistrationError> {
        self.table.register_handler(key.0, handler)
    }
    fn dispatch<'a>(
        &'a self,
        key: &'a Key,
        request: Request<Self::Input>,
    ) -> BoxFuture<'a, tok_execution::Result<Response<Self::Output>, Self::DispatchError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if key.0 == 9 {
                std::future::pending::<()>().await;
            }
            self.table.dispatch(&key.0, request).await
        })
    }
}
fn input(key: u32, budget: Duration) -> Request<((), Message<Key>)> {
    let context = Context::new(
        Identity {
            node_id: "local".into(),
            service_name: "codec".into(),
        },
        "custom",
        budget,
        CancellationToken::new(),
    )
    .unwrap();
    let mut request = Request::new(
        context,
        (
            (),
            Message {
                key: Key(key),
                body: Value { value: 7 }.encode_to_vec(),
            },
        ),
    );
    request.extensions_mut().insert(42u64);
    request
}
#[tokio::test]
async fn owned_non_clone_registry_and_key_support_dispatch_without_losing_errors_or_extensions() {
    let calls = Arc::new(AtomicUsize::new(0));
    let registry = protobuf::Registry::<Key, (), (), Infallible, _>::from_registry(Backend::new(
        Arc::clone(&calls),
    ));
    let reply = registry
        .dispatch(input(1, Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(reply.output.message.key, Key(2));
    assert_eq!(
        Value::decode(reply.output.message.body.as_slice())
            .unwrap()
            .value,
        8
    );
    assert_eq!(reply.extensions().get::<bool>(), Some(&true));
    assert!(reply.extensions().get::<u64>().is_none());
    assert!(matches!(
        registry.dispatch(input(3, Duration::from_secs(1))).await,
        Err(tok_execution::Error::Inner(DispatchError::Unknown(3)))
    ));
    let before = calls.load(Ordering::SeqCst);
    let cancelled = input(1, Duration::from_secs(1));
    cancelled.context.cancellation().cancel();
    assert!(matches!(
        registry.dispatch(cancelled).await,
        Err(tok_execution::Error::Stopped(StopReason::Cancelled))
    ));
    assert!(matches!(
        registry.dispatch(input(1, Duration::ZERO)).await,
        Err(tok_execution::Error::Stopped(StopReason::DeadlineExceeded))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), before);
}
type Erased = dyn tok_execution::Registry<
        Key = Key,
        Input = ((), Vec<u8>),
        Output = Reply<Message<Key>>,
        HandlerError = CodecError,
        DispatchError = DispatchFailure,
        RegistrationError = registry::Error<u32>,
    >;
#[tokio::test]
async fn non_clone_key_trait_object_supports_tower_calls_and_bounds_custom_dispatch() {
    let calls = Arc::new(AtomicUsize::new(0));
    let backend: Arc<Erased> = Arc::new(Backend::new(Arc::clone(&calls)));
    let registry = protobuf::Registry::<Key, (), (), Infallible, _>::from_shared_registry(backend);
    let reply = registry
        .clone()
        .oneshot(input(1, Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(reply.output.message.key, Key(2));
    assert_eq!(reply.extensions().get::<bool>(), Some(&true));
    assert!(matches!(
        registry.oneshot(input(9, Duration::from_millis(30))).await,
        Err(tok_execution::Error::Stopped(StopReason::DeadlineExceeded))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
