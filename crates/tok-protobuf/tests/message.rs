//! 通用正文适配不依赖消息编号、游戏协议或 Tonic。
#![cfg(feature = "message")]
use prost::Message as _;
use std::time::Duration;
use tok_core::Identity;
use tok_execution::{Context, Request, registry::DispatchError};
use tok_message::codec as protobuf;
use tok_protobuf::message::RegistryExt as _;
use tokio_util::sync::CancellationToken;
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
            node_id: "local".into(),
            service_name: "test".into(),
        },
        "test",
        Duration::from_secs(1),
        CancellationToken::new(),
    )
    .unwrap()
}
#[tokio::test]
async fn protobuf_adapter_accepts_string_keys_and_preserves_decode_and_business_errors() {
    let mut registry = protobuf::Registry::<String, u32, String, Denied>::new();
    registry
        .register(
            "increment".to_owned(),
            "result".to_owned(),
            |execution_request: tok_execution::Request<(u32, Value)>| async move {
                assert_eq!(
                    execution_request.extensions().get::<String>().unwrap(),
                    "caller-marker"
                );
                let (state, value) = execution_request.input;
                if value.value == 0 {
                    return Err(Denied);
                }
                Ok(tok_execution::Response::new(protobuf::Reply {
                    message: Value {
                        value: state + value.value,
                    },
                    output: "saved".to_owned(),
                }))
            },
        )
        .unwrap();
    let mut ctx = Request::new(context(), ());
    ctx.extensions_mut().insert(String::from("caller-marker"));
    let message = |body| protobuf::Message {
        key: "increment".to_owned(),
        body,
    };
    let reply = registry
        .dispatch(
            ctx.clone()
                .map(|()| (5, message(Value { value: 7 }.encode_to_vec()))),
        )
        .await
        .unwrap();
    assert_eq!(reply.output.message.key, "result");
    assert_eq!(
        Value::decode(reply.output.message.body.as_slice())
            .unwrap()
            .value,
        12
    );
    assert_eq!(reply.output.output, "saved");
    assert!(matches!(
        registry
            .dispatch(ctx.clone().map(|()| (5, message(vec![]))))
            .await,
        Err(tok_execution::Error::Inner(DispatchError::Handler(
            protobuf::Error::Handler(Denied)
        )))
    ));
    assert!(matches!(
        registry
            .dispatch(ctx.clone().map(|()| (5, message(vec![0xff]))))
            .await,
        Err(tok_execution::Error::Inner(DispatchError::Handler(
            protobuf::Error::Decode(_)
        )))
    ));
    // 编解码也在执行范围内；已取消调用不再解析坏包。
    ctx.context.cancellation().cancel();
    assert!(matches!(
        registry
            .dispatch(ctx.map(|()| (5, message(vec![0xff]))))
            .await,
        Err(tok_execution::Error::Stopped(
            tok_execution::StopReason::Cancelled
        ))
    ));
}

#[tokio::test]
async fn codec_does_not_reserve_zero_keys() {
    let mut registry = protobuf::Registry::<u32>::new();
    registry
        .register(
            0,
            0,
            |execution_request: tok_execution::Request<((), Value)>| async move {
                let (_, value) = execution_request.input;
                Ok(tok_execution::Response::new(protobuf::Reply {
                    message: value,
                    output: (),
                }))
            },
        )
        .unwrap();
    let reply = registry
        .dispatch(Request::new(
            context(),
            (
                (),
                protobuf::Message {
                    key: 0,
                    body: vec![],
                },
            ),
        ))
        .await
        .unwrap();
    assert_eq!(reply.output.message.key, 0);
}

#[tokio::test]
async fn direct_service_preserves_policy_metadata_decode_errors_and_registration_snapshots() {
    use tower::ServiceExt as _;
    let mut registry = protobuf::Registry::<u32, (), (), Denied>::new();
    registry
        .register(1, 2, |request: Request<((), Value)>| async move {
            assert_eq!(request.extensions().get::<u64>(), Some(&42));
            let mut response = tok_execution::Response::new(protobuf::Reply {
                message: request.input.1,
                output: (),
            });
            response
                .extensions_mut()
                .insert(String::from("encoded-result"));
            Ok(response)
        })
        .unwrap();
    let frozen = registry.clone();
    registry
        .register(3, 4, |request: Request<((), Value)>| async move {
            Ok(tok_execution::Response::new(protobuf::Reply {
                message: request.input.1,
                output: (),
            }))
        })
        .unwrap();
    let input = |key, body| Request::new(context(), ((), protobuf::Message { key, body }));
    assert!(matches!(
        frozen.clone().oneshot(input(3, vec![])).await,
        Err(tok_execution::Error::Inner(DispatchError::Unknown(3)))
    ));
    assert_eq!(
        registry
            .oneshot(input(3, vec![]))
            .await
            .unwrap()
            .output
            .message
            .key,
        4
    );
    let chain = tower::ServiceBuilder::new()
        .map_request(|mut request: Request<((), protobuf::Message<u32>)>| {
            request.extensions_mut().insert(42u64);
            request
        })
        .service(frozen);
    let reply = chain
        .clone()
        .oneshot(input(1, Value { value: 7 }.encode_to_vec()))
        .await
        .unwrap();
    assert_eq!(
        reply.extensions().get::<String>().unwrap(),
        "encoded-result"
    );
    assert!(reply.extensions().get::<u64>().is_none());
    assert_eq!(reply.output.message.key, 2);
    assert_eq!(
        Value::decode(reply.output.message.body.as_slice())
            .unwrap()
            .value,
        7
    );
    assert!(matches!(
        chain.clone().oneshot(input(1, vec![0xff])).await,
        Err(tok_execution::Error::Inner(DispatchError::Handler(
            protobuf::Error::Decode(_)
        )))
    ));
    let request = input(99, vec![]);
    request.context.cancellation().cancel();
    assert!(matches!(
        chain.oneshot(request).await,
        Err(tok_execution::Error::Stopped(
            tok_execution::StopReason::Cancelled
        ))
    ));
}

#[derive(Clone, Copy)]
enum Readiness {
    Ready,
    Cancel,
    Expire,
    Pending,
    Reject,
}
struct TypedService {
    token: CancellationToken,
    readiness: Readiness,
    clones: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    polls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl Clone for TypedService {
    fn clone(&self) -> Self {
        use std::sync::{Arc, atomic::Ordering};
        self.clones.fetch_add(1, Ordering::SeqCst);
        Self {
            token: self.token.clone(),
            readiness: self.readiness,
            clones: Arc::clone(&self.clones),
            calls: Arc::clone(&self.calls),
            polls: Arc::clone(&self.polls),
        }
    }
}
impl tower::Service<Request<((), Value)>> for TypedService {
    type Response = tok_execution::Response<protobuf::Reply<Value>>;
    type Error = Denied;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Denied>> {
        use std::{sync::atomic::Ordering, task::Poll};
        self.polls.fetch_add(1, Ordering::SeqCst);
        match self.readiness {
            Readiness::Ready => (),
            Readiness::Cancel => self.token.cancel(),
            Readiness::Expire => std::thread::sleep(Duration::from_millis(60)),
            Readiness::Pending => return Poll::Pending,
            Readiness::Reject => return Poll::Ready(Err(Denied)),
        }
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: Request<((), Value)>) -> Self::Future {
        use std::sync::atomic::Ordering;
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.context.operation(), "test");
        assert_eq!(request.extensions().get::<u64>(), Some(&42));
        let mut response = tok_execution::Response::new(protobuf::Reply {
            message: request.input.1,
            output: (),
        });
        response.extensions_mut().insert(true);
        std::future::ready(Ok(response))
    }
}
fn typed_service(readiness: Readiness) -> TypedService {
    TypedService {
        token: CancellationToken::new(),
        readiness,
        clones: Default::default(),
        calls: Default::default(),
        polls: Default::default(),
    }
}
fn typed_request(
    token: CancellationToken,
    budget: Duration,
    body: Vec<u8>,
) -> Request<((), protobuf::Message<u32>)> {
    let mut request = Request::new(
        context().with_cancellation(token).with_budget_limit(budget),
        ((), protobuf::Message { key: 1, body }),
    );
    request.extensions_mut().insert(42u64);
    request
}
#[tokio::test]
async fn typed_readiness_stops_before_business_without_wrapping_stops_as_codec_errors() {
    use std::sync::{Arc, atomic::Ordering};
    for readiness in [Readiness::Cancel, Readiness::Expire, Readiness::Pending] {
        let service = typed_service(readiness);
        let token = service.token.clone();
        let calls = Arc::clone(&service.calls);
        let polls = Arc::clone(&service.polls);
        let mut registry = protobuf::Registry::<u32, (), (), Denied>::new();
        registry.register_service(1, 2, service).unwrap();
        let (budget, expected) = match readiness {
            Readiness::Cancel => (Duration::from_secs(1), tok_execution::StopReason::Cancelled),
            _ => (
                Duration::from_millis(30),
                tok_execution::StopReason::DeadlineExceeded,
            ),
        };
        let result = registry
            .dispatch(typed_request(token, budget, vec![]))
            .await;
        assert!(matches!(result, Err(tok_execution::Error::Stopped(reason)) if reason == expected));
        assert!(polls.load(Ordering::SeqCst) > 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test]
async fn codec_clones_the_typed_service_once_and_preserves_both_extension_paths() {
    use std::sync::{Arc, atomic::Ordering};
    let service = typed_service(Readiness::Ready);
    let clones = Arc::clone(&service.clones);
    let calls = Arc::clone(&service.calls);
    let mut registry = protobuf::Registry::<u32, (), (), Denied>::new();
    registry.register_service(1, 2, service).unwrap();
    let input = |body| typed_request(CancellationToken::new(), Duration::from_secs(1), body);
    assert!(matches!(
        registry.dispatch(input(vec![0xff])).await,
        Err(tok_execution::Error::Inner(DispatchError::Handler(
            protobuf::Error::Decode(_)
        )))
    ));
    assert_eq!(clones.load(Ordering::SeqCst), 0);
    for expected in 1..=2 {
        let response = registry
            .dispatch(input(Value { value: 7 }.encode_to_vec()))
            .await
            .unwrap();
        assert_eq!(response.output.message.key, 2);
        assert_eq!(
            Value::decode(response.output.message.body.as_slice())
                .unwrap()
                .value,
            7
        );
        assert_eq!(response.extensions().get::<bool>(), Some(&true));
        assert!(response.extensions().get::<u64>().is_none());
        assert_eq!(clones.load(Ordering::SeqCst), expected);
        assert_eq!(calls.load(Ordering::SeqCst), expected);
    }
}
#[tokio::test]
async fn typed_readiness_failure_preserves_the_original_handler_error() {
    use std::sync::{Arc, atomic::Ordering};
    let service = typed_service(Readiness::Reject);
    let calls = Arc::clone(&service.calls);
    let mut registry = protobuf::Registry::<u32, (), (), Denied>::new();
    registry.register_service(1, 2, service).unwrap();
    let result = registry
        .dispatch(typed_request(
            CancellationToken::new(),
            Duration::from_secs(1),
            vec![],
        ))
        .await;
    assert!(matches!(
        result,
        Err(tok_execution::Error::Inner(DispatchError::Handler(
            protobuf::Error::Handler(Denied)
        )))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
