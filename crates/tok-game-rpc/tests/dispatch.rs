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
use tok_game_rpc::protobuf::{self, Message, Registry, Reply};
use tok_game_rpc::{CallContext, Client, Handler as _, Listener, RpcService, business};
use tok_proto::internal::v1::Payload;
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
    let mut ctx = Context::new(
        Identity {
            node_id: "node".into(),
            service_name: "custom".into(),
        },
        "local",
        Duration::from_secs(1),
        CancellationToken::new(),
    )
    .unwrap();
    ctx.extensions_mut().insert(42u64);
    ctx
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
    let mut registry = Registry::<u32, u32, (), Denied>::new();
    registry
        .register_service(
            1,
            2,
            tower::service_fn(move |request: Request<(u32, Value)>| {
                let observed = Arc::clone(&observed);
                async move {
                    assert_eq!(request.context.extensions().get::<u64>(), Some(&42));
                    observed.fetch_add(1, Ordering::SeqCst);
                    if request.input.1.value == 0 {
                        return Err(Denied);
                    }
                    Ok(Reply {
                        message: Value {
                            value: request.input.0 + request.input.1.value,
                        },
                        output: (),
                    })
                }
            }),
        )
        .unwrap();
    let registry = Arc::new(registry);
    let body = Value { value: 3 }.encode_to_vec();
    let local = registry
        .dispatch(Request::new(
            context(),
            (
                5,
                Message {
                    key: 1,
                    body: body.clone(),
                },
            ),
        ))
        .await
        .unwrap();
    let denied = registry
        .dispatch(Request::new(
            context(),
            (
                5,
                Message {
                    key: 1,
                    body: vec![],
                },
            ),
        ))
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
            .call(Request::new(
                context(),
                (
                    timer,
                    Payload {
                        message_id: 1,
                        body: body.clone()
                    }
                )
            ))
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
            call(),
            Payload {
                message_id: 1,
                body,
            },
        )
        .await
        .unwrap();
    let payload = result.payload.unwrap();
    assert_eq!(result.request_id, 7);
    assert_eq!(payload.message_id, local.message.key);
    assert_eq!(payload.body, local.message.body);
    for (id, body, code) in [
        (1, vec![], Code::PermissionDenied),
        (99, vec![], Code::Unimplemented),
        (1, vec![0xff], Code::InvalidArgument),
    ] {
        assert_eq!(
            client
                .call(
                    &target,
                    call(),
                    Payload {
                        message_id: id,
                        body
                    }
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

#[tokio::test]
async fn protobuf_adapter_accepts_string_keys_and_preserves_decode_and_business_errors() {
    let mut registry = protobuf::Registry::<String, u32, String, Denied>::new();
    registry
        .register(
            "increment".to_owned(),
            "result".to_owned(),
            |state, value: Value| async move {
                if value.value == 0 {
                    return Err(Denied);
                }
                Ok(protobuf::Reply {
                    message: Value {
                        value: state + value.value,
                    },
                    output: "saved".to_owned(),
                })
            },
        )
        .unwrap();
    let ctx = context();
    let message = |body| protobuf::Message {
        key: "increment".to_owned(),
        body,
    };
    let reply = registry
        .dispatch(Request::new(
            ctx.clone(),
            (5, message(Value { value: 7 }.encode_to_vec())),
        ))
        .await
        .unwrap();
    assert_eq!(reply.message.key, "result");
    assert_eq!(
        Value::decode(reply.message.body.as_slice()).unwrap().value,
        12
    );
    assert_eq!(reply.output, "saved");
    assert!(matches!(
        registry
            .dispatch(Request::new(ctx.clone(), (5, message(vec![]))))
            .await,
        Err(DispatchError::Handler(protobuf::Error::Handler(Denied)))
    ));
    assert!(matches!(
        registry
            .dispatch(Request::new(ctx.clone(), (5, message(vec![0xff]))))
            .await,
        Err(DispatchError::Handler(protobuf::Error::Decode(_)))
    ));
    // 编解码也在执行范围内；已取消调用不再解析坏包。
    ctx.cancellation().cancel();
    assert!(matches!(
        registry
            .dispatch(Request::new(ctx, (5, message(vec![0xff]))))
            .await,
        Err(DispatchError::Stopped(tok_execution::StopReason::Cancelled))
    ));
}

#[tokio::test]
async fn zero_message_id_rules_belong_to_the_game_rpc_boundary() {
    // 通用 protobuf 适配允许零键；游戏 RPC 的消息 ID 则必须非零。
    let mut registry = Registry::<u32, (), (), Denied>::new();
    for key in [0, 1] {
        registry
            .register(key, 0, |_, value: Value| async move {
                Ok(Reply {
                    message: value,
                    output: (),
                })
            })
            .unwrap();
    }
    let registry = Arc::new(registry);
    let reply = registry
        .dispatch(Request::new(
            context(),
            (
                (),
                Message {
                    key: 0,
                    body: vec![],
                },
            ),
        ))
        .await
        .unwrap();
    assert_eq!(reply.message.key, 0);
    let adapter = business::Handler::new(
        registry,
        |_| Ok(()),
        |_: Denied| Status::internal("业务失败"),
    );
    assert_eq!(
        adapter
            .call(Request::new(
                context(),
                (
                    call(),
                    Payload {
                        message_id: 0,
                        body: vec![]
                    }
                )
            ))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        adapter
            .call(Request::new(
                context(),
                (
                    call(),
                    Payload {
                        message_id: 1,
                        body: vec![]
                    }
                )
            ))
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let mut game = tok_game_rpc::HandlerRegistry::new();
    assert!(
        game.register(1, 0, |_, value: Value| async move { Ok(value) })
            .is_err()
    );
}
