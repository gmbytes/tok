//! 逻辑会话的 protobuf 注册扩展。
#![cfg(feature = "session")]
use tok_execution::{Request, Response};
use tok_protobuf::session::{EventRegistryExt as _, RegistryExt as _, TimerRegistryExt as _};
use tok_session::logic::rpc::{CallContext, HandlerRegistry};

#[derive(Clone, PartialEq, prost::Message)]
struct Value {
    #[prost(uint32, tag = "1")]
    number: u32,
}
impl tok_events::Event for Value {
    const NAME: &'static str = "test.protobuf.session";
}
#[test]
fn protobuf_registers_request_event_and_timer_on_the_same_registry() {
    let mut registry = HandlerRegistry::new();
    registry
        .register(1, 2, |request: Request<(CallContext, Value)>| async move {
            Ok(Response::new(request.input.1))
        })
        .unwrap();
    registry
        .register_event::<Value, _>(|_| Box::pin(async { Ok(Response::new(None)) }))
        .unwrap();
    registry
        .register_timer(3, |_: Request<(CallContext, Value)>| async {
            Ok(Response::new(None))
        })
        .unwrap();
}
