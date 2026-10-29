//! logic 协议来源共用同一个用户提供的 Tower 链。
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tok_execution::{Request, Response};
use tok_session::logic::rpc::{self, protocol::internal::v1 as proto};
use tonic::Status;
use tower::{Layer, Service};

#[derive(Clone)]
struct CountLayer(Arc<AtomicUsize>);
#[derive(Clone)]
struct CountService<S> {
    inner: S,
    count: Arc<AtomicUsize>,
}
impl<S> Layer<S> for CountLayer {
    type Service = CountService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        CountService {
            inner,
            count: Arc::clone(&self.0),
        }
    }
}
impl<S: Service<Request<rpc::Input>>> Service<Request<rpc::Input>> for CountService<S> {
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;
    fn poll_ready(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }
    fn call(&mut self, request: Request<rpc::Input>) -> Self::Future {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.inner.call(request)
    }
}

fn context() -> proto::CallContext {
    proto::CallContext {
        node_id: "node".into(),
        service_name: "logic".into(),
        bid: Some("42".into()),
        request_id: 1,
        session: Some(proto::SessionIdentity {
            subject_id: "42".into(),
            session_id: "s".into(),
            epoch: 1,
        }),
    }
}

#[tokio::test]
async fn all_sources_pass_through_one_service_and_mismatched_result_is_rejected() {
    use proto::logic_rpc_server::LogicRpc;
    let count = Arc::new(AtomicUsize::new(0));
    let middleware_count = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&count);
    let chain = tower::service_fn(move |request: Request<rpc::Input>| {
        let observed = Arc::clone(&observed);
        async move {
            observed.fetch_add(1, Ordering::SeqCst);
            let output = match request.input {
                rpc::Input::Message(_, _) => rpc::Output::Message(proto::CallResponse::default()),
                rpc::Input::Timer(_, _) => rpc::Output::Timer(proto::CallResponse::default()),
                rpc::Input::Event(_, _) => rpc::Output::Event(proto::EventResponse::default()),
            };
            Ok::<_, Status>(Response::new(output))
        }
    });
    let service = rpc::Service::new(
        "node",
        "logic",
        CountLayer(Arc::clone(&middleware_count)).layer(chain),
    )
    .unwrap();
    let message = proto::CallRequest {
        context: Some(context()),
        payload: Some(proto::Payload {
            message_id: 1,
            body: vec![],
        }),
        timer: None,
    };
    assert_eq!(
        service
            .call(tonic::Request::new(message.clone()))
            .await
            .unwrap()
            .into_inner()
            .request_id,
        1
    );
    let mut timer = message;
    timer.timer = Some(proto::TimerTrigger {
        timer_id: "t".into(),
        scheduled_at_ms: 1,
        fired_at_ms: 1,
    });
    assert!(service.call(tonic::Request::new(timer)).await.is_ok());
    let event = proto::EventRequest {
        context: Some(context()),
        event_id: "e-1".into(),
        event_type: "game.update".into(),
        body: vec![],
        topic: None,
    };
    assert!(
        service
            .handle_event(tonic::Request::new(event))
            .await
            .is_ok()
    );
    assert_eq!(count.load(Ordering::SeqCst), 3);
    assert_eq!(middleware_count.load(Ordering::SeqCst), 3);
    let wrong = tower::service_fn(|_: Request<rpc::Input>| async {
        Ok::<_, Status>(Response::new(rpc::Output::Event(
            proto::EventResponse::default(),
        )))
    });
    let service = rpc::Service::new("node", "logic", wrong).unwrap();
    let message = proto::CallRequest {
        context: Some(context()),
        payload: Some(proto::Payload {
            message_id: 1,
            body: vec![],
        }),
        timer: None,
    };
    assert_eq!(
        service
            .call(tonic::Request::new(message))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Internal
    );
}
