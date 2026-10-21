use std::time::Duration;
use tok_core::service::Identity;
use tok_service::Request;
use tok_service::{Context, Source};
use tokio_util::sync::CancellationToken;
pub fn context() -> Context {
    Context::new(
        Identity {
            node_id: "test".into(),
            service_name: "events".into(),
        },
        "test",
        Source::Rpc { request_id: 1 },
        Duration::from_secs(5),
        CancellationToken::new(),
    )
    .unwrap()
}
pub fn request<T>(input: T) -> Request<T> {
    Request::new(context(), input)
}
