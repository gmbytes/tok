//! 自定义 Registry 的动态替换、拦截与 trait 对象共用执行保护。
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::{BoxFuture, Identity};
use tok_execution::{
    Context, Registry, Request, Response, StopReason,
    registry::{Handler, Service},
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt as _;

// 自定义实现的键无须 Clone 或 Ord。
#[derive(Debug)]
struct Key(u32);
#[derive(Debug, thiserror::Error)]
#[error("注册被拒绝")]
struct Refused;
#[derive(Debug, thiserror::Error)]
#[error("业务拒绝")]
struct Denied;
#[derive(Debug, thiserror::Error)]
enum RoutingError {
    #[error("路由不存在")]
    Unknown,
    #[error(transparent)]
    Handler(Denied),
    #[error("路由后端不可用：{0}")]
    Unavailable(Arc<String>),
}
type Routes = BTreeMap<u32, Handler<u32, u32, Denied>>;
#[derive(Clone, Default)]
struct Dynamic {
    handlers: Arc<Mutex<Routes>>,
    intercepted: Arc<AtomicBool>,
    pending: Arc<AtomicBool>,
    calls: Arc<AtomicUsize>,
    failure: Arc<Mutex<Option<Arc<String>>>>,
}
impl Registry for Dynamic {
    type Key = Key;
    type Input = u32;
    type Output = u32;
    type HandlerError = Denied;
    type DispatchError = RoutingError;
    type RegistrationError = Refused;
    fn register_handler(
        &mut self,
        key: Key,
        handler: Handler<u32, u32, Denied>,
    ) -> Result<(), Refused> {
        if key.0 == 0 {
            return Err(Refused);
        }
        self.handlers.lock().unwrap().insert(key.0, handler);
        Ok(())
    }
    fn dispatch<'a>(
        &'a self,
        key: &'a Key,
        request: Request<u32>,
    ) -> BoxFuture<'a, Result<Response<u32>, tok_execution::Error<RoutingError>>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(error) = self.failure.lock().unwrap().clone() {
                return Err(tok_execution::Error::Inner(RoutingError::Unavailable(
                    error,
                )));
            }
            if self.pending.load(Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
            if self.intercepted.load(Ordering::SeqCst) {
                assert_eq!(request.extensions().get::<String>().unwrap(), "input");
                let mut response = Response::new(99);
                response.extensions_mut().insert(true);
                return Ok(response);
            }
            // 自定义别名路由，未命中其他键时仍保留原始键。
            let selected = if key.0 == 2 { 1 } else { key.0 };
            let handler = self
                .handlers
                .lock()
                .unwrap()
                .get(&selected)
                .cloned()
                .ok_or(tok_execution::Error::Inner(RoutingError::Unknown))?;
            handler
                .call(request)
                .await
                .map_err(|error| error.map_inner(RoutingError::Handler))
        })
    }
}
fn request(value: u32, budget: Duration) -> Request<u32> {
    let mut request = Request::new(
        Context::new(
            Identity {
                node_id: "n".into(),
                service_name: "custom".into(),
            },
            "custom",
            budget,
            CancellationToken::new(),
        )
        .unwrap(),
        value,
    );
    request.extensions_mut().insert(String::from("input"));
    request
}
#[tokio::test]
async fn dynamic_registry_updates_are_visible_through_trait_object_service_and_interception() {
    let mut registry = Dynamic::default();
    registry
        .register(Key(1), |request| async move {
            Ok(Response::new(request.input + 1))
        })
        .unwrap();
    let erased: Arc<
        dyn Registry<
                Key = Key,
                Input = u32,
                Output = u32,
                HandlerError = Denied,
                DispatchError = RoutingError,
                RegistrationError = Refused,
            >,
    > = Arc::new(registry.clone());
    let service = Service::new(erased, |_: &Request<u32>| Key(2));
    assert_eq!(
        service
            .clone()
            .oneshot(request(41, Duration::from_secs(1)))
            .await
            .unwrap()
            .output,
        42
    );
    registry
        .register(Key(1), |request| async move {
            Ok(Response::new(request.input + 2))
        })
        .unwrap();
    assert_eq!(
        service
            .clone()
            .oneshot(request(41, Duration::from_secs(1)))
            .await
            .unwrap()
            .output,
        43
    );
    assert!(
        registry
            .register(Key(0), |_| async { Ok(Response::new(0)) })
            .is_err()
    );
    registry.intercepted.store(true, Ordering::SeqCst);
    let response = service
        .oneshot(request(0, Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(response.output, 99);
    assert_eq!(response.extensions().get::<bool>(), Some(&true));
    assert!(response.extensions().get::<String>().is_none());
}
#[tokio::test]
async fn custom_dispatch_is_still_bounded_and_stopped_calls_do_not_enter_registry() {
    let registry = Dynamic::default();
    registry.pending.store(true, Ordering::SeqCst);
    let service = registry.clone().into_service(|_: &Request<u32>| Key(1));
    let cancelled = request(1, Duration::from_secs(1));
    cancelled.context.cancellation().cancel();
    assert!(matches!(
        service.clone().oneshot(cancelled).await,
        Err(tok_execution::Error::Stopped(StopReason::Cancelled))
    ));
    assert_eq!(registry.calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        service.oneshot(request(1, Duration::from_millis(20))).await,
        Err(tok_execution::Error::Stopped(StopReason::DeadlineExceeded))
    ));
    assert_eq!(registry.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn registry_owns_its_complete_error_and_service_keeps_the_original_source() {
    let registry = Dynamic::default();
    let original = Arc::new(String::from("route backend offline"));
    *registry.failure.lock().unwrap() = Some(Arc::clone(&original));
    let service = registry.into_service(|_: &Request<u32>| Key(1));
    let failure = service
        .oneshot(request(1, Duration::from_secs(1)))
        .await
        .unwrap_err();
    assert!(
        matches!(failure, tok_execution::Error::Inner(RoutingError::Unavailable(error)) if Arc::ptr_eq(&error, &original))
    );
}
