//! 服务级事件订阅的初始化和生命周期。
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_events::{
    Error, Message, Outcome, Subscriber, Task, TopicFilter, interest::Transport, service,
};
use tok_execution::{Request, Response};
use tok_lifecycle::{Builder, Init};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct TransportStub {
    calls: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    pending: bool,
    stop: Arc<Mutex<Option<CancellationToken>>>,
}
impl Transport for TransportStub {
    fn subscribe(&self, _: Subscriber, stop: CancellationToken) -> Task<'_, Task<'static, ()>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.stop.lock().unwrap() = Some(stop.clone());
        let closed = Arc::clone(&self.closed);
        Box::pin(async move {
            if self.pending {
                std::future::pending::<()>().await;
            }
            Ok(Box::pin(async move {
                stop.cancelled().await;
                closed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }) as Task<'static, ()>)
        })
    }
}
fn identity() -> tok_core::service::Identity {
    tok_core::service::Identity {
        node_id: "node".into(),
        service_name: "service".into(),
    }
}
fn config() -> service::Config {
    service::Config {
        name: "events".into(),
        event_types: vec!["config.changed".into()],
        topics: TopicFilter::All,
        group: None,
        extensions: tok_core::Extensions::default(),
        timeout: Duration::from_millis(30),
    }
}
fn chain() -> impl tower::Service<
    Request<Arc<Message>>,
    Response = Response<Outcome>,
    Error = Error,
    Future: Send + 'static,
> + Clone
+ Send
+ Sync {
    tower::service_fn(|_: Request<Arc<Message>>| async { Ok(Response::new(Outcome::Handled)) })
}

#[tokio::test]
async fn validation_and_timeout_do_not_leave_reserved_capacity() {
    let builder = Builder::new(identity(), 1, CancellationToken::new()).unwrap();
    let init = Init {
        identity: identity(),
        service: builder.handle(),
        extensions: Default::default(),
    };
    let backend = TransportStub {
        calls: Arc::new(AtomicUsize::new(0)),
        closed: Arc::new(AtomicUsize::new(0)),
        pending: true,
        stop: Default::default(),
    };
    let mut invalid = config();
    invalid.event_types.push("config.changed".into());
    assert!(matches!(
        service::install(&init, &backend, invalid, chain()).await,
        Err(service::Error::Event(_))
    ));
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        service::install(&init, &backend, config(), chain()).await,
        Err(service::Error::Timeout)
    ));
    assert!(
        backend
            .stop
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .is_cancelled()
    );
    assert!(init.service.tasks().reserve().is_ok());
}

#[tokio::test]
async fn dropping_initialization_cancels_in_progress_subscription() {
    let builder = Builder::new(identity(), 1, CancellationToken::new()).unwrap();
    let init = Init {
        identity: identity(),
        service: builder.handle(),
        extensions: Default::default(),
    };
    let backend = TransportStub {
        calls: Arc::new(AtomicUsize::new(0)),
        closed: Arc::new(AtomicUsize::new(0)),
        pending: true,
        stop: Default::default(),
    };
    {
        let mut options = config();
        options.timeout = Duration::from_secs(5);
        let install = service::install(&init, &backend, options, chain());
        tokio::pin!(install);
        tokio::select! {
            _ = &mut install => panic!("订阅不应完成"),
            () = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
    }
    assert!(
        backend
            .stop
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .is_cancelled()
    );
    assert!(init.service.tasks().reserve().is_ok());
}

#[tokio::test]
async fn subscription_is_supervised_until_service_shutdown() {
    let builder = Builder::new(identity(), 1, CancellationToken::new()).unwrap();
    let backend = TransportStub {
        calls: Arc::new(AtomicUsize::new(0)),
        closed: Arc::new(AtomicUsize::new(0)),
        pending: false,
        stop: Default::default(),
    };
    let closed = Arc::clone(&backend.closed);
    let factory = Arc::new(move |init: Init| -> tok_lifecycle::InitFuture {
        let backend = backend.clone();
        Box::pin(async move {
            service::install(&init, &backend, config(), chain()).await?;
            let stop = init.service.cancellation();
            Ok(Box::pin(async move {
                stop.cancelled().await;
                Ok(())
            }) as tok_lifecycle::task::Task)
        })
    });
    let prepared = builder
        .build(factory, Default::default(), Duration::from_secs(1))
        .await
        .unwrap();
    let running = prepared.start();
    let handle = running.handle();
    handle.request_stop();
    running.run(Duration::from_secs(1)).await.unwrap();
    assert_eq!(closed.load(Ordering::SeqCst), 1);
}
