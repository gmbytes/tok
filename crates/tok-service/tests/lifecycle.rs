//! 公共生命周期：初始化、容量、正常清理、故障与强制收束。
use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tok_core::{Extensions, service::Identity};
use tok_service::{
    lifecycle::{Builder, Error, Factory, Init, InitFuture, Phase},
    task::Task,
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

const LIMIT: Duration = Duration::from_secs(2);
struct Released(Arc<AtomicUsize>);
impl Drop for Released {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn builder(capacity: usize) -> Builder {
    Builder::new(
        Identity {
            node_id: "local".into(),
            service_name: "ordinary".into(),
        },
        capacity,
        CancellationToken::new(),
    )
    .unwrap()
}
#[tokio::test]
async fn initialization_is_separate_from_start_and_stop_waits_for_cleanup() {
    let builder = builder(2);
    let handle = builder.handle();
    let (began, started) = oneshot::channel();
    let (finish, finished) = oneshot::channel();
    let signals = std::sync::Mutex::new(Some((began, finished)));
    let factory: Arc<dyn Factory> = Arc::new(move |init: Init| -> InitFuture {
        let (began, finished) = signals.lock().unwrap().take().unwrap();
        Box::pin(async move {
            Ok(Box::pin(async move {
                init.service.cancellation().cancelled().await;
                began.send(()).unwrap();
                finished.await.unwrap();
                Ok(())
            }) as Task)
        })
    });
    let prepared = builder
        .build(factory, Extensions::default(), LIMIT)
        .await
        .unwrap();
    assert_eq!(handle.phase(), Phase::Starting);
    let service = prepared.start();
    assert_eq!(handle.phase(), Phase::Ready);
    let running = tokio::spawn(service.run(LIMIT));
    handle.request_stop();
    started.await.unwrap();
    assert_eq!(handle.phase(), Phase::Stopping);
    assert!(handle.tasks().reserve().is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(10), handle.wait_finished())
            .await
            .is_err()
    );
    finish.send(()).unwrap();
    running.await.unwrap().unwrap();
    assert_eq!(handle.phase(), Phase::Stopped);
    tokio::time::timeout(Duration::ZERO, handle.wait_finished())
        .await
        .unwrap();
}

#[tokio::test]
async fn failed_timed_out_and_panicked_initialization_release_all_resources() {
    for mode in 0..4 {
        let builder = builder(1);
        let handle = builder.handle();
        let count = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&count);
        let factory: Arc<dyn Factory> = Arc::new(move |init: Init| -> InitFuture {
            let released = Released(Arc::clone(&observed));
            Box::pin(async move {
                init.service
                    .tasks()
                    .launch(
                        "child",
                        Box::pin(async move {
                            let _released = released;
                            pending().await
                        }),
                    )
                    .await?;
                match mode {
                    0 => Err(std::io::Error::other("init rejected").into()),
                    1 => pending().await,
                    2 => panic!("init panic"),
                    _ => {
                        init.service.request_stop();
                        pending().await
                    }
                }
            })
        });
        let result = builder
            .build(factory, Extensions::default(), Duration::from_millis(20))
            .await;
        assert!(matches!(
            (mode, result),
            (0, Err(Error::Initialize(_)))
                | (1, Err(Error::InitializationTimeout))
                | (2, Err(Error::Join(_)))
                | (3, Err(Error::Cancelled))
        ));
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(handle.tasks().used_capacity(), 0);
        handle.wait_finished().await;
    }
}

#[tokio::test]
async fn initialization_supervises_background_errors_before_ready() {
    let builder = builder(1);
    let factory: Arc<dyn Factory> = Arc::new(|init: Init| -> InitFuture {
        Box::pin(async move {
            init.service
                .tasks()
                .launch(
                    "subscription",
                    Box::pin(async { Err(std::io::Error::other("subscription failed").into()) }),
                )
                .await?;
            pending().await
        })
    });
    let handle = builder.handle();
    let result = builder.build(factory, Extensions::default(), LIMIT).await;
    assert!(matches!(result, Err(Error::Run { name, .. }) if name == "subscription"));
    assert_eq!(handle.phase(), Phase::Stopped);
}

#[tokio::test]
async fn abandoning_initialization_cancels_scope_and_drop_reclaims_tasks() {
    let builder = builder(1);
    let handle = builder.handle();
    let (started, beginning) = oneshot::channel();
    let started = std::sync::Mutex::new(Some(started));
    let factory: Arc<dyn Factory> = Arc::new(move |_context: Init| -> InitFuture {
        let started = started.lock().unwrap().take().unwrap();
        Box::pin(async move {
            started.send(()).unwrap();
            pending().await
        })
    });
    let extensions = Extensions::default();
    let mut initialize = Box::pin(builder.build(factory, extensions, LIMIT));
    tokio::select! { result = &mut initialize => { drop(result); panic!("initialization unexpectedly ended"); }, _ = beginning => {} }
    drop(initialize);
    assert!(handle.cancellation().is_cancelled());
    tokio::time::timeout(LIMIT, handle.wait_finished())
        .await
        .unwrap();
}

#[tokio::test]
async fn run_failure_and_early_exit_stop_background_tasks() {
    for mode in 0..3 {
        let builder = builder(1);
        let handle = builder.handle();
        let count = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&count);
        let factory: Arc<dyn Factory> = Arc::new(move |init: Init| -> InitFuture {
            let guard = Released(Arc::clone(&observed));
            Box::pin(async move {
                let stop = init.service.cancellation();
                init.service
                    .tasks()
                    .launch(
                        "child",
                        Box::pin(async move {
                            let _guard = guard;
                            stop.cancelled().await;
                            Ok(())
                        }),
                    )
                    .await?;
                let main: Task = match mode {
                    0 => Box::pin(async { Ok(()) }),
                    1 => Box::pin(async { Err(std::io::Error::other("main failed").into()) }),
                    _ => Box::pin(async { panic!("main panic") }),
                };
                Ok(main)
            })
        });
        let service = builder
            .build(factory, Extensions::default(), LIMIT)
            .await
            .unwrap()
            .start();
        let result = service.run(LIMIT).await;
        assert!(matches!(
            (mode, result),
            (0, Err(Error::UnexpectedExit))
                | (1, Err(Error::Run { .. }))
                | (2, Err(Error::Join(_)))
        ));
        handle.wait_finished().await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn shutdown_deadline_reclaims_uncooperative_main_and_background_futures() {
    let builder = builder(1);
    let handle = builder.handle();
    let count = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&count);
    let factory: Arc<dyn Factory> = Arc::new(move |init: Init| -> InitFuture {
        let child = Released(Arc::clone(&observed));
        let main = Released(Arc::clone(&observed));
        Box::pin(async move {
            init.service
                .tasks()
                .launch(
                    "hang",
                    Box::pin(async move {
                        let _child = child;
                        pending().await
                    }),
                )
                .await?;
            Ok(Box::pin(async move {
                let _main = main;
                pending().await
            }) as Task)
        })
    });
    let service = builder
        .build(factory, Extensions::default(), LIMIT)
        .await
        .unwrap()
        .start();
    let running = tokio::spawn(service.run(Duration::from_millis(20)));
    handle.request_stop();
    assert!(matches!(
        running.await.unwrap(),
        Err(Error::ShutdownTimeout)
    ));
    assert_eq!(handle.phase(), Phase::Stopped);
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(handle.tasks().used_capacity(), 0);
}

#[tokio::test]
async fn initialization_and_submitted_tasks_keep_the_callers_tracing_scope() {
    use tracing::{Instrument, instrument::WithSubscriber};
    async {
        let builder = builder(1);
        let extensions = Extensions::default();
        let span = tracing::info_span!("service_initialization");
        let expected = span.id();
        assert!(expected.is_some());
        let factory: Arc<dyn Factory> = Arc::new(move |init: Init| -> InitFuture {
            assert_eq!(tracing::Span::current().id(), expected);
            let expected = expected.clone();
            Box::pin(async move {
                tokio::task::yield_now().await;
                assert_eq!(tracing::Span::current().id(), expected);
                init.service
                    .tasks()
                    .launch(
                        "traced",
                        Box::pin(async move {
                            assert_eq!(tracing::Span::current().id(), expected);
                            Ok(())
                        }),
                    )
                    .await?;
                Ok(Box::pin(async move {
                    init.service.cancellation().cancelled().await;
                    Ok(())
                }) as Task)
            })
        });
        let main = builder
            .build(factory, extensions, LIMIT)
            .instrument(span)
            .await
            .unwrap();
        let service = main.start();
        service.handle().request_stop();
        service.run(LIMIT).await.unwrap();
    }
    .with_subscriber(tracing_subscriber::registry())
    .await;
}

#[tokio::test]
async fn dropping_prepared_service_releases_unstarted_main_before_finished() {
    let builder = builder(1);
    let handle = builder.handle();
    let released = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&released);
    let factory: Arc<dyn Factory> = Arc::new(move |_context: Init| -> InitFuture {
        let guard = Released(Arc::clone(&observed));
        Box::pin(async move {
            Ok(Box::pin(async move {
                let _guard = guard;
                pending().await
            }) as Task)
        })
    });
    let prepared = builder
        .build(factory, Extensions::default(), LIMIT)
        .await
        .unwrap();
    assert_eq!(handle.phase(), Phase::Starting);
    drop(prepared);
    handle.wait_finished().await;
    assert_eq!(released.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn service_completion_includes_component_cleanup_after_child_task_scope_ends() {
    let builder = builder(1);
    let handle = builder.handle();
    let (ended, child_ended) = oneshot::channel();
    let (finish, finished) = oneshot::channel();
    let signals = std::sync::Mutex::new(Some((ended, finished)));
    let factory: Arc<dyn Factory> = Arc::new(move |init: Init| -> InitFuture {
        let (ended, finished) = signals.lock().unwrap().take().unwrap();
        Box::pin(async move {
            let child = tok_service::task::Supervisor::child(1, &init.service.tasks())?;
            Ok(Box::pin(async move {
                init.service.cancellation().cancelled().await;
                child.shutdown(None).await?;
                ended.send(()).unwrap();
                finished.await.unwrap();
                Ok(())
            }) as Task)
        })
    });
    let service = builder
        .build(factory, Extensions::default(), LIMIT)
        .await
        .unwrap()
        .start();
    let running = tokio::spawn(service.run(LIMIT));
    handle.request_stop();
    child_ended.await.unwrap();
    assert_eq!(handle.phase(), Phase::Stopping);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), handle.wait_finished())
            .await
            .is_err()
    );
    finish.send(()).unwrap();
    running.await.unwrap().unwrap();
    assert_eq!(handle.phase(), Phase::Stopped);
}
