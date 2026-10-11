use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tok::{
    AppConfig,
    core::{ServiceDirectory, ServiceKind},
    runtime::{Application, RoleContext, RoleFactory, RoleTask, RuntimeError, StartTask},
};

enum Behavior {
    Wait,
    FailStart,
    Exit,
    Panic,
    Hang,
    SlowStart,
    PanicStart,
}

struct TestRole {
    behavior: Behavior,
    stopped: Arc<AtomicUsize>,
}

impl RoleFactory for TestRole {
    fn start(&self, context: RoleContext) -> StartTask {
        let stopped = self.stopped.clone();
        match self.behavior {
            Behavior::FailStart => {
                Box::pin(async { Err(std::io::Error::other("启动失败").into()) })
            }
            Behavior::SlowStart => Box::pin(pending()),
            Behavior::PanicStart => panic!("测试初始化崩溃"),
            Behavior::Exit => Box::pin(async { Ok(Box::pin(async { Ok(()) }) as RoleTask) }),
            Behavior::Panic => Box::pin(async { Ok(Box::pin(panic_role()) as RoleTask) }),
            Behavior::Hang => Box::pin(async { Ok(Box::pin(pending()) as RoleTask) }),
            Behavior::Wait => Box::pin(async move {
                Ok(Box::pin(async move {
                    context.shutdown.cancelled().await;
                    stopped.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }) as RoleTask)
            }),
        }
    }
}

async fn panic_role() -> Result<(), tok::runtime::RoleError> {
    panic!("测试角色崩溃")
}

fn app() -> Application {
    Application::new(
        AppConfig::from_toml(include_str!(
            "../../../examples/demo-game/config/local.toml"
        ))
        .unwrap(),
    )
    .unwrap()
    .with_timeouts(Duration::from_millis(100), Duration::from_millis(100))
}

fn register(
    app: &mut Application,
    kind: ServiceKind,
    behavior: Behavior,
    stopped: &Arc<AtomicUsize>,
) {
    app.register(
        kind,
        TestRole {
            behavior,
            stopped: stopped.clone(),
        },
    )
    .unwrap();
}

fn assert_empty(directory: &tok::infra::discovery::MemoryDirectory) {
    for kind in [
        ServiceKind::OutsideGateway,
        ServiceKind::Player,
        ServiceKind::GlobalInstance,
    ] {
        assert!(directory.snapshot(kind).unwrap().is_empty());
    }
}

#[tokio::test]
async fn roles_are_registered_then_drained_on_shutdown() {
    let mut app = app();
    let directory = app.directory();
    let stopped = Arc::new(AtomicUsize::new(0));
    for kind in [
        ServiceKind::OutsideGateway,
        ServiceKind::Player,
        ServiceKind::GlobalInstance,
    ] {
        register(&mut app, kind, Behavior::Wait, &stopped);
    }
    let inspect = directory.clone();
    app.run_until(async move {
        tokio::time::timeout(Duration::from_secs(2), async {
            while inspect
                .snapshot(ServiceKind::GlobalInstance)
                .unwrap()
                .is_empty()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            inspect.snapshot(ServiceKind::OutsideGateway).unwrap().len(),
            1
        );
        assert_eq!(inspect.snapshot(ServiceKind::Player).unwrap().len(), 1);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(stopped.load(Ordering::SeqCst), 3);
    assert_empty(&directory);
}

#[tokio::test]
async fn startup_failure_rolls_back_started_roles() {
    let mut app = app();
    let directory = app.directory();
    let stopped = Arc::new(AtomicUsize::new(0));
    register(
        &mut app,
        ServiceKind::OutsideGateway,
        Behavior::Wait,
        &stopped,
    );
    register(&mut app, ServiceKind::Player, Behavior::FailStart, &stopped);
    register(
        &mut app,
        ServiceKind::GlobalInstance,
        Behavior::Wait,
        &stopped,
    );
    assert!(matches!(
        app.run_until(pending()).await,
        Err(RuntimeError::Start { .. })
    ));
    assert_eq!(stopped.load(Ordering::SeqCst), 1);
    assert_empty(&directory);
}

#[tokio::test]
async fn unexpected_exit_and_panic_stop_the_process() {
    for behavior in [Behavior::Exit, Behavior::Panic, Behavior::PanicStart] {
        let mut app = app();
        let directory = app.directory();
        let stopped = Arc::new(AtomicUsize::new(0));
        register(
            &mut app,
            ServiceKind::OutsideGateway,
            Behavior::Wait,
            &stopped,
        );
        register(&mut app, ServiceKind::Player, behavior, &stopped);
        register(
            &mut app,
            ServiceKind::GlobalInstance,
            Behavior::Wait,
            &stopped,
        );
        let error = app.run_until(pending()).await.unwrap_err();
        assert!(matches!(
            error,
            RuntimeError::UnexpectedExit(_) | RuntimeError::Task(_)
        ));
        assert!((1..=2).contains(&stopped.load(Ordering::SeqCst)));
        assert_empty(&directory);
    }
}

#[tokio::test]
async fn shutdown_deadline_aborts_uncooperative_role() {
    let mut app = app();
    let directory = app.directory();
    let stopped = Arc::new(AtomicUsize::new(0));
    register(
        &mut app,
        ServiceKind::OutsideGateway,
        Behavior::Hang,
        &stopped,
    );
    register(&mut app, ServiceKind::Player, Behavior::Wait, &stopped);
    register(
        &mut app,
        ServiceKind::GlobalInstance,
        Behavior::Wait,
        &stopped,
    );
    let inspect = directory.clone();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        app.run_until(async move {
            while inspect
                .snapshot(ServiceKind::GlobalInstance)
                .unwrap()
                .is_empty()
            {
                tokio::task::yield_now().await;
            }
            Ok(())
        }),
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(RuntimeError::ShutdownTimeout)));
    assert_eq!(stopped.load(Ordering::SeqCst), 2);
    assert_empty(&directory);
}

#[tokio::test]
async fn startup_deadline_and_signal_cancel_pending_initialization() {
    for signal in [false, true] {
        let mut app = app();
        let directory = app.directory();
        let stopped = Arc::new(AtomicUsize::new(0));
        register(
            &mut app,
            ServiceKind::OutsideGateway,
            Behavior::Wait,
            &stopped,
        );
        register(&mut app, ServiceKind::Player, Behavior::SlowStart, &stopped);
        register(
            &mut app,
            ServiceKind::GlobalInstance,
            Behavior::Wait,
            &stopped,
        );
        let inspect = directory.clone();
        let result = app
            .run_until(async move {
                if !signal {
                    return pending().await;
                }
                while inspect
                    .snapshot(ServiceKind::OutsideGateway)
                    .unwrap()
                    .is_empty()
                {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await;
        if signal {
            assert!(result.is_ok());
        } else {
            assert!(matches!(result, Err(RuntimeError::StartTimeout(_))));
        }
        assert_eq!(stopped.load(Ordering::SeqCst), 1);
        assert_empty(&directory);
    }
}

#[tokio::test]
async fn missing_factory_is_rejected_before_any_role_starts() {
    let mut app = app();
    let directory = app.directory();
    let stopped = Arc::new(AtomicUsize::new(0));
    register(
        &mut app,
        ServiceKind::OutsideGateway,
        Behavior::Wait,
        &stopped,
    );
    assert!(matches!(
        app.run_until(pending()).await,
        Err(RuntimeError::MissingRole(ServiceKind::Player))
    ));
    assert_empty(&directory);
}

#[tokio::test]
async fn dropping_run_future_removes_directory_entries() {
    let mut app = app();
    let directory = app.directory();
    let stopped = Arc::new(AtomicUsize::new(0));
    for kind in [
        ServiceKind::OutsideGateway,
        ServiceKind::Player,
        ServiceKind::GlobalInstance,
    ] {
        register(&mut app, kind, Behavior::Wait, &stopped);
    }
    let task = tokio::spawn(app.run_until(pending()));
    tokio::time::timeout(Duration::from_secs(2), async {
        while directory
            .snapshot(ServiceKind::GlobalInstance)
            .unwrap()
            .is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_empty(&directory);
}
