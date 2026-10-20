//! 服务工厂、启动监督和退出清理的集成测试。

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
    core::ServiceDirectory,
    runtime::{Application, RuntimeError, ServiceContext, ServiceFactory, ServiceTask, StartTask},
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

struct TestService {
    behavior: Behavior,
    stopped: Arc<AtomicUsize>,
}

impl ServiceFactory for TestService {
    fn start(&self, context: ServiceContext) -> StartTask {
        let stopped = Arc::clone(&self.stopped);
        match self.behavior {
            Behavior::FailStart => {
                Box::pin(async { Err(std::io::Error::other("启动失败").into()) })
            }
            Behavior::SlowStart => Box::pin(pending()),
            Behavior::PanicStart => panic!("测试初始化崩溃"),
            Behavior::Exit => Box::pin(async { Ok(Box::pin(async { Ok(()) }) as ServiceTask) }),
            Behavior::Panic => Box::pin(async { Ok(panic_service()) }),
            Behavior::Hang => Box::pin(async { Ok(Box::pin(pending()) as ServiceTask) }),
            Behavior::Wait => Box::pin(async move {
                Ok(Box::pin(async move {
                    context.shutdown.cancelled().await;
                    stopped.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }) as ServiceTask)
            }),
        }
    }
}

fn panic_service() -> ServiceTask {
    Box::pin(async { panic!("测试服务崩溃") })
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
    factory_name: &str,
    behavior: Behavior,
    stopped: &Arc<AtomicUsize>,
) {
    app.register_factory(
        factory_name,
        TestService {
            behavior,
            stopped: Arc::clone(stopped),
        },
    )
    .unwrap();
}

fn assert_empty(directory: &tok::infra::discovery::MemoryDirectory) {
    for service_name in ["player-gateway", "player", "guild"] {
        assert!(directory.snapshot(service_name).unwrap().is_empty());
    }
}

#[tokio::test]
async fn custom_services_use_independent_factories_and_duplicate_names_are_rejected() {
    let config = AppConfig::from_toml(
        "node_id = 'local'\ndiscovery = 'memory'\n\
         [[endpoints]]\nname = 'internal-rpc'\nprotocol = 'grpc'\naddress = '127.0.0.1:7101'\n\
         [[services]]\nservice_name = 'guild'\nendpoint = 'internal-rpc'\n\
         [[services]]\nservice_name = 'mail'\nendpoint = 'internal-rpc'",
    )
    .unwrap();
    let mut app = Application::new(config).unwrap();
    let directory = app.directory();
    let guild_stopped = Arc::new(AtomicUsize::new(0));
    let mail_stopped = Arc::new(AtomicUsize::new(0));
    register(&mut app, "guild", Behavior::Wait, &guild_stopped);
    register(&mut app, "mail", Behavior::Wait, &mail_stopped);
    assert!(matches!(
        app.register_factory("mail", TestService {
            behavior: Behavior::FailStart,
            stopped: Arc::clone(&mail_stopped),
        }),
        Err(RuntimeError::DuplicateFactory(name)) if name == "mail"
    ));
    let inspect = directory.clone();
    app.run_until(async move {
        tokio::time::timeout(Duration::from_secs(2), async {
            while inspect.snapshot("mail").unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for service_name in ["guild", "mail"] {
            let services = inspect.snapshot(service_name).unwrap();
            assert_eq!(services.len(), 1);
            assert_eq!(services[0].service_name, service_name);
            assert_eq!(services[0].address, "127.0.0.1:7101".parse().unwrap());
            assert_eq!(services[0].protocol, "grpc");
        }
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(guild_stopped.load(Ordering::SeqCst), 1);
    assert_eq!(mail_stopped.load(Ordering::SeqCst), 1);
    assert!(directory.snapshot("guild").unwrap().is_empty());
    assert!(directory.snapshot("mail").unwrap().is_empty());
}

#[tokio::test]
async fn one_factory_creates_multiple_service_identities() {
    let config = AppConfig::from_toml(
        "node_id='local'\ndiscovery='memory'\n\
         [[endpoints]]\nname='shared'\nprotocol='grpc'\naddress='127.0.0.1:7101'\n\
         [[services]]\nservice_name='first'\nfactory='worker'\nendpoint='shared'\n\
         [[services]]\nservice_name='second'\nfactory='worker'\nendpoint='shared'",
    )
    .unwrap();
    let mut app = Application::new(config).unwrap();
    let stopped = Arc::new(AtomicUsize::new(0));
    register(&mut app, "worker", Behavior::Wait, &stopped);
    let directory = app.directory();
    let inspect = directory.clone();
    app.run_until(async move {
        tokio::time::timeout(Duration::from_secs(2), async {
            while inspect.snapshot("second").unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for name in ["first", "second"] {
            let nodes = inspect.snapshot(name).unwrap();
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0].service_name, name);
        }
        assert!(inspect.snapshot("worker").unwrap().is_empty());
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(stopped.load(Ordering::SeqCst), 2);
    assert!(directory.snapshot("first").unwrap().is_empty());
    assert!(directory.snapshot("second").unwrap().is_empty());
}

#[tokio::test]
async fn services_are_registered_then_drained_on_shutdown() {
    let mut app = app();
    let directory = app.directory();
    let stopped = Arc::new(AtomicUsize::new(0));
    for factory_name in ["player-ingress", "player", "guild"] {
        register(&mut app, factory_name, Behavior::Wait, &stopped);
    }
    let inspect = directory.clone();
    app.run_until(async move {
        tokio::time::timeout(Duration::from_secs(2), async {
            while inspect.snapshot("guild").unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(inspect.snapshot("player-gateway").unwrap().len(), 1);
        assert_eq!(inspect.snapshot("player").unwrap().len(), 1);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(stopped.load(Ordering::SeqCst), 3);
    assert_empty(&directory);
}

#[tokio::test]
async fn startup_failure_rolls_back_started_services() {
    let mut app = app();
    let directory = app.directory();
    let stopped = Arc::new(AtomicUsize::new(0));
    register(&mut app, "player-ingress", Behavior::Wait, &stopped);
    register(&mut app, "player", Behavior::FailStart, &stopped);
    register(&mut app, "guild", Behavior::Wait, &stopped);
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
        register(&mut app, "player-ingress", Behavior::Wait, &stopped);
        register(&mut app, "player", behavior, &stopped);
        register(&mut app, "guild", Behavior::Wait, &stopped);
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
async fn shutdown_deadline_aborts_uncooperative_service() {
    let mut app = app();
    let directory = app.directory();
    let stopped = Arc::new(AtomicUsize::new(0));
    register(&mut app, "player-ingress", Behavior::Hang, &stopped);
    register(&mut app, "player", Behavior::Wait, &stopped);
    register(&mut app, "guild", Behavior::Wait, &stopped);
    let inspect = directory.clone();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        app.run_until(async move {
            while inspect.snapshot("guild").unwrap().is_empty() {
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
        register(&mut app, "player-ingress", Behavior::Wait, &stopped);
        register(&mut app, "player", Behavior::SlowStart, &stopped);
        register(&mut app, "guild", Behavior::Wait, &stopped);
        let inspect = directory.clone();
        let result = app
            .run_until(async move {
                if !signal {
                    return pending().await;
                }
                while inspect.snapshot("player-gateway").unwrap().is_empty() {
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
async fn missing_factory_is_rejected_before_any_service_starts() {
    let mut app = app();
    let directory = app.directory();
    let stopped = Arc::new(AtomicUsize::new(0));
    register(&mut app, "player-ingress", Behavior::Wait, &stopped);
    assert!(matches!(
        app.run_until(pending()).await,
        Err(RuntimeError::MissingFactory(name)) if name == "player"
    ));
    assert_empty(&directory);
}

#[tokio::test]
async fn dropping_run_future_removes_directory_entries() {
    let mut app = app();
    let directory = app.directory();
    let stopped = Arc::new(AtomicUsize::new(0));
    for factory_name in ["player-ingress", "player", "guild"] {
        register(&mut app, factory_name, Behavior::Wait, &stopped);
    }
    let task = tokio::spawn(app.run_until(pending()));
    tokio::time::timeout(Duration::from_secs(2), async {
        while directory.snapshot("guild").unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_empty(&directory);
}
