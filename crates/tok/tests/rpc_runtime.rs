//! 共享 RPC 端点的配置、初始化清理和发现注入集成测试。

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
    rpc::{HandlerRegistry, RpcEndpoint, RpcService},
    runtime::{Application, RuntimeError, ServiceContext, ServiceFactory, ServiceTask, StartTask},
};

struct Factory {
    fail: bool,
    starts: Arc<AtomicUsize>,
}

#[tokio::test]
async fn redis_startup_timeout_does_not_start_or_publish_business() {
    // TCP 可连接但不返回 Redis 协议，覆盖连接建立后的完整操作期限。
    let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = config("127.0.0.1:7001".parse().unwrap());
    config.discovery = tok::DiscoveryBackend::Redis;
    config.redis_discovery = Some(tok::infra::discovery::redis::RedisDiscoveryConfig {
        url: format!("redis://{}/", silent.local_addr().unwrap()),
        lease_ttl_ms: 1000,
        refresh_interval_ms: 50,
        operation_timeout_ms: 100,
        ..Default::default()
    });
    let mut app = Application::new(config).unwrap();
    let starts = Arc::new(AtomicUsize::new(0));
    for name in ["player", "guild"] {
        app.register(
            name,
            Factory {
                fail: false,
                starts: starts.clone(),
            },
        )
        .unwrap();
    }
    let local = app.directory();
    let query = app.service_directory();
    let result = tokio::time::timeout(Duration::from_secs(2), app.run_until(pending()))
        .await
        .unwrap();
    assert!(matches!(
        result,
        Err(RuntimeError::Core(tok::core::Error::DiscoveryUnavailable))
    ));
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    assert!(local.snapshot("player").unwrap().is_empty());
    assert!(query.snapshot("player").is_err());
}
impl ServiceFactory for Factory {
    fn start(&self, context: ServiceContext) -> StartTask {
        let fail = self.fail;
        self.starts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if fail {
                return Err(std::io::Error::other("初始化失败").into());
            }
            Ok(Box::pin(async move {
                context.shutdown.cancelled().await;
                Ok(())
            }) as ServiceTask)
        })
    }
}
fn config(address: std::net::SocketAddr) -> AppConfig {
    AppConfig::from_toml(&format!("node_id='local'\ndiscovery='memory'\n[[endpoints]]\nname='internal'\nprotocol='grpc'\naddress='{address}'\n[[services]]\nservice_name='player'\nendpoint='internal'\n[[services]]\nservice_name='guild'\nendpoint='internal'")).unwrap()
}
fn endpoint() -> RpcEndpoint {
    RpcEndpoint::new()
        .add_service(
            "player",
            RpcService::new("local", "player", Arc::new(HandlerRegistry::new()))
                .unwrap()
                .into_player(),
        )
        .unwrap()
        .add_service(
            "guild",
            RpcService::new("local", "guild", Arc::new(HandlerRegistry::new()))
                .unwrap()
                .into_business(),
        )
        .unwrap()
}

struct Inspect {
    observed: Arc<AtomicUsize>,
}
impl ServiceFactory for Inspect {
    fn start(&self, context: ServiceContext) -> StartTask {
        let observed = self.observed.clone();
        Box::pin(async move {
            let nodes = context.directory.snapshot("guild")?;
            assert_eq!(nodes[0].node_id, "external");
            context.directory.subscribe()?;
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(async move {
                context.shutdown.cancelled().await;
                Ok(())
            }) as ServiceTask)
        })
    }
}

#[tokio::test]
async fn service_factories_use_injected_directory_without_unregistering_external_nodes() {
    let external = tok::infra::discovery::MemoryDirectory::default();
    external
        .register(tok::core::ServiceDescriptor {
            node_id: "external".into(),
            service_name: "guild".into(),
            protocol: "grpc".into(),
            address: "127.0.0.1:7002".parse().unwrap(),
        })
        .unwrap();
    let mut app = Application::new(config("127.0.0.1:7001".parse().unwrap()))
        .unwrap()
        .with_service_directory(Arc::new(external.clone()));
    let local = app.directory();
    let observed = Arc::new(AtomicUsize::new(0));
    for name in ["player", "guild"] {
        app.register(
            name,
            Inspect {
                observed: observed.clone(),
            },
        )
        .unwrap();
    }
    app.run_until(async {
        while local.snapshot("guild").unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(observed.load(Ordering::SeqCst), 2);
    assert!(local.snapshot("guild").unwrap().is_empty());
    assert_eq!(external.snapshot("guild").unwrap().len(), 1);
}

#[tokio::test]
async fn bind_failure_happens_before_business_initialization() {
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut app = Application::new(config(occupied.local_addr().unwrap())).unwrap();
    let directory = app.directory();
    let starts = Arc::new(AtomicUsize::new(0));
    for name in ["player", "guild"] {
        app.register(
            name,
            Factory {
                fail: false,
                starts: starts.clone(),
            },
        )
        .unwrap();
    }
    app.register_rpc_endpoint("internal", endpoint()).unwrap();
    assert!(matches!(
        app.run_until(pending()).await,
        Err(RuntimeError::Start { .. })
    ));
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    assert!(directory.snapshot("player").unwrap().is_empty());
}

#[tokio::test]
async fn initialization_failure_cleans_up_shared_listener_and_directory() {
    let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reservation.local_addr().unwrap();
    let mut app = Application::new(config(address))
        .unwrap()
        .with_timeouts(Duration::from_secs(2), Duration::from_secs(2));
    let directory = app.directory();
    let starts = Arc::new(AtomicUsize::new(0));
    app.register(
        "player",
        Factory {
            fail: false,
            starts: starts.clone(),
        },
    )
    .unwrap();
    app.register("guild", Factory { fail: true, starts })
        .unwrap();
    app.register_rpc_endpoint("internal", endpoint()).unwrap();
    drop(reservation);
    assert!(matches!(
        app.run_until(pending()).await,
        Err(RuntimeError::Start { .. })
    ));
    assert!(directory.snapshot("player").unwrap().is_empty());
    assert!(directory.snapshot("guild").unwrap().is_empty());
    tokio::net::TcpListener::bind(address).await.unwrap();
}

#[tokio::test]
async fn configuration_requires_complete_and_unique_endpoint_bindings() {
    let mut app = Application::new(config("127.0.0.1:7001".parse().unwrap())).unwrap();
    assert!(app.register_rpc_endpoint("missing", endpoint()).is_err());
    let incomplete = RpcEndpoint::new()
        .add_service(
            "player",
            RpcService::new("local", "player", Arc::new(HandlerRegistry::new()))
                .unwrap()
                .into_player(),
        )
        .unwrap();
    assert!(app.register_rpc_endpoint("internal", incomplete).is_err());
    app.register_rpc_endpoint("internal", endpoint()).unwrap();
    assert!(app.register_rpc_endpoint("internal", endpoint()).is_err());
}
