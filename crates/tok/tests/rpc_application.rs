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
    application::{self, Application},
    core::ServiceDirectory,
    game_rpc::{HandlerRegistry, RpcEndpoint, RpcService},
    service::{
        lifecycle::{self, Init, InitFuture},
        task::Task,
    },
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
        app.register_factory(
            name,
            Factory {
                fail: false,
                starts: Arc::clone(&starts),
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
        Err(application::Error::Core(
            tok::core::Error::DiscoveryUnavailable
        ))
    ));
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    assert!(local.snapshot("player").unwrap().is_empty());
    assert!(query.snapshot("player").is_err());
}
impl lifecycle::Factory for Factory {
    fn initialize(&self, init: Init) -> InitFuture {
        let fail = self.fail;
        self.starts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if fail {
                return Err(std::io::Error::other("初始化失败").into());
            }
            Ok(Box::pin(async move {
                init.service.cancellation().cancelled().await;
                Ok(())
            }) as Task)
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
impl lifecycle::Factory for Inspect {
    fn initialize(&self, init: Init) -> InitFuture {
        let observed = Arc::clone(&self.observed);
        Box::pin(async move {
            let nodes = init
                .extensions
                .get::<tok::application::Binding>()
                .unwrap()
                .directory
                .snapshot("guild")?;
            assert_eq!(nodes[0].node_id, "external");
            init.extensions
                .get::<tok::application::Binding>()
                .unwrap()
                .directory
                .subscribe()?;
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(async move {
                init.service.cancellation().cancelled().await;
                Ok(())
            }) as Task)
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
        app.register_factory(
            name,
            Inspect {
                observed: Arc::clone(&observed),
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
        app.register_factory(
            name,
            Factory {
                fail: false,
                starts: Arc::clone(&starts),
            },
        )
        .unwrap();
    }
    app.register_rpc_endpoint("internal", endpoint()).unwrap();
    assert!(matches!(
        app.run_until(pending()).await,
        Err(application::Error::Start { .. })
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
    app.register_factory(
        "player",
        Factory {
            fail: false,
            starts: Arc::clone(&starts),
        },
    )
    .unwrap();
    app.register_factory("guild", Factory { fail: true, starts })
        .unwrap();
    app.register_rpc_endpoint("internal", endpoint()).unwrap();
    drop(reservation);
    assert!(matches!(
        app.run_until(pending()).await,
        Err(application::Error::Start { .. })
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

fn installing_factory(init: Init) -> InitFuture {
    Box::pin(async move {
        let binding = init
            .extensions
            .get::<tok::game_rpc::Binding>()
            .unwrap()
            .clone();
        assert!(!binding.is_installed());
        binding.install(&init, Arc::new(HandlerRegistry::new()))?;
        assert!(binding.is_installed());
        Ok(Box::pin(async move {
            init.service.cancellation().cancelled().await;
            Ok(())
        }) as Task)
    })
}
#[tokio::test]
async fn configured_rpc_factories_assemble_shared_endpoint_and_reject_missing_install_before_publish()
 {
    for install in [true, false] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let mut app = Application::new(config(address)).unwrap();
        app.register_rpc_factory("player", installing_factory, RpcService::into_player)
            .unwrap();
        app.register_rpc_factory(
            "guild",
            move |init: Init| -> InitFuture {
                if install {
                    installing_factory(init)
                } else {
                    Box::pin(async move {
                        assert!(init.extensions.get::<tok::game_rpc::Binding>().is_some());
                        Ok(Box::pin(async move {
                            init.service.cancellation().cancelled().await;
                            Ok(())
                        }) as Task)
                    })
                }
            },
            RpcService::into_business,
        )
        .unwrap();
        let directory = app.directory();
        let running = app.run_until(async {
            tokio::time::timeout(Duration::from_secs(2), async {
                while directory.snapshot("guild").unwrap().is_empty() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .map_err(std::io::Error::other)?;
            assert_eq!(directory.snapshot("player").unwrap().len(), 1);
            Ok(())
        });
        let result = tokio::time::timeout(Duration::from_secs(3), running)
            .await
            .unwrap();
        if install {
            result.unwrap();
        } else {
            assert!(
                matches!(result, Err(application::Error::Start { service_name, .. }) if service_name == "guild")
            );
        }
        assert!(directory.snapshot("player").unwrap().is_empty());
        assert!(directory.snapshot("guild").unwrap().is_empty());
        tokio::net::TcpListener::bind(address).await.unwrap();
    }
}
#[tokio::test]
async fn rpc_factory_validation_is_atomic_and_custom_bindings_can_complete_the_same_endpoint() {
    let held = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = held.local_addr().unwrap();
    let mut cfg = config(address);
    // 一个工厂的两个实例不能在同一端点覆盖同名 protobuf 协议。
    for service in &mut cfg.services {
        service.factory = Some("shared".into());
    }
    let mut app = Application::new(cfg).unwrap();
    assert!(
        app.register_rpc_factory("shared", installing_factory, RpcService::into_business)
            .is_err()
    );
    // 失败没有残留工厂或半个端点，允许按显式自定义协议完成组装。
    app.register_factory(
        "shared",
        Factory {
            fail: false,
            starts: Arc::new(AtomicUsize::new(0)),
        },
    )
    .unwrap();
    app.bind_rpc(
        "player",
        RpcService::new("local", "player", Arc::new(HandlerRegistry::new()))
            .unwrap()
            .into_player(),
    )
    .unwrap();
    assert!(
        app.bind_rpc(
            "guild",
            RpcService::new("local", "guild", Arc::new(HandlerRegistry::new()))
                .unwrap()
                .into_player()
        )
        .is_err()
    );
    app.bind_rpc(
        "guild",
        RpcService::new("local", "guild", Arc::new(HandlerRegistry::new()))
            .unwrap()
            .into_business(),
    )
    .unwrap();
    assert!(app.register_rpc_endpoint("internal", endpoint()).is_err());
    // 成功声明尚未开始监听，校验不做网络 I/O。
    drop(held);
    tokio::net::TcpListener::bind(address).await.unwrap();
}
#[tokio::test]
async fn one_rpc_factory_can_initialize_multiple_identities_on_distinct_endpoints() {
    let one = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let two = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = AppConfig::from_toml(&format!("node_id='local'\ndiscovery='memory'\n[[endpoints]]\nname='a'\nprotocol='grpc'\naddress='{}'\n[[endpoints]]\nname='b'\nprotocol='grpc'\naddress='{}'\n[[services]]\nservice_name='one'\nfactory='shared'\nendpoint='a'\n[[services]]\nservice_name='two'\nfactory='shared'\nendpoint='b'", one.local_addr().unwrap(), two.local_addr().unwrap())).unwrap();
    drop((one, two));
    let mut app = Application::new(config).unwrap();
    app.register_rpc_factory("shared", installing_factory, RpcService::into_business)
        .unwrap();
    let directory = app.directory();
    app.run_until(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            while directory.snapshot("two").unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(std::io::Error::other)?;
        assert_eq!(directory.snapshot("one").unwrap().len(), 1);
        Ok(())
    })
    .await
    .unwrap();
}

#[test]
fn rpc_factory_rejects_endpointless_service_without_retaining_registration() {
    let config = AppConfig::from_toml(
        "node_id='local'\ndiscovery='memory'\n[[services]]\nservice_name='audit'",
    )
    .unwrap();
    let mut app = Application::new(config).unwrap();
    assert!(
        app.register_rpc_factory("audit", installing_factory, RpcService::into_business)
            .is_err()
    );
    app.register_factory(
        "audit",
        Factory {
            fail: false,
            starts: Arc::new(AtomicUsize::new(0)),
        },
    )
    .unwrap();
}
