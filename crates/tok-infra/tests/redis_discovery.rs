//! 使用独立 namespace 的真实 Redis 租约与缓存集成测试，显式运行 ignored 测试。

use std::{sync::Arc, time::Duration};
use tok_core::{ServiceDescriptor, ServiceDirectory, ServiceRegistry};
use tok_infra::discovery::redis::{RedisDirectory, RedisDiscoveryConfig};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

fn config() -> RedisDiscoveryConfig {
    RedisDiscoveryConfig {
        url: std::env::var("TOK_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379/".into()),
        namespace: format!("test-{}", uuid::Uuid::new_v4()),
        lease_ttl_ms: 800,
        refresh_interval_ms: 40,
        operation_timeout_ms: 200,
        max_nodes: 8,
    }
}
fn service(port: u16) -> ServiceDescriptor {
    ServiceDescriptor {
        node_id: "game".into(),
        service_name: "guild".into(),
        protocol: "grpc".into(),
        address: format!("127.0.0.1:{port}").parse().unwrap(),
    }
}
async fn wait_for(directory: &RedisDirectory, port: Option<u16>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if directory.snapshot("guild").is_ok_and(|nodes| match port {
                Some(port) => nodes.len() == 1 && nodes[0].address.port() == port,
                None => nodes.is_empty(),
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("目录未按期收敛");
}
fn run(
    tasks: &mut JoinSet<tok_core::Result<()>>,
    directory: &Arc<RedisDirectory>,
    stop: &CancellationToken,
) -> tokio::task::AbortHandle {
    let directory = Arc::clone(directory);
    let stop = stop.child_token();
    tasks.spawn(async move { directory.run(stop).await })
}

#[tokio::test]
#[ignore = "需要真实 Redis；通过 just redis-test 显式运行"]
async fn registration_cache_watch_expiry_restart_and_old_instance_safety() {
    let config = config();
    let first = RedisDirectory::new(config.clone(), "game").unwrap();
    let replacement = RedisDirectory::new(config.clone(), "game").unwrap();
    let observer = RedisDirectory::new(config.clone(), "observer").unwrap();
    let mut isolated = config.clone();
    isolated.namespace.push_str("-other");
    let isolated = RedisDirectory::new(isolated, "game").unwrap();
    assert!(observer.snapshot("guild").is_err());
    observer.initialize().await.unwrap();
    isolated.initialize().await.unwrap();
    first.register(service(17001)).await.unwrap();
    assert!(replacement.register(service(17002)).await.is_err());
    let stop = CancellationToken::new();
    let mut tasks = JoinSet::new();
    let running = run(&mut tasks, &first, &stop);
    run(&mut tasks, &observer, &stop);
    let mut watch = observer.subscribe().unwrap();
    wait_for(&observer, Some(17001)).await;
    tokio::time::timeout(Duration::from_secs(1), watch.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(isolated.snapshot("guild").unwrap().is_empty());
    first.unregister("game", "guild").await.unwrap();
    wait_for(&observer, None).await;
    first.register(service(17001)).await.unwrap();
    wait_for(&observer, Some(17001)).await;
    // 没有注销的崩溃通过 TTL 摘除，旧实例对象仍保留以验证条件操作。
    running.abort();
    wait_for(&observer, None).await;
    replacement.register(service(17002)).await.unwrap();
    run(&mut tasks, &replacement, &stop);
    first.withdraw().await.unwrap();
    wait_for(&observer, Some(17002)).await;
    replacement.withdraw().await.unwrap();
    wait_for(&observer, None).await;
    stop.cancel();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(result) => result.unwrap(),
            Err(error) => assert!(error.is_cancelled()),
        }
    }
}

#[tokio::test]
#[ignore = "需要真实 Redis；通过 just redis-test 显式运行"]
async fn lost_lease_fails_worker_and_stale_cache_is_not_usable_forever() {
    let config = config();
    let first = RedisDirectory::new(config.clone(), "game").unwrap();
    let replacement = RedisDirectory::new(config.clone(), "game").unwrap();
    first.register(service(17003)).await.unwrap();
    first.initialize().await.unwrap();
    assert_eq!(first.snapshot("guild").unwrap().len(), 1);
    tokio::time::sleep(Duration::from_millis(config.lease_ttl_ms + 50)).await;
    assert!(first.snapshot("guild").is_err());
    replacement.register(service(17004)).await.unwrap();
    assert!(first.run(CancellationToken::new()).await.is_err());
    assert!(first.snapshot("guild").is_err());
    first.withdraw().await.unwrap();
    replacement.initialize().await.unwrap();
    assert_eq!(
        replacement.snapshot("guild").unwrap()[0].address.port(),
        17004
    );
    replacement.withdraw().await.unwrap();
}
