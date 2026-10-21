//! 真实 Redis 请求锁：独立客户端及跨进程竞争、过期、条件释放与失败。
use std::{sync::Arc, time::Duration};
use tok_core::{
    Error,
    lock::{Backend as LockBackend, Key as LockKey},
};
use tok_infra::locks::{Redis, RedisConfig};

fn test_config() -> RedisConfig {
    RedisConfig {
        url: std::env::var("TOK_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379/".into()),
        namespace: format!("lock-{}", uuid::Uuid::new_v4()),
        operation_timeout: Duration::from_millis(500),
        max_pending: 16,
    }
}
fn key(namespace: &str, bid: &str) -> LockKey {
    LockKey {
        namespace: namespace.into(),
        bid: bid.into(),
    }
}
const TTL: Duration = Duration::from_secs(3);

#[tokio::test]
#[ignore = "需要真实 Redis；通过 just locks-test 显式运行"]
async fn independent_clients_compete_and_stale_release_preserves_new_lease() {
    let config = test_config();
    let first = Redis::new(config.clone()).unwrap();
    let second = Redis::new(config).unwrap();
    let short = Duration::from_millis(80);
    let old = first
        .acquire(key("guild", "1"), Duration::ZERO, short)
        .await
        .unwrap();
    let token = old.token().to_owned();
    assert!(matches!(
        second.acquire(key("guild", "1"), Duration::ZERO, TTL).await,
        Err(Error::LockWaitTimeout)
    ));
    let different_bid = second
        .acquire(key("guild", "2"), Duration::ZERO, TTL)
        .await
        .unwrap();
    let different_namespace = second
        .acquire(key("team", "1"), Duration::ZERO, TTL)
        .await
        .unwrap();
    different_bid.release().await.unwrap();
    different_namespace.release().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let current = second
        .acquire(key("guild", "1"), Duration::ZERO, TTL)
        .await
        .unwrap();
    assert_ne!(token, current.token());
    old.release().await.unwrap();
    assert!(matches!(
        first
            .acquire(key("guild", "1"), Duration::from_millis(20), TTL)
            .await,
        Err(Error::LockWaitTimeout)
    ));
    current.release().await.unwrap();
    first
        .acquire(key("guild", "1"), Duration::ZERO, TTL)
        .await
        .unwrap()
        .release()
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "需要真实 Redis；通过 just locks-test 显式运行"]
async fn contention_has_one_winner_and_abandoned_lease_expires() {
    let config = test_config();
    let a = Redis::new(config.clone()).unwrap();
    let b = Redis::new(config).unwrap();
    let (a_result, b_result) = tokio::join!(
        a.acquire(key("guild", "1"), Duration::ZERO, TTL),
        b.acquire(key("guild", "1"), Duration::ZERO, TTL)
    );
    let lease = match (a_result, b_result) {
        (Ok(lease), Err(Error::LockWaitTimeout)) | (Err(Error::LockWaitTimeout), Ok(lease)) => {
            lease
        }
        _ => panic!("必须恰好一个持有者"),
    };
    lease.release().await.unwrap();
    let lease = a
        .acquire(key("guild", "1"), Duration::ZERO, Duration::from_millis(60))
        .await
        .unwrap();
    drop(lease);
    b.acquire(key("guild", "1"), Duration::from_millis(300), TTL)
        .await
        .unwrap()
        .release()
        .await
        .unwrap();
}

#[tokio::test]
async fn unreachable_backend_and_invalid_config_are_bounded() {
    let config = RedisConfig {
        url: "redis://127.0.0.1:1/".into(),
        operation_timeout: Duration::from_millis(50),
        ..test_config()
    };
    let backend = Redis::new(config).unwrap();
    assert!(matches!(
        backend
            .acquire(key("guild", "1"), Duration::ZERO, TTL)
            .await,
        Err(Error::CoordinationUnavailable(_))
    ));
    assert!(
        Redis::new(RedisConfig {
            namespace: "bad{tag}".into(),
            ..test_config()
        })
        .is_err()
    );
    assert!(
        Redis::new(RedisConfig {
            max_pending: 0,
            ..test_config()
        })
        .is_err()
    );
    assert!(
        !format!(
            "{:?}",
            RedisConfig {
                url: "redis://user:secret@localhost/".into(),
                ..test_config()
            }
        )
        .contains("secret")
    );
}

#[tokio::test]
#[ignore = "需要真实 Redis；通过 just locks-test 显式运行"]
async fn release_reports_backend_failure_instead_of_claiming_success() {
    let config = test_config();
    let (backend, stop, proxy, _) = proxy(&config, false).await;
    let lease = backend
        .acquire(
            key("guild", "1"),
            Duration::ZERO,
            Duration::from_millis(100),
        )
        .await
        .unwrap();
    stop.cancel();
    proxy.await.unwrap();
    assert!(matches!(
        lease.release().await,
        Err(Error::CoordinationUnavailable(_))
    ));
    tokio::time::sleep(Duration::from_millis(120)).await;
}

#[tokio::test]
#[ignore = "需要真实 Redis；通过 just locks-test 显式运行"]
async fn unknown_acquisition_result_is_not_retried_and_expires_by_ttl() {
    use std::sync::atomic::Ordering;
    let config = test_config();
    let (backend, stop, proxy, writes) = proxy(&config, true).await;
    assert!(matches!(
        backend
            .acquire(key("guild", "1"), Duration::ZERO, Duration::from_secs(2))
            .await,
        Err(Error::CoordinationUnavailable(_))
    ));
    assert_eq!(writes.load(Ordering::SeqCst), 1, "未知结果不能重试 SET");
    let client = redis::Client::open(config.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let redis_key = format!("tok:lock:{{{}:6775696c64:31}}", config.namespace);
    let remaining: i64 = redis::cmd("PTTL")
        .arg(&redis_key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(remaining > 0, "请求虽失败，远端已经持锁");
    stop.cancel();
    proxy.await.unwrap();
    // 不猜测远端 token，也不删除未知锁；另一个客户端只能等待 TTL 后取得新锁。
    Redis::new(config)
        .unwrap()
        .acquire(key("guild", "1"), Duration::from_secs(3), TTL)
        .await
        .unwrap()
        .release()
        .await
        .unwrap();
}

async fn proxy(
    config: &RedisConfig,
    mute_set_reply: bool,
) -> (
    Redis,
    tokio_util::sync::CancellationToken,
    tokio::task::JoinHandle<()>,
    Arc<std::sync::atomic::AtomicUsize>,
) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // 独立 TCP 代理只转发本测试连接，不影响共享 Redis；可丢弃 SET 的回应模拟未知结果。
    let info = redis::Client::open(config.url.as_str())
        .unwrap()
        .get_connection_info()
        .clone();
    let redis::ConnectionAddr::Tcp(host, port) = info.addr().clone() else {
        panic!("测试需要 TCP Redis URL")
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    let stop = tokio_util::sync::CancellationToken::new();
    let shutdown = stop.clone();
    let writes = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&writes);
    let proxy = tokio::spawn(async move {
        let (incoming, _) = listener.accept().await.unwrap();
        let outgoing = tokio::net::TcpStream::connect((host.as_str(), port))
            .await
            .unwrap();
        let (mut input, mut output) = incoming.into_split();
        let (mut replies, mut commands) = outgoing.into_split();
        let forward = async {
            let mut buffer = [0u8; 4096];
            let mut tail = Vec::new();
            loop {
                let count = input.read(&mut buffer).await?;
                if count == 0 {
                    break;
                }
                tail.extend_from_slice(&buffer[..count]);
                let marker = b"\r\n$3\r\nSET\r\n";
                if tail.windows(marker.len()).any(|window| window == marker) {
                    observed.fetch_add(1, Ordering::SeqCst);
                }
                // 测试请求长度有界，只保留跨读取边界识别命令所需的尾部。
                tail = tail[tail.len().saturating_sub(marker.len() - 1)..].to_vec();
                commands.write_all(&buffer[..count]).await?;
            }
            Ok::<(), std::io::Error>(())
        };
        let backward = async {
            let mut buffer = [0u8; 4096];
            loop {
                let count = replies.read(&mut buffer).await?;
                if count == 0 {
                    break;
                }
                if !mute_set_reply || observed.load(Ordering::SeqCst) == 0 {
                    output.write_all(&buffer[..count]).await?;
                }
            }
            Ok::<(), std::io::Error>(())
        };
        tokio::select! {
            result = async { tokio::try_join!(forward, backward) } => { result.unwrap(); },
            () = shutdown.cancelled() => {},
        }
    });
    // 保留认证信息、DB 和查询参数，只改连接目的地。
    let (_, rest) = config.url.split_once("://").unwrap();
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let auth = authority
        .rsplit_once('@')
        .map(|(auth, _)| format!("{auth}@"))
        .unwrap_or_default();
    let backend = Redis::new(RedisConfig {
        url: format!("redis://{auth}127.0.0.1:{proxy_port}/{path}"),
        ..config.clone()
    })
    .unwrap();
    (backend, stop, proxy, writes)
}

#[tokio::test]
#[ignore = "需要真实 Redis；通过 just locks-test 显式运行"]
async fn separate_processes_share_the_same_lock() {
    if let Ok(namespace) = std::env::var("TOK_LOCK_CHILD_NAMESPACE") {
        let backend = Redis::new(RedisConfig {
            namespace,
            ..test_config()
        })
        .unwrap();
        let result = backend
            .acquire(key("guild", "1"), Duration::from_millis(20), TTL)
            .await;
        match std::env::var("TOK_LOCK_CHILD_MODE").unwrap().as_str() {
            "blocked" => assert!(matches!(result, Err(Error::LockWaitTimeout))),
            "free" => result.unwrap().release().await.unwrap(),
            _ => panic!("非法子进程测试模式"),
        }
        return;
    }
    let config = test_config();
    let backend = Arc::new(Redis::new(config.clone()).unwrap());
    let held = backend
        .acquire(key("guild", "1"), Duration::ZERO, TTL)
        .await
        .unwrap();
    child(&config, "blocked").await;
    held.release().await.unwrap();
    child(&config, "free").await;
}
async fn child(config: &RedisConfig, mode: &str) {
    let namespace = config.namespace.clone();
    let url = config.url.clone();
    let mode = mode.to_owned();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "separate_processes_share_the_same_lock",
                "--nocapture",
            ])
            .env("TOK_LOCK_CHILD_NAMESPACE", namespace)
            .env("TOK_TEST_REDIS_URL", url)
            .env("TOK_LOCK_CHILD_MODE", mode)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
