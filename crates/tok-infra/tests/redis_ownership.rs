//! Redis 所有权的真实原子竞争、代次精度与失效语义。

use std::time::Duration;
use tok_core::{
    Error,
    coordination::{OwnershipStore, SessionOwner, SessionOwnership},
};
use tok_infra::ownership::redis::{RedisOwnership, RedisOwnershipConfig};

fn config() -> RedisOwnershipConfig {
    RedisOwnershipConfig {
        url: std::env::var("TOK_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379/".into()),
        namespace: format!("ownership-{}", uuid::Uuid::new_v4()),
        operation_timeout_ms: 500,
    }
}
fn owner(node: &str) -> SessionOwner {
    SessionOwner {
        node_id: node.into(),
        service_name: "gateway".into(),
    }
}

#[tokio::test]
#[ignore = "需要真实 Redis；通过 just ownership-test 显式运行"]
async fn transfer_compares_full_owner_and_preserves_large_epoch_precision() {
    let config = config();
    let store = RedisOwnership::new(config.clone()).unwrap();
    seed(&config, "9007199254740992").await;
    let old = acquire(&store, "s", "a", None, Duration::from_secs(3))
        .await
        .unwrap();
    let mut wrong = old.clone();
    wrong.owner.node_id = "foreign".into();
    assert!(
        store
            .transfer(wrong, owner("b"), Duration::from_secs(3))
            .await
            .is_err()
    );
    let (a, b) = tokio::join!(
        store.transfer(old.clone(), owner("b"), Duration::from_secs(3)),
        store.transfer(old.clone(), owner("c"), Duration::from_secs(3))
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let current = store.lookup("p").await.unwrap().unwrap();
    assert_eq!(current.identity.session_id, old.identity.session_id);
    assert_eq!(current.identity.epoch, 9007199254740994);
    assert!(!store.release(old.clone()).await.unwrap());
    assert!(store.renew(old, Duration::from_secs(3)).await.is_err());
    assert!(store.release(current).await.unwrap());
    cleanup(&config).await;
}
fn keys(config: &RedisOwnershipConfig) -> [String; 3] {
    // 约定的测试玩家 p = 0x70；仅删除本次独立 namespace 的三个测试键。
    [
        format!("tok:ownership:{{{}:70}}:lease", config.namespace),
        format!("tok:ownership:{{{}:70}}:epoch", config.namespace),
        format!("tok:ownership:{{{}:70}}:session", config.namespace),
    ]
}

#[tokio::test]
#[ignore = "需要真实 Redis；通过 just ownership-test 显式运行"]
async fn relogin_atomically_rejects_superseded_credentials_after_lease_release() {
    use tok_core::credentials::ResumeClaims;
    let config = config();
    let store = RedisOwnership::new(config.clone()).unwrap();
    let ttl = Duration::from_secs(2);
    let first = acquire(&store, "s1", "a", None, ttl).await.unwrap();
    let moved = store.transfer(first, owner("b"), ttl).await.unwrap();
    store.release(moved).await.unwrap();
    let claims = |session: &str| ResumeClaims {
        player_id: "p".into(),
        session_id: session.into(),
    };
    let next = store
        .acquire_for_relogin(claims("s1"), "s2".into(), owner("b"), None, ttl)
        .await
        .unwrap();
    store.release(next).await.unwrap();
    assert!(matches!(
        store
            .acquire_for_relogin(claims("s1"), "stale".into(), owner("a"), None, ttl)
            .await,
        Err(Error::OwnershipConflict)
    ));
    let fresh = store
        .acquire_for_relogin(claims("s2"), "s3".into(), owner("a"), None, ttl)
        .await
        .unwrap();
    assert_eq!(fresh.identity.session_id, "s3");
    cleanup(&config).await;
}
async fn seed(config: &RedisOwnershipConfig, epoch: &str) {
    let mut connection = redis::Client::open(config.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("SET")
        .arg(&keys(config)[1])
        .arg(epoch)
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
}
async fn cleanup(config: &RedisOwnershipConfig) {
    let mut connection = redis::Client::open(config.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("DEL")
        .arg(keys(config).as_slice())
        .query_async::<i64>(&mut connection)
        .await
        .unwrap();
}
async fn acquire(
    store: &RedisOwnership,
    session: &str,
    node: &str,
    epoch: Option<u64>,
    ttl: Duration,
) -> tok_core::Result<SessionOwnership> {
    store
        .acquire("p".into(), session.into(), owner(node), epoch, ttl)
        .await
}

#[tokio::test]
#[ignore = "需要真实 Redis；通过 just ownership-test 显式运行"]
async fn atomic_competition_takeover_full_identity_and_persistent_epochs() {
    let config = config();
    let first = RedisOwnership::new(config.clone()).unwrap();
    let second = RedisOwnership::new(config.clone()).unwrap();
    let ttl = Duration::from_secs(2);
    let (a, b) = tokio::join!(
        acquire(&first, "s1", "g1", None, ttl),
        acquire(&second, "s2", "g2", None, ttl)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let old = match (a, b) {
        (Ok(record), Err(Error::OwnershipConflict))
        | (Err(Error::OwnershipConflict), Ok(record)) => record,
        other => panic!("竞争结果错误：{other:?}"),
    };
    assert_eq!(second.lookup("p").await.unwrap(), Some(old.clone()));
    assert!(matches!(
        acquire(&second, "s3", "g3", Some(old.identity.epoch + 1), ttl).await,
        Err(Error::OwnershipConflict)
    ));
    let new = acquire(&second, "s3", "g3", Some(old.identity.epoch), ttl)
        .await
        .unwrap();
    assert!(new.identity.epoch > old.identity.epoch);
    assert!(matches!(
        first.renew(old.clone(), ttl).await,
        Err(Error::OwnershipConflict)
    ));
    assert!(!first.release(old).await.unwrap());
    for field in 0..4 {
        let mut wrong = new.clone();
        match field {
            0 => wrong.identity.session_id.push('x'),
            1 => wrong.owner.node_id.push('x'),
            2 => wrong.owner.service_name.push('x'),
            _ => wrong.identity.epoch += 1,
        }
        assert!(matches!(
            first.renew(wrong.clone(), ttl).await,
            Err(Error::OwnershipConflict)
        ));
        assert!(!first.release(wrong).await.unwrap());
    }
    second.renew(new.clone(), ttl).await.unwrap();
    assert!(second.release(new.clone()).await.unwrap());
    assert!(!second.release(new.clone()).await.unwrap());
    assert!(first.lookup("p").await.unwrap().is_none());
    let restarted = RedisOwnership::new(config.clone()).unwrap();
    let record = acquire(&restarted, "s4", "g4", None, ttl).await.unwrap();
    assert!(record.identity.epoch > new.identity.epoch);
    let mut isolated = config.clone();
    isolated.namespace.push_str("-other");
    let other = RedisOwnership::new(isolated.clone()).unwrap();
    assert!(other.lookup("p").await.unwrap().is_none());
    acquire(&other, "independent", "g5", None, ttl)
        .await
        .unwrap();
    assert_eq!(first.lookup("p").await.unwrap(), Some(record));
    cleanup(&config).await;
    cleanup(&isolated).await;
}

#[tokio::test]
#[ignore = "需要真实 Redis；通过 just ownership-test 显式运行"]
async fn expired_lease_cannot_be_renewed_or_release_new_owner() {
    let config = config();
    let store = RedisOwnership::new(config.clone()).unwrap();
    let old = acquire(&store, "expired", "g1", None, Duration::from_millis(250))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(store.lookup("p").await.unwrap().is_none());
    assert!(matches!(
        store.renew(old.clone(), Duration::from_secs(1)).await,
        Err(Error::OwnershipConflict)
    ));
    assert!(!store.release(old.clone()).await.unwrap());
    let new = acquire(&store, "new", "g2", None, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(new.identity.epoch > old.identity.epoch);
    assert!(!store.release(old).await.unwrap());
    assert_eq!(store.lookup("p").await.unwrap(), Some(new));
    cleanup(&config).await;
}

#[tokio::test]
#[ignore = "需要真实 Redis；通过 just ownership-test 显式运行"]
async fn epochs_above_double_precision_are_exact_and_overflow_does_not_replace_lease() {
    let config = config();
    seed(&config, "9007199254740991").await;
    let store = RedisOwnership::new(config.clone()).unwrap();
    let ttl = Duration::from_secs(2);
    let first = acquire(&store, "a", "g1", None, ttl).await.unwrap();
    assert_eq!(first.identity.epoch, 9_007_199_254_740_992);
    let second = acquire(&store, "b", "g2", Some(first.identity.epoch), ttl)
        .await
        .unwrap();
    assert_eq!(second.identity.epoch, 9_007_199_254_740_993);
    assert!(matches!(
        acquire(&store, "c", "g3", Some(first.identity.epoch), ttl).await,
        Err(Error::OwnershipConflict)
    ));
    assert!(!store.release(first).await.unwrap());
    seed(&config, "9223372036854775807").await;
    assert!(matches!(
        acquire(&store, "c", "g3", Some(second.identity.epoch), ttl).await,
        Err(Error::CoordinationUnavailable(_))
    ));
    assert_eq!(store.lookup("p").await.unwrap(), Some(second));
    cleanup(&config).await;
}

#[tokio::test]
async fn timeout_and_invalid_ttl_are_bounded_and_do_not_expose_credentials() {
    let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = RedisOwnership::new(RedisOwnershipConfig {
        url: format!("redis://user:secret@{}/", silent.local_addr().unwrap()),
        operation_timeout_ms: 40,
        ..Default::default()
    })
    .unwrap();
    assert!(matches!(
        acquire(&store, "s", "g", None, Duration::ZERO).await,
        Err(Error::InvalidInput(_))
    ));
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        acquire(&store, "s", "g", None, Duration::from_secs(1)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(error, Error::CoordinationUnavailable(_)));
    assert!(!error.to_string().contains("secret"));
}
