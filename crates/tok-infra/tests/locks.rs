//! 请求锁的单进程互斥、容量、期限和取消测试；使用可控时钟。
use std::{sync::Arc, time::Duration};
use tok_core::lock::Error;
use tok_core::lock::{Backend as LockBackend, Key as LockKey};
use tok_infra::locks::Memory;
use tokio::time::{advance, timeout};

fn key(namespace: &str, bid: &str) -> LockKey {
    LockKey {
        namespace: namespace.into(),
        bid: bid.into(),
    }
}
const TTL: Duration = Duration::from_secs(3);

#[tokio::test(start_paused = true)]
async fn same_key_waits_but_other_entities_and_namespaces_proceed() {
    let backend = Arc::new(Memory::new(8).unwrap());
    let held = backend
        .acquire(key("guild", "1"), Duration::ZERO, TTL)
        .await
        .unwrap();
    let other = backend
        .acquire(key("guild", "2"), Duration::ZERO, TTL)
        .await
        .unwrap();
    let different = backend
        .acquire(key("team", "1"), Duration::ZERO, TTL)
        .await
        .unwrap();
    let waiting = Arc::clone(&backend);
    let task = tokio::spawn(async move {
        waiting
            .acquire(key("guild", "1"), Duration::from_secs(1), TTL)
            .await
    });
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    held.release().await.unwrap();
    advance(Duration::from_millis(10)).await;
    let next = task.await.unwrap().unwrap();
    next.release().await.unwrap();
    other.release().await.unwrap();
    different.release().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn stale_release_cannot_delete_replacement_and_drop_waits_for_ttl() {
    let backend = Memory::new(2).unwrap();
    let old = backend
        .acquire(key("guild", "1"), Duration::ZERO, TTL)
        .await
        .unwrap();
    let token = old.token().to_owned();
    advance(TTL).await;
    let current = backend
        .acquire(key("guild", "1"), Duration::ZERO, TTL)
        .await
        .unwrap();
    assert_ne!(token, current.token());
    old.release().await.unwrap();
    assert!(matches!(
        backend
            .acquire(key("guild", "1"), Duration::ZERO, TTL)
            .await,
        Err(Error::WaitTimeout)
    ));
    drop(current);
    assert!(matches!(
        backend
            .acquire(key("guild", "1"), Duration::ZERO, TTL)
            .await,
        Err(Error::WaitTimeout)
    ));
    advance(TTL).await;
    backend
        .acquire(key("guild", "1"), Duration::ZERO, TTL)
        .await
        .unwrap()
        .release()
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn wait_is_finite_and_cancelled_waiter_returns_capacity() {
    let backend = Arc::new(Memory::new(1).unwrap());
    let held = backend
        .acquire(key("guild", "1"), Duration::ZERO, TTL)
        .await
        .unwrap();
    assert!(matches!(
        backend
            .acquire(key("guild", "2"), Duration::ZERO, TTL)
            .await,
        Err(Error::CapacityExceeded)
    ));
    let pending = Arc::clone(&backend);
    let task = tokio::spawn(async move { pending.acquire(key("guild", "1"), TTL, TTL).await });
    tokio::task::yield_now().await;
    assert!(matches!(
        backend
            .acquire(key("guild", "1"), Duration::ZERO, TTL)
            .await,
        Err(Error::CapacityExceeded)
    ));
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert!(matches!(
        backend
            .acquire(key("guild", "1"), Duration::from_millis(50), TTL)
            .await,
        Err(Error::WaitTimeout)
    ));
    held.release().await.unwrap();
    let lease = timeout(
        Duration::from_secs(1),
        backend.acquire(key("guild", "1"), Duration::ZERO, TTL),
    )
    .await
    .unwrap()
    .unwrap();
    lease.release().await.unwrap();
}

#[tokio::test]
async fn invalid_configuration_and_keys_are_rejected() {
    assert!(Memory::new(0).is_err());
    let backend = Memory::new(1).unwrap();
    for (key, ttl) in [
        (key("", "1"), TTL),
        (key("guild", " "), TTL),
        (key("guild", "1"), Duration::ZERO),
    ] {
        assert!(matches!(
            backend.acquire(key, Duration::ZERO, ttl).await,
            Err(Error::InvalidInput(_))
        ));
    }
}
