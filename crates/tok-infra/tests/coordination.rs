//! bid 路由和内存所有权竞争、接管与过期的集成测试。

use std::time::Duration;
use tok_core::{ServiceDescriptor, ServiceDirectory};
use tok_infra::{ownership::MemoryOwnership, routing::route};
use tok_player::ownership::{Owner as SessionOwner, Store as OwnershipStore};

struct Directory(Vec<ServiceDescriptor>);

#[tokio::test]
async fn latest_login_blocks_old_credentials_after_release_but_survives_transfer() {
    use tok_player::credentials::ResumeClaims;
    let store = MemoryOwnership::new(2).unwrap();
    let ttl = Duration::from_secs(2);
    let first = store
        .acquire("p".into(), "s1".into(), owner("a"), None, ttl)
        .await
        .unwrap();
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
        Err(tok_player::Error::OwnershipConflict)
    ));
    let fresh = store
        .acquire_for_relogin(claims("s2"), "s3".into(), owner("a"), None, ttl)
        .await
        .unwrap();
    assert_eq!(fresh.identity.session_id, "s3");
}

#[tokio::test]
async fn memory_transfer_compares_full_identity_and_old_cleanup_preserves_target() {
    let store = MemoryOwnership::new(8).unwrap();
    let owner = |node: &str| SessionOwner {
        node_id: node.into(),
        service_name: "gateway".into(),
    };
    let ttl = Duration::from_secs(3);
    let old = store
        .acquire("p".into(), "s".into(), owner("a"), None, ttl)
        .await
        .unwrap();
    let mut wrong = old.clone();
    wrong.identity.session_id = "different".into();
    assert!(store.transfer(wrong, owner("b"), ttl).await.is_err());
    let (a, b) = tokio::join!(
        store.transfer(old.clone(), owner("b"), ttl),
        store.transfer(old.clone(), owner("c"), ttl)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let current = store.lookup("p").await.unwrap().unwrap();
    assert_eq!(current.identity.session_id, "s");
    assert!(current.identity.epoch > old.identity.epoch);
    assert!(!store.release(old.clone()).await.unwrap());
    assert!(store.renew(old, ttl).await.is_err());
    assert_eq!(store.lookup("p").await.unwrap(), Some(current));
}
impl ServiceDirectory for Directory {
    fn snapshot(&self, _: &str) -> tok_core::discovery::Result<Vec<ServiceDescriptor>> {
        Ok(self.0.clone())
    }
}
fn node(id: &str, name: &str) -> ServiceDescriptor {
    ServiceDescriptor {
        node_id: id.into(),
        service_name: name.into(),
        protocol: "grpc".into(),
        address: "127.0.0.1:7001".parse().unwrap(),
    }
}

#[test]
fn routing_ignores_snapshot_order_and_only_moves_removed_targets() {
    let directory = Directory(vec![
        node("a", "guild"),
        node("b", "guild"),
        node("c", "guild"),
        node("other", "player"),
    ]);
    let reversed = Directory(directory.0.iter().rev().cloned().collect());
    let removed = Directory(
        directory
            .0
            .iter()
            .filter(|entry| entry.node_id != "b")
            .cloned()
            .collect(),
    );
    let mut removed_count = 0;
    for id in 0..1000 {
        let bid = id.to_string();
        let target = route(&directory, "guild", &bid).unwrap();
        assert_eq!(target, route(&reversed, "guild", &bid).unwrap());
        if target.node_id != "b" {
            assert_eq!(target, route(&removed, "guild", &bid).unwrap());
        } else {
            removed_count += 1;
        }
    }
    assert!(removed_count > 0);
    assert!(matches!(
        route(&directory, "mail", "1"),
        Err(tok_core::routing::Error::NoAvailableService(_))
    ));
    assert!(matches!(
        route(&directory, "guild", ""),
        Err(tok_core::routing::Error::InvalidInput(_))
    ));
}

fn owner(node_id: &str) -> SessionOwner {
    SessionOwner {
        node_id: node_id.into(),
        service_name: "player-gateway".into(),
    }
}

#[tokio::test]
async fn ownership_competition_takeover_and_release_are_conditional() {
    let store = MemoryOwnership::new(1).unwrap();
    let ttl = Duration::from_secs(10);
    let (a, b) = tokio::join!(
        store.acquire("42".into(), "session-a".into(), owner("a"), None, ttl),
        store.acquire("42".into(), "session-b".into(), owner("b"), None, ttl)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let first = a.or(b).unwrap();
    assert!(matches!(
        store
            .acquire("43".into(), "other".into(), owner("c"), None, ttl)
            .await,
        Err(tok_player::Error::OwnershipUnavailable(_))
    ));
    let next = store
        .acquire(
            "42".into(),
            first.identity.session_id.clone(),
            owner("new"),
            Some(first.identity.epoch),
            ttl,
        )
        .await
        .unwrap();
    assert!(next.identity.epoch > first.identity.epoch);
    assert!(matches!(
        store.renew(first.clone(), ttl).await,
        Err(tok_player::Error::OwnershipConflict)
    ));
    assert!(!store.release(first).await.unwrap());
    assert_eq!(store.lookup("42").await.unwrap(), Some(next.clone()));
    store.renew(next.clone(), ttl).await.unwrap();
    assert!(store.release(next.clone()).await.unwrap());
    let fresh = store
        .acquire("42".into(), "fresh".into(), owner("new"), None, ttl)
        .await
        .unwrap();
    assert!(fresh.identity.epoch > next.identity.epoch);
    assert!(!store.release(next).await.unwrap());
}

#[tokio::test]
async fn expired_ownership_cannot_be_renewed_and_capacity_is_reclaimed() {
    let store = MemoryOwnership::new(1).unwrap();
    let lease = store
        .acquire(
            "42".into(),
            "old".into(),
            owner("a"),
            None,
            Duration::from_millis(10),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(matches!(
        store.renew(lease.clone(), Duration::from_secs(1)).await,
        Err(tok_player::Error::OwnershipConflict)
    ));
    assert!(store.lookup("42").await.unwrap().is_none());
    let fresh = store
        .acquire(
            "43".into(),
            "new".into(),
            owner("b"),
            None,
            Duration::from_secs(1),
        )
        .await
        .unwrap();
    assert!(fresh.identity.epoch > lease.identity.epoch);
    assert!(matches!(
        store
            .acquire("44".into(), "bad".into(), owner("b"), None, Duration::ZERO)
            .await,
        Err(tok_player::Error::InvalidInput(_))
    ));
}

#[test]
fn routing_preserves_discovery_failure_and_default_subscription_uses_its_domain_error() {
    struct Unavailable;
    impl ServiceDirectory for Unavailable {
        fn snapshot(&self, _: &str) -> tok_core::discovery::Result<Vec<ServiceDescriptor>> {
            Err(tok_core::discovery::Error::Unavailable)
        }
    }
    let error = route(&Unavailable, "guild", "42").unwrap_err();
    assert!(matches!(
        error,
        tok_core::routing::Error::Discovery(tok_core::discovery::Error::Unavailable)
    ));
    let source = std::error::Error::source(&error).unwrap();
    assert!(matches!(
        source.downcast_ref::<tok_core::discovery::Error>(),
        Some(tok_core::discovery::Error::Unavailable)
    ));
    assert!(matches!(
        Unavailable.subscribe(),
        Err(tok_core::discovery::Error::Unsupported("服务目录变更订阅"))
    ));
}
