//! bid 路由和内存所有权竞争、接管与过期的集成测试。

use std::time::Duration;
use tok_core::{
    Error, ServiceDescriptor, ServiceDirectory,
    coordination::{OwnershipStore, SessionOwner},
};
use tok_infra::{ownership::MemoryOwnership, routing::route};

struct Directory(Vec<ServiceDescriptor>);
impl ServiceDirectory for Directory {
    fn snapshot(&self, _: &str) -> tok_core::Result<Vec<ServiceDescriptor>> {
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
        Err(Error::NoAvailableService(_))
    ));
    assert!(matches!(
        route(&directory, "guild", ""),
        Err(Error::InvalidInput(_))
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
        Err(Error::CoordinationUnavailable(_))
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
        Err(Error::OwnershipConflict)
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
        Err(Error::OwnershipConflict)
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
        Err(Error::InvalidInput(_))
    ));
}
