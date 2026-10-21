//! 内存目录共享、注册冲突和变更通知的集成测试。

use tok_core::{ServiceDescriptor, ServiceDirectory};
use tok_infra::discovery::MemoryDirectory;

#[tokio::test]
async fn notifications_follow_registration_and_removal_and_close_with_directory() {
    let directory = MemoryDirectory::default();
    let mut changes = directory.subscribe().unwrap();
    assert!(directory.snapshot("player").unwrap().is_empty());
    directory.register(player("a")).unwrap();
    directory.register(player("b")).unwrap();
    // 多次变更可以合并为一次通知，读取快照获取完整现状。
    changes.changed().await.unwrap();
    assert_eq!(directory.snapshot("player").unwrap().len(), 2);
    directory.unregister("a", "player").unwrap();
    changes.changed().await.unwrap();
    assert_eq!(directory.snapshot("player").unwrap(), vec![player("b")]);
    drop(directory);
    assert!(changes.changed().await.is_err());
}

fn player(node_id: &str) -> ServiceDescriptor {
    ServiceDescriptor {
        node_id: node_id.into(),
        service_name: "player".into(),
        protocol: "grpc".into(),
        address: "127.0.0.1:7001".parse().unwrap(),
    }
}

#[test]
fn cloned_directory_shares_registration_and_removal() {
    let directory = MemoryDirectory::default();
    let reader = directory.clone();
    directory.register(player("player-1")).unwrap();
    assert_eq!(reader.snapshot("player").unwrap(), vec![player("player-1")]);
    assert!(reader.snapshot("scene").unwrap().is_empty());
    assert!(reader.unregister("player-1", "player").unwrap());
    assert!(directory.snapshot("player").unwrap().is_empty());
}

#[test]
fn duplicate_registration_preserves_original_service() {
    let directory = MemoryDirectory::default();
    let original = player("player-1");
    directory.register(original.clone()).unwrap();
    let mut replacement = original.clone();
    replacement.address = "127.0.0.1:8001".parse().unwrap();
    assert!(
        matches!(directory.register(replacement), Err(tok_core::discovery::Error::RegistrationConflict(reason)) if reason.contains("player-1") && reason.contains("player"))
    );
    assert_eq!(directory.snapshot("player").unwrap(), vec![original]);
}

#[test]
fn same_service_name_on_different_nodes_is_independent() {
    let directory = MemoryDirectory::default();
    directory.register(player("node-a")).unwrap();
    directory.register(player("node-b")).unwrap();
    assert_eq!(directory.snapshot("player").unwrap().len(), 2);
    assert!(directory.unregister("node-a", "player").unwrap());
    assert!(!directory.unregister("node-a", "player").unwrap());
    assert_eq!(
        directory.snapshot("player").unwrap(),
        vec![player("node-b")]
    );
}

#[test]
fn different_services_on_same_node_are_discovered_separately() {
    let directory = MemoryDirectory::default();
    let mut guild = player("node-a");
    guild.service_name = "guild".into();
    let mut mail = guild.clone();
    mail.service_name = "mail".into();
    mail.address = "127.0.0.1:7002".parse().unwrap();
    directory.register(guild.clone()).unwrap();
    directory.register(mail.clone()).unwrap();
    assert_eq!(directory.snapshot("guild").unwrap(), vec![guild]);
    assert_eq!(directory.snapshot("mail").unwrap(), vec![mail.clone()]);
    assert!(directory.unregister("node-a", "guild").unwrap());
    assert_eq!(directory.snapshot("mail").unwrap(), vec![mail]);
}
