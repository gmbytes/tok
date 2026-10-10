use tok_core::{ServiceDescriptor, ServiceDirectory, ServiceKind};
use tok_infra::discovery::MemoryDirectory;

fn player(id: &str) -> ServiceDescriptor {
    ServiceDescriptor {
        instance_id: id.into(),
        kind: ServiceKind::Player,
        address: "127.0.0.1:7001".parse().unwrap(),
    }
}

#[test]
fn cloned_directory_shares_registration_and_removal() {
    let directory = MemoryDirectory::default();
    let reader = directory.clone();
    directory.register(player("player-1")).unwrap();
    assert_eq!(
        reader.snapshot(ServiceKind::Player).unwrap(),
        vec![player("player-1")]
    );
    assert!(reader.snapshot(ServiceKind::Scene).unwrap().is_empty());
    assert!(reader.unregister(ServiceKind::Player, "player-1").unwrap());
    assert!(directory.snapshot(ServiceKind::Player).unwrap().is_empty());
}

#[test]
fn duplicate_registration_preserves_original_service() {
    let directory = MemoryDirectory::default();
    let original = player("player-1");
    directory.register(original.clone()).unwrap();
    let mut replacement = original.clone();
    replacement.address = "127.0.0.1:8001".parse().unwrap();
    assert!(directory.register(replacement).is_err());
    assert_eq!(
        directory.snapshot(ServiceKind::Player).unwrap(),
        vec![original]
    );
}
