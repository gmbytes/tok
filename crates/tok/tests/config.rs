use tok::{AppConfig, DiscoveryBackend};

#[test]
fn example_configs_select_roles_in_the_same_binary() {
    for (source, count) in [
        (
            include_str!("../../../examples/demo-game/config/local.toml"),
            3,
        ),
        (
            include_str!("../../../examples/demo-game/config/gateway.toml"),
            1,
        ),
        (
            include_str!("../../../examples/demo-game/config/game.toml"),
            2,
        ),
    ] {
        let config = AppConfig::from_toml(source).unwrap();
        assert_eq!(config.roles.len(), count);
    }
    let local = AppConfig::from_toml(include_str!(
        "../../../examples/demo-game/config/local.toml"
    ))
    .unwrap();
    assert_eq!(local.discovery, DiscoveryBackend::Memory);
}

#[test]
fn invalid_or_misspelled_config_is_rejected() {
    let local = include_str!("../../../examples/demo-game/config/local.toml");
    assert!(AppConfig::from_toml(&local.replace("node_id", "node_name")).is_err());
    assert!(AppConfig::from_toml(&local.replace("127.0.0.1:7000", "127.0.0.1:0")).is_err());
    let duplicate = format!(
        "{local}\n{}",
        "[[roles]]\nkind = 'player'\ninstance_id = 'local-player'\naddress = '127.0.0.1:9000'"
    );
    assert!(AppConfig::from_toml(&duplicate).is_err());
}
