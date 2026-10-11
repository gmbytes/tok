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
    let descriptors = local.service_descriptors();
    assert!(
        descriptors
            .iter()
            .all(|service| service.node_id == local.node_id)
    );
    assert_eq!(descriptors[1].service_name, "player");
}

#[test]
fn invalid_or_misspelled_config_is_rejected() {
    let local = include_str!("../../../examples/demo-game/config/local.toml");
    assert!(AppConfig::from_toml(&local.replace("node_id", "node_name")).is_err());
    assert!(AppConfig::from_toml(&local.replace("127.0.0.1:7000", "127.0.0.1:0")).is_err());
    assert!(AppConfig::from_toml(&local.replace("service_name", "instance_id")).is_err());
    assert!(
        AppConfig::from_toml(&local.replace("service_name = \"player\"", "service_name = \" \""))
            .is_err()
    );
    let duplicate = format!(
        "{local}\n{}",
        "[[roles]]\nkind = 'scene'\nservice_name = 'player'\naddress = '127.0.0.1:9000'"
    );
    assert!(AppConfig::from_toml(&duplicate).is_err());
}

#[test]
fn force_trace_rules_are_parsed_and_validated() {
    let local = include_str!("../../../examples/demo-game/config/local.toml");
    let source = local.replace("log_format = \"text\"", "log_format = \"text\"\ntrace_sample_ratio = 0.0\n[[observability.trace_force_rules]]\nspan = \"client_request\"\nfields = { player_id = 123, operation = \"login\" }");
    let config = AppConfig::from_toml(&source).unwrap();
    assert_eq!(config.observability.trace_force_rules.len(), 1);
    assert_eq!(
        config.observability.trace_force_rules[0].fields["player_id"].as_u64(),
        Some(123)
    );
    assert!(
        AppConfig::from_toml(&source.replace("span = \"client_request\"", "span = \"\"")).is_err()
    );
    assert!(AppConfig::from_toml(&source.replace("player_id = 123", "player_id = [123]")).is_err());
}
