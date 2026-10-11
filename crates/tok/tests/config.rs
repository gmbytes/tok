use tok::{AppConfig, DiscoveryBackend};

#[test]
fn example_configs_select_services_in_the_same_binary() {
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
        assert_eq!(config.services.len(), count);
    }
    let local = AppConfig::from_toml(include_str!(
        "../../../examples/demo-game/config/local.toml"
    ))
    .unwrap();
    assert_eq!(local.discovery, DiscoveryBackend::Memory);
    let descriptors = local.service_descriptors().unwrap();
    assert!(
        descriptors
            .iter()
            .all(|service| service.node_id == local.node_id)
    );
    assert_eq!(descriptors[1].service_name, "player");
    assert_eq!(descriptors[1].address, descriptors[2].address);
    assert_eq!(descriptors[1].protocol, "grpc");
    assert_eq!(descriptors[0].protocol, "tcp");
    assert_ne!(descriptors[0].address, descriptors[1].address);
}

#[test]
fn invalid_endpoint_bindings_are_rejected() {
    let local = include_str!("../../../examples/demo-game/config/local.toml");
    for source in [
        local.replace("endpoint = \"internal-rpc\"", "endpoint = \"missing\""),
        local.replace("name = \"internal-rpc\"", "name = \"client\""),
        local.replace("name = \"client\"", "name = \" \""),
        local.replace("protocol = \"grpc\"", "protocol = \" \""),
        local.replace("endpoint = \"client\"", "address = \"127.0.0.1:7000\""),
    ] {
        assert!(AppConfig::from_toml(&source).is_err());
    }
    let mut config = AppConfig::from_toml(local).unwrap();
    config.endpoints.clear();
    assert!(config.validate().is_err());
    assert!(config.service_descriptors().is_err());
}

#[test]
fn endpoint_protocols_are_extensible_and_services_can_bind_separately() {
    let local = include_str!("../../../examples/demo-game/config/local.toml").replace("\r\n", "\n");
    let source = format!(
        "{}\n[[endpoints]]\nname = 'guild-rpc'\nprotocol = 'custom-rpc'\naddress = '127.0.0.1:7002'",
        local.replace(
            "service_name = \"guild\"\nendpoint = \"internal-rpc\"",
            "service_name = \"guild\"\nendpoint = \"guild-rpc\""
        )
    );
    let config = AppConfig::from_toml(&source).unwrap();
    let descriptors = config.service_descriptors().unwrap();
    assert_ne!(descriptors[1].address, descriptors[2].address);
    assert_eq!(descriptors[2].protocol, "custom-rpc");
}

#[test]
fn invalid_or_misspelled_config_is_rejected() {
    let local = include_str!("../../../examples/demo-game/config/local.toml");
    assert!(AppConfig::from_toml(&local.replace("node_id", "node_name")).is_err());
    assert!(AppConfig::from_toml(&local.replace("[[services]]", "[[roles]]")).is_err());
    assert!(AppConfig::from_toml(&local.replace("127.0.0.1:7000", "127.0.0.1:0")).is_err());
    assert!(AppConfig::from_toml(&local.replace("service_name", "instance_id")).is_err());
    assert!(
        AppConfig::from_toml(&local.replace("[[services]]", "[[services]]\nkind = 'player'"))
            .is_err()
    );
    assert!(
        AppConfig::from_toml(&local.replace("service_name = \"player\"", "service_name = \" \""))
            .is_err()
    );
    let duplicate = format!(
        "{local}\n{}",
        "[[services]]\nservice_name = 'player'\nendpoint = 'internal-rpc'"
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
