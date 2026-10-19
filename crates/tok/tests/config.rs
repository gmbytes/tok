//! 配置解析、端点绑定和可观测参数校验的集成测试。

use tok::{AppConfig, DiscoveryBackend};

#[test]
fn factory_is_optional_and_overrides_service_name() {
    let local = include_str!("../../../examples/demo-game/config/local.toml");
    let source = local.replace(
        "service_name = \"player-gateway\"",
        "service_name = \"entry-one\"",
    );
    let config = AppConfig::from_toml(&source).unwrap();
    assert_eq!(config.services[0].service_name, "entry-one");
    assert_eq!(
        config.services[0].factory.as_deref(),
        Some("player-ingress")
    );
    assert_eq!(config.services[0].factory_name(), "player-ingress");
    assert!(config.services[1].factory.is_none());
    assert_eq!(config.services[1].factory_name(), "player");
    // 通用配置不固定工厂名称，由游戏注册自己的实现。
    AppConfig::from_toml(&source.replace(
        "factory = \"player-ingress\"",
        "factory = \"custom-ingress\"",
    ))
    .unwrap();
    assert!(
        AppConfig::from_toml(&source.replace("factory = \"player-ingress\"", "factory = \"\""))
            .is_err()
    );
    let implicit =
        AppConfig::from_toml(&source.replace("factory = \"player-ingress\"", "")).unwrap();
    assert!(implicit.services[0].factory.is_none());
    assert_eq!(implicit.services[0].factory_name(), "entry-one");
    assert!(
        AppConfig::from_toml(
            &source.replace("factory = \"player-ingress\"", "gateway = \"player\"")
        )
        .is_err()
    );
    assert!(AppConfig::from_toml(&source.replace("player-ingress", &"x".repeat(65))).is_err());
    // 通用配置允许共享端点；具体接入组件决定是否必须独占。
    AppConfig::from_toml(
        &(source + "\n[[services]]\nservice_name='other'\nfactory='custom'\nendpoint='client'\n"),
    )
    .unwrap();
}

#[test]
fn redis_ownership_is_explicit_independent_and_validated() {
    let local = include_str!("../../../examples/demo-game/config/local.toml");
    let source = local.replace(
        "discovery = \"memory\"",
        "discovery = \"memory\"\nownership = \"redis\"",
    ) + "\n[redis_ownership]\nnamespace='owners-dev'\n";
    let config = AppConfig::from_toml(&source).unwrap();
    assert_eq!(config.discovery, tok::DiscoveryBackend::Memory);
    assert_eq!(config.ownership, tok::OwnershipBackend::Redis);
    for invalid in [
        source.replace("[redis_ownership]\nnamespace='owners-dev'", ""),
        source.replace("owners-dev", "owners:*"),
        source.clone() + "operation_timeout_ms=0\n",
        source.clone() + "operation_timeout_ms=6000\n",
        source.clone() + "operation_timeot_ms=100\n",
        source.replace("ownership = \"redis\"", "ownership = \"memory\""),
    ] {
        assert!(AppConfig::from_toml(&invalid).is_err());
    }
    let mut redis = config.redis_ownership.unwrap();
    redis.url = "redis://user:secret@127.0.0.1:6379/".into();
    assert!(!format!("{redis:?}").contains("secret"));
    let gateway = AppConfig::from_toml(include_str!(
        "../../../examples/demo-game/config/redis-gateway.toml"
    ))
    .unwrap();
    assert_eq!(gateway.ownership, tok::OwnershipBackend::Redis);
    assert_eq!(
        AppConfig::from_toml(local).unwrap().ownership,
        tok::OwnershipBackend::Memory
    );
}

#[test]
fn redis_configuration_requires_isolation_lease_bounds_and_reachable_advertisement() {
    let local = include_str!("../../../examples/demo-game/config/local.toml");
    let source = local.replace("discovery = \"memory\"", "discovery = \"redis\"")
        + "\n[redis_discovery]\nnamespace='test-dev'\n";
    let config = AppConfig::from_toml(&source).unwrap();
    assert_eq!(config.discovery, DiscoveryBackend::Redis);
    for invalid in [
        source.replace("[redis_discovery]\nnamespace='test-dev'", ""),
        source.replace("test-dev", "dev:*"),
        source.clone() + "lease_ttl_ms=5000\n",
        source.clone() + "max_nodes=0\n",
        source.clone() + "refresh_interva_ms=100\n",
        source.replace("127.0.0.1:7001", "0.0.0.0:7001"),
        source.replace("discovery = \"redis\"", "discovery = \"memory\""),
    ] {
        assert!(AppConfig::from_toml(&invalid).is_err());
    }
    let mut config = config;
    config.endpoints[1].address = "0.0.0.0:7001".parse().unwrap();
    config.endpoints[1].advertise_address = Some("192.0.2.10:8001".parse().unwrap());
    config.validate().unwrap();
    assert_eq!(
        config.service_descriptors().unwrap()[1].address.to_string(),
        "192.0.2.10:8001"
    );
    let redis = config.redis_discovery.as_mut().unwrap();
    redis.url = "redis://user:secret@127.0.0.1:6379/".into();
    assert!(!format!("{config:?}").contains("secret"));
}

#[test]
fn player_gateway_limits_and_lease_coverage_are_validated() {
    let original = AppConfig::from_toml(include_str!(
        "../../../examples/demo-game/config/local.toml"
    ))
    .unwrap();
    for invalid in 0..5 {
        let mut config = original.clone();
        match invalid {
            0 => config.player_gateway.queue_capacity = 0,
            1 => config.player_gateway.max_connections = 65_537,
            2 => config.player_gateway.max_frame_size = 16 * 1024 * 1024 + 1,
            3 => config.player_gateway.rpc_timeout_ms = 0,
            _ => {
                config.player_gateway.ownership_ttl_ms = config.player_gateway.renew_interval_ms
                    + config.player_gateway.rpc_timeout_ms * 2
            }
        }
        assert!(config.validate().is_err());
    }
    assert!(
        AppConfig::from_toml(
            &include_str!("../../../examples/demo-game/config/local.toml")
                .replace("queue_capacity", "queue_capcity")
        )
        .is_err()
    );
}

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
