//! HTTPS OTLP 初始化回归；单独进程隔离全局 subscriber，不连接外部采集器。
use tok_observability::{Config, Observability};

#[tokio::test]
async fn validated_https_endpoint_initializes_with_system_trust() {
    let config = Config {
        log_filter: "off".into(),
        otlp_endpoint: Some("https://localhost:4317".into()),
        ..Config::default()
    };
    config.validate().unwrap();
    let telemetry = Observability::init(&config, "https-test", "test-node")
        .await
        .unwrap();
    // Channel 延迟连接；没有业务 span 时无需运行 Collector 也能验证 TLS 构建及关闭。
    telemetry.shutdown().await.unwrap();
}
