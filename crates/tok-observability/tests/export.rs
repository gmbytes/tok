//! Prometheus 抓取、OTLP 导出和动态日志过滤的集成测试。

use std::time::Duration;

use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
    trace_service_server::{TraceService, TraceServiceServer},
};
use tok_observability::opentelemetry::trace::TraceContextExt;
use tok_observability::{
    Config, ForceTraceRule, Observability, OpenTelemetrySpanExt, SamplingConfig, metrics, tracing,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;

struct Collector(mpsc::UnboundedSender<ExportTraceServiceRequest>);

#[tonic::async_trait]
impl TraceService for Collector {
    async fn export(
        &self,
        request: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        self.0.send(request.into_inner()).unwrap();
        Ok(tonic::Response::new(ExportTraceServiceResponse::default()))
    }
}

// 进程级全局初始化只在这个测试二进制中执行一次。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_are_scrapable_and_traces_survive_log_filter_changes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(TraceServiceServer::new(Collector(sender)))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), stop.cancelled_owned())
            .await
            .unwrap();
    });
    let config = Config {
        log_filter: "warn".into(),
        metrics_address: Some("127.0.0.1:0".parse().unwrap()),
        otlp_endpoint: Some(endpoint),
        trace_sample_ratio: 1.0,
        ..Config::default()
    };
    let telemetry = Observability::init(&config, "test-game", "test-node")
        .await
        .unwrap();
    telemetry.logs.set_filter("off").unwrap();
    assert!(telemetry.logs.set_filter("tok=invalid-level").is_err());
    assert_eq!(telemetry.logs.filter().unwrap(), "off");
    {
        let span = tracing::info_span!("test_operation", player_id = 123);
        let _entered = span.enter();
        tracing::info!("日志关闭时仍应记录追踪");
    }
    telemetry
        .traces
        .set_config(SamplingConfig {
            filter: "off".into(),
            ratio: 0.0,
            force_rules: vec![ForceTraceRule {
                span: Some("forced_request".into()),
                fields: std::collections::BTreeMap::from([(
                    "player_id".into(),
                    serde_json::json!(123),
                )]),
                ..ForceTraceRule::default()
            }],
        })
        .unwrap();
    {
        let wrong_player = tracing::debug_span!("forced_request", player_id = 124_u64);
        assert!(!wrong_player.context().span().span_context().is_sampled());
    }
    {
        let late = tracing::debug_span!("forced_request", player_id = tracing::field::Empty);
        let _entered = late.enter();
        late.record("player_id", 123_u64);
        assert!(!late.context().span().span_context().is_sampled());
    }
    {
        let root = tracing::debug_span!("forced_request", player_id = 123_u64);
        assert!(root.context().span().span_context().is_sampled());
        let _entered = root.enter();
        // 移除规则不改变已经开始的链路，过滤关闭的子 span 仍继承采样。
        telemetry.traces.set_force_rules(Vec::new()).unwrap();
        let child = tracing::trace_span!("forced_child");
        assert!(child.context().span().span_context().is_sampled());
        let _child_entered = child.enter();
        tracing::trace!("forced_detail");
    }
    fn dynamic_span() {
        let span = tracing::debug_span!("dynamic_request", player_id = 789_u64);
        let _entered = span.enter();
    }
    dynamic_span();
    telemetry
        .traces
        .set_force_rules(vec![ForceTraceRule {
            span: Some("dynamic_request".into()),
            ..ForceTraceRule::default()
        }])
        .unwrap();
    dynamic_span();
    telemetry.traces.set_force_rules(Vec::new()).unwrap();
    dynamic_span();
    telemetry
        .traces
        .set_force_rules(vec![ForceTraceRule {
            span: Some("forced_downstream".into()),
            ..ForceTraceRule::default()
        }])
        .unwrap();
    {
        let root = tracing::info_span!("unsampled_parent");
        assert!(!root.context().span().span_context().is_sampled());
        let _entered = root.enter();
        let child = tracing::debug_span!("forced_downstream");
        assert!(child.context().span().span_context().is_sampled());
    }
    metrics::counter!("test_requests_total", "service_name" => "player").increment(2);
    metrics::histogram!("test_latency_seconds").record(0.01);
    let address = telemetry.metrics_address().unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), async {
        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut bytes = Vec::new();
        client.read_to_end(&mut bytes).await.unwrap();
        String::from_utf8(bytes).unwrap()
    })
    .await
    .unwrap();
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains("test_requests_total{"));
    assert!(response.contains("service=\"test-game\""));
    assert!(response.contains("service_name=\"player\""));
    assert!(response.contains("} 2"));
    assert!(response.contains("test_latency_seconds_bucket{"));
    tokio::time::timeout(Duration::from_secs(10), telemetry.shutdown())
        .await
        .unwrap()
        .unwrap();
    let request = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let mut requests = vec![request];
    while let Ok(request) = receiver.try_recv() {
        requests.push(request);
    }
    let spans: Vec<_> = requests
        .iter()
        .flat_map(|request| &request.resource_spans)
        .flat_map(|resource| &resource.scope_spans)
        .flat_map(|scope| &scope.spans)
        .collect();
    let span = spans
        .iter()
        .find(|span| span.name == "test_operation")
        .unwrap();
    assert_eq!(span.trace_id.len(), 16);
    assert_eq!(span.span_id.len(), 8);
    let forced = spans
        .iter()
        .find(|span| span.name == "forced_request")
        .unwrap();
    let child = spans
        .iter()
        .find(|span| span.name == "forced_child")
        .unwrap();
    assert_eq!(forced.trace_id, child.trace_id);
    assert_eq!(forced.span_id, child.parent_span_id);
    assert!(
        child
            .events
            .iter()
            .any(|event| event.name == "forced_detail")
    );
    assert_eq!(
        spans
            .iter()
            .filter(|span| span.name == "forced_request")
            .count(),
        1
    );
    assert_eq!(
        spans
            .iter()
            .filter(|span| span.name == "dynamic_request")
            .count(),
        1
    );
    assert!(spans.iter().any(|span| span.name == "forced_downstream"));
    assert!(!spans.iter().any(|span| span.name == "unsampled_parent"));
    assert!(TcpStream::connect(address).await.is_err());
    cancel.cancel();
    server.await.unwrap();
}
