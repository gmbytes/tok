//! 进程级可观测初始化。库只埋点，游戏入口初始化一次并在服务退出后关闭。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod context;
mod json;
mod sampling;
pub use sampling::{ForceTraceRule, SamplingConfig, TraceControl};

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{Router, routing::get};
use opentelemetry::{KeyValue, global, trace::TracerProvider};
use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator, trace::SdkTracerProvider};
use serde::Deserialize;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

/// 指标埋点库，提供计数、Gauge 和 Histogram 宏；详细 API 见上游文档。
pub use metrics;
/// OpenTelemetry 上下文与追踪类型；详细 API 见上游文档。
pub use opentelemetry;
/// 日志事件与 span 埋点库；详细 API 见上游文档。
pub use tracing;
/// 为 tracing Span 提供设置和读取 OpenTelemetry Context 的扩展方法。
///
/// `set_parent` 接入上游追踪上下文，`context` 获取当前 span 的上下文用于传播。
/// 此处重导出上游 trait，完整方法说明保留上游文档。
#[doc(inline)]
pub use tracing_opentelemetry::OpenTelemetrySpanExt;

/// 可观测初始化、配置更新与退出操作返回的错误。
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// 日志输出格式。
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LogFormat {
    /// 面向开发终端的文本输出。
    #[default]
    Text,
    /// 面向日志采集器的 JSON 输出。
    Json,
}

/// 进程级日志、Prometheus 和 OTLP 配置，省略字段时使用默认值。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// 初始日志过滤规则，可由 RUST_LOG 覆盖。
    pub log_filter: String,
    /// 日志输出格式，默认文本。
    pub log_format: LogFormat,
    /// None 禁用指标端口，Some 时仅提供 /metrics。
    pub metrics_address: Option<SocketAddr>,
    /// None 禁用导出；Some 为 HTTP/HTTPS OTLP gRPC Collector 地址。
    /// HTTPS 校验证书及主机名，使用操作系统信任的 CA。
    pub otlp_endpoint: Option<String>,
    /// 普通根 span 的级别和模块过滤规则。
    pub trace_filter: String,
    /// 通过普通过滤的根 span 采样比例，范围为 0 到 1。
    pub trace_sample_ratio: f64,
    /// 命中后优先强制采样的精确匹配规则。
    pub trace_force_rules: Vec<ForceTraceRule>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            log_filter: "info".into(),
            log_format: LogFormat::Text,
            metrics_address: None,
            otlp_endpoint: None,
            trace_filter: "info".into(),
            trace_sample_ratio: 0.1,
            trace_force_rules: Vec::new(),
        }
    }
}

impl Config {
    /// 验证过滤语法、采样规则和 OTLP 地址，不连接采集后端。
    pub fn validate(&self) -> Result<(), Error> {
        parse_filter(&self.log_filter)?;
        self.sampling_config().validate()?;
        if let Some(endpoint) = &self.otlp_endpoint {
            let uri: axum::http::Uri = endpoint.parse()?;
            if !matches!(uri.scheme_str(), Some("http" | "https")) || uri.host().is_none() {
                return Err(std::io::Error::other("OTLP endpoint 必须是 http/https 地址").into());
            }
        }
        Ok(())
    }

    /// 提取可以动态更新的追踪采样配置。
    pub fn sampling_config(&self) -> SamplingConfig {
        SamplingConfig {
            filter: self.trace_filter.clone(),
            ratio: self.trace_sample_ratio,
            force_rules: self.trace_force_rules.clone(),
        }
    }
}

fn parse_filter(value: &str) -> Result<EnvFilter, Error> {
    if value.trim().is_empty() {
        return Err(std::io::Error::other("日志或追踪过滤规则不能为空").into());
    }
    Ok(EnvFilter::builder().with_regex(false).parse(value)?)
}

type Reload = dyn Fn(EnvFilter) -> Result<(), Error> + Send + Sync;

/// 可交给管理接口或配置监听器；更新失败时保留原规则。
#[derive(Clone)]
pub struct LogControl {
    current: Arc<Mutex<String>>,
    reload: Arc<Reload>,
}

impl LogControl {
    /// 读取当前日志过滤规则。
    pub fn filter(&self) -> Result<String, Error> {
        Ok(self
            .current
            .lock()
            .map_err(|_| std::io::Error::other("日志控制锁不可用"))?
            .clone())
    }

    /// 动态替换过滤规则，校验或更新失败时保留原规则。
    pub fn set_filter(&self, directive: &str) -> Result<(), Error> {
        let filter = parse_filter(directive)?;
        let mut current = self
            .current
            .lock()
            .map_err(|_| std::io::Error::other("日志控制锁不可用"))?;
        (self.reload)(filter)?;
        *current = directive.into();
        Ok(())
    }
}

/// 进程级可观测资源与动态控制句柄，服务结束后应显式调用 shutdown。
pub struct Observability {
    /// 日志过滤规则的读写入口。
    pub logs: LogControl,
    /// 追踪过滤、采样率与强规则的读写入口。
    pub traces: TraceControl,
    metrics_address: Option<SocketAddr>,
    shutdown: CancellationToken,
    tasks: JoinSet<Result<(), std::io::Error>>,
    provider: Option<SdkTracerProvider>,
}

impl Observability {
    /// 必须在 Tokio 运行时内调用，每个进程仅初始化一次。
    /// 全局 subscriber/recorder 无法卸载，初始化失败应终止进程。
    pub async fn init(config: &Config, service_name: &str, node_id: &str) -> Result<Self, Error> {
        config.validate()?;
        let traces = TraceControl::new(config.sampling_config())?;
        let log_filter = match std::env::var("RUST_LOG") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => config.log_filter.clone(),
            Err(error) => return Err(error.into()),
        };
        let (filter, handle) = tracing_subscriber::reload::Layer::new(parse_filter(&log_filter)?);
        let listener = match config.metrics_address {
            Some(address) => Some(tokio::net::TcpListener::bind(address).await?),
            None => None,
        };
        let provider = match &config.otlp_endpoint {
            Some(endpoint) => {
                let exporter = opentelemetry_otlp::SpanExporter::builder()
                    .with_tonic()
                    .with_endpoint(endpoint)
                    .with_timeout(Duration::from_secs(3));
                let exporter =
                    if endpoint.parse::<axum::http::Uri>()?.scheme_str() == Some("https") {
                        exporter.with_tls_config(
                            tonic::transport::ClientTlsConfig::new().with_native_roots(),
                        )
                    } else {
                        exporter
                    }
                    .build()?;
                Some(
                    SdkTracerProvider::builder()
                        .with_batch_exporter(exporter)
                        .with_sampler(sampling::RuleSampler(traces.clone()))
                        .with_resource(
                            Resource::builder()
                                .with_service_name(service_name.to_owned())
                                .with_attribute(KeyValue::new("tok.node.id", node_id.to_owned()))
                                .build(),
                        )
                        .build(),
                )
            }
            None => None,
        };
        let contexts = context::Resolver::default();
        let trace_layer = provider.as_ref().map(|provider| {
            tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("tok"))
                .with_context_activation(true)
                .with_level(true)
                .with_target(true)
                .with_filter(sampling::TraceFilter {
                    control: traces.clone(),
                    contexts: contexts.clone(),
                })
        });
        let log_layer = match config.log_format {
            LogFormat::Text => tracing_subscriber::fmt::layer().with_filter(filter).boxed(),
            LogFormat::Json => tracing_subscriber::fmt::layer()
                .json()
                .event_format(json::JsonFormatter {
                    service_name: service_name.into(),
                    node_id: node_id.into(),
                    contexts: contexts.clone(),
                })
                .with_filter(filter)
                .boxed(),
        };
        tracing_subscriber::registry()
            .with(log_layer)
            .with(trace_layer)
            .with(contexts.layer())
            .try_init()?;
        global::set_text_map_propagator(TraceContextPropagator::new());
        let mut result = Self {
            traces,
            logs: LogControl {
                current: Arc::new(Mutex::new(log_filter)),
                reload: Arc::new(move |filter| {
                    handle.reload(filter)?;
                    Ok(())
                }),
            },
            metrics_address: None,
            shutdown: CancellationToken::new(),
            tasks: JoinSet::new(),
            provider,
        };
        if let Some(listener) = listener {
            result.metrics_address = Some(listener.local_addr()?);
            let recorder = metrics_exporter_prometheus::PrometheusBuilder::new()
                .set_buckets(&[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0])?
                .add_global_label("service", service_name.to_owned())
                .add_global_label("node", node_id.to_owned())
                .build_recorder();
            let handle = recorder.handle();
            metrics::set_global_recorder(recorder)?;
            let upkeep = handle.clone();
            let cancel = result.shutdown.clone();
            result.tasks.spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => return Ok(()),
                        _ = interval.tick() => upkeep.run_upkeep(),
                    }
                }
            });
            let router = Router::new().route(
                "/metrics",
                get(move || {
                    let handle = handle.clone();
                    async move {
                        (
                            [(
                                axum::http::header::CONTENT_TYPE,
                                "text/plain; version=0.0.4; charset=utf-8",
                            )],
                            handle.render(),
                        )
                    }
                }),
            );
            let cancel = result.shutdown.clone();
            result.tasks.spawn(async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(cancel.cancelled_owned())
                    .await
            });
        }
        Ok(result)
    }

    /// 返回已绑定的指标地址；未启用指标端口时返回 None。
    pub fn metrics_address(&self) -> Option<SocketAddr> {
        self.metrics_address
    }

    /// 服务结束后调用；OTLP flush 在阻塞线程执行，避免阻塞异步调度。
    pub async fn shutdown(mut self) -> Result<(), Error> {
        self.shutdown.cancel();
        let mut error: Option<Error> = None;
        let drain = async {
            while let Some(result) = self.tasks.join_next().await {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(source)) => {
                        error.get_or_insert(source.into());
                    }
                    Err(source) => {
                        error.get_or_insert(source.into());
                    }
                }
            }
        };
        if tokio::time::timeout(Duration::from_secs(5), drain)
            .await
            .is_err()
        {
            self.tasks.shutdown().await;
            error.get_or_insert(std::io::Error::other("指标服务退出超时").into());
        }
        if let Some(provider) = self.provider.take() {
            let result = tokio::task::spawn_blocking(move || {
                provider.shutdown_with_timeout(Duration::from_secs(5))
            })
            .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(source)) => {
                    error.get_or_insert(source.into());
                }
                Err(source) => {
                    error.get_or_insert(source.into());
                }
            }
        }
        error.map_or(Ok(()), Err)
    }
}

impl Drop for Observability {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[derive(Clone)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn log_reload_changes_output_and_invalid_rules_preserve_filter() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let writer = Buffer(Arc::clone(&bytes));
        let (filter, handle) =
            tracing_subscriber::reload::Layer::new(parse_filter("info").unwrap());
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(move || writer.clone())
                .with_filter(filter),
        );
        let control = LogControl {
            current: Arc::new(Mutex::new("info".into())),
            reload: Arc::new(move |filter| {
                handle.reload(filter)?;
                Ok(())
            }),
        };
        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!("hidden_before");
            control.set_filter("info,tok_observability=debug").unwrap();
            tracing::debug!("visible_after");
            assert!(control.set_filter("tok_observability=wrong").is_err());
            assert_eq!(control.filter().unwrap(), "info,tok_observability=debug");
            tracing::debug!("visible_after_invalid");
            control.set_filter("error").unwrap();
            tracing::info!("hidden_after");
        });
        let output = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(!output.contains("hidden_before"));
        assert!(!output.contains("hidden_after"));
        assert!(output.contains("visible_after"));
        assert!(output.contains("visible_after_invalid"));
    }

    #[test]
    fn invalid_observability_config_is_rejected() {
        for config in [
            Config {
                trace_sample_ratio: 1.1,
                ..Config::default()
            },
            Config {
                trace_sample_ratio: f64::NAN,
                ..Config::default()
            },
            Config {
                log_filter: "bad=invalid".into(),
                ..Config::default()
            },
            Config {
                otlp_endpoint: Some("collector:4317".into()),
                ..Config::default()
            },
        ] {
            assert!(config.validate().is_err());
        }
    }

    #[test]
    fn json_logs_contain_trace_correlation_fields() {
        let provider = SdkTracerProvider::builder().build();
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let writer = Buffer(Arc::clone(&bytes));
        let contexts = context::Resolver::default();
        let subscriber = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .event_format(json::JsonFormatter {
                        service_name: "game".into(),
                        node_id: "node".into(),
                        contexts: contexts.clone(),
                    })
                    .with_writer(move || writer.clone()),
            )
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer("test"))
                    .with_context_activation(true),
            )
            .with(contexts.layer());
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("request", player_id = 123);
            let _entered = span.enter();
            tracing::info!("test");
        });
        let value: serde_json::Value = serde_json::from_slice(&bytes.lock().unwrap()).unwrap();
        assert_eq!(value["service"], "game");
        assert_eq!(value["node_id"], "node");
        assert_eq!(value["trace_id"].as_str().unwrap().len(), 32);
        assert_eq!(value["span_id"].as_str().unwrap().len(), 16);
        assert_eq!(value["fields"]["message"], "test");
        provider.shutdown().unwrap();
    }
    #[test]
    fn json_correlation_follows_event_parent_instead_of_ambient_span() {
        use opentelemetry::trace::TraceContextExt;

        let provider = SdkTracerProvider::builder().build();
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let writer = Buffer(Arc::clone(&bytes));
        let contexts = context::Resolver::default();
        let subscriber = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .event_format(json::JsonFormatter {
                        service_name: "game".into(),
                        node_id: "node".into(),
                        contexts: contexts.clone(),
                    })
                    .with_writer(move || writer.clone()),
            )
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer("test"))
                    .with_context_activation(true),
            )
            .with(contexts.layer());
        tracing::subscriber::with_default(subscriber, || {
            let parent = tracing::info_span!(parent: None, "explicit_parent");
            // 关联事件也可以是尚未进入的 span 的首次使用。
            tracing::info!(parent: &parent, "explicit_without_ambient");
            let expected = parent.context();
            let expected = expected.span().span_context().clone();
            let other = tracing::info_span!(parent: None, "other");
            let _entered = other.enter();
            tracing::info!(parent: &parent, "explicit_with_other_ambient");
            tracing::info!(parent: None, "explicit_root");
            tracing::info!("contextual");
            let text = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
            let logs: Vec<serde_json::Value> = text
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(logs.len(), 4);
            for log in &logs[..2] {
                assert_eq!(log["trace_id"], expected.trace_id().to_string());
                assert_eq!(log["span_id"], expected.span_id().to_string());
            }
            assert!(logs[2].get("trace_id").is_none());
            assert!(logs[2].get("span_id").is_none());
            assert_eq!(
                logs[3]["span_id"],
                other.context().span().span_context().span_id().to_string()
            );
        });
        provider.shutdown().unwrap();
    }

    #[test]
    fn sampling_freezes_at_first_context_use_and_preserves_remote_parent_setup() {
        use opentelemetry::trace::{
            SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
        };

        let control = TraceControl::new(SamplingConfig {
            filter: "off".into(),
            ratio: 0.0,
            force_rules: Vec::new(),
        })
        .unwrap();
        let provider = SdkTracerProvider::builder()
            .with_sampler(sampling::RuleSampler(control.clone()))
            .build();
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::layer()
                .with_tracer(provider.tracer("test"))
                .with_context_activation(true),
        );
        tracing::subscriber::with_default(subscriber, || {
            let pending =
                tracing::info_span!(parent: None, "pending", player_id = tracing::field::Empty);
            let decided =
                tracing::info_span!(parent: None, "decided", player_id = tracing::field::Empty);
            assert!(!decided.context().span().span_context().is_sampled());
            control
                .set_force_rules(vec![ForceTraceRule {
                    fields: std::collections::BTreeMap::from([(
                        "player_id".into(),
                        serde_json::json!(123),
                    )]),
                    ..ForceTraceRule::default()
                }])
                .unwrap();
            pending.record("player_id", 123_u64);
            decided.record("player_id", 123_u64);
            assert!(pending.context().span().span_context().is_sampled());
            assert!(!decided.context().span().span_context().is_sampled());
            control.set_force_rules(Vec::new()).unwrap();
            assert!(pending.context().span().span_context().is_sampled());
            let incoming = tracing::info_span!(parent: None, "incoming");
            let remote = SpanContext::new(
                TraceId::from(23),
                SpanId::from(42),
                TraceFlags::SAMPLED,
                true,
                TraceState::default(),
            );
            incoming
                .set_parent(opentelemetry::Context::new().with_remote_span_context(remote.clone()))
                .unwrap();
            let context = incoming.context();
            assert_eq!(context.span().span_context().trace_id(), remote.trace_id());
            assert!(context.span().span_context().is_sampled());
        });
        provider.shutdown().unwrap();
    }

    #[test]
    fn json_correlation_ignores_log_span_filtering() {
        use opentelemetry::trace::TraceContextExt;

        let provider = SdkTracerProvider::builder().build();
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let writer = Buffer(Arc::clone(&bytes));
        let contexts = context::Resolver::default();
        let subscriber = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .event_format(json::JsonFormatter {
                        service_name: "game".into(),
                        node_id: "node".into(),
                        contexts: contexts.clone(),
                    })
                    .with_writer(move || writer.clone())
                    .with_filter(parse_filter("info").unwrap()),
            )
            .with(
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer("test"))
                    .with_context_activation(true),
            )
            .with(contexts.layer());
        tracing::subscriber::with_default(subscriber, || {
            let parent = tracing::debug_span!(parent: None, "hidden_parent");
            tracing::info!(parent: &parent, "explicit_hidden_parent");
            let ancestor = tracing::info_span!(parent: None, "visible_ancestor");
            let _ancestor = ancestor.enter();
            let child = tracing::debug_span!("hidden_child");
            let _child = child.enter();
            tracing::info!("contextual_hidden_child");
            tracing::info!(parent: &parent, "explicit_with_other_ambient");
            tracing::info!(parent: None, "explicit_root");
            let text = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
            let logs: Vec<serde_json::Value> = text
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(logs.len(), 4);
            let expected = parent.context();
            for log in [&logs[0], &logs[2]] {
                assert_eq!(
                    log["trace_id"],
                    expected.span().span_context().trace_id().to_string()
                );
                assert_eq!(
                    log["span_id"],
                    expected.span().span_context().span_id().to_string()
                );
            }
            assert_eq!(
                logs[1]["span_id"],
                child.context().span().span_context().span_id().to_string()
            );
            assert_ne!(
                logs[1]["span_id"],
                ancestor
                    .context()
                    .span()
                    .span_context()
                    .span_id()
                    .to_string()
            );
            assert!(logs[3].get("trace_id").is_none());
            assert!(logs[3].get("span_id").is_none());
        });
        provider.shutdown().unwrap();
    }

    #[test]
    fn context_registration_does_not_enable_filtered_instrumentation() {
        use std::sync::atomic::{AtomicBool, Ordering};

        for directive in ["off", "info"] {
            let subscriber = tracing_subscriber::registry()
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_writer(std::io::sink)
                        .with_filter(parse_filter(directive).unwrap()),
                )
                .with(context::Resolver::default().layer());
            tracing::subscriber::with_default(subscriber, || {
                assert!(!tracing::enabled!(tracing::Level::TRACE));
                assert!(!tracing::enabled!(tracing::Level::DEBUG));
                assert_eq!(tracing::enabled!(tracing::Level::INFO), directive == "info");
                let evaluated = AtomicBool::new(false);
                tracing::trace!(
                    value = {
                        evaluated.store(true, Ordering::SeqCst);
                        42
                    }
                );
                let span = tracing::debug_span!(
                    "disabled",
                    value = {
                        evaluated.store(true, Ordering::SeqCst);
                        42
                    }
                );
                assert!(span.is_disabled());
                assert!(!evaluated.load(Ordering::SeqCst));
            });
        }
    }
}
