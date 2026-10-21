//! 任意 RPC 协议适配器共用的执行期限、追踪与处理链。
use std::time::Duration;
use tok_core::{Extensions, Identity};
use tok_execution::{Context, Request, StopReason};
use tokio_util::sync::CancellationToken;
use tonic::{Status, metadata::MetadataMap};
use tower::{Layer as _, Service, ServiceExt};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// 与协议正文无关的服务端执行入口；协议适配器负责解码、认证与路由。
#[derive(Clone)]
pub struct Server {
    identity: Identity,
    budget: Duration,
    cancellation: CancellationToken,
    extensions: Extensions,
}
impl Server {
    /// 建立服务身份与默认 30 秒预算；不注册网络服务。
    pub fn new(identity: Identity) -> Result<Self, crate::Error> {
        if identity.node_id.trim().is_empty() || identity.service_name.trim().is_empty() {
            return Err(crate::Error::Configuration("RPC 服务身份不能为空".into()));
        }
        Ok(Self {
            identity,
            budget: Duration::from_secs(30),
            cancellation: CancellationToken::new(),
            extensions: Extensions::default(),
        })
    }
    /// 查询当前服务的节点与逻辑服务名。
    pub fn identity(&self) -> &Identity {
        &self.identity
    }
    /// 安装服务预算、停止信号与类型化能力；远端期限只能缩短预算。
    pub fn with_execution(
        mut self,
        budget: Duration,
        cancellation: CancellationToken,
        extensions: Extensions,
    ) -> Result<Self, crate::Error> {
        if budget.is_zero() || budget > Duration::from_secs(86_400) {
            return Err(crate::Error::Configuration(
                "RPC 执行预算须大于零且不超过一天".into(),
            ));
        }
        self.budget = budget;
        self.cancellation = cancellation;
        self.extensions = extensions;
        Ok(self)
    }
    /// 从 RPC 元数据建立原调用范围；稳定操作名及协议扩展由可信适配器提供。
    pub fn context(
        &self,
        metadata: &MetadataMap,
        operation: impl Into<String>,
    ) -> Result<Context, Status> {
        let budget = crate::timeout::request_budget(metadata, self.budget)?;
        let operation = operation.into();
        let span = tracing::info_span!("rpc_server", node_id=%self.identity.node_id, service_name=%self.identity.service_name, operation=%operation);
        #[expect(
            clippy::let_underscore_must_use,
            reason = "追踪关联失败不能中断有效调用。"
        )]
        let _ = span.set_parent(crate::propagation::extract(metadata));
        let mut context = Context::new(
            self.identity.clone(),
            operation,
            budget,
            self.cancellation.child_token(),
        )
        .map_err(|_| Status::invalid_argument("调用执行上下文无效"))?
        .with_span(span);
        *context.extensions_mut() = self.extensions.clone();
        Ok(context)
    }
    /// 执行任意类型化协议入口，包含就绪等待、取消、期限和指标；不隐式入队或重试。
    ///
    /// 请求上下文应由本入口的 context 建立或从中派生；此方法保留其原期限与停止信号。
    pub async fn execute<T, R, S>(&self, request: Request<T>, service: S) -> Result<R, Status>
    where
        S: Service<Request<T>, Response = R, Error = Status> + Clone,
    {
        if request.context.service() != &self.identity {
            return Err(Status::failed_precondition("RPC 执行服务与入口不匹配"));
        }
        let started = std::time::Instant::now();
        let result = tok_execution::Layer
            .layer(service)
            .oneshot(request)
            .await
            .map_err(|error| match error {
                tok_execution::Error::Inner(error) => error,
                tok_execution::Error::Stopped(StopReason::Cancelled) => {
                    Status::cancelled("调用已取消")
                }
                tok_execution::Error::Stopped(StopReason::DeadlineExceeded) => {
                    Status::deadline_exceeded("调用预算已耗尽")
                }
            });
        metrics::histogram!("tok_rpc_server_duration_seconds", "service_name"=>self.identity.service_name.clone()).record(started.elapsed().as_secs_f64());
        metrics::counter!("tok_rpc_server_calls_total", "service_name"=>self.identity.service_name.clone(), "result"=>if result.is_ok(){"ok"}else{"error"}).increment(1);
        result
    }
}
