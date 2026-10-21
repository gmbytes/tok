use std::future::Future;
use std::{collections::BTreeMap, sync::Mutex, time::Duration};
use tok_core::ServiceDescriptor;
use tonic::{
    Request, Response, Status,
    transport::{Channel, Endpoint},
};
use tracing::Instrument;

type EndpointFactory = dyn Fn(&ServiceDescriptor) -> Result<Endpoint, crate::Error> + Send + Sync;

/// 可共享的通用 RPC 客户端，管理有界 Channel 缓存、调用期限、追踪和指标；协议方法由适配器提供。
///
/// 缓存键包含节点和地址，共享端点的不同业务服务复用同一 Channel。
/// 节点变更时可显式 invalidate；缓存满时
/// 清除一个旧键，已有 Channel 克隆仍可完成在途请求。默认使用明文内部 gRPC，可通过端点工厂定制传输。
pub struct Client {
    timeout: Duration,
    capacity: usize,
    endpoint_factory: Box<EndpointFactory>,
    channels: Mutex<BTreeMap<(String, std::net::SocketAddr), Channel>>,
}

impl Client {
    /// 设置连接及协议适配器使用的调用期限和缓存容量，期限须大于零且不超过一天，容量须大于零。
    pub fn new(timeout: Duration, capacity: usize) -> Result<Self, crate::Error> {
        if timeout.is_zero() || timeout > Duration::from_secs(86_400) || capacity == 0 {
            return Err(crate::Error::Configuration(
                "RPC 期限须大于零且不超过一天，连接缓存容量须大于零".into(),
            ));
        }
        Ok(Self {
            timeout,
            capacity,
            endpoint_factory: Box::new(move |target| {
                Ok(Endpoint::from_shared(format!("http://{}", target.address))?
                    .connect_timeout(timeout))
            }),
            channels: Mutex::new(BTreeMap::new()),
        })
    }

    /// 设置创建 Tonic 连接端点的同步工厂，允许定制 URI、TLS、连接期限及 HTTP/2 参数。
    ///
    /// 工厂在缓存未命中时于缓存锁外执行，不应阻塞或创建后台任务；并发未命中时可能重复调用。
    /// 缓存按节点和地址共享，因此同一节点和地址必须使用一致的 URI 与传输配置，不应按业务服务名变化。
    /// 更换工厂会撤销本实例的缓存，已取得的 Channel 仍可完成在途调用。
    /// 自定义工厂负责连接配置，execute 的整体调用期限仍由 Client 管理。
    /// TLS 需在应用依赖中启用 Tonic 对应的 TLS feature。
    pub fn with_endpoint_factory<F>(mut self, factory: F) -> Self
    where
        F: Fn(&ServiceDescriptor) -> Result<Endpoint, crate::Error> + Send + Sync + 'static,
    {
        self.endpoint_factory = Box::new(factory);
        self.channels = Mutex::new(BTreeMap::new());
        self
    }

    /// 在 Tokio 运行时内返回 Channel 的廉价克隆；未缓存时创建延迟连接，实际发送请求时才建立连接。
    pub fn channel(&self, target: &ServiceDescriptor) -> Result<Channel, Status> {
        if target.protocol != "grpc"
            || target.address.ip().is_unspecified()
            || target.address.port() == 0
        {
            return Err(Status::invalid_argument("RPC 目标需要可连接的 grpc 地址"));
        }
        let key = (target.node_id.clone(), target.address);
        if let Some(channel) = self
            .channels
            .lock()
            .map_err(|_| Status::internal("连接缓存锁不可用"))?
            .get(&key)
            .cloned()
        {
            return Ok(channel);
        }
        let endpoint = (self.endpoint_factory)(target).map_err(|error| {
            Status::failed_precondition(format!("RPC 连接端点配置失败：{error}"))
        })?;
        let channel = endpoint.connect_lazy();
        let mut channels = self
            .channels
            .lock()
            .map_err(|_| Status::internal("连接缓存锁不可用"))?;
        // 工厂执行期间可能已有并发调用填入缓存，优先复用已发布的连接。
        if let Some(channel) = channels.get(&key) {
            return Ok(channel.clone());
        }
        if channels.len() >= self.capacity {
            channels.pop_first();
        }
        channels.insert(key, channel.clone());
        Ok(channel)
    }

    /// 节点移除或端点变化时撤销缓存，在途 Channel 克隆仍可完成调用。
    pub fn invalidate(&self, node_id: &str) -> Result<(), Status> {
        self.channels
            .lock()
            .map_err(|_| Status::internal("连接缓存锁不可用"))?
            .retain(|(node, _), _| node != node_id);
        Ok(())
    }

    /// 默认调用期限；协议适配器据此设置请求 deadline 和本地等待上限。
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// 执行任意生成协议的单次调用，传播追踪、设置有限期限并记录指标；不自动重试。
    ///
    /// 请求中的 grpc-timeout 只能缩短默认期限；准备与发送均计入预算，超期结果不会作为成功返回。
    pub async fn execute<Req, Res, F, Fut>(
        &self,
        target: &ServiceDescriptor,
        request_id: u64,
        mut request: Request<Req>,
        span: tracing::Span,
        send: F,
    ) -> Result<Res, Status>
    where
        F: FnOnce(Channel, Request<Req>) -> Fut,
        Fut: Future<Output = Result<Response<Res>, Status>>,
    {
        let started = std::time::Instant::now();
        let budget = crate::timeout::request_budget(request.metadata(), self.timeout)?;
        let deadline = tokio::time::Instant::now() + budget;
        let channel = self.channel(target)?;
        crate::propagation::inject(request.metadata_mut(), &span);
        let result = async {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Status::deadline_exceeded("内部 RPC 超时，未自动重试"));
            }
            request.set_timeout(remaining);
            let call = send(channel, request).instrument(span);
            if tokio::time::Instant::now() >= deadline {
                return Err(Status::deadline_exceeded("内部 RPC 超时，未自动重试"));
            }
            let result = tokio::time::timeout_at(deadline, call).await;
            if tokio::time::Instant::now() >= deadline {
                return Err(Status::deadline_exceeded("内部 RPC 超时，未自动重试"));
            }
            result.unwrap_or_else(|_| Err(Status::deadline_exceeded("内部 RPC 超时，未自动重试")))
        }
        .await;
        metrics::histogram!("tok_rpc_client_duration_seconds", "service_name" => target.service_name.clone()).record(started.elapsed().as_secs_f64());
        metrics::counter!("tok_rpc_client_calls_total", "service_name" => target.service_name.clone(), "result" => if result.is_ok() { "ok" } else { "error" }).increment(1);
        if let Err(status) = &result {
            tracing::error!(node_id = %target.node_id, service_name = %target.service_name, request_id, code = ?status.code(), "内部 RPC 失败，未自动重试");
        }
        Ok(result?.into_inner())
    }
}
