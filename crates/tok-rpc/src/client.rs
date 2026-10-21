use std::future::Future;
use std::{collections::BTreeMap, sync::Mutex, time::Duration};
use tok_core::ServiceDescriptor;
use tonic::{
    Request, Response, Status,
    transport::{Channel, Endpoint},
};
use tracing::Instrument;

/// 通用的有界内部 RPC Channel 缓存及传输配置，不包含具体协议调用。
///
/// 缓存键包含节点和地址，共享端点的不同业务服务复用同一 Channel。
/// 节点变更时可显式 invalidate；缓存满时
/// 清除一个旧键，已有 Channel 克隆仍可完成在途请求。此版本仅支持明文内部 gRPC。
pub struct RpcClients {
    timeout: Duration,
    capacity: usize,
    channels: Mutex<BTreeMap<(String, std::net::SocketAddr), Channel>>,
}

impl RpcClients {
    /// 设置连接及协议适配器使用的调用期限和缓存容量，两者必须大于零。
    pub fn new(timeout: Duration, capacity: usize) -> Result<Self, crate::Error> {
        if timeout.is_zero() || capacity == 0 {
            return Err(crate::Error::Configuration(
                "RPC 期限和连接缓存容量必须大于零".into(),
            ));
        }
        Ok(Self {
            timeout,
            capacity,
            channels: Mutex::new(BTreeMap::new()),
        })
    }

    /// 在 Tokio 运行时内返回 Channel 的廉价克隆；首次调用时建立连接。
    pub fn channel(&self, target: &ServiceDescriptor) -> Result<Channel, Status> {
        if target.protocol != "grpc"
            || target.address.ip().is_unspecified()
            || target.address.port() == 0
        {
            return Err(Status::invalid_argument("RPC 目标需要可连接的 grpc 地址"));
        }
        let key = (target.node_id.clone(), target.address);
        let mut channels = self
            .channels
            .lock()
            .map_err(|_| Status::internal("连接缓存锁不可用"))?;
        if let Some(channel) = channels.get(&key) {
            return Ok(channel.clone());
        }
        let endpoint = Endpoint::from_shared(format!("http://{}", target.address))
            .map_err(|_| Status::invalid_argument("RPC 地址无效"))?
            .connect_timeout(self.timeout);
        let channel = endpoint.connect_lazy();
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
}

impl RpcClients {
    /// 执行任意生成协议的单次调用，传播追踪、设置有限期限并记录指标；不自动重试。
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
        let channel = self.channel(target)?;
        request.set_timeout(self.timeout());
        crate::propagation::inject(request.metadata_mut(), &span);
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(self.timeout(), send(channel, request).instrument(span))
            .await
            .unwrap_or_else(|_| Err(Status::deadline_exceeded("内部 RPC 超时，未自动重试")));
        metrics::histogram!("tok_rpc_client_duration_seconds", "service_name" => target.service_name.clone()).record(started.elapsed().as_secs_f64());
        metrics::counter!("tok_rpc_client_calls_total", "service_name" => target.service_name.clone(), "result" => if result.is_ok() { "ok" } else { "error" }).increment(1);
        if let Err(status) = &result {
            tracing::error!(node_id = %target.node_id, service_name = %target.service_name, request_id, code = ?status.code(), "内部 RPC 失败，未自动重试");
        }
        Ok(result?.into_inner())
    }
}
