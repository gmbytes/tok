//! 客户端连接管理与可组合的 gRPC 执行处理链。
//!
//! Call 是连接准备前的输入，Layer 将其转换为 Outgoing 并约束内部就绪和执行。
//! Service 每次调用克隆内层；共享限额和其他状态应由内层策略显式共享。
mod call;
mod middleware;
mod sender;
use crate::Outgoing;
pub use call::Call;
pub use middleware::{Layer, ResponseFuture, Service};
use std::future::Future;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tok_core::ServiceDescriptor;
use tonic::{
    Request, Response, Status,
    transport::{Channel, Endpoint},
};
use tower::Layer as _;

type EndpointFactory = dyn Fn(&ServiceDescriptor) -> Result<Endpoint, crate::Error> + Send + Sync;

/// 可廉价克隆的通用 RPC 客户端，管理有界 Channel 缓存、调用期限、追踪和指标；协议方法由适配器提供。
///
/// 克隆共享缓存和端点工厂；缓存键包含节点和地址，共享端点的服务复用同一 Channel。
/// 节点变更时可显式 invalidate；缓存满时
/// 清除一个旧键，已有 Channel 克隆仍可完成在途请求。默认使用明文内部 gRPC，可通过端点工厂定制传输。
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
}
struct Shared {
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
            shared: Arc::new(Shared {
                timeout,
                capacity,
                endpoint_factory: Box::new(move |target| {
                    Ok(Endpoint::from_shared(format!("http://{}", target.address))?
                        .connect_timeout(timeout))
                }),
                channels: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    /// 设置创建 Tonic 连接端点的同步工厂，允许定制 URI、TLS、连接期限及 HTTP/2 参数。
    ///
    /// 工厂在缓存未命中时于缓存锁外执行，不应阻塞或创建后台任务；并发未命中时可能重复调用。
    /// 缓存按节点和地址共享，因此同一节点和地址必须使用一致的 URI 与传输配置，不应按业务服务名变化。
    /// 更换工厂会为本实例建立独立配置与空缓存；其他 Client 克隆及已取得的 Channel 不受影响。
    /// 自定义工厂负责连接配置，execute 的整体调用期限仍由 Client 管理。
    /// TLS 需在应用依赖中启用 Tonic 对应的 TLS feature。
    pub fn with_endpoint_factory<F>(mut self, factory: F) -> Self
    where
        F: Fn(&ServiceDescriptor) -> Result<Endpoint, crate::Error> + Send + Sync + 'static,
    {
        self.shared = Arc::new(Shared {
            timeout: self.shared.timeout,
            capacity: self.shared.capacity,
            endpoint_factory: Box::new(factory),
            channels: Mutex::new(BTreeMap::new()),
        });
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
            .shared
            .channels
            .lock()
            .map_err(|_| Status::internal("连接缓存锁不可用"))?
            .get(&key)
            .cloned()
        {
            return Ok(channel);
        }
        let endpoint = (self.shared.endpoint_factory)(target).map_err(|error| {
            Status::failed_precondition(format!("RPC 连接端点配置失败：{error}"))
        })?;
        let channel = endpoint.connect_lazy();
        let mut channels = self
            .shared
            .channels
            .lock()
            .map_err(|_| Status::internal("连接缓存锁不可用"))?;
        // 工厂执行期间可能已有并发调用填入缓存，优先复用已发布的连接。
        if let Some(channel) = channels.get(&key) {
            return Ok(channel.clone());
        }
        if channels.len() >= self.shared.capacity {
            channels.pop_first();
        }
        channels.insert(key, channel.clone());
        Ok(channel)
    }

    /// 节点移除或端点变化时撤销缓存，在途 Channel 克隆仍可完成调用。
    pub fn invalidate(&self, node_id: &str) -> Result<(), Status> {
        self.shared
            .channels
            .lock()
            .map_err(|_| Status::internal("连接缓存锁不可用"))?
            .retain(|(node, _), _| node != node_id);
        Ok(())
    }

    /// 默认调用期限；协议适配器据此设置请求 deadline 和本地等待上限。
    pub fn timeout(&self) -> Duration {
        self.shared.timeout
    }

    /// 构造标准 Tower 执行 Layer，克隆共享本客户端的连接缓存和配置。
    pub fn layer(&self) -> Layer {
        Layer::new(self)
    }

    /// 执行已组装 Tower 中间件的出站调用；就绪等待和调用共用原有限期限。
    ///
    /// 目标已解析，Channel 复用、追踪注入及整体期限由本客户端负责；中间件接收
    /// Outgoing，可读取目标身份、关联序号并修改请求。路由选点应在进入本方法前完成。
    /// 不要求输入或 Service 为 static，可使用借用；中间件自行决定是否引入队列或重试。
    /// 返回类型化回应正文，与 execute 一致，不主动添加重试。
    pub async fn execute_service<Req, Res, S>(
        &self,
        target: &ServiceDescriptor,
        request_id: u64,
        request: Request<Req>,
        span: tracing::Span,
        service: S,
    ) -> Result<Res, Status>
    where
        S: tower::Service<Outgoing<Req>, Response = Response<Res>, Error = Status>,
    {
        self.layer()
            .layer(service)
            .call_once(Call {
                target: target.clone(),
                request_id,
                request,
                span,
            })
            .await
            .map(Response::into_inner)
    }

    /// 执行任意生成协议的单次调用，传播追踪、设置有限期限并记录指标；不自动重试。
    ///
    /// 请求中的 grpc-timeout 只能缩短默认期限；准备与发送均计入预算，超期结果不会作为成功返回。
    pub async fn execute<Req, Res, F, Fut>(
        &self,
        target: &ServiceDescriptor,
        request_id: u64,
        request: Request<Req>,
        span: tracing::Span,
        send: F,
    ) -> Result<Res, Status>
    where
        F: FnOnce(Channel, Request<Req>) -> Fut,
        Fut: Future<Output = Result<Response<Res>, Status>>,
    {
        self.execute_service(target, request_id, request, span, sender::Sender::new(send))
            .await
    }
}
