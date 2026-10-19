use std::{collections::BTreeMap, sync::Mutex, time::Duration};
use tok_core::{CallContext, ServiceDescriptor};
use tok_proto::internal::v1::{self as proto, CallRequest, CallResponse, Payload};
use tonic::{
    Request, Status,
    transport::{Channel, Endpoint},
};
use tracing::Instrument;

/// 有界的内部 RPC Channel 缓存，不重试业务请求。
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
    /// 设置每次调用的期限和缓存容量，两者必须大于零。
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

    /// 调用 player 入口；上下文必须携带与 bid 匹配的有效会话字段。
    /// 字段校验不能代替网关的所有权和内部身份认证。
    pub async fn player_call(
        &self,
        target: &ServiceDescriptor,
        context: CallContext,
        payload: Payload,
    ) -> Result<CallResponse, Status> {
        self.call(target, context, payload, true).await
    }
    /// 调用普通业务入口，不自动加锁或重试。
    pub async fn business_call(
        &self,
        target: &ServiceDescriptor,
        context: CallContext,
        payload: Payload,
    ) -> Result<CallResponse, Status> {
        self.call(target, context, payload, false).await
    }

    async fn call(
        &self,
        target: &ServiceDescriptor,
        context: CallContext,
        payload: Payload,
        player: bool,
    ) -> Result<CallResponse, Status> {
        if !player && context.timer.is_some() {
            return Err(Status::invalid_argument("计时触发只能调用 player 入口"));
        }
        if context.node_id != target.node_id || context.service_name != target.service_name {
            return Err(Status::invalid_argument("调用上下文与目标服务不匹配"));
        }
        let channel = self.channel(target)?;
        let request_id = context.request_id;
        let timer = context.timer.map(|timer| proto::TimerTrigger {
            timer_id: timer.timer_id,
            scheduled_at_ms: timer.scheduled_at_ms,
            fired_at_ms: timer.fired_at_ms,
        });
        let context = proto::CallContext {
            node_id: context.node_id,
            service_name: context.service_name,
            bid: context.bid,
            request_id,
            session: context.session.map(|identity| proto::SessionIdentity {
                player_id: identity.player_id,
                session_id: identity.session_id,
                epoch: identity.epoch,
            }),
        };
        let mut request = Request::new(CallRequest {
            timer,
            context: Some(context),
            payload: Some(payload),
        });
        request.set_timeout(self.timeout);
        let span = tracing::info_span!("rpc_client", node_id = %target.node_id, service_name = %target.service_name, request_id);
        crate::propagation::inject(request.metadata_mut(), &span);
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            self.timeout,
            async move {
                if player {
                    proto::player_rpc_client::PlayerRpcClient::new(channel)
                        .call(request)
                        .await
                } else {
                    proto::business_rpc_client::BusinessRpcClient::new(channel)
                        .call(request)
                        .await
                }
            }
            .instrument(span),
        )
        .await
        .unwrap_or_else(|_| Err(Status::deadline_exceeded("内部 RPC 超时，未自动重试")));
        metrics::histogram!("tok_rpc_client_duration_seconds", "service_name" => target.service_name.clone()).record(started.elapsed().as_secs_f64());
        metrics::counter!("tok_rpc_client_calls_total", "service_name" => target.service_name.clone(), "result" => if result.is_ok() { "ok" } else { "error" }).increment(1);
        if let Err(status) = &result {
            tracing::error!(node_id = %target.node_id, service_name = %target.service_name, request_id, code = ?status.code(), "内部 RPC 失败，未自动重试");
        }
        let response = result?;
        let response = response.into_inner();
        if response.request_id != request_id {
            return Err(Status::data_loss("RPC 回应序号不匹配"));
        }
        Ok(response)
    }
}
