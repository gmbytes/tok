use tok_core::ServiceDescriptor;
use tonic::{Request, Status, transport::Channel};

/// 已选定目标和连接的类型化出站调用，供客户端 Tower 中间件和协议发送器使用。
///
/// 由 Client::execute_service 创建；追踪与初始 grpc-timeout 已注入，请求仍受本地
/// 整体期限约束。目标只读，不通过修改描述符重新路由；本类型不限定 protobuf 或玩家。
pub struct Outgoing<T> {
    target: ServiceDescriptor,
    request_id: u64,
    channel: Channel,
    deadline: tokio::time::Instant,
    /// 发送给生成客户端的请求，可在中间件中检查或修改 metadata 与正文。
    pub request: Request<T>,
}
impl<T> Outgoing<T> {
    pub(crate) fn new(
        target: ServiceDescriptor,
        request_id: u64,
        channel: Channel,
        request: Request<T>,
        deadline: tokio::time::Instant,
    ) -> Self {
        Self {
            target,
            request_id,
            channel,
            request,
            deadline,
        }
    }
    /// 当前实际连接对应的目标服务，调用前由路由组件选定。
    pub fn target(&self) -> &ServiceDescriptor {
        &self.target
    }
    /// 调用方提供的关联序号，只用于关联与诊断，不提供重试或幂等保证。
    pub fn request_id(&self) -> u64 {
        self.request_id
    }
    /// 包含就绪等待和所有中间件执行的本地绝对截止点。
    pub fn deadline(&self) -> tokio::time::Instant {
        self.deadline
    }

    /// 在延迟处理后检查是否仍允许继续发起调用，不回滚已发生的副作用。
    pub fn check(&self) -> Result<(), Status> {
        if tokio::time::Instant::now() >= self.deadline {
            Err(Status::deadline_exceeded("内部 RPC 超时，未自动重试"))
        } else {
            Ok(())
        }
    }

    /// 协议发送前消费调用，按当前剩余时间缩短 grpc-timeout，并取出连接和请求。
    ///
    /// 延迟、排队等中间件消耗预算后，发送器应在实际调用生成客户端前执行本方法。
    /// 请求 metadata 中更短的期限仍保留，过期时不返回连接与请求。
    pub fn into_parts(mut self) -> Result<(Channel, Request<T>), Status> {
        self.check()?;
        let remaining = self
            .deadline
            .saturating_duration_since(tokio::time::Instant::now());
        let budget = crate::timeout::request_budget(self.request.metadata(), remaining)?;
        if budget.is_zero() {
            return Err(Status::deadline_exceeded("内部 RPC 超时，未自动重试"));
        }
        self.request.set_timeout(budget);
        Ok((self.channel, self.request))
    }
}
