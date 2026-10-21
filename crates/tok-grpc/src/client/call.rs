use tok_core::ServiceDescriptor;
use tonic::{Request, Status, transport::Channel};

/// 连接准备前的 gRPC 调用输入，供客户端执行 Layer 及其外层策略使用。
///
/// 作为 tok_execution::Request 的正文传入客户端 Layer，保留调用范围与本地扩展。
/// 外层策略可在连接获取前选择目标或拒绝请求；排队不能延长上下文中的原期限。
pub struct Call<T> {
    /// 已由调用方选定、尚未获取连接的服务目标。
    pub target: ServiceDescriptor,
    /// 原始 gRPC 请求；更短的 grpc-timeout 会限制执行预算。
    pub request: Request<T>,
}

/// 已选定目标和连接的类型化出站调用，供客户端 Tower 中间件和协议发送器使用。
///
/// 由客户端 Layer 在连接准备阶段创建；追踪与初始 grpc-timeout 已注入，请求仍受本地
/// 整体期限约束。持有 Channel 不保证远端已连通或就绪。目标只读，不通过修改描述符重新路由；本类型不限定 protobuf 或玩家。
pub struct PreparedCall<T> {
    target: ServiceDescriptor,
    channel: Channel,
    /// 发送给生成客户端的请求，可在中间件中检查或修改 metadata 与正文。
    pub request: Request<T>,
}
impl<T> PreparedCall<T> {
    pub(crate) fn new(target: ServiceDescriptor, channel: Channel, request: Request<T>) -> Self {
        Self {
            target,
            channel,
            request,
        }
    }
    /// 当前实际连接对应的目标服务，调用前由路由组件选定。
    pub fn target(&self) -> &ServiceDescriptor {
        &self.target
    }
    /// 协议发送前消费调用，按当前剩余时间缩短 grpc-timeout，并取出连接和请求。
    ///
    /// 延迟、排队等中间件消耗预算后，发送器应在实际调用生成客户端前执行本方法。
    /// 传入包装此正文的 Request.context，使用其当前期限与取消信号；更短 metadata 期限仍保留。
    /// 过期或取消时不返回连接与请求，本类型不另外保存可漂移的执行范围。
    pub fn into_parts(
        mut self,
        context: &tok_execution::Context,
    ) -> Result<(Channel, Request<T>), Status> {
        context.check().map_err(|reason| match reason {
            tok_execution::StopReason::Cancelled => Status::cancelled("调用已取消"),
            tok_execution::StopReason::DeadlineExceeded => {
                Status::deadline_exceeded("内部 RPC 超时，未自动重试")
            }
        })?;
        let remaining = context.remaining();
        let budget = crate::timeout::request_budget(self.request.metadata(), remaining)?;
        if budget.is_zero() {
            return Err(Status::deadline_exceeded("内部 RPC 超时，未自动重试"));
        }
        self.request.set_timeout(budget);
        Ok((self.channel, self.request))
    }
}
