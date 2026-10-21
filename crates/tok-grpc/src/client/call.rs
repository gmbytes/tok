use tok_core::ServiceDescriptor;

/// 连接准备前的 gRPC 调用输入，供客户端执行 Layer 及其外层策略使用。
///
/// 外层策略可在连接获取前选择目标或拒绝请求；进入客户端 Layer 后才建立其本地期限。
/// 外层就绪或排队等待需由外层另行约束，或将策略放入客户端 Layer 内。
pub struct Call<T> {
    /// 已由调用方选定、尚未获取连接的服务目标。
    pub target: ServiceDescriptor,
    /// 调用关联序号，不自动提供重试或业务幂等。
    pub request_id: u64,
    /// 原始 gRPC 请求；更短的 grpc-timeout 会限制执行预算。
    pub request: tonic::Request<T>,
    /// 本次出站追踪范围，由调用入口提供。
    pub span: tracing::Span,
}
