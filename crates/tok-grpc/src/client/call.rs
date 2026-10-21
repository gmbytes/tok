use tok_core::ServiceDescriptor;

/// 连接准备前的 gRPC 调用输入，供客户端执行 Layer 及其外层策略使用。
///
/// 作为 tok_execution::Request 的正文传入客户端 Layer，保留调用范围与本地扩展。
/// 外层策略可在连接获取前选择目标或拒绝请求；排队不能延长上下文中的原期限。
pub struct Call<T> {
    /// 已由调用方选定、尚未获取连接的服务目标。
    pub target: ServiceDescriptor,
    /// 调用关联序号，不自动提供重试或业务幂等。
    pub request_id: u64,
    /// 原始 gRPC 请求；更短的 grpc-timeout 会限制执行预算。
    pub request: tonic::Request<T>,
}
