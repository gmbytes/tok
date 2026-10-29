//! 普通编号消息 RPC 的目标与关联信息。

/// 普通业务调用的目标和关联信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallContext {
    /// 目标部署节点。
    pub node_id: String,
    /// 目标业务服务名。
    pub service_name: String,
    /// 目标业务 ID；管理类调用可以没有目标实体。
    pub bid: Option<String>,
    /// 关联请求与回应，不承担幂等或自动重试语义。
    pub request_id: u64,
}
