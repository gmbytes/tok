//! 逻辑业务 RPC 的目标、关联信息与会话身份。

use crate::SessionIdentity;
use crate::TimerTrigger;

/// 逻辑业务 RPC 调用上下文；服务入口要求有效且与 bid 匹配的会话身份。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallContext {
    /// 目标部署节点。
    pub node_id: String,
    /// 目标业务服务名，显式绑定到对应的 protobuf 服务。
    pub service_name: String,
    /// 目标业务 ID；管理类调用可以没有目标实体。
    pub bid: Option<String>,
    /// 关联请求与回应，不承担幂等或自动重试语义。
    pub request_id: u64,
    /// 逻辑会话身份；入口校验时要求非空。
    pub session: Option<SessionIdentity>,
    /// 网关调度产生的内部计时来源；客户端业务请求为 None。
    pub timer: Option<TimerTrigger>,
}
