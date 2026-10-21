//! RPC 目标、关联信息及可选玩家执行身份。

use tok_player::SessionIdentity;
use tok_player::TimerTrigger;

/// RPC 业务调用上下文；服务身份与业务实体 ID 分别管理。
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
    /// player 调用的会话身份；其他服务可以没有会话。
    pub session: Option<SessionIdentity>,
    /// 网关调度产生的内部计时来源；客户端业务请求为 None。
    pub timer: Option<TimerTrigger>,
}
