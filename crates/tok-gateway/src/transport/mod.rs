//! 接入协议适配与能力描述，具体协议实现后续按选型加入。

/// 接入组件需要声明的逻辑通信能力。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportCapabilities {
    /// 是否支持关联业务请求与回应。
    pub request_response: bool,
    /// 是否支持服务端主动下行。
    pub server_push: bool,
    /// 是否支持逻辑会话恢复及消息接续。
    pub resume: bool,
}
