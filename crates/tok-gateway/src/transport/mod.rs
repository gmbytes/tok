//! 接入协议适配与能力描述，具体协议实现后续按选型加入。

/// 接入组件需要声明的逻辑通信能力。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportCapabilities {
    pub request_response: bool,
    pub server_push: bool,
    pub resume: bool,
}
