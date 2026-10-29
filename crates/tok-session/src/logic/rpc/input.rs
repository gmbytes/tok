//! 逻辑执行入口的来源和结果；协议适配负责可信来源校验。
use super::CallContext;
use std::sync::Arc;
use tok_proto::internal::v1::{CallResponse, EventResponse, Payload};

/// 一次逻辑业务执行；事件和 timer 只能由可信服务端入口构造。
pub enum Input {
    /// 客户端业务消息。
    Message(CallContext, Payload),
    /// 由 session 驱动的服务器事件。
    Event(CallContext, Arc<tok_events::Message>),
    /// 由 session 调度的内部 timer。
    Timer(CallContext, Payload),
}

/// 逻辑执行的来源专属结果，由协议边界校验与转换。
#[derive(Debug)]
pub enum Output {
    /// 客户端业务消息的回应。
    Message(CallResponse),
    /// 服务器事件的处理结果。
    Event(EventResponse),
    /// timer 的内部执行结果。
    Timer(CallResponse),
}
