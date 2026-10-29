//! 编号业务消息的本地处理与传输适配。
//!
//! [`Registry`] 在 protobuf 编解码和通用执行链之间施加非零消息编号约束，
//! 保留调用方的状态、附加结果与业务错误类型，可直接作为 Tower Service 组合公共 Layer；
//! 本地调用不需要 RPC 信封。
//! [`rpc`] 提供 gRPC 客户端、服务端以及玩家会话、事件和计时协议适配。
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod message;
/// 泛型键 protobuf 正文编解码与执行链适配。
pub mod protobuf;
mod registry;
pub mod rpc;

pub use message::{Message, Reply};
pub use registry::{Error, Registry};
