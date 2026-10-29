//! 编号业务消息的本地处理与传输适配。
//!
//! [`Registry`] 是默认的 protobuf 编号消息注册器，在注册请求与回应时检查非零编号，
//! 保留调用方的状态、附加结果与业务错误类型，可直接作为 Tower Service 组合公共 Layer；
//! 本地调用不需要 RPC 信封。
//! [`rpc`] 提供普通业务消息的 gRPC 客户端、服务端和可选请求锁适配。
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod message;
/// 泛型键 protobuf 正文编解码与执行链适配。
pub mod protobuf;
mod registry;
pub mod rpc;

pub use message::{Message, Reply};
pub use registry::{Error, Registry};
