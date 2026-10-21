//! 与业务协议无关的 gRPC 设施：连接复用、共享端点与类型化执行。
//!
//! Client::execute 接入任意生成客户端，Server 接入生成的 gRPC 服务处理链。
//! 游戏消息、玩家事件与 timer 的适配位于 tok-message，本 crate 不理解这些正文。
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

mod client;
mod listener;
mod propagation;
mod server;
mod timeout;
pub use client::Client;
pub use listener::{Listener, Readiness};
pub use server::Server;
/// gRPC 协议、服务与状态类型。
pub use tonic;
/// 通用 Service、Layer 与处理链工具。
pub use tower;

/// RPC 配置、监听及传输错误；协议业务错误由上层定义。
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// 端点、执行预算或客户端参数无效。
    #[error("RPC 配置错误：{0}")]
    Configuration(String),
    /// gRPC 传输失败。
    #[error("RPC 传输失败：{0}")]
    Transport(#[from] tonic::transport::Error),
    /// 监听等底层 I/O 操作失败。
    #[error("RPC 监听 I/O 操作失败：{0}")]
    Io(#[from] std::io::Error),
}
