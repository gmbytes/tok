//! 与业务协议无关的 gRPC 设施：连接复用、共享端点与类型化执行。
//!
//! Client 与 Server 是主要配置入口，client / server 提供标准 Tower Layer、Service 与调用 Future。
//! client::Call 在连接准备前接入外层策略，Outgoing 向内层提供已选定连接和类型化请求。
//! Client::execute、execute_service 与 Server::execute 是同一执行链的便捷方法。
//! Listener::from_server 接收包含监听级 Tower Layer 的 Tonic Server。
//! 游戏消息、玩家事件与 timer 的适配位于 tok-message，本 crate 不理解这些正文。
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod client;
mod listener;
mod outgoing;
mod propagation;
pub mod server;
mod timeout;
pub use client::Client;
pub use listener::{Listener, Readiness};
pub use outgoing::Outgoing;
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
