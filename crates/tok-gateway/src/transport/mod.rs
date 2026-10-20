//! 统一 Gateway 的传输接入契约，初期提供 TCP 监听适配。

use std::{future::Future, pin::Pin};
use tokio::net::{TcpListener, TcpStream};

/// 等待一项接入的异步操作，必须可取消且不能遗留后台任务。
pub type AcceptTask<'a, C> = Pin<Box<dyn Future<Output = std::io::Result<C>> + Send + 'a>>;

/// 监听接入适配；连接对象由组件解释，不强制其他协议套用 TCP socket。
#[expect(
    clippy::module_name_repetitions,
    reason = "公开传输契约明确限定网关接入，避免与内部 RPC 传输混淆。"
)]
pub trait GatewayTransport: Send {
    /// 每次接入交给组件处理的对象。
    type Connection: Send + 'static;
    /// 等待一次接入，取消后应可继续等待，不能丢失已交付给调用方的连接。
    fn accept(&mut self) -> AcceptTask<'_, Self::Connection>;
}
impl GatewayTransport for TcpListener {
    type Connection = TcpStream;
    fn accept(&mut self) -> AcceptTask<'_, TcpStream> {
        Box::pin(async move { TcpListener::accept(self).await.map(|(stream, _)| stream) })
    }
}
