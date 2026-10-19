//! PlayerGateway 兼容接口，内部委托统一 Gateway 运行框架。

use crate::{
    Gateway, Result,
    player::{Authenticator, PlayerConfig, PlayerExecutor, PlayerIngress, SessionManager},
};
use std::sync::Arc;
use tok_core::coordination::{OwnershipStore, SessionOwner};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// 保留原有 PlayerGateway 接口；新代码推荐使用 Gateway::player。
pub struct PlayerGateway(Gateway<PlayerIngress>);
impl PlayerGateway {
    /// 组装统一 Gateway 与 PlayerIngress，注入认证、所有权和执行后端。
    pub fn new(
        config: PlayerConfig,
        owner: SessionOwner,
        authenticator: Arc<dyn Authenticator>,
        ownership: Arc<dyn OwnershipStore>,
        executor: Arc<dyn PlayerExecutor>,
    ) -> Result<Self> {
        Gateway::player(config, owner, authenticator, ownership, executor).map(Self)
    }
    /// 获取玩家 session 的查询、提交与通知入口。
    pub fn sessions(&self) -> SessionManager {
        self.0.ingress().sessions()
    }
    /// 由统一 Gateway 接管 TCP 监听并监督连接和 session 任务。
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> std::io::Result<()> {
        self.0.serve(listener, shutdown).await
    }
}

/// 登录凭证字节数上限的兼容导出。
pub use super::ingress::MAX_CREDENTIAL_BYTES;
