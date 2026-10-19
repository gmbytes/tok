//! Player 接入组件：组装认证、玩家所有权与串行 session，不拥有监听运行循环。

use crate::{
    ErrorCode, Gateway, GatewayContext, GatewayIngress, GatewayTask, Result,
    codec::FrameCodec,
    player::{Authenticator, PlayerConfig, PlayerExecutor, SessionManager},
};
use std::{sync::Arc, time::Duration};
use tok_core::coordination::{OwnershipStore, SessionOwner};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

/// 登录凭证的最大 UTF-8 字节数，不作为日志字段。
pub const MAX_CREDENTIAL_BYTES: usize = 1024;

/// 玩家接入语义，连接及 session 任务交给统一 Gateway 启动和监督。
pub struct PlayerIngress {
    config: Arc<PlayerConfig>,
    authenticator: Arc<dyn Authenticator>,
    sessions: SessionManager,
    codec: FrameCodec,
}
impl PlayerIngress {
    /// 注入网关任务入口与玩家后端；创建过程不启动监听或 session 任务。
    pub fn new(
        context: GatewayContext,
        config: PlayerConfig,
        owner: SessionOwner,
        authenticator: Arc<dyn Authenticator>,
        ownership: Arc<dyn OwnershipStore>,
        executor: Arc<dyn PlayerExecutor>,
    ) -> Result<Self> {
        config.validate().map_err(|_| ErrorCode::InvalidRequest)?;
        if owner.node_id.trim().is_empty() || owner.service_name.trim().is_empty() {
            return Err(ErrorCode::InvalidRequest.into());
        }
        let codec = FrameCodec::new(
            config.max_frame_size,
            Duration::from_millis(config.io_timeout_ms),
        )?;
        let config = Arc::new(config);
        let shutdown = context.cancellation().child_token();
        let sessions = SessionManager::new(
            config.clone(),
            owner,
            ownership,
            executor,
            context,
            shutdown,
        );
        Ok(Self {
            config,
            authenticator,
            sessions,
            codec,
        })
    }
    /// 获取活动 session 的查询、提交与通知句柄，不能创建未认证会话。
    pub fn sessions(&self) -> SessionManager {
        self.sessions.clone()
    }
}
impl GatewayIngress for PlayerIngress {
    type Connection = TcpStream;
    fn connection(&self, stream: TcpStream, shutdown: CancellationToken) -> GatewayTask {
        let config = self.config.clone();
        let codec = self.codec.clone();
        let authenticator = self.authenticator.clone();
        let sessions = self.sessions.clone();
        Box::pin(async move {
            super::connection::connection(stream, config, codec, authenticator, sessions, shutdown)
                .await
                .map_err(std::io::Error::other)
        })
    }
    fn stop(&self) {
        self.sessions.close_all();
    }
    fn finish(&self) -> GatewayTask {
        let sessions = self.sessions.clone();
        Box::pin(async move {
            sessions.release_all().await;
            Ok(())
        })
    }
}
impl Gateway<PlayerIngress> {
    /// 便捷组装玩家组件；运行时只处理通用接入、任务监督和退出。
    pub fn player(
        config: PlayerConfig,
        owner: SessionOwner,
        authenticator: Arc<dyn Authenticator>,
        ownership: Arc<dyn OwnershipStore>,
        executor: Arc<dyn PlayerExecutor>,
    ) -> Result<Self> {
        let limits = config.runtime_limits();
        Self::new(limits, |context| {
            PlayerIngress::new(context, config, owner, authenticator, ownership, executor)
        })
    }
}
