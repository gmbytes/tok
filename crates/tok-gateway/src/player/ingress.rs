//! Player 接入组件：组装认证、玩家所有权与串行 session，不拥有监听运行循环。

use crate::{
    ErrorCode, Gateway, GatewayIngress, GatewayTask, Result,
    codec::FrameCodec,
    player::{Authenticator, Config, Executor, SessionManager},
};
use std::{sync::Arc, time::Duration};
use tok_core::coordination::{OwnershipStore, SessionOwner};
use tok_service::task;
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

/// 登录凭证的最大 UTF-8 字节数，不作为日志字段。
pub const MAX_CREDENTIAL_BYTES: usize = 1024;

/// 玩家接入语义，连接及 session 任务交给统一 Gateway 启动和监督。
pub struct Ingress {
    config: Arc<Config>,
    authenticator: Arc<dyn Authenticator>,
    sessions: SessionManager,
    codec: FrameCodec,
}
impl Ingress {
    /// 注入网关内部 RPC 目录并登记玩家管理器，不受交接开关限制。
    pub fn with_gateway_rpc(
        mut self,
        directory: Arc<dyn tok_core::ServiceDirectory>,
        service: &crate::rpc::GatewayRpcService,
    ) -> Result<Self> {
        self.sessions = self.sessions.with_gateway_directory(directory);
        service.register_player(self.sessions.clone())?;
        Ok(self)
    }
    /// 组装纯本地签名凭证组件，验证不访问网络。
    pub fn with_resume_token_codec(
        mut self,
        store: Arc<dyn tok_core::credentials::ResumeTokenCodec>,
    ) -> Self {
        self.sessions = self.sessions.with_resume_token_codec(store);
        self
    }
    /// 注入网关任务入口与玩家后端；创建过程不启动监听或 session 任务。
    pub fn new(
        tasks: task::Handle,
        config: Config,
        owner: SessionOwner,
        authenticator: Arc<dyn Authenticator>,
        ownership: Arc<dyn OwnershipStore>,
        executor: Arc<dyn Executor>,
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
        let shutdown = tasks.cancellation().child_token();
        let sessions = SessionManager::new(
            Arc::clone(&config),
            owner,
            ownership,
            executor,
            tasks,
            shutdown,
        )?;
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
impl GatewayIngress for Ingress {
    type Connection = TcpStream;
    fn connection(&self, stream: TcpStream, shutdown: CancellationToken) -> GatewayTask {
        let config = Arc::clone(&self.config);
        let codec = self.codec.clone();
        let authenticator = Arc::clone(&self.authenticator);
        let sessions = self.sessions.clone();
        Box::pin(async move {
            super::connection::serve_connection(
                stream,
                config,
                codec,
                authenticator,
                sessions,
                shutdown,
            )
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
            sessions.release_all_ownership().await;
            Ok(())
        })
    }
}
impl Gateway<Ingress> {
    /// 便捷组装玩家组件；运行时只处理通用接入、任务监督和退出。
    pub fn player(
        config: Config,
        owner: SessionOwner,
        authenticator: Arc<dyn Authenticator>,
        ownership: Arc<dyn OwnershipStore>,
        executor: Arc<dyn Executor>,
    ) -> Result<Self> {
        let limits = config.gateway_limits();
        Self::new(limits, |tasks| {
            Ingress::new(tasks, config, owner, authenticator, ownership, executor)
        })
    }
}
