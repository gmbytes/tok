//! Player 接入组件：组装认证、玩家所有权与串行 session，不拥有监听运行循环。

use crate::{
    ErrorCode, Gateway, GatewayIngress, GatewayTask, Result,
    codec::FrameCodec,
    player::{Authenticator, Config, Executor, SessionManager},
};
use std::{sync::Arc, time::Duration};
use tok_core::coordination::{OwnershipStore, SessionOwner};

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
    rpc: Option<crate::rpc::GatewayRpcService>,
}
impl Ingress {
    /// 安装每会话扩展状态、生命周期通知与显式交接快照适配。
    pub fn with_session_extension(
        mut self,
        extension: Arc<dyn super::session::extension::Factory>,
    ) -> Result<Self> {
        self.sessions = self.sessions.with_session_extension(extension)?;
        Ok(self)
    }
    /// 在开始接入前安装准入、请求、事件、timer 和新下行处理链。
    pub fn with_pipeline(mut self, pipeline: super::Pipeline) -> Self {
        self.sessions = self.sessions.with_pipeline(pipeline);
        self
    }
    /// 初始化玩家管理器作用域能力；开始接入前完成安装。
    pub fn with_extensions(mut self, extensions: tok_core::execution::Extensions) -> Self {
        self.sessions = self.sessions.with_extensions(extensions);
        self
    }
    /// 注入网关内部 RPC 目录并登记玩家管理器，不受交接开关限制。
    pub fn with_gateway_rpc(
        mut self,
        directory: Arc<dyn tok_core::ServiceDirectory>,
        service: &crate::rpc::GatewayRpcService,
    ) -> Self {
        self.sessions = self.sessions.with_gateway_directory(directory);
        self.rpc = Some(service.clone());
        self
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
        gateway: crate::Handle,
        config: Config,
        owner: SessionOwner,
        authenticator: Arc<dyn Authenticator>,
        ownership: Arc<dyn OwnershipStore>,
        executor: Arc<dyn Executor>,
    ) -> Result<Self> {
        config.validate().map_err(|_| ErrorCode::InvalidRequest)?;
        if owner.node_id != gateway.identity().node_id
            || owner.service_name != gateway.identity().service_name
        {
            return Err(ErrorCode::InvalidRequest.into());
        }
        let codec = FrameCodec::new(
            config.max_frame_size,
            Duration::from_millis(config.io_timeout_ms),
        )?;
        let config = Arc::new(config);
        let shutdown = gateway.cancellation().child_token();
        let sessions = SessionManager::new(
            Arc::clone(&config),
            owner,
            ownership,
            executor,
            gateway,
            shutdown,
        )?;
        Ok(Self {
            config,
            authenticator,
            sessions,
            codec,
            rpc: None,
        })
    }
    /// 获取活动 session 的查询、提交与通知句柄，不能创建未认证会话。
    pub fn sessions(&self) -> SessionManager {
        self.sessions.clone()
    }
}
impl GatewayIngress for Ingress {
    type Connection = TcpStream;
    fn initialize(&self) -> Result<()> {
        if let Some(service) = &self.rpc {
            service.register_player(self.sessions.clone())?;
        }
        Ok(())
    }
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
        if let Some(service) = &self.rpc
            && let Err(error) = service.unregister_player(&self.sessions)
        {
            tracing::error!(code=?error.code,"撤销网关 RPC 注册失败");
        }
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
        Self::new(
            tok_core::service::Identity {
                node_id: owner.node_id.clone(),
                service_name: owner.service_name.clone(),
            },
            limits,
            |tasks| Ingress::new(tasks, config, owner, authenticator, ownership, executor),
        )
    }
}
