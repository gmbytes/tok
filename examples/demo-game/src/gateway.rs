//! 本地开发的 TCP 网关组装与示例认证，生产应用应替换认证实现和安全适配。

use std::{sync::Arc, time::Duration};
use tok::{
    core::coordination::{OwnershipStore, SessionOwner},
    gateway::{
        ErrorCode, Gateway,
        player::{
            AuthenticatedPlayer, AuthenticationTask, Authenticator, PlayerConfig, RpcPlayerExecutor,
        },
    },
    infra::routing::RendezvousRouter,
    rpc::RpcClients,
    runtime::{ServiceContext, ServiceFactory, ServiceTask, StartTask},
};

/// 仅供本地开发的 demo:<正整数 ID> 凭证，不是 SDK 或生产认证方案。
pub struct DemoAuthenticator;
impl Authenticator for DemoAuthenticator {
    fn authenticate(&self, credential: String) -> AuthenticationTask<'_> {
        Box::pin(async move {
            let player_id = credential
                .strip_prefix("demo:")
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .ok_or(ErrorCode::AuthenticationFailed)?;
            Ok(AuthenticatedPlayer {
                player_id: player_id.to_string(),
            })
        })
    }
}

/// 初始化 TCP 监听并组装统一 Gateway 与 PlayerIngress。
pub struct PlayerIngressFactory {
    config: PlayerConfig,
    ownership: Arc<dyn OwnershipStore>,
}
impl PlayerIngressFactory {
    /// 配置经验证的开发网关，注入内存或共享 Redis 所有权。
    pub fn new(config: PlayerConfig, ownership: Arc<dyn OwnershipStore>) -> Self {
        Self { config, ownership }
    }
}
impl ServiceFactory for PlayerIngressFactory {
    fn start(&self, context: ServiceContext) -> StartTask {
        let config = self.config.clone();
        let ownership = self.ownership.clone();
        Box::pin(async move {
            if context.service.protocol != "tcp" {
                return Err(std::io::Error::other("示例玩家网关要求 tcp 端点").into());
            }
            let listener = tokio::net::TcpListener::bind(context.listen_address).await?;
            let clients = Arc::new(RpcClients::new(
                Duration::from_millis(config.rpc_timeout_ms),
                64,
            )?);
            let executor = Arc::new(RpcPlayerExecutor::new(
                "player",
                Arc::new(RendezvousRouter::new(context.directory)),
                clients,
            )?);
            let gateway = Gateway::player(
                config,
                SessionOwner {
                    node_id: context.service.node_id,
                    service_name: context.service.service_name,
                },
                Arc::new(DemoAuthenticator),
                ownership,
                executor,
            )?;
            Ok(Box::pin(async move {
                gateway.serve(listener, context.shutdown).await?;
                Ok(())
            }) as ServiceTask)
        })
    }
}
