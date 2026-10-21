//! 本地开发的 TCP 网关组装与示例认证，生产应用应替换认证实现和安全适配。

mod middleware;
use std::{sync::Arc, time::Duration};
use tok::{
    application::Binding,
    game_rpc::RpcClients,
    gateway::{ErrorCode, Gateway, player},
    infra::routing::RendezvousRouter,
    player::ownership::{Owner as SessionOwner, Store as OwnershipStore},
    service::{
        lifecycle::{Factory, Init, InitFuture},
        task::Task,
    },
};

/// 仅供本地开发的 demo:<正整数 ID> 凭证，不是 SDK 或生产认证方案。
pub struct DemoAuthenticator;
impl player::Authenticator for DemoAuthenticator {
    fn authenticate(&self, credential: String) -> player::AuthenticationTask<'_> {
        Box::pin(async move {
            let player_id = credential
                .strip_prefix("demo:")
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .ok_or(ErrorCode::AuthenticationFailed)?;
            Ok(player::AuthenticatedIdentity {
                player_id: player_id.to_string(),
            })
        })
    }
}

/// 初始化 TCP 监听并组装统一 Gateway 与 player::Ingress。
pub struct PlayerIngressFactory {
    events: Option<tok::events::nats::Config>,
    config: player::Config,
    ownership: Arc<dyn OwnershipStore>,
    tokens: Option<Arc<dyn tok::player::credentials::ResumeTokenCodec>>,
    gateway_rpc: Option<tok::gateway::rpc::GatewayRpcService>,
}
impl PlayerIngressFactory {
    /// 配置经验证的开发网关，注入内存或共享 Redis 所有权。
    pub fn new(config: player::Config, ownership: Arc<dyn OwnershipStore>) -> Self {
        Self {
            events: None,
            config,
            ownership,
            tokens: None,
            gateway_rpc: None,
        }
    }
    /// 显式开启 Core NATS；启动时确认订阅，消费任务由 Gateway 监督。
    pub fn with_events(mut self, events: tok::events::nats::Config) -> Self {
        self.events = Some(events);
        self
    }
    /// 注入本地恢复凭证组件。
    pub fn with_resume_token_codec(
        mut self,
        tokens: Arc<dyn tok::player::credentials::ResumeTokenCodec>,
    ) -> Self {
        self.tokens = Some(tokens);
        self
    }
    /// 注入统一网关内部 RPC 服务，由初始化后的玩家接入登记管理器。
    pub fn with_gateway_rpc(mut self, service: tok::gateway::rpc::GatewayRpcService) -> Self {
        self.gateway_rpc = Some(service);
        self
    }
}
impl Factory for PlayerIngressFactory {
    fn initialize(&self, init: Init) -> InitFuture {
        let config = self.config.clone();
        let ownership = Arc::clone(&self.ownership);
        let tokens = self.tokens.clone();
        let gateway_rpc = self.gateway_rpc.clone();
        let events = self.events.clone();
        Box::pin(async move {
            let binding = init
                .extensions
                .get::<Binding>()
                .cloned()
                .ok_or_else(|| std::io::Error::other("示例玩家网关缺少端点配置"))?;
            if binding.service.protocol != "tcp" {
                return Err(std::io::Error::other("示例玩家网关要求 tcp 端点").into());
            }
            let listener = tokio::net::TcpListener::bind(binding.listen_address).await?;
            let clients = Arc::new(RpcClients::new(
                Duration::from_millis(config.rpc_timeout_ms),
                64,
            )?);
            let executor = Arc::new(player::RpcExecutor::new(
                "player",
                Arc::new(RendezvousRouter::new(Arc::clone(&binding.directory))),
                tok::game_rpc::player::Client::new(clients),
            )?);
            let pipeline = player::Pipeline::new(Arc::<player::RpcExecutor>::clone(&executor))
                .with_admission_layer(middleware::Observe)
                .with_request_layer(middleware::Observe)
                .with_event_layer(middleware::Observe)
                .with_timer_layer(middleware::Observe)
                .with_downlink_layer(middleware::Observe);
            let limits = config.gateway_limits();
            let bus = match events {
                Some(events) => Some(tok::events::nats::Bus::connect(events).await?),
                None => None,
            };
            let gateway = Gateway::new_in(limits, &init, |gateway_context| {
                let ingress = player::Ingress::new(
                    gateway_context,
                    config.clone(),
                    SessionOwner {
                        node_id: binding.service.node_id,
                        service_name: binding.service.service_name,
                    },
                    Arc::new(DemoAuthenticator),
                    ownership,
                    executor,
                )?
                .with_pipeline(pipeline)
                .with_session_extension(Arc::new(middleware::SessionStatistics))?;
                let ingress = match tokens {
                    Some(tokens) => ingress.with_resume_token_codec(tokens),
                    None => ingress,
                };
                let ingress = match gateway_rpc {
                    Some(service) => ingress.with_gateway_rpc(binding.directory, &service),
                    None => ingress,
                };
                if let Some(bus) = &bus {
                    let ingress =
                        ingress.with_topic_policy(|_| Ok(vec![crate::events::TOPIC.into()]));
                    let sessions = ingress.sessions();
                    let capabilities = sessions.extensions();
                    let topics = tok::events::TopicFilter::Exact(tok::events::Topic::default());
                    // 内置组件只投递本地成员，各网关独立消费；同组组件须自行承担目标路由。
                    let components = vec![
                        (
                            "player.forward".into(),
                            crate::events::player_forwarder(
                                init.identity.clone(),
                                sessions.clone(),
                                topics.clone(),
                                None,
                            )
                            .map_err(|_| tok::gateway::ErrorCode::InvalidRequest)?
                            .with_extensions(capabilities.clone()),
                        ),
                        (
                            "player.notify".into(),
                            crate::events::value_notifier(
                                init.identity.clone(),
                                sessions,
                                topics,
                                None,
                            )
                            .map_err(|_| tok::gateway::ErrorCode::InvalidRequest)?
                            .with_extensions(capabilities),
                        ),
                    ];
                    let pool = tok::events::interest::Pool::new(
                        Default::default(),
                        Arc::new(bus.clone()),
                        init.service.tasks(),
                        components,
                    )
                    .map_err(|_| tok::gateway::ErrorCode::InvalidRequest)?;
                    return ingress.with_topic_interests(
                        pool,
                        vec!["player.forward".into(), "player.notify".into()],
                    );
                }
                Ok(ingress)
            })?;
            Ok(Box::pin(async move {
                let result = gateway.serve(listener, init.service.cancellation()).await;
                if let Some(bus) = bus {
                    bus.close().await?;
                }
                result?;
                Ok(())
            }) as Task)
        })
    }
}
