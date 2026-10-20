//! 示例游戏自己的协议和业务模块，框架 crate 不依赖这些模块。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod client;
pub mod gateway;
pub mod guild;
pub mod player;
pub mod protocol;

use prost::Message;
use protocol::GameMessage;
use std::{sync::Arc, time::Duration};
use tok::{
    AppConfig,
    core::{CallContext, ServiceDirectory, SessionIdentity},
    observability::Error,
    rpc::{RpcClients, RpcEndpoint, RpcService, protocol::internal::v1::Payload},
    runtime::{Application, ServiceContext, ServiceFactory, ServiceTask, StartTask},
};

struct LifecycleService;
impl ServiceFactory for LifecycleService {
    fn start(&self, context: ServiceContext) -> StartTask {
        Box::pin(async move {
            Ok(Box::pin(async move {
                context.shutdown.cancelled().await;
                Ok(())
            }) as ServiceTask)
        })
    }
}

/// 组装 TCP 网关与内部 RPC；所有权按配置选择，公会请求锁默认使用 Memory。
pub fn application(config: AppConfig) -> Result<Application, Error> {
    application_with_locks(config, Arc::new(tok::infra::locks::Memory::new(1024)?))
}

/// 显式注入独立的公会请求锁后端；多进程共享业务须使用 Redis 等共享后端。
pub fn application_with_locks(
    config: AppConfig,
    locks: Arc<dyn tok::core::coordination::LockBackend>,
) -> Result<Application, Error> {
    let mut app = Application::new(config.clone())?;
    let ownership: Arc<dyn tok::core::coordination::OwnershipStore> = match config.ownership {
        tok::OwnershipBackend::Memory => Arc::new(tok::infra::ownership::MemoryOwnership::new(
            config.player_gateway.max_sessions,
        )?),
        tok::OwnershipBackend::Redis => {
            Arc::new(tok::infra::ownership::redis::RedisOwnership::new(
                config.redis_ownership.clone().ok_or_else(|| {
                    tok::core::Error::InvalidConfig("Redis 所有权缺少 redis_ownership 配置".into())
                })?,
            )?)
        }
    };
    let gateway_rpc =
        tok::gateway::rpc::GatewayRpcService::new(config.player_gateway.max_handoff_bytes)?;
    let tokens: Arc<dyn tok::core::credentials::ResumeTokenCodec> =
        Arc::new(match config.resume_token.clone() {
            Some(keys) => tok::gateway::player::resume_token::SignedResumeTokens::new(keys)?,
            None => tok::gateway::player::resume_token::SignedResumeTokens::ephemeral()?,
        });
    if config.player_gateway.handoff_enabled
        && !config.services.iter().any(|service| {
            service.service_name == config.player_gateway.gateway_rpc_service_name
                && service.factory_name() == "gateway-rpc"
                && config.endpoints.iter().any(|endpoint| {
                    endpoint.name == service.endpoint && endpoint.protocol == "grpc"
                })
        })
    {
        return Err(tok::core::Error::InvalidConfig(
            "跨网关交接需要配置内部 gateway-rpc 服务".into(),
        )
        .into());
    }
    for service in &config.services {
        let endpoint = config
            .endpoints
            .iter()
            .find(|entry| entry.name == service.endpoint)
            .ok_or_else(|| {
                tok::core::Error::InvalidConfig(format!("服务端点未配置：{}", service.endpoint))
            })?;
        match service.factory_name() {
            "player-ingress" => {
                if endpoint.protocol != "tcp"
                    || config
                        .services
                        .iter()
                        .filter(|entry| entry.endpoint == service.endpoint)
                        .count()
                        != 1
                {
                    return Err(tok::core::Error::InvalidConfig(
                        "Player 接入组件要求独占 TCP 端点".into(),
                    )
                    .into());
                }
            }
            "scene-ingress" => {
                return Err(tok::core::Error::NotImplemented("Scene 网关接入组件").into());
            }
            "player" | "guild" | "gateway-rpc" => {
                if endpoint.protocol != "grpc" {
                    return Err(tok::core::Error::InvalidConfig(
                        "示例业务组件要求 grpc 端点".into(),
                    )
                    .into());
                }
            }
            factory => {
                return Err(tok::core::Error::InvalidConfig(format!(
                    "示例未注册初始化工厂：{factory}"
                ))
                .into());
            }
        }
    }
    app.register_factory(
        "player-ingress",
        gateway::PlayerIngressFactory::new(config.player_gateway.clone(), ownership)
            .with_resume_token_codec(tokens)
            .with_gateway_rpc(gateway_rpc.clone()),
    )?;
    app.register_factory("player", LifecycleService)?;
    app.register_factory("guild", LifecycleService)?;
    app.register_factory("gateway-rpc", LifecycleService)?;
    let state = Arc::new(player::State::new(config.player_gateway.max_sessions)?);
    for endpoint in &config.endpoints {
        if endpoint.protocol != "grpc" {
            continue;
        }
        let mut rpc = RpcEndpoint::new();
        let mut count = 0;
        for service in config
            .services
            .iter()
            .filter(|service| service.endpoint == endpoint.name)
        {
            rpc = match service.factory_name() {
                "gateway-rpc" => {
                    rpc.add_service(&service.service_name, gateway_rpc.clone().into_rpc())?
                }
                "player" => rpc.add_service(
                    &service.service_name,
                    RpcService::new(
                        &config.node_id,
                        &service.service_name,
                        Arc::new(player::handlers(
                            Arc::clone(&state),
                            app.service_directory(),
                        )?),
                    )?
                    .into_player(),
                )?,
                "guild" => rpc.add_service(
                    &service.service_name,
                    RpcService::new(
                        &config.node_id,
                        &service.service_name,
                        guild::locked_handler(&service.service_name, Arc::clone(&locks))?,
                    )?
                    .into_business(),
                )?,
                other => {
                    return Err(
                        std::io::Error::other(format!("示例尚未提供 RPC 服务：{other}")).into(),
                    );
                }
            };
            count += 1;
        }
        if count > 0 {
            app.register_rpc_endpoint(&endpoint.name, rpc)?;
        }
    }
    Ok(app)
}

/// 内部 RPC 验证，不代表客户端认证或 session 执行器已经实现。
pub async fn rpc_smoke(directory: &dyn ServiceDirectory) -> Result<(), Error> {
    let player = tok::infra::routing::route(directory, "player", "42")?;
    let guild = tok::infra::routing::route(directory, "guild", "7")?;
    let clients = RpcClients::new(Duration::from_secs(2), 8)?;
    let response = clients
        .player_call(
            &player,
            CallContext {
                timer: None,
                node_id: player.node_id.clone(),
                service_name: player.service_name.clone(),
                bid: Some("42".into()),
                request_id: 1,
                session: Some(SessionIdentity {
                    player_id: "42".into(),
                    session_id: "rpc-smoke".into(),
                    epoch: 1,
                }),
            },
            Payload {
                message_id: protocol::v1::GetPlayerRequest::MESSAGE_ID,
                body: protocol::v1::GetPlayerRequest { player_id: 9999 }.encode_to_vec(),
            },
        )
        .await?;
    let payload = response
        .payload
        .ok_or_else(|| std::io::Error::other("player 回应缺少 payload"))?;
    let body = protocol::v1::GetPlayerResponse::decode(payload.body.as_slice())?;
    if response.request_id != 1
        || payload.message_id != protocol::v1::GetPlayerResponse::MESSAGE_ID
        || body.player_id != 42
    {
        return Err(std::io::Error::other("player 回应与可信上下文不匹配").into());
    }
    let response = clients
        .business_call(
            &guild,
            CallContext {
                timer: None,
                node_id: guild.node_id.clone(),
                service_name: guild.service_name.clone(),
                bid: Some("7".into()),
                request_id: 2,
                session: None,
            },
            Payload {
                message_id: protocol::v1::GetGuildRequest::MESSAGE_ID,
                body: protocol::v1::GetGuildRequest { guild_id: 7 }.encode_to_vec(),
            },
        )
        .await?;
    let payload = response
        .payload
        .ok_or_else(|| std::io::Error::other("guild 回应缺少 payload"))?;
    let body = protocol::v1::GetGuildResponse::decode(payload.body.as_slice())?;
    if response.request_id != 2
        || payload.message_id != protocol::v1::GetGuildResponse::MESSAGE_ID
        || body.guild_id != 7
    {
        return Err(std::io::Error::other("guild 回应不匹配").into());
    }
    Ok(())
}
