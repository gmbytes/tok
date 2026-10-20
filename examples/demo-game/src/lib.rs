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

/// 组装客户端 TCP 网关和共享内部 gRPC 服务，按配置选择内存或 Redis 所有权。
pub fn application(config: AppConfig) -> Result<Application, Error> {
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
            "player" | "guild" => {
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
        gateway::PlayerIngressFactory::new(config.player_gateway.clone(), ownership),
    )?;
    app.register_factory("player", LifecycleService)?;
    app.register_factory("guild", LifecycleService)?;
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
                "player" => rpc.add_service(
                    &service.service_name,
                    RpcService::new(
                        &config.node_id,
                        &service.service_name,
                        Arc::new(player::handlers(state.clone(), app.service_directory())?),
                    )?
                    .into_player(),
                )?,
                "guild" => rpc.add_service(
                    &service.service_name,
                    RpcService::new(
                        &config.node_id,
                        &service.service_name,
                        Arc::new(guild::handlers()?),
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
