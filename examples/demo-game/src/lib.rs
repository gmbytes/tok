//! 示例游戏自己的协议和业务模块，框架 crate 不依赖这些模块。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

pub mod client;
pub mod events;
pub mod gateway;
pub mod guild;
pub mod player;
pub mod protocol;

use prost::Message;
use protocol::GameMessage;
use std::{sync::Arc, time::Duration};
use tok::{
    AppConfig,
    application::Application,
    core::ServiceDirectory,
    observability::Error,
    player::SessionIdentity,
    rpc::{CallContext, RpcClients, RpcService, protocol::internal::v1::Payload},
    service::{
        lifecycle::{Init, InitTask},
        task::Task,
    },
};

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
    let ownership: Arc<dyn tok::player::ownership::Store> = match config.ownership {
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
    let tokens: Arc<dyn tok::player::credentials::ResumeTokenCodec> =
        Arc::new(match config.resume_token.clone() {
            Some(keys) => tok::gateway::player::resume_token::SignedResumeTokens::new(keys)?,
            None => tok::gateway::player::resume_token::SignedResumeTokens::ephemeral()?,
        });
    if config.player_gateway.handoff_enabled
        && !config.services.iter().any(|service| {
            service.service_name == config.player_gateway.gateway_rpc_service_name
                && service.factory_name() == "gateway-rpc"
                && config.endpoints.iter().any(|endpoint| {
                    service.endpoint.as_deref() == Some(endpoint.name.as_str())
                        && endpoint.protocol == "grpc"
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
            .find(|entry| service.endpoint.as_deref() == Some(entry.name.as_str()))
            .ok_or_else(|| {
                tok::core::Error::InvalidConfig(format!(
                    "示例服务缺少网络端点：{}",
                    service.service_name
                ))
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
    let mut ingress = gateway::PlayerIngressFactory::new(config.player_gateway.clone(), ownership)
        .with_resume_token_codec(tokens)
        .with_gateway_rpc(gateway_rpc.clone());
    if let Some(events) = config.events.clone() {
        ingress = ingress.with_events(events);
    }
    app.register_factory("player-ingress", ingress)?;

    // 自定义 protobuf 服务沿用同一 Factory 生命周期；端点由 Application 按配置合并。
    app.register_factory("gateway-rpc", |init: Init| -> InitTask {
        Box::pin(async move {
            Ok(Box::pin(async move {
                init.service.cancellation().cancelled().await;
                Ok(())
            }) as Task)
        })
    })?;
    for service in config
        .services
        .iter()
        .filter(|service| service.factory_name() == "gateway-rpc")
    {
        app.bind_rpc(&service.service_name, gateway_rpc.clone().into_rpc())?;
    }
    let capacity = config.player_gateway.max_sessions;
    app.register_rpc_factory(
        "player",
        move |init: Init| -> InitTask {
            Box::pin(async move {
                let endpoints = init
                    .extensions
                    .get::<tok::application::Binding>()
                    .ok_or_else(|| std::io::Error::other("服务缺少发现能力"))?;
                let handlers = player::handlers(
                    Arc::new(player::State::new(capacity)?),
                    Arc::clone(&endpoints.directory),
                )?;
                init.extensions
                    .get::<tok::rpc::Binding>()
                    .ok_or_else(|| std::io::Error::other("服务缺少 RPC 绑定"))?
                    .install(&init, Arc::new(handlers))?;
                Ok(Box::pin(async move {
                    init.service.cancellation().cancelled().await;
                    Ok(())
                }) as Task)
            })
        },
        RpcService::into_player,
    )?;
    app.register_rpc_factory(
        "guild",
        move |init: Init| -> InitTask {
            let locks = Arc::clone(&locks);
            Box::pin(async move {
                let handlers = guild::locked_handler(&init.identity.service_name, locks)?;
                init.extensions
                    .get::<tok::rpc::Binding>()
                    .ok_or_else(|| std::io::Error::other("服务缺少 RPC 绑定"))?
                    .install(&init, handlers)?;
                Ok(Box::pin(async move {
                    init.service.cancellation().cancelled().await;
                    Ok(())
                }) as Task)
            })
        },
        RpcService::into_business,
    )?;
    Ok(app)
}

/// 内部 RPC 验证，不代表客户端认证或 session 执行器已经实现。
pub async fn rpc_smoke(directory: &dyn ServiceDirectory) -> Result<(), Error> {
    let player = tok::infra::routing::route(directory, "player", "42")?;
    let guild = tok::infra::routing::route(directory, "guild", "7")?;
    let clients = Arc::new(RpcClients::new(Duration::from_secs(2), 8)?);
    let player_client = tok::rpc::player::Client::new(Arc::clone(&clients));
    let business_client = tok::rpc::business::Client::new(clients);
    let response = player_client
        .call(
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
    let response = business_client
        .call(
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
