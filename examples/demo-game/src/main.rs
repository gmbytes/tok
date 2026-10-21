//! 示例游戏二进制：客户端登录、内部 RPC 调用验证及协作退出。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

use std::{path::PathBuf, time::Duration};

use clap::Parser;
use tok::{
    AppConfig,
    application::shutdown_signal,
    observability::{Error, Observability, tracing},
};

#[derive(Parser)]
#[command(about = "tok TCP 客户端与内部 RPC 开发示例")]
struct Args {
    /// 服务配置文件路径。
    #[arg(long, default_value = "examples/demo-game/config/local.toml")]
    config: PathBuf,
    /// 只验证配置，不要求对应发现后端已实现。
    #[arg(long)]
    check_config: bool,
    /// 指定运行秒数后优雅退出，用于自动化验证；省略时持续运行。
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    run_for: Option<u64>,
    /// 验证 player 与 guild 的内部 RPC 后通过正常生命周期退出。
    #[arg(long, conflicts_with_all = ["check_config", "run_for"])]
    rpc_smoke: bool,
    /// 使用真实 TCP 验证客户端登录、状态读写和公会调用后退出。
    #[arg(long, conflicts_with_all = ["check_config", "run_for", "rpc_smoke"])]
    client_smoke: bool,
    /// 使用真实 NATS、TCP 与 RPC 验证内部事件和客户端广播后退出。
    #[arg(long, conflicts_with_all = ["check_config", "run_for", "rpc_smoke", "client_smoke"])]
    event_smoke: bool,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    let source = std::fs::read_to_string(&args.config)?;
    let config = AppConfig::from_toml(&source)?;
    if args.check_config {
        println!("配置验证通过");
        return Ok(());
    }
    let events_config = config.events.clone();
    if args.event_smoke && events_config.is_none() {
        return Err(std::io::Error::other("event-smoke 需要 [events] 配置").into());
    }
    let client_service = if args.client_smoke || args.event_smoke {
        Some(
            config
                .services
                .iter()
                .find(|service| service.factory_name() == "player-ingress")
                .map(|service| service.service_name.clone())
                .ok_or_else(|| {
                    std::io::Error::other(
                        "客户端 smoke 需要配置 factory='player-ingress' 的接入服务",
                    )
                })?,
        )
    } else {
        None
    };
    let telemetry =
        Observability::init(&config.observability, "demo-game", &config.node_id).await?;
    tracing::info!(node_id = %config.node_id, discovery = ?config.discovery, services = config.services.len(), metrics_address = ?telemetry.metrics_address(), "进程启动");
    let result = Box::pin(async {
        let application = demo_game::application(config)?;
        let directory = application.service_directory();
        tracing::info!("运行客户端 TCP 与内部 RPC，按 Ctrl+C 退出");
        application
            .run_until(async {
                if args.rpc_smoke || args.client_smoke || args.event_smoke {
                    return tokio::time::timeout(Duration::from_secs(10), async {
                        while !directory
                            .snapshot("player")
                            .is_ok_and(|nodes| !nodes.is_empty())
                            || !directory
                                .snapshot("guild")
                                .is_ok_and(|nodes| !nodes.is_empty())
                            || client_service.as_deref().is_some_and(|name| {
                                !directory
                                    .snapshot(name)
                                    .is_ok_and(|nodes| !nodes.is_empty())
                            })
                        {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                        if let Some(client_service) = client_service.as_deref() {
                            let gateway = directory
                                .snapshot(client_service)
                                .map_err(std::io::Error::other)?
                                .into_iter()
                                .next()
                                .ok_or_else(|| std::io::Error::other("玩家网关尚未就绪"))?;
                            if args.event_smoke {
                                demo_game::events::smoke(
                                    gateway.address,
                                    events_config
                                        .clone()
                                        .ok_or_else(|| std::io::Error::other("缺少 events 配置"))?,
                                )
                                .await
                                .map_err(std::io::Error::other)?;
                                tracing::info!("NATS 转发与广播验证通过");
                            } else {
                                demo_game::client::smoke(gateway.address)
                                    .await
                                    .map_err(std::io::Error::other)?;
                                tracing::info!("TCP 登录、玩家状态与公会调用验证通过");
                            }
                        } else {
                            demo_game::rpc_smoke(directory.as_ref())
                                .await
                                .map_err(std::io::Error::other)?;
                            tracing::info!("player 与 guild 内部 RPC 验证通过");
                        }
                        Ok(())
                    })
                    .await
                    .map_err(std::io::Error::other)?;
                }
                match args.run_for {
                    Some(seconds) => tokio::select! {
                        result = shutdown_signal() => result,
                        _ = tokio::time::sleep(Duration::from_secs(seconds)) => Ok(()),
                    },
                    None => shutdown_signal().await,
                }
            })
            .await?;
        tracing::info!("所有服务已退出");
        Ok::<(), Error>(())
    })
    .await;
    if let Err(error) = &result {
        tracing::error!(error = %error, "进程运行失败");
    }
    let flush = telemetry.shutdown().await;
    result?;
    flush
}
