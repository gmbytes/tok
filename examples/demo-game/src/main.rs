//! 示例游戏二进制：客户端登录、内部 RPC 调用验证及协作退出。

use std::{path::PathBuf, time::Duration};

use clap::Parser;
use tok::{
    AppConfig,
    observability::{Error, Observability, tracing},
    runtime::shutdown_signal,
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
    let client_service = config
        .services
        .iter()
        .find(|service| service.factory_name() == "player-ingress")
        .map(|service| service.service_name.clone());
    if args.client_smoke && client_service.is_none() {
        return Err(std::io::Error::other(
            "客户端 smoke 需要配置 factory='player-ingress' 的接入服务",
        )
        .into());
    }
    let telemetry =
        Observability::init(&config.observability, "demo-game", &config.node_id).await?;
    tracing::info!(node_id = %config.node_id, discovery = ?config.discovery, services = config.services.len(), metrics_address = ?telemetry.metrics_address(), "进程启动");
    let result = async {
        let application = demo_game::application(config)?;
        let directory = application.service_directory();
        tracing::info!("运行客户端 TCP 与内部 RPC，按 Ctrl+C 退出");
        application
            .run_until(async {
                if args.rpc_smoke || args.client_smoke {
                    return tokio::time::timeout(Duration::from_secs(10), async {
                        while !directory
                            .snapshot("player")
                            .is_ok_and(|nodes| !nodes.is_empty())
                            || !directory
                                .snapshot("guild")
                                .is_ok_and(|nodes| !nodes.is_empty())
                            || (args.client_smoke
                                && !directory
                                    .snapshot(
                                        client_service
                                            .as_deref()
                                            .expect("客户端 smoke 已校验接入组件"),
                                    )
                                    .is_ok_and(|nodes| !nodes.is_empty()))
                        {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                        if args.client_smoke {
                            let gateway = directory
                                .snapshot(
                                    client_service
                                        .as_deref()
                                        .expect("客户端 smoke 已校验接入组件"),
                                )
                                .map_err(std::io::Error::other)?
                                .into_iter()
                                .next()
                                .ok_or_else(|| std::io::Error::other("玩家网关尚未就绪"))?;
                            demo_game::client::smoke(gateway.address)
                                .await
                                .map_err(std::io::Error::other)?;
                            tracing::info!("TCP 登录、玩家状态与公会调用验证通过");
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
    }
    .await;
    if let Err(error) = &result {
        tracing::error!(error = %error, "进程运行失败");
    }
    let flush = telemetry.shutdown().await;
    result?;
    flush
}
