use std::{path::PathBuf, time::Duration};

use clap::Parser;
use tok::{
    AppConfig,
    observability::{Error, Observability, tracing},
    runtime::{
        Application, ServiceContext, ServiceFactory, ServiceTask, StartTask, shutdown_signal,
    },
};

#[derive(Parser)]
#[command(about = "tok 单进程多服务生命周期示例")]
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
}

struct LifecycleService;

impl ServiceFactory for LifecycleService {
    fn start(&self, context: ServiceContext) -> StartTask {
        Box::pin(async move {
            tracing::info!(service_name = %context.service.service_name, "服务启动（生命周期示例，未监听网络）");
            Ok(Box::pin(async move {
                context.shutdown.cancelled().await;
                tracing::info!(service_name = %context.service.service_name, "服务退出");
                Ok(())
            }) as ServiceTask)
        })
    }
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
    let service_names: Vec<_> = config
        .services
        .iter()
        .map(|service| service.service_name.clone())
        .collect();
    let telemetry =
        Observability::init(&config.observability, "demo-game", &config.node_id).await?;
    tracing::info!(node_id = %config.node_id, discovery = ?config.discovery, services = config.services.len(), metrics_address = ?telemetry.metrics_address(), "进程启动");
    let result = async {
        let mut application = Application::new(config)?;
        for service_name in service_names {
            application.register(service_name, LifecycleService)?;
        }
        tracing::info!("运行服务生命周期，按 Ctrl+C 退出；尚未启动游戏网络服务");
        application
            .run_until(async {
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
