use std::{error::Error, path::PathBuf};

use clap::Parser;
use tok::{AppConfig, DiscoveryBackend, core::ServiceDirectory, infra::discovery::MemoryDirectory};

#[derive(Parser)]
#[command(about = "tok 游戏接入骨架：验证角色配置与内存服务目录")]
struct Args {
    /// 角色配置文件路径。
    #[arg(long, default_value = "examples/demo-game/config/local.toml")]
    config: PathBuf,
    /// 只验证配置，不要求对应发现后端已实现。
    #[arg(long)]
    check_config: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let source = std::fs::read_to_string(&args.config)?;
    let config = AppConfig::from_toml(&source)?;
    println!(
        "节点 {}，发现后端 {:?}，角色数 {}",
        config.node_id,
        config.discovery,
        config.roles.len()
    );
    if args.check_config {
        println!("配置验证通过");
        return Ok(());
    }
    if config.discovery != DiscoveryBackend::Memory {
        return Err(tok::core::Error::NotImplemented("Kubernetes 服务发现").into());
    }
    let directory = MemoryDirectory::default();
    for descriptor in config.service_descriptors() {
        directory.register(descriptor)?;
    }
    for role in &config.roles {
        println!("{:?}：{:?}", role.kind, directory.snapshot(role.kind)?);
    }
    println!("工程骨架初始化完成；尚未启动网络服务，示例正常退出。");
    Ok(())
}
