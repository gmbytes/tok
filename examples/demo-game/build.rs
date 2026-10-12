//! 从示例游戏 protobuf 定义生成游戏消息代码。

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/demo/v1/player.proto");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure().compile_with_config(
        config,
        &["proto/demo/v1/player.proto"],
        &["proto"],
    )?;
    Ok(())
}
