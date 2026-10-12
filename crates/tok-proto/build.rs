//! 从框架 protobuf 定义生成内部 RPC 消息、客户端及服务端代码。

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/tok/internal/v1/common.proto");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure().compile_with_config(
        config,
        &["proto/tok/internal/v1/common.proto"],
        &["proto"],
    )?;
    Ok(())
}
