//! 从示例游戏 protobuf 定义生成游戏消息代码。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

#[path = "build/message_ids.rs"]
mod message_ids;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/demo/v1/player.proto",
        "proto/demo/v1/message_id.proto",
    ];
    for path in protos {
        println!("cargo:rerun-if-changed={path}");
    }
    println!("cargo:rerun-if-changed=build/message_ids.rs");
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    let descriptors = config.load_fds(&protos, &["proto"])?;
    let mut messages = Vec::new();
    let mut values = None;
    for file in &descriptors.file {
        if file.package.as_deref() != Some("demo.v1") {
            continue;
        }
        messages.extend(
            file.message_type
                .iter()
                .map(|message| message.name.as_deref().unwrap_or_default().to_owned()),
        );
        for enumeration in &file.enum_type {
            if enumeration.name.as_deref().unwrap_or_default() == "MessageId" {
                values = Some(
                    enumeration
                        .value
                        .iter()
                        .map(|value| {
                            (
                                value.name.as_deref().unwrap_or_default().to_owned(),
                                value.number.unwrap_or_default(),
                            )
                        })
                        .collect::<Vec<_>>(),
                );
            }
        }
    }
    let values = values.ok_or("demo.v1 缺少 MessageId 枚举")?;
    let mappings = message_ids::generate(&messages, &values)?;
    tonic_prost_build::configure().compile_fds_with_config(descriptors, config)?;
    std::fs::write(
        std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("demo.message_ids.rs"),
        mappings,
    )?;
    Ok(())
}
