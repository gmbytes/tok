//! 示例游戏自己的 protobuf 消息，与框架内部 RPC 协议分开维护。

/// 示例游戏 v1 消息，由构建脚本自动生成。
#[allow(missing_docs)] // 仅豁免生成代码，不覆盖游戏业务模块。
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/demo.v1.rs"));
}
