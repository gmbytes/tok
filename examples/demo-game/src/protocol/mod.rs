//! 示例游戏自己的 protobuf 消息，与框架内部 RPC 协议分开维护。

/// 由游戏 proto 的 MessageId 枚举按名称生成的静态消息关联。
pub trait GameMessage: prost::Message {
    /// 稳定的游戏消息 ID；数值只在 proto 中维护。
    const MESSAGE_ID: u32;
}

/// 示例游戏 v1 消息，由构建脚本自动生成。
#[allow(missing_docs)] // 仅豁免生成代码，不覆盖游戏业务模块。
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/demo.v1.rs"));
    include!(concat!(env!("OUT_DIR"), "/demo.message_ids.rs"));
}
