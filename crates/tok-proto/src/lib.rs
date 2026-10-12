//! 框架内部协议；具体游戏的客户端协议保留在游戏项目中。

/// 框架内部 RPC 协议的版本命名空间。
pub mod internal {
    /// v1 协议，由构建脚本从 protobuf 自动生成。
    #[allow(missing_docs)] // 仅豁免生成代码，手写协议模块仍需文档。
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/tok.internal.v1.rs"));
    }
}
