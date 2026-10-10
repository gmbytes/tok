//! 框架内部协议；具体游戏的客户端协议保留在游戏项目中。

pub mod internal {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/tok.internal.v1.rs"));
    }
}
