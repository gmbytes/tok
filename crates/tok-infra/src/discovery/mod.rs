//! 服务目录的开发后端与生产后端扩展入口。

pub mod kubernetes;
pub mod memory;
pub mod redis;

pub use memory::MemoryDirectory;
