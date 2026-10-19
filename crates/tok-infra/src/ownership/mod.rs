//! session 所有权后端：单进程 Memory 与跨进程 Redis 租约。

pub mod memory;
pub mod redis;

pub use memory::MemoryOwnership;
