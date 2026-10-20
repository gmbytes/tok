//! session 所有权后端：单进程 Memory 与跨进程 Redis 租约。

pub mod memory;
pub mod redis;

#[expect(
    clippy::module_name_repetitions,
    reason = "公开类型保留后端或所有权角色名称，便于调用方区分 Memory 与 Redis 实现。"
)]
pub use memory::MemoryOwnership;
