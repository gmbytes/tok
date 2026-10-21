//! 类型化调用执行：上下文、请求、Tower 处理链及期限、取消和追踪约束。
//!
//! 主要入口为 Context、Request、Response、Layer 和同步 handler 适配；不管理服务启动或后台任务。
//! registry 提供自定义键分发与 into_service 路由入口，可在整张分发表外组合 Tower Layer。
//! lock 提供显式取键和结果判断的通用请求锁中间件，不依赖业务上下文或传输类型。
//! 服务构建、生命周期和任务监督由独立的 tok-lifecycle 提供。
#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]
mod context;
mod execution;
mod handler;
pub mod lock;
/// 按自定义键选择处理链，不限定输入、结果或编码格式。
pub mod registry;
mod request;
mod response;
pub use context::{Context, ContextError, StopReason};
pub use execution::{Call, Error, Layer, Service};
pub use handler::{SyncHandler, sync_handler};
pub use registry::Registry;
pub use request::Request;
pub use response::Response;
