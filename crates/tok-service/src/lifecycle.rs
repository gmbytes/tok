//! 服务生命周期编排：构建 Builder，初始化为 Prepared，启动为 Running，再驱动退出。
//!
//! 工厂负责业务状态和入口，task::Supervisor 负责通用任务范围；服务就绪只由 Prepared::start 推进。
mod factory;
mod service;

pub use factory::{Factory, Init, InitTask};
pub use service::{Builder, Error, Handle, Phase, Prepared, Running};
