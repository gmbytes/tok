use super::Handle;
use crate::task::{BoxError, Task};
use std::{future::Future, pin::Pin};
use tok_core::{Extensions, service::Identity};

/// 初始化状态、入口与处理链，成功后返回服务主任务。
pub type InitTask = Pin<Box<dyn Future<Output = Result<Task, BoxError>> + Send>>;

/// 初始化所需的服务身份、运行范围和组装能力，不强制服务绑定网络端点。
#[derive(Clone)]
pub struct Init {
    /// 本次构建的服务身份。
    pub identity: Identity,
    /// 本服务的有界任务入口及生命周期查询句柄。
    pub service: Handle,
    /// 由组装方安装的端点、发现或业务能力；不跨进程传播。
    pub extensions: Extensions,
}

/// 初始化状态、路由及 Layer，完成后才允许发布就绪。
///
/// 初始化可借助运行句柄提交受监督任务；不得自行遗留脱管任务。
/// future 被丢弃时，资源必须具备 RAII 释放路径，不承诺异步清理回调必达。
pub trait Factory: Send + Sync {
    /// 返回可取消的初始化 future，主任务负责持续运行及协作清理。
    fn initialize(&self, init: Init) -> InitTask;
}
impl<F> Factory for F
where
    F: Fn(Init) -> InitTask + Send + Sync,
{
    fn initialize(&self, init: Init) -> InitTask {
        self(init)
    }
}
