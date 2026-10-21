use super::TimerTrigger;
use crate::{Error, Result, service::Identity};
use std::{
    any::{Any, TypeId},
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

/// 由可信入口创建的调用来源；不是客户端正文可选择的分发类型。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// 可信服务端内部操作，例如生命周期变化或主动下行。
    Internal,
    /// 服务间请求；序号只关联当前请求与回应。
    Rpc {
        /// RPC 请求关联序号。
        request_id: u64,
    },
    /// 已通过接入校验的客户端业务请求。
    Client {
        /// 客户端请求关联序号。
        request_id: u64,
        /// 已注册的业务消息 ID。
        message_id: u32,
    },
    /// 服务器事件；本地派生与远程接收使用相同的业务执行来源。
    Event {
        /// 原事件的稳定 ID；转发和派生执行不能重新生成。
        event_id: String,
    },
    /// 可信调度器产生的内部计时触发。
    Timer(TimerTrigger),
}

/// 当前调用不得继续发起或接受执行结果的原因；不表示副作用已回滚。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StopReason {
    /// 调用或所属运行范围已取消。
    #[error("调用已取消")]
    Cancelled,
    /// 整次调用的有限预算已耗尽，包括内部就绪等待。
    #[error("调用预算已耗尽")]
    DeadlineExceeded,
}

/// 类型化执行元信息与能力集合；克隆共享已有值，不强制业务状态进入共享锁。
#[derive(Clone, Default)]
pub struct Extensions {
    values: BTreeMap<TypeId, Arc<dyn Any + Send + Sync>>,
}
impl Extensions {
    /// 安装同类型能力并返回旧值；克隆上下文后替换集合不影响原上下文。
    pub fn insert<T: Any + Send + Sync>(&mut self, value: T) -> Option<Arc<T>> {
        self.values
            .insert(TypeId::of::<T>(), Arc::new(value))
            .and_then(|old| old.downcast::<T>().ok())
    }
    /// 合并本进程能力，同类型由传入集合覆盖；不修改其他克隆的集合。
    pub fn extend(&mut self, other: &Self) {
        self.values.extend(
            other
                .values
                .iter()
                .map(|(key, value)| (*key, Arc::clone(value))),
        );
    }
    /// 移除当前上下文的指定能力；不修改其他上下文克隆中的集合。
    pub fn remove<T: Any + Send + Sync>(&mut self) -> Option<Arc<T>> {
        self.values
            .remove(&TypeId::of::<T>())
            .and_then(|value| value.downcast::<T>().ok())
    }
    /// 查询能力；可变状态应由能力自身的类型与并发规则表达。
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|value| value.downcast_ref())
    }
}
impl std::fmt::Debug for Extensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extensions")
            .field("types", &self.values.len())
            .finish()
    }
}

/// 与 RPC、事件编码和网关无关的执行上下文；由 RPC、事件及类型化本地执行共用。
///
/// 入口适配器负责认证、来源与关联字段校验。本结构不构成调用者认证，
/// 不能直接反序列化不可信正文作为上下文。跨进程只传播允许的元数据。
/// 业务实体身份、路由输入和选定端点不属于固定字段，应以各自的类型传入正文或扩展。
#[derive(Debug, Clone)]
pub struct Context {
    service: Identity,
    operation: String,
    source: Source,
    deadline: Instant,
    cancellation: CancellationToken,
    span: tracing::Span,
    extensions: Extensions,
}
impl Context {
    /// 建立有限预算并捕获当前 span；零预算合法，但执行时立即超时。
    pub fn new(
        service: Identity,
        operation: impl Into<String>,
        source: Source,
        budget: Duration,
        cancellation: CancellationToken,
    ) -> Result<Self> {
        let operation = operation.into();
        if service.node_id.trim().is_empty() || service.service_name.trim().is_empty() {
            return Err(Error::InvalidInput("执行服务身份不能为空".into()));
        }
        Self::validate_operation(&operation)?;
        let deadline = Instant::now()
            .checked_add(budget)
            .ok_or_else(|| Error::InvalidInput("执行期限超出单调时钟范围".into()))?;
        Ok(Self {
            service,
            operation,
            source,
            deadline,
            cancellation,
            span: tracing::Span::current(),
            extensions: Extensions::default(),
        })
    }
    /// 为嵌套调用派生上下文，继承当前服务身份、原期限、追踪和扩展；父取消向子传播。
    ///
    /// 子取消不取消父调用；本方法不会重新分配一份预算或自动进入队列。
    /// 此方法保持当前执行服务身份，不负责构建出站 RPC 的路由目标或跨服务上下文。
    pub fn child(&self, operation: impl Into<String>, source: Source) -> Result<Self> {
        let operation = operation.into();
        Self::validate_operation(&operation)?;
        Ok(Self {
            service: self.service.clone(),
            operation,
            source,
            deadline: self.deadline,
            cancellation: self.cancellation.child_token(),
            span: self.span.clone(),
            extensions: self.extensions.clone(),
        })
    }
    /// 安装入口已建立的执行 span；不改变预算和取消范围。
    pub fn with_span(mut self, span: tracing::Span) -> Self {
        self.span = span;
        self
    }
    /// 当前执行所属的服务身份，不包含监听端点，也不表示出站调用的目标服务。
    pub fn service(&self) -> &Identity {
        &self.service
    }
    /// 由入口适配器填写的稳定操作标识，在当前服务内标识方法、事件处理入口或内部操作。
    ///
    /// 不包含实体 ID、请求 ID 等动态值；可用于日志、指标和中间件匹配。
    /// 构造时只检查非空和长度，标识与注册入口的一致性由适配器保证。
    pub fn operation(&self) -> &str {
        &self.operation
    }
    /// 可信入口声明的来源与对应关联信息。
    pub fn source(&self) -> &Source {
        &self.source
    }
    /// 本进程的单调时钟截止点；不作为网络协议直接传输。
    pub fn deadline(&self) -> Instant {
        self.deadline
    }
    /// 从原调用期限扣减后的剩余时间。
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
    /// 调用的取消信号；信号取消不代表所属资源已完成清理。
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
    /// 已关联的追踪 span。
    pub fn span(&self) -> &tracing::Span {
        &self.span
    }
    /// 只读访问本调用安装的类型化能力。
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }
    /// 由可信组装层或中间件安装能力。
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.extensions
    }
    /// 执行前及接受结果前的本地预算检查；不能消除检查后的暂停窗口。
    pub fn check(&self) -> std::result::Result<(), StopReason> {
        if self.cancellation.is_cancelled() {
            return Err(StopReason::Cancelled);
        }
        if self.remaining().is_zero() {
            return Err(StopReason::DeadlineExceeded);
        }
        Ok(())
    }
    fn validate_operation(operation: &str) -> Result<()> {
        if operation.trim().is_empty() || operation.len() > 256 {
            return Err(Error::InvalidInput("执行操作名为空或超出上限".into()));
        }
        Ok(())
    }
}
