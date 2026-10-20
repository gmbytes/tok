use crate::{Error, Event, Message, validate_name};
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};

/// 当前订阅处理阶段的结果，不代表后续 session 业务执行完成。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// 不处理此事件或没有可准入的本地目标。
    Ignored,
    /// 当前 handler 的职责已完成；网关 handler 仅表示已完成准入。
    Handled,
    /// 所有可处理目标均被拒绝。
    Rejected,
    /// 一部分目标准入，另一部分拒绝。
    Partial,
}
impl Outcome {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Ignored => "ignored",
            Self::Handled => "handled",
            Self::Rejected => "rejected",
            Self::Partial => "partial",
        }
    }
}

/// 服务器订阅者的异步处理任务；运行时退出时可能被取消。
pub type Task<'a> = Pin<Box<dyn Future<Output = Result<Outcome, Error>> + Send + 'a>>;

/// 与网关及玩家无关的事件处理扩展点。
pub trait Handler: Send + Sync {
    /// 明确订阅的稳定事件名称；不接受通配符，组装时固定。
    fn event_types(&self) -> Vec<&str>;
    /// 处理服务器事件；失败记录后跳过，不自动重试或向发布者返回业务结果。
    fn handle(&self, message: Arc<Message>) -> Task<'_>;
}
type Registered = dyn Fn(Arc<Message>) -> Task<'static> + Send + Sync;

/// 按稳定事件名注册强类型异步处理函数；每个实例代表独立订阅者的兴趣集合。
///
/// 业务服务、网关均使用此表，可在闭包中捕获自己的状态、会话目录或发布连接。
/// 未注册事件直接忽略；同名重复注册拒绝，多个服务可分别订阅相同类型。
pub struct Registry {
    capacity: usize,
    handlers: BTreeMap<String, Arc<Registered>>,
}
impl Registry {
    /// 设置正数注册容量，不建立连接或启动任务。
    pub fn new(capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::InvalidCapacity);
        }
        Ok(Self {
            capacity,
            handlers: BTreeMap::new(),
        })
    }
    /// 注册事件处理函数；目标筛选与后续动作由订阅者决定。
    pub fn register<E, F, Fut>(&mut self, handler: F) -> Result<(), Error>
    where
        E: Event + prost::Message + Default,
        F: Fn(Arc<Message>, E) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Outcome, Error>> + Send + 'static,
    {
        validate_name(E::NAME)?;
        if self.handlers.contains_key(E::NAME) {
            return Err(Error::InvalidInput("事件处理函数重复注册"));
        }
        if self.handlers.len() >= self.capacity {
            return Err(Error::CapacityExceeded);
        }
        let handler = Arc::new(handler);
        self.handlers.insert(
            E::NAME.into(),
            Arc::new(move |message| {
                let handler = Arc::clone(&handler);
                Box::pin(async move {
                    let payload = E::decode(message.body.as_slice())
                        .map_err(|_| Error::InvalidInput("事件正文解码失败"))?;
                    handler(message, payload).await
                })
            }),
        );
        Ok(())
    }
}
impl Handler for Registry {
    fn event_types(&self) -> Vec<&str> {
        self.handlers.keys().map(String::as_str).collect()
    }
    fn handle(&self, message: Arc<Message>) -> Task<'_> {
        Box::pin(async move {
            message.validate(1_048_576)?;
            if let Some(handler) = self.handlers.get(&message.event_type) {
                return handler(message).await;
            }
            Ok(Outcome::Ignored)
        })
    }
}
