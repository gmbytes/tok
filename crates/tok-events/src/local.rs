//! 当前业务调用内的同步分发，不进入 session 队列，不产生独立任务或客户端回应。
//!
//! 按注册顺序执行，首错停止；已完成的修改不回滚，也不重试或去重。

use crate::{Error, Event, contract::validate_name};

type Handler<E, C, Failure> = dyn Fn(&mut C, &E) -> Result<(), Failure> + Send + Sync;

/// 一种事件的有界订阅者集合；初始化时注册，运行时可通过 Arc 只读共享。
///
/// 显式借用当前业务状态，同一状态的串行访问仍由调用方的 session 等机制保障。
/// 不创建队列、不持有业务状态，也不缓存已发生的事件；零订阅者视为成功。
pub struct Dispatcher<E, C, Failure> {
    capacity: usize,
    handlers: Vec<Box<Handler<E, C, Failure>>>,
}
impl<E: Event, C, Failure> Dispatcher<E, C, Failure> {
    /// 设置订阅者上限并验证事件名；capacity 必须大于零。
    pub fn new(capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::InvalidCapacity);
        }
        validate_name(E::NAME)?;
        Ok(Self {
            capacity,
            handlers: Vec::new(),
        })
    }
    /// 按顺序注册同步订阅者；达到容量上限时拒绝，保留原有注册。
    pub fn register(
        &mut self,
        handler: impl Fn(&mut C, &E) -> Result<(), Failure> + Send + Sync + 'static,
    ) -> Result<(), Error> {
        if self.handlers.len() >= self.capacity {
            return Err(Error::CapacityExceeded);
        }
        self.handlers.push(Box::new(handler));
        Ok(())
    }
    /// 在当前调用栈中依次执行，成功返回执行数量，首个业务错误原样返回。
    ///
    /// 本方法不生成 event_id；重复调用会再次执行全部订阅者，部分更新不自动回滚。
    pub fn dispatch(&self, context: &mut C, event: &E) -> Result<usize, Failure> {
        for handler in &self.handlers {
            handler(context, event)?;
        }
        Ok(self.handlers.len())
    }
}
