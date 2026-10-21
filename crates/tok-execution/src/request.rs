use crate::Context;

/// 执行上下文与类型化输入；正文可以是具体消息，也可以包含当前状态的可变借用。
#[derive(Debug)]
pub struct Request<T> {
    /// 由可信入口建立的上下文；不从客户端正文自动恢复。
    pub context: Context,
    /// 当前处理者明确接受的输入，协议解码由入口适配承担。
    pub input: T,
}
impl<T> Request<T> {
    /// 组合上下文和输入；不入队、不编码、不启动任务。
    pub fn new(context: Context, input: T) -> Self {
        Self { context, input }
    }
    /// 转换正文并移交原执行上下文，不重新分配预算或丢弃扩展。
    pub fn map<U>(self, transform: impl FnOnce(T) -> U) -> Request<U> {
        Request {
            context: self.context,
            input: transform(self.input),
        }
    }
}
