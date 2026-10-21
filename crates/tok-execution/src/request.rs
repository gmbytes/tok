use crate::Context;
use tok_core::Extensions;

/// 类型化输入、执行范围及请求侧元信息；正文可以包含局部借用。
///
/// 正文转换通过本类型的方法保留扩展；上下文派生由 Context 负责。输入扩展不作为输出元信息回传，
/// 不跨进程序列化，也不自动回写到其他克隆容器。
#[derive(Debug, Clone)]
pub struct Request<T> {
    /// 由可信入口建立的执行范围，不从客户端正文自动恢复。
    pub context: Context,
    /// 当前处理者明确接受的输入。
    pub input: T,
    extensions: Extensions,
}
impl<T> Request<T> {
    /// 建立请求，初始请求扩展为空；不入队、不编码、不启动任务。
    pub fn new(context: Context, input: T) -> Self {
        Self {
            context,
            input,
            extensions: Extensions::default(),
        }
    }
    /// 从已有部分重组请求，保留本地元信息和能力。
    pub fn from_parts(context: Context, input: T, extensions: Extensions) -> Self {
        Self {
            context,
            input,
            extensions,
        }
    }
    /// 拆分执行范围、正文和请求扩展，不清空或序列化能力。
    pub fn into_parts(self) -> (Context, T, Extensions) {
        (self.context, self.input, self.extensions)
    }
    /// 查询请求侧元信息与本地能力。
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }
    /// 由可信适配器或中间件修改请求扩展。
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.extensions
    }
    /// 安装完整请求扩展；覆盖当前容器，不修改其他请求。
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }
    /// 转换正文并移交原执行范围与请求扩展。
    pub fn map<U>(self, transform: impl FnOnce(T) -> U) -> Request<U> {
        Request::from_parts(self.context, transform(self.input), self.extensions)
    }
    /// 尝试转换正文，成功时保留执行范围与请求扩展。
    pub fn try_map<U, E>(self, transform: impl FnOnce(T) -> Result<U, E>) -> Result<Request<U>, E> {
        Ok(Request::from_parts(
            self.context,
            transform(self.input)?,
            self.extensions,
        ))
    }
}
