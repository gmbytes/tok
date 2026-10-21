use tok_core::Extensions;

/// 类型化成功结果及沿返回路径传递的本地元信息，不携带执行 Context。
///
/// 外层中间件可保留请求 Context，并读取本回应中的结果元信息。
/// 失败仍通过 Result 的错误分支表达，错误元信息由具体错误类型承担。
#[derive(Debug, Clone)]
pub struct Response<T> {
    /// 当前处理者产生的成功结果。
    pub output: T,
    extensions: Extensions,
}
impl<T> Response<T> {
    /// 封装成功结果，初始输出扩展为空。
    pub fn new(output: T) -> Self {
        Self {
            output,
            extensions: Extensions::default(),
        }
    }
    /// 从已有结果与扩展重组回应。
    pub fn from_parts(output: T, extensions: Extensions) -> Self {
        Self { output, extensions }
    }
    /// 查询沿返回路径传递的输出元信息。
    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }
    /// 由处理器或中间件安装输出元信息，不回写请求扩展。
    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.extensions
    }
    /// 安装完整输出扩展，覆盖当前容器。
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }
    /// 转换结果并移交原输出扩展。
    pub fn map<U>(self, transform: impl FnOnce(T) -> U) -> Response<U> {
        Response::from_parts(transform(self.output), self.extensions)
    }
    /// 尝试转换结果，成功时保留输出扩展；失败由调用方错误类型承载。
    pub fn try_map<U, E>(
        self,
        transform: impl FnOnce(T) -> Result<U, E>,
    ) -> Result<Response<U>, E> {
        Ok(Response::from_parts(
            transform(self.output)?,
            self.extensions,
        ))
    }
    /// 取出成功结果；仅在明确的终端边界丢弃输出扩展。
    pub fn into_inner(self) -> T {
        self.output
    }
    /// 拆分结果与输出扩展，供协议或聚合适配器使用。
    pub fn into_parts(self) -> (T, Extensions) {
        (self.output, self.extensions)
    }
}
impl<T> From<T> for Response<T> {
    fn from(output: T) -> Self {
        Self::new(output)
    }
}
