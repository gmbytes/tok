//! 正文编解码契约，不规定线协议、路由或错误类型。

/// 将一个业务值编码为正文。
pub trait Encode<T> {
    /// 编码失败的具体类型。
    type Error;
    /// 编码借用的业务值。
    fn encode(&self, value: &T) -> Result<Vec<u8>, Self::Error>;
}

/// 将正文解码为业务值。
pub trait Decode<T> {
    /// 解码失败的具体类型。
    type Error;
    /// 解码借用的正文。
    fn decode(&self, body: &[u8]) -> Result<T, Self::Error>;
}
