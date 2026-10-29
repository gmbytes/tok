/// 已编码的正文与调用方定义的消息键。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message<K = u32> {
    /// 分发或回应键，合法性由协议边界判断。
    pub key: K,
    /// 已编码正文；编码格式由具体适配器决定。
    pub body: Vec<u8>,
}
/// 强类型或已编码的回应与调用方定义的附加结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply<M, O = ()> {
    /// 回应消息。
    pub message: M,
    /// 交给调用方消费的附加结果。
    pub output: O,
}
