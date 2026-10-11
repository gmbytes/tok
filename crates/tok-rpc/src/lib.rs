//! 内部 RPC 接入层。后续提供连接复用与用户可组合的 Tower Service/Layer。
//! 超时、实例 bid 分布式锁、观测等策略由用户按服务或方法组装，默认不添加重试层。

pub use tok_proto as protocol;
pub use tonic;
pub use tower;
