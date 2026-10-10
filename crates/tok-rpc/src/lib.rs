//! 内部 RPC 接入层。后续提供连接复用、超时及观测中间件，不添加重试层。

pub use tok_proto as protocol;
pub use tonic;
pub use tower;
