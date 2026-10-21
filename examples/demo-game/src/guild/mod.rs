//! 公会请求示例：业务处理不依赖 RPC，组装时显式安装协议适配和请求锁。

use crate::protocol::GameMessage;
use crate::protocol::v1::{GetGuildRequest, GetGuildResponse};
use tok::message::{Registry, Reply};

/// 公会业务处理错误，由 RPC 适配边界决定状态映射。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// 请求正文与可信业务目标不一致。
    TargetMismatch,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TargetMismatch => f.write_str("公会请求与目标业务 ID 不匹配"),
        }
    }
}
impl std::error::Error for Error {}

/// 注册可供本地和 RPC 共用的公会处理链，输入状态是已解析的公会 ID。
pub fn handlers() -> Result<Registry<u64, (), Error>, tok::message::Error> {
    let mut registry = Registry::new();
    registry.register(
        GetGuildRequest::MESSAGE_ID,
        GetGuildResponse::MESSAGE_ID,
        |guild_id, request: GetGuildRequest| async move {
            if guild_id != request.guild_id {
                return Err(Error::TargetMismatch);
            }
            Ok(Reply {
                message: GetGuildResponse {
                    guild_id: request.guild_id,
                },
                output: (),
            })
        },
    )?;
    Ok(registry)
}

/// 用请求锁后端和 RPC 适配包住公会分发；跨节点部署时须注入共享后端。
pub fn locked_handler(
    namespace: &str,
    backend: std::sync::Arc<dyn tok::core::lock::Backend>,
) -> Result<std::sync::Arc<dyn tok::message::rpc::Handler>, tok::message::rpc::Error> {
    use std::sync::Arc;
    use tok::message::rpc::{
        Handler, TowerHandler, business, lock,
        tonic::Status,
        tower::{self, Layer},
    };
    let registry = Arc::new(
        handlers().map_err(|error| tok::message::rpc::Error::Configuration(error.to_string()))?,
    );
    let adapter = Arc::new(business::Handler::new(
        registry,
        |context| {
            context
                .bid
                .as_deref()
                .and_then(|bid| bid.parse::<u64>().ok())
                .ok_or_else(|| Status::invalid_argument("公会调用需要有效目标 ID"))
        },
        |error: Error| Status::invalid_argument(error.to_string()),
    ));
    let service = tower::service_fn(move |request| {
        let adapter = Arc::clone(&adapter);
        async move { adapter.call(request).await }
    });
    let layer = lock::Layer::new(backend, namespace, tok::execution::lock::Config::default())?;
    Ok(Arc::new(TowerHandler(layer.layer(service))))
}
