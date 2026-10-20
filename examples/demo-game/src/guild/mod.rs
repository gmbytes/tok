//! 公会请求示例；组装时显式安装请求锁，持久化仍待后续实现。

use crate::protocol::GameMessage;
use crate::protocol::v1::{GetGuildRequest, GetGuildResponse};
use tok::rpc::{HandlerRegistry, tonic::Status};

/// 注册示例公会请求，校验消息与上下文中的业务 ID 一致。
pub fn handlers() -> Result<HandlerRegistry, tok::rpc::Error> {
    let mut registry = HandlerRegistry::new();
    registry.register(
        GetGuildRequest::MESSAGE_ID,
        GetGuildResponse::MESSAGE_ID,
        |context, request: GetGuildRequest| async move {
            if context.bid.as_deref() != Some(request.guild_id.to_string().as_str()) {
                return Err(Status::invalid_argument("公会请求与目标业务 ID 不匹配"));
            }
            Ok(GetGuildResponse {
                guild_id: request.guild_id,
            })
        },
    )?;
    Ok(registry)
}

/// 用给定请求锁后端包住公会分发；跨节点部署时须注入共享后端。
pub fn locked_handler(
    namespace: &str,
    backend: std::sync::Arc<dyn tok::core::coordination::LockBackend>,
) -> Result<std::sync::Arc<dyn tok::rpc::CallHandler>, tok::rpc::Error> {
    use std::sync::Arc;
    use tok::rpc::{
        CallHandler, TowerHandler, lock,
        tower::{self, Layer},
    };
    let registry = Arc::new(handlers()?);
    let service = tower::service_fn(move |(context, payload)| {
        let registry = Arc::clone(&registry);
        async move { registry.call(context, payload).await }
    });
    let layer = lock::Layer::new(backend, lock::Config::new(namespace))?;
    Ok(Arc::new(TowerHandler(layer.layer(service))))
}
