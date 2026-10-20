//! 由游戏注入的凭证校验，不从业务正文取得玩家身份。

use crate::Result;
use std::{future::Future, pin::Pin};

/// 认证扩展点产生的可信玩家身份。
#[derive(Debug, Clone)]
pub struct AuthenticatedIdentity {
    /// 游戏确认并规范化的非空玩家业务 ID，别名必须映射到同一 ID。
    pub player_id: String,
}

/// 凭证校验 future，受网关的认证期限控制。
pub type AuthenticationTask<'a> =
    Pin<Box<dyn Future<Output = Result<AuthenticatedIdentity>> + Send + 'a>>;

/// 游戏认证扩展点；实现不能记录原始凭证。
pub trait Authenticator: Send + Sync {
    /// 校验原始登录凭证并返回可信玩家身份，失败时返回有限错误码。
    fn authenticate(&self, credential: String) -> AuthenticationTask<'_>;
}
