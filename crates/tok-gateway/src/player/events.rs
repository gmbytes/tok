//! 游戏订阅者显式选择的会话投递动作；不属于通用事件传输协议。
use std::sync::Arc;
use tok_events::{Error, Message};

pub use super::session::events::Report;

/// 当前网关的会话筛选条件；未知玩家不会触发加载或创建。
#[derive(Clone, Debug)]
pub enum Target {
    /// 所有本地在线会话。
    All,
    /// 已加入指定主题的本地在线会话。
    Topic(String),
    /// 显式指定的玩家 ID，分发时去重。
    Players(Vec<String>),
}
/// 订阅者决定的会话动作，客户端通知与内部事件分别携带所需数据。
#[derive(Clone, Debug)]
pub enum Action {
    /// 下发客户端通知，不执行 player。
    Notify {
        /// 非零客户端消息 ID。
        message_id: u32,
        /// 通知正文，不要求与原事件正文相同。
        body: Vec<u8>,
    },
    /// 排入会话 FIFO，由注入的 Executor 处理。
    Forward(Arc<Message>),
}
/// 服务端订阅者转换出的本地投递；不编码为 NATS 消息。
#[derive(Clone, Debug)]
pub struct Delivery {
    /// 本地会话筛选条件。
    pub target: Target,
    /// 会话动作。
    pub action: Action,
}
impl Delivery {
    /// 校验有界目标和动作；会话还会检查在线、冻结、所有权和容量。
    pub fn validate(&self, max_bytes: usize) -> Result<(), Error> {
        match &self.target {
            Target::Topic(topic) if topic.trim().is_empty() || topic.len() > 256 => {
                return Err(Error::InvalidInput("事件主题无效"));
            }
            Target::Players(players)
                if players.is_empty()
                    || players.len() > 1024
                    || players
                        .iter()
                        .any(|id| id.trim().is_empty() || id.len() > 1024) =>
            {
                return Err(Error::InvalidInput("事件玩家目标无效"));
            }
            _ => {}
        }
        match &self.action {
            Action::Notify { message_id, body } if *message_id == 0 || body.len() > max_bytes => {
                Err(Error::InvalidInput("通知消息 ID 或正文无效"))
            }
            Action::Forward(event) => event.validate(max_bytes),
            _ => Ok(()),
        }
    }
}

impl From<Report> for tok_events::Outcome {
    fn from(report: Report) -> Self {
        match (report.admitted > 0, report.rejected > 0) {
            (true, true) => Self::Partial,
            (true, false) => Self::Handled,
            (false, true) => Self::Rejected,
            (false, false) => Self::Ignored,
        }
    }
}

/// 创建玩家事件转发处理者；目标选择由游戏定义，原始事件排入 session FIFO。
///
/// 此内置实现仅查找本地在线会话，不加载离线玩家。订阅的 group 由外部组件组装决定；
/// 多网关部署若使用同组消费，须另行保证消息路由到拥有目标会话的网关。
pub fn forward<E, F>(
    sessions: super::SessionManager,
    select: F,
) -> Result<tok_events::Registry, Error>
where
    E: tok_events::Event + prost::Message + Default,
    F: Fn(E) -> Result<Target, Error> + Send + Sync + 'static,
{
    let mut handlers = tok_events::Registry::new(1)?;
    handlers.register::<E, _, _>(move |message, event| {
        let target = select(event);
        let sessions = sessions.clone();
        async move {
            let report = sessions
                .deliver(&Delivery {
                    target: target?,
                    action: Action::Forward(message),
                })
                .await
                .map_err(|_| Error::Unavailable("玩家事件准入失败"))?;
            Ok(report.into())
        }
    })?;
    Ok(handlers)
}

/// 创建广播通知处理者；游戏将事件映射为本地目标及客户端通知正文。
///
/// 可选择全部会话、主题或指定玩家。此处理者只发送 Notify，不执行 player；
/// 消费组由外部组件组装决定，不隐含全局广播或跨网关路由。
pub fn notify<E, F>(
    sessions: super::SessionManager,
    message_id: u32,
    map: F,
) -> Result<tok_events::Registry, Error>
where
    E: tok_events::Event + prost::Message + Default,
    F: Fn(E) -> Result<(Target, Vec<u8>), Error> + Send + Sync + 'static,
{
    if message_id == 0 {
        return Err(Error::InvalidInput("通知消息 ID 必须非零"));
    }
    let mut handlers = tok_events::Registry::new(1)?;
    handlers.register::<E, _, _>(move |_, event| {
        let notification = map(event);
        let sessions = sessions.clone();
        async move {
            let (target, body) = notification?;
            let report = sessions
                .deliver(&Delivery {
                    target,
                    action: Action::Notify { message_id, body },
                })
                .await
                .map_err(|_| Error::Unavailable("通知准入失败"))?;
            Ok(report.into())
        }
    })?;
    Ok(handlers)
}
