use crate::{Error, Handler, TopicFilter, validate_name};
use std::sync::Arc;

/// 一个可独立组装的事件订阅组件：显式选择 topic 兴趣，处理者声明事件类型，group 决定实例间的消费方式。
///
/// 同一运行时可安装多个组件，也可用不同组件订阅同一种事件。
/// group 不提供玩家归属路由；组件必须保证消息能够到达真正的目标。
#[derive(Clone)]
pub struct Subscriber {
    pub(crate) service: tok_core::service::Identity,
    pub(crate) extensions: tok_core::Extensions,
    pub(crate) handler: Arc<dyn Handler>,
    pub(crate) group: Option<String>,
    pub(crate) topics: TopicFilter,
}
impl Subscriber {
    /// 组合处理者、主题兴趣与消费组；None 表示每个实例独立接收。
    pub fn new(
        service: tok_core::service::Identity,
        handler: Arc<dyn Handler>,
        topics: TopicFilter,
        group: Option<&str>,
    ) -> Result<Self, Error> {
        if service.node_id.trim().is_empty() || service.service_name.trim().is_empty() {
            return Err(Error::InvalidInput("订阅服务身份不能为空"));
        }
        if let Some(group) = group {
            validate_name(group)?;
        }
        Ok(Self {
            service,
            extensions: tok_core::Extensions::default(),
            handler,
            topics,
            group: group.map(str::to_owned),
        })
    }
    /// 为固定或动态兴趣选择主题范围；保留事件类型、身份、能力及消费组。
    pub fn with_topics(mut self, topics: TopicFilter) -> Self {
        self.topics = topics;
        self
    }
    /// 安装本服务的本地能力，接收时复制到执行上下文，不向远端传播。
    pub fn with_extensions(mut self, extensions: tok_core::Extensions) -> Self {
        self.extensions = extensions;
        self
    }
    /// 此组件声明的主题兴趣；不从本地会话成员自动推导。
    pub fn topics(&self) -> &TopicFilter {
        &self.topics
    }
    /// 此组件声明的消费组；不从网关身份或服务名称推导。
    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }
}
