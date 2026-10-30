//! 业务路由主题与订阅兴趣，独立于传输 subject 和会话成员。
use crate::Error;

/// 一次发布的路由主题；默认作用域与任意具名主题均不相同。
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Topic(Option<String>);
impl Topic {
    /// 创建具名主题，允许点、冒号、斜线及 Unicode，最多 256 个 UTF-8 字节。
    /// 空名、空白、控制字符及 NATS 通配符 * / > 均拒绝；不作大小写或 Unicode 归一化。
    pub fn named(name: impl Into<String>) -> Result<Self, Error> {
        let name = name.into();
        if name.is_empty()
            || name.len() > 256
            || name
                .chars()
                .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '*' | '>'))
        {
            return Err(Error::InvalidInput("事件 topic 无效"));
        }
        Ok(Self(Some(name)))
    }
    /// 具名主题的原始名称；None 表示独立的默认作用域。
    pub fn as_str(&self) -> Option<&str> {
        self.0.as_deref()
    }
    pub(crate) fn from_wire(name: Option<String>) -> Result<Self, Error> {
        name.map_or_else(|| Ok(Self::default()), Self::named)
    }
}

/// 订阅匹配范围；All 仅用于订阅，不能成为发布主题。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TopicFilter {
    /// 只匹配一个主题；Exact(Topic::default()) 不包含其他主题。
    Exact(Topic),
    /// 匹配默认与所有具名主题，仍受事件类型和 namespace 限制。
    All,
}
impl TopicFilter {
    /// 判断具体发布主题是否符合此兴趣，不涉及会话成员或消费组。
    pub fn matches(&self, topic: &Topic) -> bool {
        match self {
            Self::Exact(expected) => expected == topic,
            Self::All => true,
        }
    }
}
