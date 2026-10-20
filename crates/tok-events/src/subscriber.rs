use crate::{Error, Handler, validate_name};
use std::sync::Arc;

/// 一个可独立组装的事件订阅组件：处理者声明事件类型，group 决定实例间的消费方式。
///
/// 同一运行时可安装多个组件，也可用不同组件订阅同一种事件。
/// group 不提供玩家归属路由；组件必须保证消息能够到达真正的目标。
#[derive(Clone)]
pub struct Subscriber {
    pub(crate) handler: Arc<dyn Handler>,
    pub(crate) group: Option<String>,
}
impl Subscriber {
    /// 组合处理者与消费组；None 表示每个实例独立接收。
    pub fn new(handler: Arc<dyn Handler>, group: Option<&str>) -> Result<Self, Error> {
        if let Some(group) = group {
            validate_name(group)?;
        }
        Ok(Self {
            handler,
            group: group.map(str::to_owned),
        })
    }
    /// 此组件声明的消费组；不从网关身份或服务名称推导。
    pub fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }
}
