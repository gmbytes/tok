//! 会话作用域状态及生命周期观察；迁移必须显式定义快照格式。
use super::SessionHandle;
use crate::{ErrorCode, Result};
use tok_core::Extensions;
use tok_player::SessionIdentity;
use tok_service::Request;

/// 已完成的会话生命周期变化；连接代次供游戏过滤迟到观察。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    /// 新会话扩展状态创建完成。
    Initialized,
    /// 初次登录绑定已建立。
    Bound {
        /// 当前连接代次。
        generation: u64,
    },
    /// 当前连接已解除绑定。
    Detached {
        /// 被解除的连接代次。
        generation: u64,
    },
    /// 恢复绑定建立，正在补发。
    Resuming {
        /// 新连接代次。
        generation: u64,
    },
    /// 缓存补发和完成标记已写出。
    Resumed {
        /// 完成恢复的连接代次。
        generation: u64,
    },
    /// 新业务已冻结，即将采集交接快照。
    HandoffPreparing,
    /// 交接取消并恢复本地准入。
    HandoffCancelled,
    /// 所有权提交已确认，本地入口将关闭。
    HandoffCommitted,
    /// 目标网关已恢复扩展状态。
    Restored,
    /// 会话正在停止，至多通知一次。
    Closing,
    /// 内建执行及子 future 已收束，处理通知后发布结束信号，至多通知一次。
    Closed,
}
/// 每个逻辑会话的状态工厂与快照适配；游戏可自行组合多个状态组件。
///
/// 回调同步且应短小，不执行阻塞 I/O；初次创建和恢复允许失败。
/// 生命周期通知在通道锁外发生，不回滚框架状态；通知 panic 会记录并关闭会话。
pub trait Factory: Send + Sync {
    /// 稳定格式名称，目标网关必须安装同名适配；版本可纳入名称或正文。
    fn kind(&self) -> &str;
    /// 在认证和取得所有权后创建本会话状态；此时尚无 SessionHandle 能力。
    fn initialize(&self, request: Request<SessionIdentity>) -> Result<Extensions>;
    /// 已冻结并排空前序执行后采集状态，不得静默省略需要迁移的数据。
    fn snapshot(&self, request: Request<SessionHandle>) -> Result<Vec<u8>>;
    /// 提交所有权之前检查快照格式和必要内容，不能改变业务状态。
    fn validate(&self, snapshot: &[u8]) -> Result<()>;
    /// 使用目标端已确认的新身份恢复状态；失败时关闭接管，不恢复旧端。
    fn restore(&self, request: Request<(SessionIdentity, &[u8])>) -> Result<Extensions>;
    /// 观察已完成的变化，通过上下文取得网关、管理器和当前会话能力。
    /// 不得等待本会话结束；异步工作须显式交给仍开放的任务范围。
    fn on_change(&self, _request: Request<Change>) {}
}
pub(super) struct Empty;
impl Factory for Empty {
    fn kind(&self) -> &str {
        "tok.empty.v1"
    }
    fn initialize(&self, _: Request<SessionIdentity>) -> Result<Extensions> {
        Ok(Extensions::default())
    }
    fn snapshot(&self, _: Request<SessionHandle>) -> Result<Vec<u8>> {
        Ok(Vec::new())
    }
    fn validate(&self, snapshot: &[u8]) -> Result<()> {
        if snapshot.is_empty() {
            Ok(())
        } else {
            Err(ErrorCode::ResumeUnavailable.into())
        }
    }
    fn restore(&self, request: Request<(SessionIdentity, &[u8])>) -> Result<Extensions> {
        self.validate(request.input.1)?;
        Ok(Extensions::default())
    }
}
