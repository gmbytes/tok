//! 主题反向索引；只保存弱引用，索引锁不与会话锁或目录锁嵌套读取。
use super::{handle::SessionHandle, state::SessionState};
use crate::{ErrorCode, Result};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, Weak},
};

// 完整会话身份防止旧会话清理时删除新登录或交接后的成员。
type Key = (String, String, u64);
#[derive(Default)]
pub(super) struct Index {
    topics: Mutex<BTreeMap<String, BTreeMap<Key, Weak<SessionState>>>>,
}
impl Index {
    pub(super) fn members(&self, topic: &str) -> Result<Vec<SessionHandle>> {
        // 先复制弱引用并释放索引锁，再提升引用。最后一个强引用的析构也会清理索引，
        // 不能在持有索引锁时提升或丢弃会话强引用。
        let members: Vec<_> = self
            .topics
            .lock()
            .map_err(|_| ErrorCode::Internal)?
            .get(topic)
            .map(|members| members.values().cloned().collect())
            .unwrap_or_default();
        Ok(members
            .into_iter()
            .filter_map(|weak| weak.upgrade())
            .filter(|state| !state.cancel.is_cancelled())
            .map(SessionHandle)
            .collect())
    }
}
// 当前会话在主题反向索引中的成员登记；不表示 session 归属或 NATS 订阅。
pub(super) struct TopicMembership {
    index: Arc<Index>,
    key: Key,
    session: Weak<SessionState>,
}
impl TopicMembership {
    pub(super) fn new(index: Arc<Index>, session: &Arc<SessionState>) -> Self {
        let identity = &session.ownership.identity;
        Self {
            index,
            key: (
                identity.player_id.clone(),
                identity.session_id.clone(),
                identity.epoch,
            ),
            session: Arc::downgrade(session),
        }
    }
    pub(super) fn join(&self, topic: &str) -> Result<()> {
        self.index
            .topics
            .lock()
            .map_err(|_| ErrorCode::Internal)?
            .entry(topic.into())
            .or_default()
            .insert(self.key.clone(), Weak::clone(&self.session));
        Ok(())
    }
    pub(super) fn leave(&self, topic: &str) -> Result<()> {
        let mut topics = self.index.topics.lock().map_err(|_| ErrorCode::Internal)?;
        if let Some(members) = topics.get_mut(topic) {
            members.remove(&self.key);
            if members.is_empty() {
                topics.remove(topic);
            }
        }
        Ok(())
    }
    pub(super) fn clear(&self, names: &BTreeSet<String>) {
        let Ok(mut topics) = self.index.topics.lock() else {
            tracing::error!("主题索引锁不可用，无法清理成员");
            return;
        };
        for name in names {
            if let Some(members) = topics.get_mut(name) {
                members.remove(&self.key);
                if members.is_empty() {
                    topics.remove(name);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        codec::VERSION,
        player::{
            Config,
            session::{
                channel::{Channel, Outbound},
                state::Completion,
                timer::Scheduler,
            },
        },
        protocol::{self, Frame, frame::Content},
    };
    use std::sync::atomic::AtomicBool;
    use tok_player::{
        SessionIdentity,
        ownership::{Owner as SessionOwner, Record as SessionOwnership},
    };
    use tokio::sync::{Notify, mpsc};
    use tokio_util::sync::CancellationToken;

    fn session(
        index: &Arc<Index>,
        player: &str,
        epoch: u64,
    ) -> (SessionHandle, mpsc::Receiver<Outbound>) {
        let cancel = CancellationToken::new();
        let (sender, mut outgoing) = mpsc::channel(8);
        let (jobs, _) = mpsc::channel(8);
        let state = Arc::new(SessionState {
            extension: Arc::new(crate::player::session::extension::Empty),
            closing_notified: std::sync::atomic::AtomicBool::new(false),
            manager: std::sync::Weak::new(),
            extensions: Default::default(),
            pipeline: crate::player::pipeline::test_pipeline(),
            execution_budget: std::time::Duration::from_millis(Config::default().rpc_timeout_ms),
            downlink_gate: tokio::sync::Mutex::new(()),
            topic_gate: tokio::sync::Mutex::new(()),
            timers: Arc::new(Scheduler::new(8, 4096)),
            initial_execution: None,
            ownership: SessionOwnership {
                identity: SessionIdentity {
                    player_id: player.into(),
                    session_id: "same-session".into(),
                    epoch,
                },
                owner: SessionOwner {
                    node_id: "node".into(),
                    service_name: "gateway".into(),
                },
            },
            cancel: cancel.clone(),
            completion: Completion {
                done: AtomicBool::new(false),
                notify: Notify::new(),
            },
            job_sender: jobs,
            channel: Arc::new(Channel::new(sender, &Config::default(), cancel)),
            resume_token: String::new(),
        });
        state
            .channel
            .bind_topic_membership(Arc::clone(index), &state)
            .unwrap();
        state
            .channel
            .complete_login(Frame {
                version: VERSION,
                content: Some(Content::LoggedIn(protocol::LoginResponse::default())),
            })
            .unwrap();
        outgoing.try_recv().unwrap();
        (SessionHandle(state), outgoing)
    }

    #[tokio::test]
    async fn membership_updates_and_old_generation_cleanup_preserve_current_members() {
        let index = Arc::new(Index::default());
        let (old, _old_output) = session(&index, "1", 1);
        let (current, _current_output) = session(&index, "1", 2);
        let (other, _other_output) = session(&index, "2", 1);
        old.join_topic("guild:7").await.unwrap();
        current.join_topic("guild:7").await.unwrap();
        current.join_topic("guild:7").await.unwrap();
        other.join_topic("guild:8").await.unwrap();
        assert_eq!(index.members("guild:7").unwrap().len(), 2);
        old.close();
        let members = index.members("guild:7").unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].identity().epoch, 2);
        drop(members);
        assert!(old.join_topic("guild:7").await.is_err());
        current.leave_topic("guild:7").await.unwrap();
        assert!(index.members("guild:7").unwrap().is_empty());
        assert_eq!(index.members("guild:8").unwrap().len(), 1);
        drop(other);
        assert!(index.topics.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn index_does_not_keep_sessions_alive_and_admission_rechecks_stale_snapshots() {
        let index = Arc::new(Index::default());
        let (handle, mut output) = session(&index, "1", 1);
        handle.join_topic("guild:7").await.unwrap();
        let candidates = index.members("guild:7").unwrap();
        handle.leave_topic("guild:7").await.unwrap();
        assert!(
            candidates[0]
                .notify_event(9, b"stale", Some("guild:7"))
                .await
                .is_err()
        );
        assert!(output.try_recv().is_err());
        assert!(
            handle
                .0
                .channel
                .replay_after(0)
                .unwrap()
                .unwrap()
                .frames
                .is_empty()
        );
        drop(candidates);
        handle.join_topic("guild:7").await.unwrap();
        let weak = Arc::downgrade(&handle.0);
        drop(handle);
        assert!(weak.upgrade().is_none());
        assert!(index.topics.lock().unwrap().is_empty());
    }
}
