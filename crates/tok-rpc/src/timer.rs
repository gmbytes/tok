//! 内部 timer 更新在共享类型与 protobuf 之间的转换。

use tok_player::{TimerSpec, TimerUpdate};
use tok_proto::internal::v1 as proto;
use tonic::Status;

/// 编码内部计时更新；业务容量和时刻校验由 session 调度器执行。
pub fn encode_update(update: TimerUpdate) -> proto::TimerUpdate {
    let update = match update {
        TimerUpdate::ReplaceAll(timers) => {
            proto::timer_update::Update::ReplaceAll(proto::TimerSet {
                timers: timers.into_iter().map(encode_spec).collect(),
            })
        }
        TimerUpdate::Patch { upsert, cancel } => {
            proto::timer_update::Update::Patch(proto::TimerPatch {
                upsert: upsert.into_iter().map(encode_spec).collect(),
                cancel,
            })
        }
    };
    proto::TimerUpdate {
        update: Some(update),
    }
}

/// 解码内部计时更新；缺失更新分支属于无效的服务端回应。
pub fn decode_update(update: proto::TimerUpdate) -> Result<TimerUpdate, Status> {
    Ok(
        match update
            .update
            .ok_or_else(|| Status::internal("计时更新缺少操作"))?
        {
            proto::timer_update::Update::ReplaceAll(set) => {
                TimerUpdate::ReplaceAll(set.timers.into_iter().map(decode_spec).collect())
            }
            proto::timer_update::Update::Patch(patch) => TimerUpdate::Patch {
                upsert: patch.upsert.into_iter().map(decode_spec).collect(),
                cancel: patch.cancel,
            },
        },
    )
}
fn encode_spec(spec: TimerSpec) -> proto::TimerSpec {
    proto::TimerSpec {
        timer_id: spec.timer_id,
        message_id: spec.message_id,
        body: spec.body,
        due_at_ms: spec.due_at_ms,
        interval_ms: spec.interval_ms,
    }
}
fn decode_spec(spec: proto::TimerSpec) -> TimerSpec {
    TimerSpec {
        timer_id: spec.timer_id,
        message_id: spec.message_id,
        body: spec.body,
        due_at_ms: spec.due_at_ms,
        interval_ms: spec.interval_ms,
    }
}
