//! 生产完成的本地业务处理；由 timer 入口在当前 player 调用中直接分发。

use super::PlayerState;
use tok::{
    events::{Event, Input, Registry},
    rpc::tonic::Status,
};

const OUTPUT: i64 = 1;

pub(super) struct Completed {
    pub(super) scheduled_at_ms: i64,
}
impl Event for Completed {
    const NAME: &'static str = "demo.player.production.completed.v1";
}

pub(super) fn dispatcher() -> Result<Registry<PlayerState, (), Status>, tok::events::Error> {
    let mut dispatcher = Registry::new(1)?;
    dispatcher.register_service::<Completed, _>(tok::service::sync_handler(
        |request: tok::service::Request<Input<'_, PlayerState, Completed>>| {
            let stored = request.input.state;
            let event = &request.input.event.payload;
            if stored.production_due != Some(event.scheduled_at_ms) {
                return Ok(());
            }
            // 先完成可失败的计算，再更新状态；本地分发器自身不提供回滚。
            let value = stored
                .value
                .checked_add(OUTPUT)
                .ok_or_else(|| Status::out_of_range("示例计时数值溢出"))?;
            stored.value = value;
            stored.production_due = None;
            Ok(())
        },
    ))?;
    Ok(dispatcher)
}
