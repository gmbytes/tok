//! 生产耗时、内部触发及产出由服务端决定，客户端正文不能覆盖规则。

use demo_game::{player, protocol::v1};
use prost::Message;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tok::{
    core::{CallContext, SessionIdentity, TimerTrigger, TimerUpdate},
    infra::discovery::MemoryDirectory,
    rpc::{CallHandler, protocol::internal::v1::Payload, timer::decode_update, tonic::Code},
};

#[tokio::test]
async fn production_rules_are_not_taken_from_client_timer_or_reward_fields() {
    let registry = player::handlers(
        Arc::new(player::State::new(4).unwrap()),
        Arc::new(MemoryDirectory::default()),
    )
    .unwrap();
    let context = CallContext {
        node_id: "node".into(),
        service_name: "player".into(),
        bid: Some("42".into()),
        request_id: 1,
        session: Some(SessionIdentity {
            player_id: "42".into(),
            session_id: "session".into(),
            epoch: 1,
        }),
        timer: None,
    };
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    // 试图夹带 delay=0、interval=1、delta=127；空业务请求没有这些字段。
    let result = registry
        .call(
            context.clone(),
            Payload {
                message_id: player::START_PRODUCTION,
                body: vec![0x10, 0, 0x18, 1, 0x20, 127],
            },
        )
        .await
        .unwrap();
    let after = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let TimerUpdate::Patch { upsert, cancel } = decode_update(result.timers.unwrap()).unwrap()
    else {
        panic!("预期生产计时需求")
    };
    assert!(cancel.is_empty());
    assert_eq!(upsert.len(), 1);
    let spec = &upsert[0];
    assert_eq!(spec.timer_id, "production");
    assert_eq!(spec.message_id, player::FINISH_PRODUCTION);
    assert_eq!(spec.interval_ms, None);
    assert!(spec.body.is_empty());
    assert!((before + 50..=after + 50).contains(&spec.due_at_ms));
    let finish = Payload {
        message_id: player::FINISH_PRODUCTION,
        body: vec![8, 127],
    };
    assert_eq!(
        registry
            .call(context.clone(), finish.clone())
            .await
            .unwrap_err()
            .code(),
        Code::Unimplemented
    );
    let mut internal = context.clone();
    internal.timer = Some(TimerTrigger {
        timer_id: "production".into(),
        scheduled_at_ms: spec.due_at_ms,
        fired_at_ms: spec.due_at_ms,
    });
    registry.call(internal, finish).await.unwrap();
    let result = registry
        .call(
            context,
            Payload {
                message_id: player::GET_PLAYER,
                body: v1::GetPlayerRequest { player_id: 9999 }.encode_to_vec(),
            },
        )
        .await
        .unwrap();
    let player = v1::GetPlayerResponse::decode(result.payload.unwrap().body.as_slice()).unwrap();
    assert_eq!(player.player_id, 42);
    assert_eq!(player.value, 1);
}
