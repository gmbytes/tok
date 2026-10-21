//! 生产耗时、内部触发及产出由服务端决定，客户端正文不能覆盖规则。

use demo_game::protocol::GameMessage;
use demo_game::{player, protocol::v1};
use prost::Message;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tok::{
    infra::discovery::MemoryDirectory,
    message::rpc::{
        CallContext, Handler, protocol::internal::v1::Payload, timer::decode_update, tonic::Code,
    },
    player::TimerTrigger,
    player::{SessionIdentity, TimerUpdate},
};

fn context() -> CallContext {
    CallContext {
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
    }
}

#[tokio::test]
async fn production_rules_are_not_taken_from_client_timer_or_reward_fields() {
    let registry = player::handlers(
        Arc::new(player::State::new(4).unwrap()),
        Arc::new(MemoryDirectory::default()),
    )
    .unwrap();
    let context = context();
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    // 试图夹带 delay=0、interval=1、delta=127；空业务请求没有这些字段。
    let result = registry
        .call(execution((
            context.clone(),
            Payload {
                message_id: v1::StartProductionRequest::MESSAGE_ID,
                body: vec![0x10, 0, 0x18, 1, 0x20, 127],
            },
        )))
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
    assert_eq!(spec.message_id, v1::ProductionFinished::MESSAGE_ID);
    assert_eq!(spec.interval_ms, None);
    assert!(spec.body.is_empty());
    assert!((before + 50..=after + 50).contains(&spec.due_at_ms));
    // 快速重登的新 session 通过状态请求重新取得服务端保存的完整 timer 集合。
    let mut reloaded = context.clone();
    reloaded.session.as_mut().unwrap().session_id = "new-session".into();
    let initialized = registry
        .call(execution((
            reloaded,
            Payload {
                message_id: v1::GetPlayerRequest::MESSAGE_ID,
                body: vec![],
            },
        )))
        .await
        .unwrap();
    let TimerUpdate::ReplaceAll(restored) = decode_update(initialized.timers.unwrap()).unwrap()
    else {
        panic!("初始化应返回全量计时集合");
    };
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].due_at_ms, spec.due_at_ms);
    assert_eq!(restored[0].timer_id, spec.timer_id);
    let finish = Payload {
        message_id: v1::ProductionFinished::MESSAGE_ID,
        body: vec![8, 127],
    };
    assert_eq!(
        registry
            .call(execution((context.clone(), finish.clone())))
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
    let mut stale = internal.clone();
    let stale_timer = stale.timer.as_mut().unwrap();
    stale_timer.scheduled_at_ms += 1;
    stale_timer.fired_at_ms += 1;
    registry
        .call(execution((stale, finish.clone())))
        .await
        .unwrap();
    let unchanged = registry
        .call(execution((
            context.clone(),
            Payload {
                message_id: v1::GetPlayerRequest::MESSAGE_ID,
                body: vec![],
            },
        )))
        .await
        .unwrap();
    assert_eq!(
        v1::GetPlayerResponse::decode(unchanged.payload.unwrap().body.as_slice())
            .unwrap()
            .value,
        0
    );
    let TimerUpdate::ReplaceAll(pending) = decode_update(unchanged.timers.unwrap()).unwrap() else {
        panic!("应保留未完成生产")
    };
    assert_eq!(pending[0].due_at_ms, spec.due_at_ms);
    registry
        .call(execution((internal.clone(), finish.clone())))
        .await
        .unwrap();
    // 重复 timer 仍经过本地分发，但业务根据保存的生产时刻避免重复产出。
    registry.call(execution((internal, finish))).await.unwrap();
    let result = registry
        .call(execution((
            context,
            Payload {
                message_id: v1::GetPlayerRequest::MESSAGE_ID,
                body: v1::GetPlayerRequest { player_id: 9999 }.encode_to_vec(),
            },
        )))
        .await
        .unwrap();
    let player = v1::GetPlayerResponse::decode(result.payload.unwrap().body.as_slice()).unwrap();
    assert_eq!(player.player_id, 42);
    assert_eq!(player.value, 1);
}

#[tokio::test]
async fn local_production_error_reaches_the_original_call_without_partial_state_updates() {
    let registry = player::handlers(
        Arc::new(player::State::new(4).unwrap()),
        Arc::new(MemoryDirectory::default()),
    )
    .unwrap();
    let context = context();
    registry
        .call(execution((
            context.clone(),
            Payload {
                message_id: v1::SetPlayerRequest::MESSAGE_ID,
                body: v1::SetPlayerRequest { value: i64::MAX }.encode_to_vec(),
            },
        )))
        .await
        .unwrap();
    let started = registry
        .call(execution((
            context.clone(),
            Payload {
                message_id: v1::StartProductionRequest::MESSAGE_ID,
                body: vec![],
            },
        )))
        .await
        .unwrap();
    let TimerUpdate::Patch { upsert, .. } = decode_update(started.timers.unwrap()).unwrap() else {
        panic!("缺少生产 timer")
    };
    let due = upsert[0].due_at_ms;
    let mut internal = context.clone();
    internal.timer = Some(TimerTrigger {
        timer_id: "production".into(),
        scheduled_at_ms: due,
        fired_at_ms: due,
    });
    let error = registry
        .call(execution((
            internal,
            Payload {
                message_id: v1::ProductionFinished::MESSAGE_ID,
                body: vec![],
            },
        )))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::OutOfRange);
    let result = registry
        .call(execution((
            context,
            Payload {
                message_id: v1::GetPlayerRequest::MESSAGE_ID,
                body: vec![],
            },
        )))
        .await
        .unwrap();
    assert_eq!(
        v1::GetPlayerResponse::decode(result.payload.unwrap().body.as_slice())
            .unwrap()
            .value,
        i64::MAX
    );
    let TimerUpdate::ReplaceAll(pending) = decode_update(result.timers.unwrap()).unwrap() else {
        panic!("错误不能清除原生产状态")
    };
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].due_at_ms, due);
}

fn execution<T>(input: T) -> tok::execution::Request<T> {
    let context = tok::execution::Context::new(
        tok::core::service::Identity {
            node_id: "node".into(),
            service_name: "player".into(),
        },
        "test",
        std::time::Duration::from_secs(5),
        tokio_util::sync::CancellationToken::new(),
    )
    .unwrap();
    tok::execution::Request::new(context, input)
}
