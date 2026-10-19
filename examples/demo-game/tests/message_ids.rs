//! 消息 ID 的协议稳定性，以及构建期名称关联的完整性校验。

#[path = "../build/message_ids.rs"]
mod message_ids;

use demo_game::protocol::{GameMessage, v1::*};

#[test]
fn generated_ids_match_the_proto_catalog_and_keep_wire_values() {
    let ids = [
        GetPlayerRequest::MESSAGE_ID,
        GetPlayerResponse::MESSAGE_ID,
        GetGuildRequest::MESSAGE_ID,
        GetGuildResponse::MESSAGE_ID,
        SetPlayerRequest::MESSAGE_ID,
        SetPlayerResponse::MESSAGE_ID,
        QueryGuildRequest::MESSAGE_ID,
        QueryGuildResponse::MESSAGE_ID,
        StartProductionRequest::MESSAGE_ID,
        StartProductionResponse::MESSAGE_ID,
        ProductionFinished::MESSAGE_ID,
    ];
    // 已有协议值保持不变；声明顺序和 Rust 模块重排不能改变消息 ID。
    assert_eq!(ids, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    for id in ids {
        assert_eq!(MessageId::try_from(id as i32).unwrap() as u32, id);
    }
    assert_eq!(MessageId::Unspecified as u32, 0);
}

#[test]
fn generation_requires_complete_unique_name_matches() {
    let messages = ["GetPlayerRequest".to_owned(), "GetURLResponse".to_owned()];
    let valid = [
        ("MESSAGE_ID_UNSPECIFIED".into(), 0),
        ("MESSAGE_ID_GET_PLAYER_REQUEST".into(), 1),
        ("MESSAGE_ID_GET_URL_RESPONSE".into(), 2),
    ];
    let generated = message_ids::generate(&messages, &valid).unwrap();
    assert!(generated.contains("impl super::GameMessage for GetPlayerRequest"));
    assert!(generated.contains("impl super::GameMessage for GetUrlResponse"));
    assert!(message_ids::generate(&messages, &valid[..2]).is_err());
    assert!(message_ids::generate(&messages, &valid[1..]).is_err());
    for (index, replacement) in [
        (1, ("MESSAGE_ID_TYPO".into(), 1)),
        (2, (valid[2].0.clone(), 1)),
        (1, (valid[1].0.clone(), -1)),
        (1, (valid[1].0.clone(), 0)),
    ] {
        let mut invalid = valid.clone();
        invalid[index] = replacement;
        assert!(message_ids::generate(&messages, &invalid).is_err());
    }
    assert!(
        message_ids::generate(&["GetURLResponse".into(), "GetUrlResponse".into()], &valid).is_err()
    );
}
