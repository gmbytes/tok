use super::*;
use tokio::io::AsyncReadExt;

fn message(kind: MessageKind, size: usize) -> LogicalMessage {
    LogicalMessage {
        kind,
        message_id: 8,
        request_id: 12,
        server_seq: Some(999),
        error_code: 0,
        body: vec![7; size],
    }
}

fn start(downlink: &Downlink) {
    downlink
        .start(Frame {
            version: VERSION,
            content: Some(Content::LoggedIn(protocol::LoginResponse::default())),
        })
        .unwrap();
}

#[tokio::test]
async fn login_precedes_publication_and_ack_does_not_wait_for_socket_completion() {
    let (sender, mut queue) = mpsc::channel(8);
    let downlink = Downlink::new(sender, &PlayerConfig::default());
    assert_eq!(
        downlink
            .publish(message(MessageKind::Notify, 0), 0)
            .unwrap_err()
            .code,
        ErrorCode::InvalidState
    );
    start(&downlink);
    downlink
        .publish(message(MessageKind::Notify, 0), 0)
        .unwrap();
    downlink
        .publish(message(MessageKind::Response, 0), 4)
        .unwrap();
    assert!(matches!(queue.recv().await, Some(Outbound::Frame(_, _))));
    let batch = downlink.replay_after(0).unwrap().unwrap();
    assert_eq!(batch.through_seq, 2);
    let Some(Outbound::Encoded(body)) = queue.recv().await else {
        panic!()
    };
    assert!(Arc::ptr_eq(&body, &batch.frames[0]));
    let Some(Content::Notify(notify)) = Frame::decode(body.as_ref()).unwrap().content else {
        panic!()
    };
    assert_eq!(notify.server_seq, 1);
    // writer 尚未接收回应，也允许确认已发布的 2，避免读写完成记录竞争。
    downlink.acknowledge(2).unwrap();
    assert!(downlink.replay_after(2).unwrap().unwrap().frames.is_empty());
    assert!(downlink.replay_after(0).unwrap().is_none());
}

#[tokio::test]
async fn normalized_response_cache_and_actual_wire_body_are_identical() {
    let config = PlayerConfig {
        max_frame_size: 64,
        ..Default::default()
    };
    let (sender, mut queue) = mpsc::channel(8);
    let downlink = Downlink::new(sender, &config);
    start(&downlink);
    queue.recv().await.unwrap();
    assert_eq!(
        downlink
            .publish(message(MessageKind::Notify, 100), 0)
            .unwrap_err()
            .code,
        ErrorCode::FrameTooLarge
    );
    downlink
        .publish(message(MessageKind::Response, 100), 4)
        .unwrap();
    let batch = downlink.replay_after(0).unwrap().unwrap();
    assert_eq!(batch.through_seq, 1);
    let Some(Outbound::Encoded(body)) = queue.recv().await else {
        panic!()
    };
    let Some(Content::Response(response)) = Frame::decode(body.as_ref()).unwrap().content else {
        panic!()
    };
    assert_eq!(response.server_seq, 1);
    assert_eq!(response.request_id, 12);
    assert_eq!(response.message_id, 4);
    assert_eq!(response.error_code, ErrorCode::FrameTooLarge as u32);
    assert!(response.body.is_empty());
    let codec = crate::codec::FrameCodec::new(64, std::time::Duration::from_secs(1)).unwrap();
    let (mut writer, mut reader) = tokio::io::duplex(128);
    codec.write_body(&mut writer, &body).await.unwrap();
    let len = reader.read_u32().await.unwrap() as usize;
    let mut wire = vec![0; len];
    reader.read_exact(&mut wire).await.unwrap();
    assert_eq!(wire.as_slice(), batch.frames[0].as_ref());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_publishers_share_one_sequence_and_fifo() {
    let (sender, mut queue) = mpsc::channel(129);
    let downlink = Arc::new(Downlink::new(sender, &PlayerConfig::default()));
    start(&downlink);
    let mut tasks = Vec::new();
    for n in 0..128 {
        let downlink = downlink.clone();
        tasks.push(tokio::spawn(async move {
            downlink
                .publish(
                    message(
                        if n % 2 == 0 {
                            MessageKind::Notify
                        } else {
                            MessageKind::Response
                        },
                        0,
                    ),
                    4,
                )
                .unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    queue.recv().await.unwrap();
    for seq in 1..=128 {
        let Some(Outbound::Encoded(body)) = queue.recv().await else {
            panic!()
        };
        let seq_actual = match Frame::decode(body.as_ref()).unwrap().content.unwrap() {
            Content::Response(response) => response.server_seq,
            Content::Notify(notify) => notify.server_seq,
            other => panic!("意外帧：{other:?}"),
        };
        assert_eq!(seq_actual, seq);
    }
    assert_eq!(downlink.replay_after(0).unwrap().unwrap().frames.len(), 128);
}

#[tokio::test]
async fn failed_enqueue_never_reuses_sequence_and_close_releases_cache() {
    let (sender, mut queue) = mpsc::channel(1);
    let downlink = Downlink::new(sender, &PlayerConfig::default());
    start(&downlink);
    queue.recv().await.unwrap();
    downlink
        .publish(message(MessageKind::Notify, 0), 0)
        .unwrap();
    assert_eq!(
        downlink
            .publish(message(MessageKind::Notify, 0), 0)
            .unwrap_err()
            .code,
        ErrorCode::SessionClosed
    );
    let batch = downlink.replay_after(0).unwrap().unwrap();
    assert_eq!(batch.through_seq, 2);
    let weak = Arc::downgrade(&batch.frames[1]);
    drop(batch);
    downlink.close();
    assert!(weak.upgrade().is_none());
    assert_eq!(
        downlink.replay_after(0).unwrap_err().code,
        ErrorCode::SessionClosed
    );
    assert_eq!(
        downlink
            .publish(message(MessageKind::Notify, 0), 0)
            .unwrap_err()
            .code,
        ErrorCode::SessionClosed
    );
}
