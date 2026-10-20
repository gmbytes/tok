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

fn complete_login(channel: &SessionChannel) -> ConnectionBinding {
    channel
        .complete_login(Frame {
            version: VERSION,
            content: Some(Content::LoggedIn(protocol::LoginResponse::default())),
        })
        .unwrap()
}

#[tokio::test]
async fn login_precedes_publication_and_ack_does_not_wait_for_socket_completion() {
    let (sender, mut queue) = mpsc::channel(8);
    let channel = SessionChannel::new(sender, &PlayerConfig::default(), CancellationToken::new());
    assert_eq!(
        channel
            .publish(message(MessageKind::Notify, 0), 0)
            .unwrap_err()
            .code,
        ErrorCode::InvalidState
    );
    let id = complete_login(&channel);
    channel.publish(message(MessageKind::Notify, 0), 0).unwrap();
    channel
        .publish(message(MessageKind::Response, 0), 4)
        .unwrap();
    assert!(matches!(queue.recv().await, Some(Outbound::Frame(_, _))));
    let batch = channel.replay_after(0).unwrap().unwrap();
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
    channel.acknowledge(&id, 2).unwrap();
    assert!(channel.replay_after(2).unwrap().unwrap().frames.is_empty());
    assert!(channel.replay_after(0).unwrap().is_none());
}

#[tokio::test]
async fn normalized_response_cache_and_actual_wire_body_are_identical() {
    let config = PlayerConfig {
        max_frame_size: 64,
        ..Default::default()
    };
    let (sender, mut queue) = mpsc::channel(8);
    let channel = SessionChannel::new(sender, &config, CancellationToken::new());
    complete_login(&channel);
    queue.recv().await.unwrap();
    assert_eq!(
        channel
            .publish(message(MessageKind::Notify, 100), 0)
            .unwrap_err()
            .code,
        ErrorCode::FrameTooLarge
    );
    channel
        .publish(message(MessageKind::Response, 100), 4)
        .unwrap();
    let batch = channel.replay_after(0).unwrap().unwrap();
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
    let channel = Arc::new(SessionChannel::new(
        sender,
        &PlayerConfig::default(),
        CancellationToken::new(),
    ));
    complete_login(&channel);
    let mut tasks = Vec::new();
    for n in 0..128 {
        let channel = channel.clone();
        tasks.push(tokio::spawn(async move {
            channel
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
    assert_eq!(channel.replay_after(0).unwrap().unwrap().frames.len(), 128);
}

#[tokio::test]
async fn failed_enqueue_detaches_and_preserves_cache_until_session_close() {
    let (sender, mut queue) = mpsc::channel(1);
    let channel = SessionChannel::new(sender, &PlayerConfig::default(), CancellationToken::new());
    complete_login(&channel);
    queue.recv().await.unwrap();
    channel.publish(message(MessageKind::Notify, 0), 0).unwrap();
    channel.publish(message(MessageKind::Notify, 0), 0).unwrap();
    let batch = channel.replay_after(0).unwrap().unwrap();
    assert_eq!(batch.through_seq, 2);
    let weak = Arc::downgrade(&batch.frames[1]);
    drop(batch);
    channel.close();
    assert!(weak.upgrade().is_none());
    assert_eq!(
        channel.replay_after(0).unwrap_err().code,
        ErrorCode::SessionClosed
    );
    assert_eq!(
        channel
            .publish(message(MessageKind::Notify, 0), 0)
            .unwrap_err()
            .code,
        ErrorCode::SessionClosed
    );
}

#[tokio::test(start_paused = true)]
async fn grace_deadline_expires_even_without_new_business_or_renewal() {
    let (sender, mut queue) = mpsc::channel(8);
    let cancel = CancellationToken::new();
    let channel = Arc::new(SessionChannel::new(
        sender,
        &PlayerConfig {
            disconnect_grace_ms: 100,
            ..Default::default()
        },
        cancel.clone(),
    ));
    let id = complete_login(&channel);
    queue.recv().await.unwrap();
    channel.detach(&id);
    let expiry = {
        let channel = channel.clone();
        tokio::spawn(async move { channel.run_grace_timer().await })
    };
    tokio::time::advance(std::time::Duration::from_millis(99)).await;
    assert!(!cancel.is_cancelled());
    tokio::time::advance(std::time::Duration::from_millis(1)).await;
    expiry.await.unwrap();
    assert!(cancel.is_cancelled());
    assert!(channel.replay_after(0).is_err());
}

#[tokio::test]
async fn recovery_orders_concurrent_downlink_and_rejects_every_stale_binding_operation() {
    let (sender, mut old_queue) = mpsc::channel(8);
    let channel = Arc::new(SessionChannel::new(
        sender,
        &PlayerConfig::default(),
        CancellationToken::new(),
    ));
    let old = complete_login(&channel);
    old_queue.recv().await.unwrap();
    channel.admit_request(&old, 12, || Ok(())).unwrap();
    channel.publish(message(MessageKind::Notify, 0), 0).unwrap();
    let (sender, mut queue) = mpsc::channel(1);
    let current_binding = channel
        .rebind_connection(sender, 0, protocol::ResumeResponse::default())
        .unwrap();
    assert!(old.cancellation.is_cancelled());
    assert_eq!(
        channel.acknowledge(&old, 1).unwrap_err().code,
        ErrorCode::SessionReplaced
    );
    assert_eq!(
        channel
            .admit_request::<()>(&old, 13, || panic!("旧连接不能提交"))
            .unwrap_err()
            .code,
        ErrorCode::SessionReplaced
    );
    assert_eq!(
        channel
            .publish_from(Some(&old), message(MessageKind::Response, 0), 4)
            .unwrap_err()
            .code,
        ErrorCode::SessionReplaced
    );
    channel.detach(&old);
    channel.close_bound_session(&old);
    assert!(!current_binding.cancellation.is_cancelled());
    channel.publish(message(MessageKind::Notify, 0), 0).unwrap();
    let Some(Outbound::Replay(recovery)) = queue.recv().await else {
        panic!()
    };
    let codec = crate::codec::FrameCodec::new(1024, std::time::Duration::from_secs(1)).unwrap();
    let (mut writer, mut reader) = tokio::io::duplex(1024);
    recovery.write_to(&codec, &mut writer).await.unwrap();
    let Some(Content::Resumed(resumed)) = codec.read(&mut reader).await.unwrap().unwrap().content
    else {
        panic!()
    };
    assert_eq!(resumed.last_request_id, 12);
    for seq in [1, 2] {
        let Some(Content::Notify(notify)) = codec.read(&mut reader).await.unwrap().unwrap().content
        else {
            panic!()
        };
        assert_eq!(notify.server_seq, seq);
    }
    let Some(Content::Recovered(done)) = codec.read(&mut reader).await.unwrap().unwrap().content
    else {
        panic!()
    };
    assert_eq!(done.through_seq, 2);
    channel.acknowledge(&current_binding, 2).unwrap();
    assert_eq!(
        channel
            .admit_request(&current_binding, 12, || Ok(()))
            .unwrap_err()
            .code,
        ErrorCode::InvalidRequest
    );
    channel
        .admit_request(&current_binding, 13, || Ok(()))
        .unwrap();
    channel.publish(message(MessageKind::Notify, 0), 0).unwrap();
    let Some(Outbound::Encoded(body)) = queue.recv().await else {
        panic!()
    };
    let Some(Content::Notify(notify)) = Frame::decode(body.as_ref()).unwrap().content else {
        panic!()
    };
    assert_eq!(notify.server_seq, 3);
}

#[tokio::test(start_paused = true)]
async fn successful_rebind_cancels_old_grace_deadline_and_late_cleanup_cannot_restart_it() {
    let (sender, mut queue) = mpsc::channel(8);
    let cancel = CancellationToken::new();
    let channel = Arc::new(SessionChannel::new(
        sender,
        &PlayerConfig {
            disconnect_grace_ms: 100,
            ..Default::default()
        },
        cancel.clone(),
    ));
    let old = complete_login(&channel);
    queue.recv().await.unwrap();
    channel.detach(&old);
    let expiry = {
        let channel = channel.clone();
        tokio::spawn(async move { channel.run_grace_timer().await })
    };
    tokio::time::advance(std::time::Duration::from_millis(99)).await;
    let (sender, _queue) = mpsc::channel(8);
    let current_binding = channel
        .rebind_connection(sender, 0, protocol::ResumeResponse::default())
        .unwrap();
    channel.detach(&old);
    tokio::time::advance(std::time::Duration::from_millis(100)).await;
    assert!(!cancel.is_cancelled());
    channel.detach(&current_binding);
    tokio::time::advance(std::time::Duration::from_millis(100)).await;
    expiry.await.unwrap();
    assert!(cancel.is_cancelled());
}

#[tokio::test]
async fn gap_created_during_replay_never_produces_a_success_marker() {
    let (sender, mut queue) = mpsc::channel(8);
    let channel = Arc::new(SessionChannel::new(
        sender,
        &PlayerConfig {
            replay_capacity: 1,
            ..Default::default()
        },
        CancellationToken::new(),
    ));
    let old = complete_login(&channel);
    queue.recv().await.unwrap();
    channel.publish(message(MessageKind::Notify, 0), 0).unwrap();
    channel.detach(&old);
    let (sender, mut queue) = mpsc::channel(8);
    channel
        .rebind_connection(sender, 0, protocol::ResumeResponse::default())
        .unwrap();
    channel.publish(message(MessageKind::Notify, 0), 0).unwrap();
    channel.publish(message(MessageKind::Notify, 0), 0).unwrap();
    let Some(Outbound::Replay(recovery)) = queue.recv().await else {
        panic!()
    };
    let codec = FrameCodec::new(1024, std::time::Duration::from_secs(1)).unwrap();
    let (mut writer, mut reader) = tokio::io::duplex(1024);
    assert_eq!(
        recovery
            .write_to(&codec, &mut writer)
            .await
            .unwrap_err()
            .code,
        ErrorCode::ResumeUnavailable
    );
    drop(writer);
    assert!(matches!(
        codec.read(&mut reader).await.unwrap().unwrap().content,
        Some(Content::Resumed(_))
    ));
    assert!(matches!(
        codec.read(&mut reader).await.unwrap().unwrap().content,
        Some(Content::Notify(_))
    ));
    assert!(codec.read(&mut reader).await.unwrap().is_none());
}
