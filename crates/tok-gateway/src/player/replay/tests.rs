use super::*;

fn push(cache: &mut ReplayCache, bytes: usize) {
    let seq = cache.next().unwrap();
    cache.push(seq, vec![seq as u8; bytes].into());
}

#[tokio::test(start_paused = true)]
async fn count_eviction_ack_progress_and_missing_ranges() {
    let mut cache = ReplayCache::new(&PlayerConfig {
        replay_capacity: 2,
        ..Default::default()
    });
    for _ in 0..3 {
        push(&mut cache, 4);
    }
    assert!(cache.after(0).is_none());
    let batch = cache.after(1).unwrap();
    assert_eq!(batch.through_seq, 3);
    assert_eq!(
        batch.frames.iter().map(|body| body[0]).collect::<Vec<_>>(),
        [2, 3]
    );
    assert!(cache.after(4).is_none());
    cache.acknowledge(0).unwrap();
    cache.acknowledge(2).unwrap();
    cache.acknowledge(1).unwrap();
    cache.acknowledge(2).unwrap();
    assert!(cache.after(1).is_none());
    assert_eq!(cache.after(2).unwrap().frames.len(), 1);
    assert_eq!(cache.bytes, 4);
    assert_eq!(
        cache.acknowledge(4).unwrap_err().code,
        ErrorCode::InvalidRequest
    );
    assert_eq!(cache.ack, 2);
    cache.acknowledge(3).unwrap();
    assert!(cache.after(3).unwrap().frames.is_empty());
    assert_eq!(cache.bytes, 0);
    assert_eq!(cache.next().unwrap(), 4);
}

#[tokio::test(start_paused = true)]
async fn byte_limit_counts_actual_body_and_keeps_exact_boundary() {
    let mut cache = ReplayCache::new(&PlayerConfig {
        replay_max_bytes: 8,
        ..Default::default()
    });
    push(&mut cache, 4);
    push(&mut cache, 4);
    assert_eq!(cache.after(0).unwrap().frames.len(), 2);
    push(&mut cache, 5);
    assert!(cache.after(1).is_none());
    assert_eq!(cache.after(2).unwrap().frames[0].len(), 5);
    assert_eq!(cache.bytes, 5);
}

#[tokio::test(start_paused = true)]
async fn ttl_uses_original_publication_and_queries_do_not_extend_it() {
    let mut cache = ReplayCache::new(&PlayerConfig {
        replay_ttl_ms: 100,
        ..Default::default()
    });
    push(&mut cache, 3);
    tokio::time::advance(Duration::from_millis(50)).await;
    push(&mut cache, 3);
    let snapshot = cache.after(0).unwrap();
    assert_eq!(snapshot.frames.len(), 2);
    tokio::time::advance(Duration::from_millis(49)).await;
    assert_eq!(cache.after(0).unwrap().frames.len(), 2);
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(cache.after(0).is_none());
    assert_eq!(cache.after(1).unwrap().frames.len(), 1);
    // 原快照可读取，但不会延长缓存保留时间。
    assert_eq!(snapshot.frames.len(), 2);
    tokio::time::advance(Duration::from_millis(50)).await;
    cache.acknowledge(0).unwrap();
    assert!(cache.after(1).is_none());
    assert!(cache.after(2).unwrap().frames.is_empty());
    assert_eq!(cache.bytes, 0);
}

#[test]
fn sequence_exhaustion_does_not_wrap_and_clear_preserves_high_watermark() {
    let mut cache = ReplayCache::new(&PlayerConfig::default());
    cache.high = u64::MAX;
    assert_eq!(cache.next().unwrap_err().code, ErrorCode::SessionClosed);
    cache.clear();
    assert_eq!(cache.high, u64::MAX);
    assert!(cache.after(u64::MAX).unwrap().frames.is_empty());
}

#[test]
fn replay_configuration_requires_bounded_values_and_room_for_one_frame() {
    for config in [
        PlayerConfig {
            replay_capacity: 0,
            ..Default::default()
        },
        PlayerConfig {
            replay_capacity: 65_537,
            ..Default::default()
        },
        PlayerConfig {
            replay_ttl_ms: 0,
            ..Default::default()
        },
        PlayerConfig {
            replay_ttl_ms: 86_400_001,
            ..Default::default()
        },
        PlayerConfig {
            replay_max_bytes: 64 * 1024 * 1024 + 1,
            ..Default::default()
        },
        PlayerConfig {
            replay_max_bytes: 1_048_575,
            ..Default::default()
        },
    ] {
        assert!(config.validate().is_err());
    }
    PlayerConfig::default().validate().unwrap();
}
