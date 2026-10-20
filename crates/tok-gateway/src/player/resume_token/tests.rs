use super::*;

fn identity(player: &str, session: &str) -> SessionIdentity {
    SessionIdentity {
        player_id: player.into(),
        session_id: session.into(),
        epoch: 1,
    }
}

#[tokio::test(start_paused = true)]
async fn credentials_expire_independently_and_new_login_revokes_previous_token() {
    let resume_tokens = ResumeTokens::new(&PlayerConfig {
        resume_token_ttl_ms: 100,
        ..Default::default()
    });
    let first = resume_tokens.issue(&identity("1", "old")).unwrap();
    assert_eq!(first.len(), 64);
    assert_eq!(resume_tokens.validate(&first).unwrap().session_id, "old");
    let second = resume_tokens.issue(&identity("1", "new")).unwrap();
    assert_ne!(first, second);
    assert!(resume_tokens.validate(&first).is_err());
    assert!(resume_tokens.validate(&"0".repeat(64)).is_err());
    tokio::time::advance(Duration::from_millis(99)).await;
    assert_eq!(resume_tokens.validate(&second).unwrap().player_id, "1");
    tokio::time::advance(Duration::from_millis(1)).await;
    assert!(resume_tokens.validate(&second).is_err());
}

#[tokio::test(start_paused = true)]
async fn capacity_never_evicts_other_valid_credentials_and_expired_slots_are_reused() {
    let resume_tokens = ResumeTokens::new(&PlayerConfig {
        max_resume_tokens: 1,
        resume_token_ttl_ms: 100,
        ..Default::default()
    });
    let token = resume_tokens.issue(&identity("1", "one")).unwrap();
    assert!(
        matches!(resume_tokens.issue(&identity("2", "two")), Err(error) if error.code == ErrorCode::CapacityExceeded)
    );
    assert!(resume_tokens.validate(&token).is_ok());
    assert!(resume_tokens.issue(&identity("1", "replacement")).is_ok());
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(resume_tokens.issue(&identity("2", "two")).is_ok());
}
