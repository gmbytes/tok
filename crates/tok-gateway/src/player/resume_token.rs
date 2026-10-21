//! 使用固定共享密钥的 HMAC-SHA256 恢复凭证，验证路径不执行任何 I/O。

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use prost::Message;
use serde::Deserialize;
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tok_player::{Error, Result};
use tok_player::{
    SessionIdentity,
    credentials::{ResumeClaims, ResumeTokenCodec},
};

/// 接收的恢复凭证最大字节数，解码前校验，避免无界解析。
pub const MAX_RESUME_TOKEN_BYTES: usize = 4096;
const PREFIX: &str = "tok-resume-hs256-v1";

/// 启动时加载的固定共享密钥；Debug 隐藏密钥。
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// 32 字节随机共享密钥的十六进制编码；各网关使用相同值，不写日志。
    pub secret_key: String,
    /// 允许签发时刻领先本机的毫秒数；不延长过期时间，默认 5000。
    #[serde(default = "default_clock_skew")]
    pub clock_skew_ms: u64,
}
fn default_clock_skew() -> u64 {
    5000
}
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("secret_key", &"<redacted>")
            .field("clock_skew_ms", &self.clock_skew_ms)
            .finish()
    }
}
impl Config {
    /// 校验固定共享密钥和时钟容差，不访问外部服务。
    pub fn validate(&self) -> Result<()> {
        SignedResumeTokens::new(self.clone()).map(|_| ())
    }
}
fn hex_key(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 || !value.is_ascii() {
        return Err(Error::Core(tok_core::Error::InvalidConfig(
            "凭证密钥必须为 32 字节十六进制编码".into(),
        )));
    }
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| Error::Core(tok_core::Error::InvalidConfig("凭证密钥编码无效".into())))?;
    }
    Ok(bytes)
}
#[derive(Clone, PartialEq, Message)]
struct Payload {
    #[prost(string, tag = "1")]
    player_id: String,
    #[prost(string, tag = "2")]
    session_id: String,
    #[prost(uint64, tag = "3")]
    issued_at_ms: u64,
    #[prost(uint64, tag = "4")]
    expires_at_ms: u64,
}
fn now_ms() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::CredentialUnavailable)?
        .as_millis();
    u64::try_from(millis).map_err(|_| Error::CredentialUnavailable)
}
/// 本地 HMAC 凭证；启动时固定密钥，维护时更换密钥并重启，旧凭证随之失效。
pub struct SignedResumeTokens {
    mac: Hmac<Sha256>,
    clock_skew_ms: u64,
}
impl SignedResumeTokens {
    /// 加载 32 字节共享密钥，后续签发与验证只使用本地状态。
    pub fn new(config: Config) -> Result<Self> {
        if config.clock_skew_ms > 60000 {
            return Err(Error::Core(tok_core::Error::InvalidConfig(
                "凭证时钟容差无效".into(),
            )));
        }
        let mac = Hmac::<Sha256>::new_from_slice(&hex_key(&config.secret_key)?)
            .map_err(|_| Error::Core(tok_core::Error::InvalidConfig("凭证共享密钥无效".into())))?;
        Ok(Self {
            mac,
            clock_skew_ms: config.clock_skew_ms,
        })
    }
    /// 为单进程开发生成临时随机共享密钥；重启后旧凭证失效。
    pub fn ephemeral() -> Result<Self> {
        let mut secret = [0; 32];
        getrandom::fill(&mut secret).map_err(|_| Error::CredentialUnavailable)?;
        Self::new(Config {
            secret_key: secret.iter().map(|byte| format!("{byte:02x}")).collect(),
            clock_skew_ms: default_clock_skew(),
        })
    }
    fn issue_at(&self, identity: &SessionIdentity, ttl: Duration, now: u64) -> Result<String> {
        if identity.player_id.trim().is_empty()
            || identity.player_id.len() > 256
            || identity.session_id.trim().is_empty()
            || identity.session_id.len() > 1024
            || identity.epoch == 0
            || ttl < Duration::from_millis(1)
            || ttl > Duration::from_secs(86400)
        {
            return Err(Error::Core(tok_core::Error::InvalidInput(
                "恢复凭证身份或有效期无效".into(),
            )));
        }
        let expires = now
            .checked_add(ttl.as_millis() as u64)
            .ok_or(Error::CredentialUnavailable)?;
        let payload = Payload {
            player_id: identity.player_id.clone(),
            session_id: identity.session_id.clone(),
            issued_at_ms: now,
            expires_at_ms: expires,
        };
        let signing_input = format!(
            "{PREFIX}.{}",
            URL_SAFE_NO_PAD.encode(payload.encode_to_vec())
        );
        let mut mac = self.mac.clone();
        mac.update(signing_input.as_bytes());
        let token = format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        );
        if token.len() > MAX_RESUME_TOKEN_BYTES {
            return Err(Error::Core(tok_core::Error::InvalidInput(
                "恢复凭证过长".into(),
            )));
        }
        Ok(token)
    }
    fn verify_at(&self, token: &str, now: u64) -> Result<Option<ResumeClaims>> {
        if token.len() > MAX_RESUME_TOKEN_BYTES {
            return Ok(None);
        }
        let parts: Vec<_> = token.split('.').collect();
        if parts.len() != 3 || parts[0] != PREFIX {
            return Ok(None);
        }
        let Some((signing_input, _)) = token.rsplit_once('.') else {
            return Ok(None);
        };
        let Ok(signature) = URL_SAFE_NO_PAD.decode(parts[2]) else {
            return Ok(None);
        };
        let mut mac = self.mac.clone();
        mac.update(signing_input.as_bytes());
        if mac.verify_slice(&signature).is_err() {
            return Ok(None);
        }
        let Ok(payload) = URL_SAFE_NO_PAD.decode(parts[1]) else {
            return Ok(None);
        };
        let Ok(payload) = Payload::decode(payload.as_slice()) else {
            return Ok(None);
        };
        if payload.player_id.trim().is_empty()
            || payload.player_id.len() > 256
            || payload.session_id.trim().is_empty()
            || payload.session_id.len() > 1024
            || payload.issued_at_ms > now.saturating_add(self.clock_skew_ms)
            || payload.expires_at_ms <= now
            || payload.expires_at_ms <= payload.issued_at_ms
            || payload.expires_at_ms - payload.issued_at_ms > 86400000
        {
            return Ok(None);
        }
        Ok(Some(ResumeClaims {
            player_id: payload.player_id,
            session_id: payload.session_id,
        }))
    }
}
impl ResumeTokenCodec for SignedResumeTokens {
    fn issue(&self, identity: &SessionIdentity, ttl: Duration) -> Result<String> {
        self.issue_at(identity, ttl, now_ms()?)
    }
    fn verify(&self, token: &str) -> Result<Option<ResumeClaims>> {
        self.verify_at(token, now_ms()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(seed: u8) -> Config {
        Config {
            secret_key: format!("{seed:02x}").repeat(32),
            clock_skew_ms: 5,
        }
    }
    fn identity() -> SessionIdentity {
        SessionIdentity {
            player_id: "42".into(),
            session_id: "session-a".into(),
            epoch: 1,
        }
    }
    #[test]
    fn independent_gateways_verify_locally_with_the_same_keys() {
        let a = SignedResumeTokens::new(config(1)).unwrap();
        let b = SignedResumeTokens::new(config(1)).unwrap();
        let token = a
            .issue_at(&identity(), Duration::from_millis(100), 100)
            .unwrap();
        assert_eq!(
            b.verify_at(&token, 150).unwrap().unwrap().session_id,
            "session-a"
        );
        let mut newer = identity();
        newer.session_id = "new-login".into();
        a.issue_at(&newer, Duration::from_millis(100), 110).unwrap();
        // 签名仍有效；能否进入会话由最新登录标记控制，不靠本地撤销表。
        assert!(b.verify_at(&token, 150).unwrap().is_some());
        let other = SignedResumeTokens::new(config(2)).unwrap();
        assert!(other.verify_at(&token, 150).unwrap().is_none());
    }
    #[test]
    fn authentication_binds_format_and_payload() {
        let codec = SignedResumeTokens::new(config(1)).unwrap();
        let token = codec
            .issue_at(&identity(), Duration::from_millis(100), 100)
            .unwrap();
        let mut parts: Vec<_> = token.split('.').map(str::to_owned).collect();
        let mut payload =
            Payload::decode(URL_SAFE_NO_PAD.decode(&parts[1]).unwrap().as_slice()).unwrap();
        payload.player_id = "attacker".into();
        parts[1] = URL_SAFE_NO_PAD.encode(payload.encode_to_vec());
        assert!(codec.verify_at(&parts.join("."), 150).unwrap().is_none());
        assert!(
            codec
                .verify_at(&token.replace(PREFIX, "unknown-format"), 150)
                .unwrap()
                .is_none()
        );
        for invalid in [
            "0".repeat(64),
            "x".repeat(MAX_RESUME_TOKEN_BYTES + 1),
            token + ".extra",
        ] {
            assert!(codec.verify_at(&invalid, 150).unwrap().is_none());
        }
    }
    #[test]
    fn expiry_is_not_refreshed_and_future_issuance_is_bounded() {
        let codec = SignedResumeTokens::new(config(1)).unwrap();
        let token = codec
            .issue_at(&identity(), Duration::from_millis(100), 100)
            .unwrap();
        assert!(codec.verify_at(&token, 94).unwrap().is_none());
        assert!(codec.verify_at(&token, 95).unwrap().is_some());
        assert!(codec.verify_at(&token, 199).unwrap().is_some());
        assert!(codec.verify_at(&token, 200).unwrap().is_none());
    }
    #[test]
    fn maintenance_key_change_invalidates_old_credentials() {
        let before = SignedResumeTokens::new(config(1)).unwrap();
        let after = SignedResumeTokens::new(config(2)).unwrap();
        let old = before
            .issue_at(&identity(), Duration::from_millis(100), 100)
            .unwrap();
        let new = after
            .issue_at(&identity(), Duration::from_millis(100), 100)
            .unwrap();
        assert!(after.verify_at(&old, 150).unwrap().is_none());
        assert!(after.verify_at(&new, 150).unwrap().is_some());
        assert!(before.verify_at(&new, 150).unwrap().is_none());
    }
    #[test]
    fn secrets_are_redacted_and_invalid_configuration_is_rejected() {
        let mut settings = config(1);
        assert!(!format!("{settings:?}").contains(&settings.secret_key));
        for invalid in ["".into(), "ab".repeat(31), "ab".repeat(33), "zz".repeat(32)] {
            settings.secret_key = invalid;
            assert!(settings.validate().is_err());
        }
        let mut settings = config(1);
        settings.clock_skew_ms = 60001;
        assert!(settings.validate().is_err());
        assert!(SignedResumeTokens::ephemeral().is_ok());
    }
}
