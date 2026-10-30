//! NATS 即时与 JetStream 持久事件传输适配。

#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

/// JetStream 持久事件消费。
pub mod jetstream;
mod nats;

pub use nats::{Bus, Config, Subscription};
