use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
};

use opentelemetry::{
    Context, KeyValue, Value,
    trace::{Link, SpanKind, TraceContextExt, TraceId},
};
use opentelemetry_sdk::trace::{Sampler, SamplingResult, ShouldSample};
use serde::Deserialize;
use tracing::{Metadata, Subscriber};
use tracing_subscriber::{
    filter::Targets,
    layer::{Context as LayerContext, Filter},
};

use crate::Error;

/// 规则之间为 OR；单条规则中的名称、target 和全部字段为 AND，精确匹配。
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ForceTraceRule {
    pub span: Option<String>,
    pub target: Option<String>,
    pub fields: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SamplingConfig {
    pub filter: String,
    pub ratio: f64,
    pub force_rules: Vec<ForceTraceRule>,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            filter: "info".into(),
            ratio: 0.1,
            force_rules: Vec::new(),
        }
    }
}

impl SamplingConfig {
    pub fn validate(&self) -> Result<(), Error> {
        State::new(self.clone()).map(|_| ())
    }
}

#[derive(Debug)]
struct State {
    config: SamplingConfig,
    targets: Targets,
}

impl State {
    fn new(config: SamplingConfig) -> Result<Self, Error> {
        if config.filter.trim().is_empty() {
            return Err(std::io::Error::other("追踪过滤规则不能为空").into());
        }
        let targets = config.filter.parse()?;
        if !config.ratio.is_finite() || !(0.0..=1.0).contains(&config.ratio) {
            return Err(std::io::Error::other("追踪采样率必须在 0 到 1 之间").into());
        }
        for rule in &config.force_rules {
            if rule.span.is_none() && rule.target.is_none() && rule.fields.is_empty() {
                return Err(
                    std::io::Error::other("强追踪规则至少需要名称、target 或字段条件").into(),
                );
            }
            if rule
                .span
                .iter()
                .chain(rule.target.iter())
                .any(|value| value.trim().is_empty())
                || rule.fields.iter().any(|(key, value)| {
                    key.trim().is_empty()
                        || !matches!(
                            value,
                            serde_json::Value::String(_)
                                | serde_json::Value::Number(_)
                                | serde_json::Value::Bool(_)
                        )
                })
            {
                return Err(std::io::Error::other("强追踪规则需要非空名称及标量字段值").into());
            }
        }
        Ok(Self { config, targets })
    }
}

/// 与 SDK sampler 共享快照，更新影响后续采样决定，不修改已开始的 span。
#[derive(Debug, Clone)]
pub struct TraceControl {
    state: Arc<RwLock<Arc<State>>>,
}

impl TraceControl {
    pub fn new(config: SamplingConfig) -> Result<Self, Error> {
        Ok(Self {
            state: Arc::new(RwLock::new(Arc::new(State::new(config)?))),
        })
    }

    pub fn config(&self) -> Result<SamplingConfig, Error> {
        Ok(self
            .state
            .read()
            .map_err(|_| std::io::Error::other("追踪控制锁不可用"))?
            .config
            .clone())
    }

    /// 过滤、ratio 和规则整体替换；校验失败保留旧配置。
    pub fn set_config(&self, config: SamplingConfig) -> Result<(), Error> {
        let state = Arc::new(State::new(config)?);
        *self
            .state
            .write()
            .map_err(|_| std::io::Error::other("追踪控制锁不可用"))? = state;
        Ok(())
    }

    pub fn set_force_rules(&self, rules: Vec<ForceTraceRule>) -> Result<(), Error> {
        let mut current = self
            .state
            .write()
            .map_err(|_| std::io::Error::other("追踪控制锁不可用"))?;
        let mut config = current.config.clone();
        config.force_rules = rules;
        *current = Arc::new(State::new(config)?);
        Ok(())
    }
}

fn field_matches(expected: &serde_json::Value, actual: &Value) -> bool {
    let text = actual.as_str();
    match expected {
        serde_json::Value::String(value) => text == value.as_str(),
        serde_json::Value::Number(value) => text
            .parse::<serde_json::Number>()
            .is_ok_and(|actual| actual == *value),
        serde_json::Value::Bool(value) => text == value.to_string(),
        _ => false,
    }
}

impl ForceTraceRule {
    fn matches(&self, name: &str, attributes: &[KeyValue]) -> bool {
        self.span.as_ref().is_none_or(|span| span == name)
            && self.target.as_ref().is_none_or(|target| {
                attributes.iter().any(|attr| {
                    attr.key.as_str() == "target" && attr.value.as_str() == target.as_str()
                })
            })
            && self.fields.iter().all(|(key, value)| {
                attributes
                    .iter()
                    .any(|attr| attr.key.as_str() == key && field_matches(value, &attr.value))
            })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RuleSampler(pub TraceControl);

impl ShouldSample for RuleSampler {
    fn should_sample(
        &self,
        parent_context: Option<&Context>,
        trace_id: TraceId,
        name: &str,
        span_kind: &SpanKind,
        attributes: &[KeyValue],
        links: &[Link],
    ) -> SamplingResult {
        let state = self.0.state.read().map(|state| state.clone());
        let Ok(state) = state else {
            return Sampler::AlwaysOff.should_sample(
                parent_context,
                trace_id,
                name,
                span_kind,
                attributes,
                links,
            );
        };
        if state
            .config
            .force_rules
            .iter()
            .any(|rule| rule.matches(name, attributes))
        {
            let mut result = Sampler::AlwaysOn.should_sample(
                parent_context,
                trace_id,
                name,
                span_kind,
                attributes,
                links,
            );
            result
                .attributes
                .push(KeyValue::new("tok.force_sampled", true));
            return result;
        }
        if let Some(parent) = parent_context {
            let span = parent.span();
            let context = span.span_context();
            if context.is_valid() {
                let sampler = if context.is_sampled() {
                    Sampler::AlwaysOn
                } else {
                    Sampler::AlwaysOff
                };
                return sampler.should_sample(
                    parent_context,
                    trace_id,
                    name,
                    span_kind,
                    attributes,
                    links,
                );
            }
        }
        let target = attributes
            .iter()
            .find(|attr| attr.key.as_str() == "target")
            .map(|attr| attr.value.as_str());
        let level = attributes
            .iter()
            .find(|attr| attr.key.as_str() == "level")
            .and_then(|attr| attr.value.as_str().parse::<tracing::Level>().ok());
        let eligible = target
            .zip(level)
            .is_some_and(|(target, level)| state.targets.would_enable(&target, &level));
        let sampler = if eligible {
            Sampler::TraceIdRatioBased(state.config.ratio)
        } else {
            Sampler::AlwaysOff
        };
        sampler.should_sample(parent_context, trace_id, name, span_kind, attributes, links)
    }
}

/// 所有 span 进入 SDK 以读取创建字段并沿用父级决定，普通根 span 在 sampler 中过滤。
/// 不以 metadata 预过滤 span，避免字段规则或已采样链路被截断。
pub(crate) struct TraceFilter(pub TraceControl);

impl<S: Subscriber> Filter<S> for TraceFilter {
    fn enabled(&self, metadata: &Metadata<'_>, _: &LayerContext<'_, S>) -> bool {
        if metadata.is_span() {
            return true;
        }
        let context = Context::current();
        if context.span().span_context().is_sampled() {
            return true;
        }
        self.0.state.read().is_ok_and(|state| {
            state
                .targets
                .would_enable(metadata.target(), metadata.level())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{SpanContext, SpanId, TraceFlags, TraceState};
    use opentelemetry_sdk::trace::SamplingDecision;

    fn decision(
        control: &TraceControl,
        parent: Option<&Context>,
        name: &str,
        fields: &[KeyValue],
    ) -> SamplingDecision {
        RuleSampler(control.clone())
            .should_sample(
                parent,
                TraceId::from(1),
                name,
                &SpanKind::Internal,
                fields,
                &[],
            )
            .decision
    }

    fn parent(sampled: bool) -> Context {
        Context::new().with_remote_span_context(SpanContext::new(
            TraceId::from(2),
            SpanId::from(3),
            if sampled {
                TraceFlags::SAMPLED
            } else {
                TraceFlags::NOT_SAMPLED
            },
            true,
            TraceState::default(),
        ))
    }

    #[test]
    fn rules_override_parent_and_ratio_but_unmatched_spans_inherit() {
        let control = TraceControl::new(SamplingConfig {
            filter: "off".into(),
            ratio: 0.0,
            force_rules: vec![ForceTraceRule {
                span: Some("request".into()),
                target: Some("game::player".into()),
                fields: BTreeMap::from([
                    ("player_id".into(), serde_json::json!(123)),
                    ("business".into(), serde_json::json!("login")),
                ]),
            }],
        })
        .unwrap();
        let attributes = [
            KeyValue::new("target", "game::player"),
            KeyValue::new("level", "DEBUG"),
            KeyValue::new("player_id", "123"),
            KeyValue::new("business", "login"),
        ];
        assert_eq!(
            decision(&control, Some(&parent(false)), "request", &attributes),
            SamplingDecision::RecordAndSample
        );
        assert_eq!(
            decision(&control, None, "other", &attributes),
            SamplingDecision::Drop
        );
        assert_eq!(
            decision(&control, None, "request", &attributes[..3]),
            SamplingDecision::Drop
        );
        assert_eq!(
            decision(&control, Some(&parent(true)), "child", &[]),
            SamplingDecision::RecordAndSample
        );
        assert_eq!(
            decision(&control, Some(&parent(false)), "child", &[]),
            SamplingDecision::Drop
        );
    }

    #[test]
    fn live_updates_are_shared_and_invalid_config_is_preserved() {
        let control = TraceControl::new(SamplingConfig {
            ratio: 0.0,
            ..SamplingConfig::default()
        })
        .unwrap();
        let sampler = RuleSampler(control.clone());
        let attrs = [
            KeyValue::new("target", "game"),
            KeyValue::new("level", "INFO"),
        ];
        assert_eq!(
            decision(&control, None, "request", &attrs),
            SamplingDecision::Drop
        );
        control
            .set_config(SamplingConfig {
                ratio: 1.0,
                ..SamplingConfig::default()
            })
            .unwrap();
        assert_eq!(
            sampler
                .should_sample(
                    None,
                    TraceId::from(1),
                    "request",
                    &SpanKind::Internal,
                    &attrs,
                    &[]
                )
                .decision,
            SamplingDecision::RecordAndSample
        );
        control
            .set_force_rules(vec![ForceTraceRule {
                span: Some("special".into()),
                ..ForceTraceRule::default()
            }])
            .unwrap();
        let previous = control.config().unwrap();
        assert!(
            control
                .set_force_rules(vec![ForceTraceRule::default()])
                .is_err()
        );
        assert!(
            control
                .set_config(SamplingConfig {
                    ratio: f64::NAN,
                    ..SamplingConfig::default()
                })
                .is_err()
        );
        assert!(
            control
                .set_force_rules(vec![ForceTraceRule {
                    fields: BTreeMap::from([("player_id".into(), serde_json::Value::Null)]),
                    ..ForceTraceRule::default()
                }])
                .is_err()
        );
        assert_eq!(control.config().unwrap(), previous);
        control
            .set_config(SamplingConfig {
                filter: "warn,game::player=debug".into(),
                ratio: 1.0,
                force_rules: Vec::new(),
            })
            .unwrap();
        assert_eq!(
            decision(&control, None, "request", &attrs),
            SamplingDecision::Drop
        );
        assert_eq!(
            decision(
                &control,
                None,
                "request",
                &[
                    KeyValue::new("target", "game::player::login"),
                    KeyValue::new("level", "DEBUG")
                ]
            ),
            SamplingDecision::RecordAndSample
        );
    }
}
