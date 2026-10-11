use std::fmt;

use opentelemetry::trace::TraceContextExt;
use tracing::{Event, Subscriber};
use tracing_subscriber::{
    fmt::{
        FmtContext,
        format::{FormatEvent, JsonFields, Writer},
    },
    registry::LookupSpan,
};

/// 保留 tracing-subscriber 的 JSON 结构，增加进程标识与 OTel 关联字段。
pub(crate) struct JsonFormatter {
    pub service_name: String,
    pub node_id: String,
}

impl<S> FormatEvent<S, JsonFields> for JsonFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, JsonFields>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let context = opentelemetry::Context::current();
        let span = context.span();
        let span_context = span.span_context();
        let mut output = String::new();
        tracing_subscriber::fmt::format().json().format_event(
            ctx,
            Writer::new(&mut output),
            event,
        )?;
        let mut value: serde_json::Value = serde_json::from_str(&output).map_err(|_| fmt::Error)?;
        value["service"] = self.service_name.clone().into();
        value["node_id"] = self.node_id.clone().into();
        if span_context.is_valid() {
            value["trace_id"] = span_context.trace_id().to_string().into();
            value["span_id"] = span_context.span_id().to_string().into();
        }
        writeln!(writer, "{value}")
    }
}
