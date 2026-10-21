use opentelemetry::{
    global,
    propagation::{Extractor, Injector},
};
use tonic::metadata::{KeyAndValueRef, MetadataMap};
use tracing_opentelemetry::OpenTelemetrySpanExt;

struct MetadataExtractor<'a>(&'a MetadataMap);
impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key)?.to_str().ok()
    }
    fn keys(&self) -> Vec<&str> {
        self.0
            .iter()
            .filter_map(|entry| match entry {
                KeyAndValueRef::Ascii(key, _) => Some(key.as_str()),
                _ => None,
            })
            .collect()
    }
}
struct MetadataInjector<'a>(&'a mut MetadataMap);
impl Injector for MetadataInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(key), Ok(value)) = (
            key.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>(),
            value.parse(),
        ) {
            self.0.insert(key, value);
        }
    }
}
pub fn inject(metadata: &mut MetadataMap, span: &tracing::Span) {
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&span.context(), &mut MetadataInjector(metadata))
    });
}
pub fn extract(metadata: &MetadataMap) -> opentelemetry::Context {
    global::get_text_map_propagator(|propagator| propagator.extract(&MetadataExtractor(metadata)))
}
