//! OpenTelemetry configuration for Lambda Workers.

use std::{
    collections::HashMap,
    env,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use opentelemetry::trace::{SpanId, TraceId, TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    trace::{IdGenerator, RandomIdGenerator, SdkTracerProvider},
};
use temporalio_client::Url;
use temporalio_common::telemetry::{
    OtelCollectorOptions, TelemetryOptions, build_otlp_metric_exporter, metrics::CoreMeter,
};
use temporalio_sdk::{Runtime, runtime::RuntimeOptions};
use tracing_subscriber::layer::SubscriberExt as _;

use crate::ShutdownHook;

const DEFAULT_ENDPOINT: &str = "http://localhost:4317";
const DEFAULT_SERVICE_NAME: &str = "temporal-lambda-worker";
const ENV_AWS_LAMBDA_FUNCTION_NAME: &str = "AWS_LAMBDA_FUNCTION_NAME";
const ENV_OTEL_EXPORTER_OTLP_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const ENV_OTEL_SERVICE_NAME: &str = "OTEL_SERVICE_NAME";

/// Options for Lambda-oriented OpenTelemetry metrics and tracing.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct OpenTelemetryOptions {
    /// OTLP gRPC collector endpoint.
    ///
    /// Defaults to `OTEL_EXPORTER_OTLP_ENDPOINT`, then `http://localhost:4317`, which is the
    /// endpoint exposed by the AWS Distro for OpenTelemetry Collector Lambda layer.
    pub endpoint: Option<Url>,
    /// OpenTelemetry `service.name` resource attribute.
    ///
    /// Defaults to `OTEL_SERVICE_NAME`, `AWS_LAMBDA_FUNCTION_NAME`, then
    /// `temporal-lambda-worker`.
    pub service_name: Option<String>,
    /// Interval between periodic metric exports. Defaults to ten seconds.
    pub metric_export_interval: Duration,
}

impl Default for OpenTelemetryOptions {
    fn default() -> Self {
        Self {
            endpoint: None,
            service_name: None,
            metric_export_interval: Duration::from_secs(10),
        }
    }
}

pub(crate) struct OpenTelemetryIntegration {
    runtime: Arc<Runtime>,
    flush_hook: ShutdownHook,
}

impl OpenTelemetryIntegration {
    pub(crate) fn new(options: OpenTelemetryOptions) -> Result<Self, anyhow::Error> {
        if options.metric_export_interval.is_zero() {
            anyhow::bail!("OpenTelemetry metric export interval must be greater than zero");
        }
        let endpoint = match options.endpoint {
            Some(endpoint) => endpoint,
            None => resolve_endpoint(|name| env::var(name).ok())?,
        };
        let service_name = options
            .service_name
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| resolve_service_name(|name| env::var(name).ok()));
        let meter = Arc::new(build_otlp_metric_exporter(
            OtelCollectorOptions::builder()
                .url(endpoint.clone())
                .metric_periodicity(options.metric_export_interval)
                .global_tags(HashMap::from([(
                    "service.name".to_owned(),
                    service_name.clone(),
                )]))
                .build(),
        )?);
        let span_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint.to_string())
            .build()?;
        let tracer_provider = SdkTracerProvider::builder()
            .with_batch_exporter(span_exporter)
            .with_id_generator(AwsXrayIdGenerator)
            .with_resource(Resource::builder().with_service_name(service_name).build())
            .build();
        let tracer = tracer_provider.tracer("temporal-sdk");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let telemetry_options = TelemetryOptions::builder()
            .metrics(meter.clone() as Arc<dyn CoreMeter>)
            .subscriber_override(Arc::new(subscriber))
            .build();
        let runtime_options = RuntimeOptions::builder()
            .telemetry_options(telemetry_options)
            .build()
            .map_err(anyhow::Error::msg)?;
        let runtime = Arc::new(Runtime::new_assume_tokio(runtime_options)?);
        let flush_hook: ShutdownHook = Arc::new(move |_| {
            let meter = meter.clone();
            let tracer_provider = tracer_provider.clone();
            Box::pin(async move {
                tokio::task::spawn_blocking(move || {
                    let metric_result = meter.force_flush();
                    let trace_result = tracer_provider.force_flush().map_err(anyhow::Error::from);
                    match (metric_result, trace_result) {
                        (Ok(()), Ok(())) => Ok(()),
                        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
                        (Err(metric_error), Err(trace_error)) => Err(anyhow::anyhow!(
                            "metric flush failed: {metric_error}; trace flush failed: {trace_error}"
                        )),
                    }
                })
                .await??;
                Ok(())
            })
        });

        Ok(Self {
            runtime,
            flush_hook,
        })
    }

    pub(crate) fn runtime(&self) -> Arc<Runtime> {
        self.runtime.clone()
    }

    pub(crate) fn flush_hook(&self) -> ShutdownHook {
        self.flush_hook.clone()
    }
}

fn resolve_endpoint(getenv: impl Fn(&str) -> Option<String>) -> Result<Url, anyhow::Error> {
    let endpoint = getenv(ENV_OTEL_EXPORTER_OTLP_ENDPOINT)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_owned());
    Ok(Url::parse(&endpoint)?)
}

fn resolve_service_name(getenv: impl Fn(&str) -> Option<String>) -> String {
    getenv(ENV_OTEL_SERVICE_NAME)
        .filter(|value| !value.trim().is_empty())
        .or_else(|| getenv(ENV_AWS_LAMBDA_FUNCTION_NAME).filter(|value| !value.trim().is_empty()))
        .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_owned())
}

#[derive(Debug)]
struct AwsXrayIdGenerator;

impl IdGenerator for AwsXrayIdGenerator {
    fn new_trace_id(&self) -> TraceId {
        xray_trace_id(
            SystemTime::now(),
            RandomIdGenerator::default().new_trace_id(),
        )
    }

    fn new_span_id(&self) -> SpanId {
        RandomIdGenerator::default().new_span_id()
    }
}

fn xray_trace_id(timestamp: SystemTime, random: TraceId) -> TraceId {
    let mut bytes = random.to_bytes();
    let epoch_seconds = timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs() as u32;
    bytes[..4].copy_from_slice(&epoch_seconds.to_be_bytes());
    TraceId::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_lambda_open_telemetry_defaults() {
        assert_eq!(
            resolve_endpoint(|_| None).unwrap().as_str(),
            "http://localhost:4317/"
        );
        assert_eq!(resolve_service_name(|_| None), DEFAULT_SERVICE_NAME);
    }

    #[test]
    fn environment_overrides_open_telemetry_defaults() {
        assert_eq!(
            resolve_endpoint(|name| (name == ENV_OTEL_EXPORTER_OTLP_ENDPOINT)
                .then(|| "http://collector:4317".to_owned()))
            .unwrap()
            .as_str(),
            "http://collector:4317/"
        );
        assert_eq!(
            resolve_service_name(|name| match name {
                ENV_OTEL_SERVICE_NAME => Some("explicit".to_owned()),
                ENV_AWS_LAMBDA_FUNCTION_NAME => Some("function".to_owned()),
                _ => None,
            }),
            "explicit"
        );
        assert_eq!(
            resolve_service_name(
                |name| (name == ENV_AWS_LAMBDA_FUNCTION_NAME).then(|| "function".to_owned())
            ),
            "function"
        );
    }

    #[test]
    fn creates_xray_compatible_trace_id() {
        let random = TraceId::from_bytes([0x55; 16]);
        let timestamp = UNIX_EPOCH + Duration::from_secs(0x1234_5678);
        let trace_id = xray_trace_id(timestamp, random).to_bytes();

        assert_eq!(&trace_id[..4], &[0x12, 0x34, 0x56, 0x78]);
        assert_eq!(&trace_id[4..], &[0x55; 12]);
    }
}
