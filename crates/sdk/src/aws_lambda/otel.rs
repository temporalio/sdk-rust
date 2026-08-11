//! OpenTelemetry defaults for Temporal workers running in AWS Lambda.
//!
//! [`OpenTelemetryPlugin`] configures Core metrics and Rust `tracing` spans for export through an
//! OpenTelemetry collector such as the AWS Distro for OpenTelemetry (ADOT) Lambda layer. It also
//! flushes both providers after worker shutdown without shutting them down, so the providers remain
//! usable across warm Lambda invocations.

use crate::{
    WorkerOptions,
    interceptors::WorkerInterceptor,
    plugins::{ClientAndWorkerPlugin, WorkerPlugin},
};
use std::{collections::HashMap, env, sync::Arc, time::Duration};
use temporalio_client::{ClientPlugin, ErasedClientPlugin, PluginError, Url};
use temporalio_common::telemetry::{
    CoreOtelMeter, CoreOtelTracer, OtelCollectorOptions, OtelTraceOptions, OtlpProtocol,
    TelemetryOptions, build_otlp_metric_exporter, build_otlp_trace_exporter, metrics::CoreMeter,
};

const DEFAULT_COLLECTOR_ENDPOINT: &str = "http://localhost:4317";
const DEFAULT_SERVICE_NAME: &str = "temporal-lambda-worker";
const PLUGIN_NAME: &str = "aws-lambda-opentelemetry";

/// Configuration for [`OpenTelemetryPlugin`].
#[derive(Clone, Debug, bon::Builder)]
#[non_exhaustive]
pub struct OpenTelemetryOptions {
    /// How often metrics are exported. Defaults to 10 seconds.
    #[builder(default = Duration::from_secs(10))]
    pub metric_periodicity: Duration,
    /// OTel service name. An empty value falls back to `OTEL_SERVICE_NAME`, then
    /// `AWS_LAMBDA_FUNCTION_NAME`, then `temporal-lambda-worker`.
    #[builder(default, into)]
    pub service_name: String,
    /// OTLP collector URL. An empty value falls back to `OTEL_EXPORTER_OTLP_ENDPOINT`, then the
    /// local ADOT gRPC endpoint at `http://localhost:4317`.
    #[builder(default, into)]
    pub collector_endpoint: String,
    /// Optional headers sent to both metric and trace exporters.
    #[builder(default)]
    pub headers: HashMap<String, String>,
    /// OTLP transport used for metric and trace export. Defaults to gRPC.
    #[builder(default = OtlpProtocol::Grpc)]
    pub protocol: OtlpProtocol,
}

impl Default for OpenTelemetryOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Error creating an AWS Lambda OpenTelemetry plugin.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OpenTelemetryPluginError {
    /// The configured collector endpoint is not a valid URL.
    #[error("invalid OpenTelemetry collector endpoint '{endpoint}': {source}")]
    InvalidCollectorEndpoint {
        /// Invalid endpoint value.
        endpoint: String,
        /// URL parsing failure.
        #[source]
        source: url::ParseError,
    },
    /// The metric exporter could not be created.
    #[error("failed to create OpenTelemetry metric exporter: {0}")]
    MetricExporter(#[source] anyhow::Error),
    /// The trace exporter could not be created.
    #[error("failed to create OpenTelemetry trace exporter: {0}")]
    TraceExporter(#[source] anyhow::Error),
}

#[derive(Debug)]
struct OpenTelemetryProviders {
    meter: Arc<CoreOtelMeter>,
    tracer: Arc<CoreOtelTracer>,
}

impl OpenTelemetryProviders {
    fn force_flush(&self) {
        if let Err(error) = self.meter.force_flush() {
            tracing::warn!(%error, "Failed to flush OpenTelemetry metrics at worker shutdown");
        }
        if let Err(error) = self.tracer.force_flush() {
            tracing::warn!(%error, "Failed to flush OpenTelemetry traces at worker shutdown");
        }
    }
}

/// Reusable metrics and tracing configuration for AWS Lambda workers.
///
/// Construct this while a Tokio runtime is active, apply its telemetry configuration when creating
/// the Temporal [`crate::Runtime`], and register the plugin on [`ClientOptions`]. Registering it on
/// the client automatically propagates its shutdown flush hook to workers.
///
/// ```no_run
/// # use temporalio_client::ClientOptions;
/// # use temporalio_sdk::Runtime;
/// # use temporalio_sdk::aws_lambda::otel::OpenTelemetryPlugin;
/// # use temporalio_sdk::runtime::RuntimeOptions;
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let plugin = OpenTelemetryPlugin::new(Default::default())?;
/// let runtime = Runtime::new_assume_tokio(
///     RuntimeOptions::builder()
///         .telemetry_options(plugin.telemetry_options())
///         .build()
///         .unwrap(),
/// )?;
/// let client_options = ClientOptions::new("default").plugin(plugin).build();
/// # let _ = (runtime, client_options);
/// # Ok(())
/// # }
/// ```
///
/// The tracing exporter covers the Rust SDK's existing `tracing` spans. The Rust SDK does not yet
/// propagate OpenTelemetry trace context through Temporal headers, so workflow and activity spans
/// are not currently joined into a cross-service distributed trace.
#[derive(Clone, Debug)]
pub struct OpenTelemetryPlugin {
    providers: Arc<OpenTelemetryProviders>,
}

impl OpenTelemetryPlugin {
    /// Build OTLP metric and trace exporters with Lambda-oriented defaults.
    pub fn new(options: OpenTelemetryOptions) -> Result<Self, OpenTelemetryPluginError> {
        let service_name = resolve_service_name(&options);
        let collector_endpoint = resolve_collector_endpoint(&options);
        let collector_url = Url::parse(&collector_endpoint).map_err(|source| {
            OpenTelemetryPluginError::InvalidCollectorEndpoint {
                endpoint: collector_endpoint,
                source,
            }
        })?;
        let global_tags = HashMap::from([("service.name".to_owned(), service_name)]);
        let meter = build_otlp_metric_exporter(
            OtelCollectorOptions::builder()
                .url(collector_url.clone())
                .headers(options.headers.clone())
                .metric_periodicity(options.metric_periodicity)
                .global_tags(global_tags.clone())
                .protocol(options.protocol)
                .build(),
        )
        .map_err(OpenTelemetryPluginError::MetricExporter)?;
        let tracer = build_otlp_trace_exporter(
            OtelTraceOptions::builder()
                .url(collector_url)
                .headers(options.headers)
                .global_tags(global_tags)
                .protocol(options.protocol)
                .use_aws_xray_id_generator(true)
                .build(),
        )
        .map_err(OpenTelemetryPluginError::TraceExporter)?;
        Ok(Self {
            providers: Arc::new(OpenTelemetryProviders {
                meter: Arc::new(meter),
                tracer: Arc::new(tracer),
            }),
        })
    }

    /// Create telemetry options containing both the plugin's metrics and tracing configuration.
    pub fn telemetry_options(&self) -> TelemetryOptions {
        let mut options = TelemetryOptions::default();
        self.apply_to_telemetry_options(&mut options);
        options
    }

    /// Apply both metrics and tracing to existing telemetry options.
    ///
    /// Installing a subscriber override causes [`TelemetryOptions::logging`] to be ignored, as
    /// described by [`TelemetryOptions::subscriber_override`].
    pub fn apply_to_telemetry_options(&self, options: &mut TelemetryOptions) {
        self.apply_metrics(options);
        self.apply_tracing(options);
    }

    /// Apply only the plugin's Core metrics exporter to telemetry options.
    pub fn apply_metrics(&self, options: &mut TelemetryOptions) {
        options.metrics = Some(self.providers.meter.clone() as Arc<dyn CoreMeter>);
        options.attach_service_name = false;
    }

    /// Apply only the plugin's Rust `tracing` span exporter to telemetry options.
    pub fn apply_tracing(&self, options: &mut TelemetryOptions) {
        options.subscriber_override = Some(self.providers.tracer.trace_subscriber());
    }
}

impl ClientPlugin for OpenTelemetryPlugin {
    fn name(&self) -> &str {
        PLUGIN_NAME
    }
}

impl WorkerPlugin for OpenTelemetryPlugin {
    fn name(&self) -> &str {
        PLUGIN_NAME
    }

    fn configure_worker_options(&self, options: &mut WorkerOptions) -> Result<(), PluginError> {
        options.worker_interceptor(FlushOnShutdown(self.providers.clone()));
        Ok(())
    }
}

impl From<OpenTelemetryPlugin> for ErasedClientPlugin {
    fn from(plugin: OpenTelemetryPlugin) -> Self {
        ClientAndWorkerPlugin::new(plugin).into()
    }
}

struct FlushOnShutdown(Arc<OpenTelemetryProviders>);

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for FlushOnShutdown {
    fn on_shutdown_complete(&self, _sdk_worker: &crate::Worker) {
        self.0.force_flush();
    }
}

fn resolve_service_name(options: &OpenTelemetryOptions) -> String {
    if !options.service_name.is_empty() {
        return options.service_name.clone();
    }
    env::var("OTEL_SERVICE_NAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            env::var("AWS_LAMBDA_FUNCTION_NAME")
                .ok()
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_owned())
}

fn resolve_collector_endpoint(options: &OpenTelemetryOptions) -> String {
    if !options.collector_endpoint.is_empty() {
        return options.collector_endpoint.clone();
    }
    env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_COLLECTOR_ENDPOINT.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_options_take_precedence() {
        let options = OpenTelemetryOptions::builder()
            .service_name("explicit-service")
            .collector_endpoint("http://collector.example:4317")
            .build();

        assert_eq!(resolve_service_name(&options), "explicit-service");
        assert_eq!(
            resolve_collector_endpoint(&options),
            "http://collector.example:4317"
        );
    }

    #[test]
    fn defaults_match_adot_lambda_layer() {
        let options = OpenTelemetryOptions::default();

        if env::var_os("OTEL_SERVICE_NAME").is_none()
            && env::var_os("AWS_LAMBDA_FUNCTION_NAME").is_none()
        {
            assert_eq!(resolve_service_name(&options), DEFAULT_SERVICE_NAME);
        }
        if env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_none() {
            assert_eq!(
                resolve_collector_endpoint(&options),
                DEFAULT_COLLECTOR_ENDPOINT
            );
        }
        assert_eq!(options.metric_periodicity, Duration::from_secs(10));
    }

    #[test]
    fn invalid_collector_endpoint_is_reported() {
        let error = OpenTelemetryPlugin::new(
            OpenTelemetryOptions::builder()
                .collector_endpoint("not a URL")
                .build(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            OpenTelemetryPluginError::InvalidCollectorEndpoint { .. }
        ));
    }

    #[tokio::test]
    async fn plugin_applies_exporters_and_worker_flush_hook() {
        let plugin = OpenTelemetryPlugin::new(
            OpenTelemetryOptions::builder()
                .service_name("test-service")
                .collector_endpoint("http://localhost:4317")
                .metric_periodicity(Duration::from_secs(60))
                .build(),
        )
        .unwrap();
        let telemetry = plugin.telemetry_options();
        assert!(telemetry.metrics.is_some());
        assert!(telemetry.subscriber_override.is_some());
        assert!(!telemetry.attach_service_name);

        let mut worker_options = WorkerOptions::new("test-task-queue").build();
        WorkerPlugin::configure_worker_options(&plugin, &mut worker_options).unwrap();
        assert_eq!(worker_options.worker_interceptors.len(), 1);
    }
}
