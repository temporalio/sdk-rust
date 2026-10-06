//! OpenTelemetry configuration for Lambda Workers.

use std::{collections::HashMap, env, sync::Arc, time::Duration};

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    trace::{BatchSpanProcessor, SdkTracerProvider},
};
use temporalio_client::{ClientOptions, ClientPlugin, Url};
use temporalio_common::telemetry::{
    OtelCollectorOptions, TelemetryOptions, build_otlp_metric_exporter, metrics::CoreMeter,
};
use temporalio_sdk::{
    Runtime, SimplePlugin, WorkerOptions, WorkerPlugin,
    opentelemetry::{
        INSTRUMENTATION_SCOPE, OpenTelemetryPlugin, WorkflowIdGenerator, WorkflowSpanProcessor,
    },
    runtime::RuntimeOptions,
};
use tokio::sync::Semaphore;

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
    /// Timeout for each collector request. Defaults to one second.
    ///
    /// Keep this shorter than the shutdown hook buffer so an unavailable collector does not
    /// consume the invocation's entire cleanup window.
    pub export_timeout: Duration,
}

impl Default for OpenTelemetryOptions {
    fn default() -> Self {
        Self {
            endpoint: None,
            service_name: None,
            metric_export_interval: Duration::from_secs(10),
            export_timeout: Duration::from_secs(1),
        }
    }
}

pub(crate) struct OpenTelemetryIntegration {
    runtime: Arc<Runtime>,
    flush_hook: ShutdownHook,
    plugin: SimplePlugin,
}

impl OpenTelemetryIntegration {
    pub(crate) fn new(options: OpenTelemetryOptions) -> Result<Self, anyhow::Error> {
        if options.metric_export_interval.is_zero() {
            anyhow::bail!("OpenTelemetry metric export interval must be greater than zero");
        }
        if options.export_timeout.is_zero() {
            anyhow::bail!("OpenTelemetry export timeout must be greater than zero");
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
                .export_timeout(options.export_timeout)
                .global_tags(HashMap::from([(
                    "service.name".to_owned(),
                    service_name.clone(),
                )]))
                .build(),
        )?);
        let span_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint.to_string())
            .with_timeout(options.export_timeout)
            .build()?;
        let tracer_provider = SdkTracerProvider::builder()
            .with_span_processor(WorkflowSpanProcessor::new(
                BatchSpanProcessor::builder(span_exporter).build(),
            ))
            .with_id_generator(WorkflowIdGenerator::default())
            .with_resource(Resource::builder().with_service_name(service_name).build())
            .build();
        let plugin = OpenTelemetryPlugin::builder()
            .tracer(tracer_provider.tracer(INSTRUMENTATION_SCOPE))
            .build();
        let telemetry_options = TelemetryOptions::builder()
            .metrics(meter.clone() as Arc<dyn CoreMeter>)
            .build();
        let runtime_options = RuntimeOptions::builder()
            .telemetry_options(telemetry_options)
            .build()
            .map_err(anyhow::Error::msg)?;
        let runtime = Arc::new(Runtime::from_current_tokio(runtime_options)?);
        let flushers = BlockingFlushers {
            metrics: Arc::new(move || meter.force_flush()),
            traces: Arc::new(move || tracer_provider.force_flush().map_err(anyhow::Error::from)),
            gate: Arc::new(Semaphore::new(1)),
        };
        let flush_hook: ShutdownHook = Arc::new(move |_| {
            let flushers = flushers.clone();
            Box::pin(async move { flushers.flush().await })
        });

        Ok(Self {
            runtime,
            flush_hook,
            plugin,
        })
    }

    pub(crate) fn runtime(&self) -> Arc<Runtime> {
        self.runtime.clone()
    }

    pub(crate) fn flush_hook(&self) -> ShutdownHook {
        self.flush_hook.clone()
    }

    pub(crate) fn configure(
        &self,
        client: &mut ClientOptions,
        worker: &mut WorkerOptions,
    ) -> Result<(), anyhow::Error> {
        self.plugin.configure_client_options(client)?;
        self.plugin.configure_worker_options(worker)?;
        Ok(())
    }
}

#[derive(Clone)]
struct BlockingFlushers {
    metrics: Arc<dyn Fn() -> anyhow::Result<()> + Send + Sync>,
    traces: Arc<dyn Fn() -> anyhow::Result<()> + Send + Sync>,
    gate: Arc<Semaphore>,
}

impl BlockingFlushers {
    async fn flush(&self) -> anyhow::Result<()> {
        let permit = Arc::new(self.gate.clone().try_acquire_owned().map_err(|_| {
            anyhow::anyhow!("a telemetry flush from the previous invocation is still running")
        })?);
        let metrics = self.metrics.clone();
        let traces = self.traces.clone();
        let metric_permit = permit.clone();
        // Blocking provider calls cannot be cancelled. Keep the permit inside both tasks so a
        // timed-out invocation cannot queue more flushes when the environment is reused.
        let metrics = tokio::task::spawn_blocking(move || {
            let _permit = metric_permit;
            metrics()
        });
        let traces = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            traces()
        });
        let (metrics, traces) = tokio::join!(metrics, traces);
        match (metrics?, traces?) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(metric_error), Err(trace_error)) => Err(anyhow::anyhow!(
                "metric flush failed: {metric_error}; trace flush failed: {trace_error}"
            )),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    use tokio::sync::mpsc::unbounded_channel;

    #[tokio::test]
    async fn cancelled_flush_stays_single_flight_until_both_exporters_finish() {
        let (started_tx, mut started_rx) = unbounded_channel();
        let (metric_tx, metric_rx) = mpsc::channel();
        let (trace_tx, trace_rx) = mpsc::channel();
        let metric_rx = Mutex::new(Some(metric_rx));
        let trace_rx = Mutex::new(Some(trace_rx));
        let metric_started = started_tx.clone();
        let flushers = BlockingFlushers {
            metrics: Arc::new(move || {
                metric_started.send(()).unwrap();
                if let Some(rx) = metric_rx.lock().unwrap().take() {
                    rx.recv().unwrap();
                }
                Ok(())
            }),
            traces: Arc::new(move || {
                started_tx.send(()).unwrap();
                if let Some(rx) = trace_rx.lock().unwrap().take() {
                    rx.recv().unwrap();
                }
                Ok(())
            }),
            gate: Arc::new(Semaphore::new(1)),
        };
        let running = flushers.clone();
        let invocation = tokio::spawn(async move { running.flush().await });
        started_rx.recv().await.unwrap();
        started_rx.recv().await.unwrap();
        invocation.abort();
        assert!(invocation.await.unwrap_err().is_cancelled());
        assert!(
            flushers
                .flush()
                .await
                .unwrap_err()
                .to_string()
                .contains("previous invocation")
        );
        metric_tx.send(()).unwrap();
        assert!(flushers.flush().await.is_err());
        trace_tx.send(()).unwrap();
        drop(flushers.gate.acquire().await.unwrap());
        flushers.flush().await.unwrap();
    }

    #[tokio::test]
    async fn collector_failure_does_not_skip_traces_or_disable_warm_flushes() {
        let exported = Arc::new(AtomicUsize::new(0));
        let traces = exported.clone();
        let flushers = BlockingFlushers {
            metrics: Arc::new(|| anyhow::bail!("collector unavailable")),
            traces: Arc::new(move || {
                traces.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }),
            gate: Arc::new(Semaphore::new(1)),
        };
        for _ in 0..2 {
            assert!(
                flushers
                    .flush()
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("collector unavailable")
            );
        }
        assert_eq!(exported.load(Ordering::Relaxed), 2);
    }

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
}
