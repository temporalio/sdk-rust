//! OpenTelemetry configuration for Lambda Workers.

use std::{collections::HashMap, env, sync::Arc, time::Duration};

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{
    Resource,
    trace::{BatchSpanProcessor, SdkTracerProvider},
};
use temporalio_client::{ClientOptions, ClientPlugin, Url};
use temporalio_common::telemetry::{
    OtelCollectorOptions, TelemetryOptions, build_otlp_metric_exporter, metrics::CoreMeter,
};
use temporalio_sdk::{
    Runtime, WorkerOptions,
    opentelemetry::{
        INSTRUMENTATION_SCOPE, OpenTelemetryPlugin, WorkflowIdGenerator, WorkflowSpanProcessor,
    },
    runtime::RuntimeOptions,
};
use tokio::sync::Mutex;
use tonic::transport::ClientTlsConfig;

use crate::ShutdownHook;

const DEFAULT_ENDPOINT: &str = "http://localhost:4317";
const DEFAULT_SERVICE_NAME: &str = "temporal-lambda-worker";

/// Options for OTLP metrics and tracing through a Lambda collector extension.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct OpenTelemetryOptions {
    /// OTLP gRPC collector endpoint.
    ///
    /// Defaults to `OTEL_EXPORTER_OTLP_ENDPOINT`, then `http://localhost:4317`.
    pub endpoint: Option<Url>,
    /// `service.name`, defaulting to `OTEL_SERVICE_NAME`, `AWS_LAMBDA_FUNCTION_NAME`, then
    /// `temporal-lambda-worker`.
    pub service_name: Option<String>,
    /// Interval between metric exports. Defaults to ten seconds.
    pub metric_export_interval: Duration,
    /// Timeout for each collector request. Defaults to one second.
    ///
    /// Keep shorter than the hook budget so an unavailable collector leaves time for cleanup.
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

pub(crate) fn configure(
    options: OpenTelemetryOptions,
    client: &mut ClientOptions,
    worker: &mut WorkerOptions,
    shutdown_hooks: &mut Vec<ShutdownHook>,
    runtime: &mut Option<Arc<Runtime>>,
) -> Result<(), anyhow::Error> {
    if options.metric_export_interval.is_zero() || options.export_timeout.is_zero() {
        anyhow::bail!("OpenTelemetry export interval and timeout must be greater than zero");
    }
    let endpoint = match options.endpoint {
        Some(endpoint) => endpoint,
        None => Url::parse(
            &env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_ENDPOINT.to_owned()),
        )?,
    };
    let service_name = options
        .service_name
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            env::var("OTEL_SERVICE_NAME")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .or_else(|| {
            env::var("AWS_LAMBDA_FUNCTION_NAME")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_owned());
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
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .with_timeout(options.export_timeout);
    let exporter = if matches!(endpoint.scheme(), "https" | "grpcs") {
        exporter.with_tls_config(ClientTlsConfig::new().with_native_roots())
    } else {
        exporter
    }
    .build()?;
    let provider = SdkTracerProvider::builder()
        .with_span_processor(WorkflowSpanProcessor::new(
            BatchSpanProcessor::builder(exporter).build(),
        ))
        .with_id_generator(WorkflowIdGenerator::default())
        .with_resource(Resource::builder().with_service_name(service_name).build())
        .build();
    let plugin = OpenTelemetryPlugin::builder()
        .tracer(provider.tracer(INSTRUMENTATION_SCOPE))
        .build();
    plugin.configure_client_options(client)?;
    worker.worker_plugin(plugin);
    let runtime_options = RuntimeOptions::builder()
        .telemetry_options(
            TelemetryOptions::builder()
                .metrics(meter.clone() as Arc<dyn CoreMeter>)
                .build(),
        )
        .build()
        .map_err(anyhow::Error::msg)?;
    *runtime = Some(Arc::new(Runtime::from_current_tokio(runtime_options)?));
    let flushers = Arc::new(Mutex::new(BlockingFlushers {
        metrics: Box::new(move || meter.force_flush()),
        traces: Box::new(move || provider.force_flush().map_err(anyhow::Error::from)),
    }));
    shutdown_hooks.insert(
        0,
        Arc::new(move |_| Box::pin(BlockingFlushers::flush(flushers.clone()))),
    );
    Ok(())
}

struct BlockingFlushers {
    metrics: Box<dyn Fn() -> anyhow::Result<()> + Send + Sync>,
    traces: Box<dyn Fn() -> anyhow::Result<()> + Send + Sync>,
}

impl BlockingFlushers {
    async fn flush(flushers: Arc<Mutex<Self>>) -> anyhow::Result<()> {
        let guard = Arc::new(flushers.try_lock_owned().map_err(|_| {
            anyhow::anyhow!("a telemetry flush from the previous invocation is still running")
        })?);
        let metric_guard = guard.clone();
        // Blocking provider calls cannot be cancelled. Both tasks retain the guard so subsequent
        // invocations cannot queue more flushes while either previous export is still running.
        let metrics = tokio::task::spawn_blocking(move || (metric_guard.metrics)());
        let traces = tokio::task::spawn_blocking(move || (guard.traces)());
        let (metrics, traces) = tokio::join!(metrics, traces);
        match (metrics?, traces?) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(metrics), Err(traces)) => Err(anyhow::anyhow!(
                "metric flush failed: {metrics}; trace flush failed: {traces}"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex as StdMutex, mpsc};
    use tokio::sync::mpsc::unbounded_channel;

    #[tokio::test]
    async fn cancelled_flush_stays_single_flight_until_both_exporters_finish() {
        let (started_tx, mut started_rx) = unbounded_channel();
        let (metric_tx, metric_rx) = mpsc::channel();
        let (trace_tx, trace_rx) = mpsc::channel();
        let metric_rx = StdMutex::new(Some(metric_rx));
        let trace_rx = StdMutex::new(Some(trace_rx));
        let metric_started = started_tx.clone();
        let flushers = Arc::new(Mutex::new(BlockingFlushers {
            metrics: Box::new(move || {
                metric_started.send(()).unwrap();
                if let Some(rx) = metric_rx.lock().unwrap().take() {
                    rx.recv().unwrap();
                }
                Ok(())
            }),
            traces: Box::new(move || {
                started_tx.send(()).unwrap();
                if let Some(rx) = trace_rx.lock().unwrap().take() {
                    rx.recv().unwrap();
                }
                Ok(())
            }),
        }));
        let running = flushers.clone();
        let invocation = tokio::spawn(BlockingFlushers::flush(running));
        started_rx.recv().await.unwrap();
        started_rx.recv().await.unwrap();
        invocation.abort();
        assert!(invocation.await.unwrap_err().is_cancelled());
        assert!(BlockingFlushers::flush(flushers.clone()).await.is_err());
        metric_tx.send(()).unwrap();
        assert!(BlockingFlushers::flush(flushers.clone()).await.is_err());
        trace_tx.send(()).unwrap();
        drop(flushers.lock().await);
        BlockingFlushers::flush(flushers).await.unwrap();
    }
}
