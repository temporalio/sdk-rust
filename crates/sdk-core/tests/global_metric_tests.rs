// All non-main.rs tests ignore dead common code so that the linter doesn't complain about about it.
#[allow(dead_code)]
mod common;

use crate::common::get_integ_runtime_options;
use common::CoreWfStarter;
use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};
use temporalio_common::telemetry::{
    Logger, OtelCollectorOptions, TelemetryOptions, build_otlp_metric_exporter,
    construct_filter_string, metrics::CoreMeter, telemetry_init_global,
};
use temporalio_sdk_core::CoreRuntime;
use tracing::Level;
use tracing_subscriber::fmt::MakeWriter;

struct CapturingWriter {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl MakeWriter<'_> for CapturingWriter {
    type Writer = CapturingHandle;

    fn make_writer(&self) -> Self::Writer {
        CapturingHandle(self.buf.clone())
    }
}

struct CapturingHandle(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturingHandle {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut b = self.0.lock();
        b.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// TODO: This test is not actually run in CI right now because for it to actually work requires
//   a number of fixes in upstream libraries:
//
// * It regularly panics because of: https://github.com/tokio-rs/tracing/issues/1656
// * Otel doesn't appear to actually be logging any warnings/errors on connection failure
// * This whole thing is supposed to show a workaround for https://github.com/open-telemetry/opentelemetry-rust/issues/2697
#[tokio::test]
async fn otel_errors_logged_as_errors() {
    // Set up tracing subscriber to capture ERROR logs
    let logs = Arc::new(Mutex::new(Vec::new()));
    let writer = CapturingWriter { buf: logs.clone() };
    let subscriber = Arc::new(
        tracing_subscriber::fmt()
            .with_writer(writer)
            .with_env_filter("debug")
            .finish(),
    );
    let opts = OtelCollectorOptions::builder()
        .url("https://localhost:12345/v1/metrics".parse().unwrap()) // Nothing bound on that port
        .build();
    let exporter = build_otlp_metric_exporter(opts).unwrap();

    // Global initialization is needed to capture (some) otel logging.
    telemetry_init_global(
        TelemetryOptions::builder()
            .subscriber_override(subscriber)
            .build(),
    )
    .unwrap();
    let telemopts = TelemetryOptions::builder()
        .metrics(Arc::new(exporter) as Arc<dyn CoreMeter>)
        // Importantly, _not_ using subscriber override, is using console.
        .logging(Logger::Console {
            filter: construct_filter_string(Level::INFO, Level::WARN),
            format: None,
        })
        .build();

    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime("otel_errors_logged_as_errors", rt);
    let _worker = starter.get_core_worker().await;

    tracing::debug!("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ should be in global log");

    // Wait to allow exporter to attempt sending metrics and fail.
    // Windows takes a while to fail the network attempt for some reason so 5s.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let logs = logs.lock();
    let log_str = String::from_utf8_lossy(&logs).into_owned();
    drop(logs);

    // The core worker _isn't_ using the fallback, and shouldn't be captured
    assert!(
        !log_str.contains("Initializing worker"),
        "Core logging shouldn't have been caught by fallback"
    );
    assert!(
        log_str.contains("@@@@@@@@@"),
        "Expected fallback log not found in logs: {log_str}",
    );
    // TODO: OTel just doesn't actually log useful errors right now 🤷, see issues at top of test
    assert!(
        log_str.contains("ERROR"),
        "Expected ERROR log not found in logs: {log_str}",
    );
    assert!(
        log_str.contains("Metrics exporter otlp failed with the grpc server returns error"),
        "Expected an OTel exporter error message in logs: {log_str}",
    );
}
