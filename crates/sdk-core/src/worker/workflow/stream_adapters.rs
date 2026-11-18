//! What ties the Stream calls to Core's own types: its metrics, its run inputs and its errors.

use crate::{
    WorkerConfig, WorkflowErrorType,
    telemetry::metrics::{MetricsContext, stream_store, workflow_type},
    worker::workflow::{
        LocalResolution, LocalResolutionMsg, WFMachinesError,
        streams::{CallError, RunInput, RunSink, StreamMetrics, WorkerStreams},
        workflow_stream::{LocalInput, LocalInputs},
    },
};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::sync::mpsc::UnboundedSender;
use tracing::Span;

impl From<CallError> for WFMachinesError {
    fn from(error: CallError) -> Self {
        match error {
            CallError::Nondeterminism(message) => WFMachinesError::Nondeterminism(message),
            CallError::Fatal(message) => WFMachinesError::Fatal(message),
            CallError::Stream(message) => WFMachinesError::Stream(message),
        }
    }
}

impl StreamMetrics for MetricsContext {
    fn has_meter(&self) -> bool {
        MetricsContext::has_meter(self)
    }

    fn with_store(&self, store: &str) -> Arc<dyn StreamMetrics> {
        Arc::new(self.with_new_attrs([stream_store(store)]))
    }

    fn with_workflow_and_store(&self, ty: &str, store: &str) -> Arc<dyn StreamMetrics> {
        Arc::new(self.with_new_attrs([workflow_type(ty.to_string()), stream_store(store)]))
    }

    fn stream_read_started(&self) {
        MetricsContext::stream_read_started(self)
    }

    fn stream_read_failed(&self) {
        MetricsContext::stream_read_failed(self)
    }

    fn stream_read_records(&self, records: u64) {
        MetricsContext::stream_read_records(self, records)
    }

    fn stream_append_started(&self) {
        MetricsContext::stream_append_started(self)
    }

    fn stream_append_failed(&self) {
        MetricsContext::stream_append_failed(self)
    }

    fn stream_append_latency(&self, dur: Duration) {
        MetricsContext::stream_append_latency(self, dur)
    }

    fn stream_close_started(&self) {
        MetricsContext::stream_close_started(self)
    }

    fn stream_close_failed(&self) {
        MetricsContext::stream_close_failed(self)
    }

    fn stream_close_latency(&self, dur: Duration) {
        MetricsContext::stream_close_latency(self, dur)
    }

    fn stream_close_abandoned(&self) {
        MetricsContext::stream_close_abandoned(self)
    }

    fn stream_replay_read_started(&self) {
        MetricsContext::stream_replay_read_started(self)
    }

    fn stream_replay_read_failed(&self) {
        MetricsContext::stream_replay_read_failed(self)
    }

    fn stream_replay_read_latency(&self, dur: Duration) {
        MetricsContext::stream_replay_read_latency(self, dur)
    }
}

/// Sends what the Stream manager gives to the runs into the Worker's local inputs.
pub(crate) struct LocalSink(pub(crate) UnboundedSender<LocalInput>);

impl RunSink for LocalSink {
    fn send(&self, input: RunInput, span: Span) {
        let input = match input {
            RunInput::HeartbeatTimeout(run_id) => LocalInputs::HeartbeatTimeout(run_id),
            RunInput::Bump => LocalInputs::BumpStream,
            RunInput::Resolution(run_id, resolution) => {
                LocalInputs::LocalResolution(LocalResolutionMsg {
                    run_id,
                    res: LocalResolution::Stream(resolution),
                })
            }
        };
        let _ = self.0.send(LocalInput { input, span });
    }

    fn closed(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(self.0.closed())
    }
}

/// The Stream state of a Worker, from its config and its metrics. A Worker with no meter keeps no
/// metrics here, so that it builds no attributes for a call.
pub(crate) fn worker_streams(
    config: &Arc<WorkerConfig>,
    metrics: &MetricsContext,
) -> WorkerStreams {
    let failure_config = config.clone();
    let streams = WorkerStreams::new(
        config.stream_runtimes.clone(),
        !config.replays_only,
        metrics
            .has_meter()
            .then(|| Arc::new(metrics.clone()) as Arc<dyn StreamMetrics>),
        Box::new(move |workflow_type| {
            failure_config.should_fail_workflow(workflow_type, &WorkflowErrorType::StreamReplay)
        }),
    );
    #[cfg(test)]
    let streams = streams.with_test_hooks(config.stream_test_hooks.clone());
    streams
}
