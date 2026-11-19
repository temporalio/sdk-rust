//! The Stream calls of Workflow code. Core uses this module, and its API can change in any
//! release. A Store needs nothing from it.

mod calls;
mod manager;

pub use calls::{StreamCalls, StreamJob};
pub use manager::{
    MAX_FAILURE_MESSAGE_BYTES, MAX_MARKER_ENTRY_BYTES, MAX_MARKER_RECORD_BYTES, MAX_READ_MIN_GAP,
    MAX_READ_PAGE_RECORDS, MAX_STORE_NAME_BYTES, RunStreams, STORE_RETRY_BOUND,
    STREAM_MARKER_DATA_KEY, STREAM_PAGE_MAX_RECORDS, STREAM_REPLAY_ERROR_TYPE, StreamManager,
    StreamRequest, StreamResolution, StreamRuntimes, StreamTestHooks, WorkerStreams, WriteOutage,
    hydrate_stream_markers, input_over_limit, is_stream_replay_error, is_stream_replay_failure,
    is_stream_replay_unavailable, needs_stream_records, stream_marker_payload_of,
};

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tracing::Span;

/// The errors that the Stream calls of a run give to the machines.
#[derive(Debug)]
pub enum CallError {
    /// Workflow code and History disagree.
    Nondeterminism(String),
    /// A command or a marker entry that no valid run gives.
    Fatal(String),
    /// A call that the Worker can't serve. The text is the whole message.
    Stream(String),
}

/// The metrics that the Stream calls count, which Core implements with its own.
pub trait StreamMetrics: Send + Sync + 'static {
    /// False for a meter that counts nothing, so that a call site builds no attributes for it.
    fn has_meter(&self) -> bool;
    /// The metrics with the attribute of the Store `store` added.
    fn with_store(&self, store: &str) -> Arc<dyn StreamMetrics>;
    /// The metrics with the attributes of the Workflow type and of the Store added.
    fn with_workflow_and_store(&self, workflow_type: &str, store: &str) -> Arc<dyn StreamMetrics>;
    /// A read started.
    fn stream_read_started(&self);
    /// A read ended with a failure.
    fn stream_read_failed(&self);
    /// Workflow code got `records` Records of a read.
    fn stream_read_records(&self, records: u64);
    /// An append started.
    fn stream_append_started(&self);
    /// An append ended with a failure.
    fn stream_append_failed(&self);
    /// The time an append took.
    fn stream_append_latency(&self, dur: Duration);
    /// A close started.
    fn stream_close_started(&self);
    /// A close ended with a failure.
    fn stream_close_failed(&self);
    /// The time a close took.
    fn stream_close_latency(&self, dur: Duration);
    /// A graceful shutdown gave up a close that the Store didn't answer.
    fn stream_close_abandoned(&self);
    /// A replay started to read the Records of a marker entry.
    fn stream_replay_read_started(&self);
    /// A replay could not read or check the Records of a marker entry.
    fn stream_replay_read_failed(&self);
    /// The time a replay's read of a marker entry took.
    fn stream_replay_read_latency(&self, dur: Duration);
}

/// What the Stream manager gives to the Worker's runs.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // A value lives only until the sink takes it.
pub enum RunInput {
    /// The Workflow Task of the run with this id reached its heartbeat time.
    HeartbeatTimeout(String),
    /// Wakes the pending polls for activations, with no new result.
    Bump,
    /// A result of a call of the run with this id.
    Resolution(String, StreamResolution),
}

/// Where the Stream manager sends what it gives to the Worker's runs.
pub trait RunSink: Send + Sync + 'static {
    /// Sends `input`, which the receiver handles under `span`.
    fn send(&self, input: RunInput, span: Span);
    /// Resolves once nothing receives the inputs, which is when the Worker is gone.
    fn closed(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}
