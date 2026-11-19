//! The Stream calls of Workflow code. Core uses this module, and its API can change in any
//! release. A Store needs nothing from it.

mod calls;
mod manager;

pub use calls::{StreamCalls, StreamJob};
pub use manager::{
    MAX_FAILURE_MESSAGE_BYTES, MAX_MARKER_ENTRY_BYTES, MAX_STORE_NAME_BYTES,
    STREAM_MARKER_DATA_KEY, StreamManager, StreamRequest, StreamResolution, StreamRuntimes,
    StreamTestHooks, WorkerStreams, WriteOutage, input_over_limit, stream_marker_payload_of,
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
}

/// The metrics that the Stream calls count, which Core implements with its own.
pub trait StreamMetrics: Send + Sync + 'static {
    /// False for a meter that counts nothing, so that a call site builds no attributes for it.
    fn has_meter(&self) -> bool;
    /// The metrics with the attribute of the Store `store` added.
    fn with_store(&self, store: &str) -> Arc<dyn StreamMetrics>;
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
