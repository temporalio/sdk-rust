//! The Stream runtime, a plain object that lang's bridge builds per client.

use crate::{
    Reason, StreamError, StreamResult, StreamWriter,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        MemoryStreamConfig, OpenWriterRequest, ReadRecordsRequest, ReadRecordsResponse,
    },
    store::Store,
};
use prost::Message;
use std::sync::Arc;
use temporalio_common::telemetry::metrics::TemporalMeter;

/// The Stream runtime on one Store. It serves the five Stream service operations and the writers.
///
/// A bridge builds one for each client with a Stream backend, and that client's Workers share it.
/// A Store crate, such as `temporalio-streams-redis`, builds it with its own Store.
pub struct StreamRuntime {
    name: &'static str,
    backend: String,
}

impl std::fmt::Debug for StreamRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamRuntime")
            .field("name", &self.name)
            .field("backend", &self.backend)
            .finish_non_exhaustive()
    }
}

pub(crate) fn not_implemented() -> StreamError {
    StreamError::new(Reason::Unsupported, "Not implemented")
}

impl StreamRuntime {
    /// Builds a runtime on `store`. `name` identifies the runtime in logs. `backend` is the name
    /// that lang registered the runtime under, and `meter` is lang's runtime meter, if set. No
    /// call of this runtime uses the Store or records a metric, so the runtime keeps neither.
    pub fn new(
        name: &'static str,
        backend: impl Into<String>,
        store: Arc<dyn Store>,
        meter: Option<TemporalMeter>,
    ) -> Arc<Self> {
        let _ = (store, meter);
        Arc::new(Self {
            name,
            backend: backend.into(),
        })
    }

    /// A name that identifies the runtime in logs.
    pub fn name(&self) -> &str {
        self.name
    }

    /// Creates a Stream, or gets it with USE_EXISTING.
    pub async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        let _ = request;
        Err(not_implemented())
    }

    /// Appends one batch as one Producer.
    pub async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        let _ = request;
        Err(not_implemented())
    }

    /// Closes a Stream.
    pub async fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> StreamResult<CloseStreamResponse> {
        let _ = request;
        Err(not_implemented())
    }

    /// Reads one page of items from an offset.
    pub async fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> StreamResult<ReadRecordsResponse> {
        let _ = request;
        Err(not_implemented())
    }

    /// Gets the status, the newest offset and the Retention of a Stream.
    pub async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        let _ = request;
        Err(not_implemented())
    }

    /// Opens a writer for an Activity, a Local Activity or a client Producer. The bridge holds the
    /// writer, and dropping it releases it.
    pub async fn open_writer(&self, request: OpenWriterRequest) -> StreamResult<StreamWriter> {
        let _ = request;
        Err(not_implemented())
    }
}

/// The Stream runtime for tests.
#[derive(Debug)]
pub struct MemoryStreamRuntime;

impl MemoryStreamRuntime {
    /// Takes an encoded [MemoryStreamConfig]. Its field numbers are those of the same fields in
    /// the config of another Store, such as Redis, so a backend can pass that config too. A bad
    /// config fails with [Reason::Invalid]. The crate has no memory Store, so a good config fails
    /// with [Reason::Unsupported].
    pub fn from_config_bytes(
        config: &[u8],
        meter: Option<TemporalMeter>,
    ) -> StreamResult<Arc<StreamRuntime>> {
        MemoryStreamConfig::decode(config)
            .map_err(|error| StreamError::invalid(format!("The config is bad: {error}")))?;
        let _ = meter;
        Err(not_implemented())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bad_memory_config_is_invalid() {
        let error = MemoryStreamRuntime::from_config_bytes(&[0xff], None).unwrap_err();
        assert_eq!(error.reason, Reason::Invalid);
    }
}
