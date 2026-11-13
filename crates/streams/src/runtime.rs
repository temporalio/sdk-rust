//! The Stream runtime, a plain object that lang's bridge builds per client.

use crate::{
    Reason, StreamError, StreamResult,
    digest::batch_digest,
    epoch::ProducerKind,
    ids::{check_producer, check_required, check_stream_id},
    memory::MemoryStore,
    metrics::Metrics,
    offset::offset_to_wire,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        Producer, StreamIdConflictPolicy, StreamRef, StreamStatus,
    },
    record::{check_records, data_records},
    store::{
        Batch, Capabilities, ConflictPolicy, Create, CreateOutcome, DEFAULT_RETENTION,
        MAX_SEQUENCE, Store,
    },
};
use std::{sync::Arc, time::Duration};
use temporalio_common::telemetry::metrics::TemporalMeter;

/// The Stream runtime on one Store. It serves the five Stream service operations. A Store has no
/// namespaces, so the runtime ignores a request's `namespace`. The SDK sets it, and only the
/// server reads it.
///
/// A bridge builds one for each client with a Stream Store, and that client's Workers share it.
/// A Store crate, such as `temporalio-streams-redis`, builds it with its own Store.
pub struct StreamRuntime {
    /// The name of the Store, which the refs that the runtime builds carry.
    store_name: String,
    pub(crate) store: Arc<dyn Store>,
    pub(crate) metrics: Metrics,
}

impl std::fmt::Debug for StreamRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamRuntime")
            .field("store_name", &self.store_name)
            .finish_non_exhaustive()
    }
}

pub(crate) fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

/// The conflict policy of a create. Unspecified is FAIL, as for a Workflow start.
fn conflict_policy(value: i32) -> StreamResult<ConflictPolicy> {
    match StreamIdConflictPolicy::try_from(value) {
        Ok(StreamIdConflictPolicy::Unspecified | StreamIdConflictPolicy::Fail) => {
            Ok(ConflictPolicy::Fail)
        }
        Ok(StreamIdConflictPolicy::UseExisting) => Ok(ConflictPolicy::UseExisting),
        Err(_) => Err(StreamError::invalid(format!(
            "The conflict policy {value} is unknown"
        ))),
    }
}

pub(crate) fn stream_id(stream_ref: Option<&StreamRef>) -> StreamResult<&str> {
    let id = stream_ref
        .map(|stream_ref| stream_ref.id.as_str())
        .unwrap_or_default();
    check_stream_id(id)?;
    Ok(id)
}

/// Checks the Producer of a close. Any kind may close.
fn close_producer(producer: Option<&Producer>) -> StreamResult<&Producer> {
    let producer = producer.ok_or_else(|| StreamError::invalid("The request has no Producer"))?;
    check_producer(producer)?;
    check_required(producer)?;
    Ok(producer)
}

impl StreamRuntime {
    /// Builds a runtime on `store`. `store_name` is the name that lang registered the Store under,
    /// which the refs that the runtime builds carry. The runtime records metrics on `meter`, lang's
    /// runtime meter, if set. A Store that connects later, such as a [crate::store::LazyStore],
    /// lets a bridge build the runtime with no async runtime.
    pub fn new(
        store_name: impl Into<String>,
        store: Arc<dyn Store>,
        meter: Option<TemporalMeter>,
    ) -> Arc<Self> {
        let meter = meter.unwrap_or_else(TemporalMeter::no_op);
        Arc::new(Self {
            store_name: store_name.into(),
            store,
            metrics: Metrics::new(&meter),
        })
    }

    /// Creates a Stream, or gets it with USE_EXISTING.
    pub async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        stream_id(request.stream_ref.as_ref())?;
        let stream = request.stream_ref.clone().unwrap_or_default();
        let conflict = conflict_policy(request.id_conflict_policy)?;
        // The Store config holds the Retention, so a create takes the Store default.
        let create = Create {
            retention: None,
            conflict,
            request_id: &request.request_id,
        };
        let created = self.store.create(&stream, &create).await?;
        Ok(CreateStreamResponse {
            r#ref: Some(StreamRef {
                id: stream.id,
                store: self.store_name.clone(),
            }),
            created: created.outcome == CreateOutcome::Created,
        })
    }

    /// Appends one batch as one Producer. A Workflow appends as the Producer of its Local Activity,
    /// so a Workflow Producer is refused.
    pub async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        stream_id(request.r#ref.as_ref())?;
        let stream = request.r#ref.clone().unwrap_or_default();
        let producer = request
            .producer
            .as_ref()
            .ok_or_else(|| StreamError::invalid("The request has no Producer"))?;
        check_producer(producer)?;
        if ProducerKind::try_from(producer)? == ProducerKind::Workflow {
            return Err(StreamError::invalid(
                "A Workflow writes with the Producer of its Local Activity",
            ));
        }
        check_required(producer)?;
        if !(0..=MAX_SEQUENCE).contains(&request.sequence) {
            return Err(StreamError::invalid(
                "The sequence must be from 1 to 2^53 - 1, or zero for the next one",
            ));
        }
        check_records(&request.records)?;
        // Without the fence, a stale batch would reach Readers.
        let capabilities = self.store.capabilities();
        capabilities.require(Capabilities::PRODUCER_FENCE)?;
        if request.sequence == 0 {
            // Only the Store can number the batch in the same step as its dedupe check.
            capabilities.require(Capabilities::NEXT_SEQUENCE)?;
        }
        let digest = batch_digest(&request.records);
        let records = data_records(producer, request.records)?;
        let batch = Batch {
            producer,
            sequence: request.sequence,
            records: &records,
            digest,
        };
        let appended = self.store.append(&stream, &batch).await;
        if let Err(error) = &appended
            && error.reason == Reason::Fenced
        {
            self.metrics.fenced();
        }
        // An empty batch has no offset, which is the empty wire value.
        let (first_offset, last_offset) = appended?.offsets.unwrap_or_default();
        Ok(AppendRecordsResponse {
            first_offset: offset_to_wire(first_offset),
            last_offset: offset_to_wire(last_offset),
        })
    }

    /// Closes a Stream. Its readers drain what is left, and later appends fail.
    pub async fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> StreamResult<CloseStreamResponse> {
        stream_id(request.r#ref.as_ref())?;
        let stream = request.r#ref.clone().unwrap_or_default();
        let producer = close_producer(request.producer.as_ref())?;
        self.store.capabilities().require(Capabilities::CLOSE)?;
        self.store.close(&stream, producer).await?;
        Ok(CloseStreamResponse {})
    }

    /// Gets the status and the newest offset of a Stream.
    pub async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        stream_id(request.r#ref.as_ref())?;
        let stream = request.r#ref.clone().unwrap_or_default();
        let (state, _) = self.store.read(&stream, 0, 0).await?;
        if !state.exists {
            return Err(StreamError::new(
                Reason::NotFound,
                "Nobody created the Stream",
            ));
        }
        // A closed Stream that expired has lost its log, and every read fails.
        let status = if state.expired() {
            StreamStatus::Expired
        } else if state.closed {
            StreamStatus::Closed
        } else {
            StreamStatus::Open
        };
        Ok(DescribeStreamResponse {
            status: status as i32,
            last_offset: offset_to_wire(state.newest),
        })
    }
}

/// The settings of the memory Store runtime. A field left `None` takes its default.
#[derive(Clone, Debug, Default)]
pub struct MemoryStreamOptions {
    /// The Retention of each Stream. `None` is [DEFAULT_RETENTION].
    pub retention: Option<Duration>,
    /// How long an open Stream stays after its last write. `None` keeps an open Stream until its
    /// close.
    pub last_write_ttl: Option<Duration>,
}

/// The Stream runtime on the memory Store, for tests. It serves the same calls with the same
/// rules as every other Store, and it keeps its Streams in this process only.
#[derive(Debug)]
pub struct MemoryStreamRuntime;

impl MemoryStreamRuntime {
    /// Builds a runtime on a new memory Store. A zero duration in `options` is the same as `None`.
    /// The type only names this constructor, so it returns the runtime and not `Self`.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        store_name: impl Into<String>,
        options: MemoryStreamOptions,
        meter: Option<TemporalMeter>,
    ) -> Arc<StreamRuntime> {
        let positive = |value: Option<Duration>| value.filter(|value| !value.is_zero());
        let store = Arc::new(MemoryStore::new(
            positive(options.retention).unwrap_or(DEFAULT_RETENTION),
            positive(options.last_write_ttl),
        ));
        StreamRuntime::new(store_name, store, meter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_zero_or_negative_duration_is_unset_and_a_huge_one_stays() {
        let proto = |seconds, nanos| Some(prost_types::Duration { seconds, nanos });
        assert_eq!(duration(None), None);
        assert_eq!(duration(proto(0, 0)), None);
        assert_eq!(duration(proto(-5, 0)), None);
        assert_eq!(duration(proto(0, -5)), None);
        assert_eq!(
            duration(proto(i64::MAX, 999_999_999)),
            Some(Duration::new(i64::MAX as u64, 999_999_999))
        );
    }
}
