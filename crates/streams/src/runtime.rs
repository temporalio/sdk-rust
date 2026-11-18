//! The Stream runtime, a plain object that lang's bridge builds per client.

use crate::{
    Reason, StreamError, StreamResult, StreamStore,
    ids::{check_producer, check_required, check_stream_id},
    metrics::Metrics,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        Producer, ReadRecordsRequest, ReadRecordsResponse, StreamIdConflictPolicy, StreamRef,
        StreamStatus, producer::Kind,
    },
    record::{check_bounds, check_records},
    store,
};
use std::sync::Arc;
use temporalio_common::telemetry::metrics::TemporalMeter;

/// The highest batch `sequence`. A Store that keeps numbers as 64-bit floats holds an integer
/// exactly up to this value.
pub const MAX_SEQUENCE: i64 = (1 << 53) - 1;

/// The Stream runtime on one Store. It has one typed method for each operation, and
/// [StreamRuntime::call] is the byte form of the same methods. A bridge builds one for each client
/// with a Stream Store, and that client's Workers share it. A Store has no namespaces, so the
/// runtime ignores a request's `namespace`. The SDK sets it, and only the server reads it.
///
/// The runtime checks each request before the Store sees it, and each answer after it. The checks
/// are the same for every Store.
///
/// - A Stream id and a Producer id are opaque text of 1 to 4096 bytes. The caller builds both.
/// - A Workflow Producer appends and closes with its Workflow Task as the epoch, so that a Store
///   can refuse a write of an older task.
/// - A batch has at most 1000 Records and 4 MiB, and a Record at most 1536 KiB.
/// - A read gives at most 1000 Records and waits at most 60 s. A page over the caps of its request
///   is cut.
/// - An answer with a bad offset fails with [Reason::StorageCorrupt].
pub struct StreamRuntime {
    store: Arc<dyn StreamStore>,
    metrics: Metrics,
}

impl std::fmt::Debug for StreamRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamRuntime").finish_non_exhaustive()
    }
}

/// Refuses a conflict policy that the Stream service doesn't know. The Store applies the policy,
/// and takes an unspecified one as FAIL, as for a Workflow start.
fn check_conflict_policy(value: i32) -> StreamResult<()> {
    StreamIdConflictPolicy::try_from(value)
        .map(drop)
        .map_err(|_| StreamError::invalid(format!("The conflict policy {value} is unknown")))
}

fn stream_id(stream_ref: Option<&StreamRef>) -> StreamResult<&str> {
    let id = stream_ref
        .map(|stream_ref| stream_ref.id.as_str())
        .unwrap_or_default();
    check_stream_id(id)?;
    Ok(id)
}

/// Refuses a create answer with no ref, or with the ref of another Stream.
fn check_created(response: &CreateStreamResponse, id: &str) -> StreamResult<()> {
    match response.r#ref.as_ref() {
        None => Err(StreamError::storage_corrupt(
            "The Store answered a create with no ref",
        )),
        Some(stream_ref) if stream_ref.id != id => Err(StreamError::storage_corrupt(
            "The Store answered a create with the ref of another Stream",
        )),
        Some(_) => Ok(()),
    }
}

/// Refuses a describe answer with a long `last_offset`, no status, or a status that the Stream
/// service doesn't know. An empty `last_offset` is fine, since a Stream may have no Record.
fn check_described(response: &DescribeStreamResponse) -> StreamResult<()> {
    store::offset_bound(&response.last_offset, "a last offset")?;
    match StreamStatus::try_from(response.status) {
        Ok(StreamStatus::Unspecified) => Err(StreamError::storage_corrupt(
            "The Store answered a describe with no status",
        )),
        Ok(_) => Ok(()),
        Err(_) => Err(StreamError::storage_corrupt(format!(
            "The Store answered a describe with the unknown status {}",
            response.status
        ))),
    }
}

/// Checks the Producer of a close. Any kind may close, and a Workflow closes with its Workflow
/// Task as the epoch, as it appends.
fn check_close_producer(producer: Option<&Producer>) -> StreamResult<()> {
    let producer = producer.ok_or_else(|| StreamError::invalid("The request has no Producer"))?;
    check_producer(producer)?;
    // With no epoch, a close from a failed Workflow Task would pass the fence.
    if let Some(Kind::Workflow(workflow)) = producer.kind.as_ref()
        && workflow.epoch.is_none()
    {
        return Err(StreamError::invalid(
            "A close from a Workflow needs the Workflow Task as its epoch",
        ));
    }
    check_required(producer)
}

/// Checks an append before the Store sees it, and gives its Producer.
fn append_producer(request: &AppendRecordsRequest) -> StreamResult<&Producer> {
    stream_id(request.r#ref.as_ref())?;
    let producer = request
        .producer
        .as_ref()
        .ok_or_else(|| StreamError::invalid("The request has no Producer"))?;
    check_producer(producer)?;
    // With no epoch, a write from an older Workflow Task would pass the fence.
    if let Some(Kind::Workflow(workflow)) = producer.kind.as_ref()
        && workflow.epoch.is_none()
    {
        return Err(StreamError::invalid(
            "An append from a Workflow needs the Workflow Task as its epoch",
        ));
    }
    check_required(producer)?;
    if !(0..=MAX_SEQUENCE).contains(&request.sequence) {
        return Err(StreamError::invalid(
            "The sequence must be from 1 to 2^53 - 1, or zero for the next one",
        ));
    }
    check_records(&request.records)?;
    Ok(producer)
}

impl StreamRuntime {
    /// Builds the runtime on `store`. It records metrics on `meter`, lang's runtime meter, if set.
    pub fn new(store: Arc<dyn StreamStore>, meter: Option<TemporalMeter>) -> Arc<Self> {
        let meter = meter.unwrap_or_else(TemporalMeter::no_op);
        Arc::new(Self {
            store,
            metrics: Metrics::new(&meter),
        })
    }

    /// Creates a Stream, or gets it with USE_EXISTING.
    pub async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        let id = stream_id(request.stream_ref.as_ref())?.to_string();
        check_conflict_policy(request.id_conflict_policy)?;
        let created = self.store.create_stream(request).await?;
        check_created(&created, &id)?;
        Ok(created)
    }

    /// Appends one batch as one Producer. A Workflow Producer appends with its Workflow Task as the
    /// epoch, so that the fence refuses a write of an older task.
    pub async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        let producer = append_producer(&request)?;
        // The same bounds hold for every Store, so a batch over them fails here too.
        check_bounds(producer, &request.records)?;
        let appended = store::append(self.store.as_ref(), request).await;
        if let Err(error) = &appended
            && error.reason == Reason::Fenced
        {
            self.metrics.fenced();
        }
        appended
    }

    /// Reads one page after `from_offset`. The Store waits up to the request's `wait` for a Record
    /// when it has none to give, and the metric `stream_blocking_reads_waiting` counts the read for
    /// that time.
    pub async fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> StreamResult<ReadRecordsResponse> {
        stream_id(request.r#ref.as_ref())?;
        store::read(self.store.as_ref(), &self.metrics, request).await
    }

    /// Closes a Stream. Its readers drain what is left, and later appends fail.
    pub async fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> StreamResult<CloseStreamResponse> {
        stream_id(request.r#ref.as_ref())?;
        check_close_producer(request.producer.as_ref())?;
        self.store.close_stream(request).await
    }

    /// Gets the status and the newest offset of a Stream.
    pub async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        stream_id(request.r#ref.as_ref())?;
        let described = self.store.describe_stream(request).await?;
        check_described(&described)?;
        Ok(described)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ids::MAX_OFFSET_BYTES,
        proto::{AppendRecord, ClientProducer, WorkflowProducer},
        record::MAX_BATCH_RECORDS,
        test_store::{Scripted, runtime},
    };
    use std::{
        error::Error,
        sync::atomic::{AtomicI64, Ordering},
    };
    use temporalio_common::{
        protos::temporal::api::common::v1::Payload,
        telemetry::{
            TaskQueueLabelStrategy,
            metrics::{
                CoreMeter, Counter, CounterBase, Gauge, GaugeF64, Histogram, HistogramDuration,
                HistogramF64, MetricAttributable, MetricAttributes, MetricParameters,
                NewAttributes, NoOpCoreMeter, UpDownCounter, UpDownCounterBase,
            },
        },
    };

    /// A value that a counter and an up-down counter add into.
    struct Cell(Arc<AtomicI64>);

    impl CounterBase for Cell {
        fn adds(&self, value: u64) {
            self.0.fetch_add(value as i64, Ordering::SeqCst);
        }
    }

    impl UpDownCounterBase for Cell {
        fn adds(&self, value: i64) {
            self.0.fetch_add(value, Ordering::SeqCst);
        }
    }

    impl MetricAttributable<Box<dyn CounterBase>> for Cell {
        fn with_attributes(
            &self,
            _: &MetricAttributes,
        ) -> Result<Box<dyn CounterBase>, Box<dyn Error>> {
            Ok(Box::new(Cell(self.0.clone())))
        }
    }

    impl MetricAttributable<Box<dyn UpDownCounterBase>> for Cell {
        fn with_attributes(
            &self,
            _: &MetricAttributes,
        ) -> Result<Box<dyn UpDownCounterBase>, Box<dyn Error>> {
            Ok(Box::new(Cell(self.0.clone())))
        }
    }

    /// A meter for the runtime's two instruments, the counter `stream_fenced` and the up-down
    /// counter `stream_blocking_reads_waiting`. It drops the values of any other instrument.
    #[derive(Debug, Default)]
    struct Meter {
        fenced: Arc<AtomicI64>,
        waiting: Arc<AtomicI64>,
    }

    impl CoreMeter for Meter {
        fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
            NoOpCoreMeter.new_attributes(attribs)
        }

        fn extend_attributes(
            &self,
            existing: MetricAttributes,
            attribs: NewAttributes,
        ) -> MetricAttributes {
            NoOpCoreMeter.extend_attributes(existing, attribs)
        }

        fn counter(&self, _: MetricParameters) -> Counter {
            Counter::new(Arc::new(Cell(self.fenced.clone())))
        }

        fn histogram(&self, params: MetricParameters) -> Histogram {
            NoOpCoreMeter.histogram(params)
        }

        fn histogram_f64(&self, params: MetricParameters) -> HistogramF64 {
            NoOpCoreMeter.histogram_f64(params)
        }

        fn histogram_duration(&self, params: MetricParameters) -> HistogramDuration {
            NoOpCoreMeter.histogram_duration(params)
        }

        fn gauge(&self, params: MetricParameters) -> Gauge {
            NoOpCoreMeter.gauge(params)
        }

        fn gauge_f64(&self, params: MetricParameters) -> GaugeF64 {
            NoOpCoreMeter.gauge_f64(params)
        }

        fn up_down_counter(&self, _: MetricParameters) -> UpDownCounter {
            UpDownCounter::new(Arc::new(Cell(self.waiting.clone())))
        }
    }

    /// A runtime on a new [Scripted] Store, with a meter that the test reads.
    fn metered() -> (Arc<Scripted>, Arc<StreamRuntime>, Arc<Meter>) {
        let store = Arc::new(Scripted::default());
        let meter = Arc::new(Meter::default());
        let temporal_meter = TemporalMeter::new(
            meter.clone(),
            NewAttributes::default(),
            TaskQueueLabelStrategy::UseNormal,
        );
        let runtime = StreamRuntime::new(store.clone(), Some(temporal_meter));
        (store, runtime, meter)
    }

    fn stream_ref() -> Option<StreamRef> {
        Some(StreamRef {
            id: "s".to_string(),
            ..Default::default()
        })
    }

    fn client() -> Option<Producer> {
        Some(Producer {
            id: "client:n:p".to_string(),
            kind: Some(Kind::Client(ClientProducer {})),
        })
    }

    fn record() -> AppendRecord {
        AppendRecord {
            body: Some(Payload::default()),
        }
    }

    fn append(sequence: i64, records: Vec<AppendRecord>) -> AppendRecordsRequest {
        AppendRecordsRequest {
            r#ref: stream_ref(),
            producer: client(),
            sequence,
            records,
            ..Default::default()
        }
    }

    fn read() -> ReadRecordsRequest {
        ReadRecordsRequest {
            r#ref: stream_ref(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn a_read_counts_as_waiting_while_the_store_holds_it() {
        let (store, runtime, meter) = metered();
        store.hold_reads.store(true, Ordering::SeqCst);
        let held = tokio::spawn({
            let runtime = runtime.clone();
            async move { runtime.read_records(read()).await }
        });
        store.entered.notified().await;
        assert_eq!(meter.waiting.load(Ordering::SeqCst), 1);
        store.release.notify_one();
        held.await.unwrap().unwrap();
        assert_eq!(meter.waiting.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_read_whose_future_is_dropped_stops_counting_as_waiting() {
        let (store, runtime, meter) = metered();
        store.hold_reads.store(true, Ordering::SeqCst);
        let held = tokio::spawn(async move { runtime.read_records(read()).await });
        store.entered.notified().await;
        assert_eq!(meter.waiting.load(Ordering::SeqCst), 1);
        held.abort();
        assert!(held.await.unwrap_err().is_cancelled());
        assert_eq!(meter.waiting.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn only_a_fenced_append_counts_as_fenced() {
        let (store, runtime, meter) = metered();
        *store.error.lock() = Some(StreamError::new(Reason::Closed, "closed"));
        runtime
            .append_records(append(1, vec![record()]))
            .await
            .unwrap_err();
        assert_eq!(meter.fenced.load(Ordering::SeqCst), 0);
        *store.error.lock() = Some(StreamError::new(Reason::Fenced, "fenced"));
        let error = runtime
            .append_records(append(1, vec![record()]))
            .await
            .unwrap_err();
        assert_eq!(error.reason, Reason::Fenced);
        assert_eq!(meter.fenced.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_bad_request_fails_before_the_store_for_every_operation() {
        let (store, runtime) = runtime();
        // A request that reaches the Store gets this error, so it can't pass for an invalid one.
        *store.error.lock() = Some(StreamError::new(Reason::Closed, "reached the Store"));
        let bad_create = |stream_ref, id_conflict_policy| CreateStreamRequest {
            stream_ref,
            id_conflict_policy,
            ..Default::default()
        };
        let errors = [
            runtime
                .create_stream(bad_create(None, 0))
                .await
                .unwrap_err(),
            runtime
                .create_stream(bad_create(stream_ref(), 99))
                .await
                .unwrap_err(),
            runtime
                .append_records(append(-1, vec![record()]))
                .await
                .unwrap_err(),
            runtime
                .append_records(append(MAX_SEQUENCE + 1, vec![record()]))
                .await
                .unwrap_err(),
            runtime
                .append_records(append(1, vec![AppendRecord::default()]))
                .await
                .unwrap_err(),
            runtime
                .close_stream(CloseStreamRequest {
                    r#ref: stream_ref(),
                    producer: None,
                    ..Default::default()
                })
                .await
                .unwrap_err(),
            runtime
                .close_stream(CloseStreamRequest {
                    r#ref: stream_ref(),
                    producer: Some(Producer {
                        id: "wf:r".to_string(),
                        kind: Some(Kind::Workflow(WorkflowProducer {
                            run_id: "r".to_string(),
                            epoch: None,
                        })),
                    }),
                    ..Default::default()
                })
                .await
                .unwrap_err(),
            runtime
                .describe_stream(DescribeStreamRequest::default())
                .await
                .unwrap_err(),
        ];
        for error in errors {
            assert_eq!(error.reason, Reason::Invalid, "{}", error.message);
        }
        let big = vec![record(); MAX_BATCH_RECORDS + 1];
        let error = runtime.append_records(append(1, big)).await.unwrap_err();
        assert_eq!(error.reason, Reason::TooLarge);
        // The highest sequence is inside the bound, so the Store gets that append.
        let error = runtime
            .append_records(append(MAX_SEQUENCE, vec![record()]))
            .await
            .unwrap_err();
        assert_eq!(error.message, "reached the Store");
    }

    #[tokio::test]
    async fn a_create_answer_needs_the_ref_of_its_request() {
        let (store, runtime) = runtime();
        let create = || {
            runtime.create_stream(CreateStreamRequest {
                stream_ref: stream_ref(),
                ..Default::default()
            })
        };
        assert_eq!(create().await.unwrap().r#ref, stream_ref());
        // The Store may set its own `store`, and USE_EXISTING may get a Stream that existed.
        let named = StreamRef {
            id: "s".to_string(),
            store: "redis".to_string(),
        };
        *store.created.lock() = Some(CreateStreamResponse {
            r#ref: Some(named.clone()),
            created: false,
        });
        assert_eq!(create().await.unwrap().r#ref, Some(named));
        let other = StreamRef {
            id: "other".to_string(),
            ..Default::default()
        };
        for (answer, message) in [
            (None, "The Store answered a create with no ref"),
            (
                Some(other),
                "The Store answered a create with the ref of another Stream",
            ),
        ] {
            *store.created.lock() = Some(CreateStreamResponse {
                r#ref: answer,
                created: true,
            });
            let error = create().await.unwrap_err();
            assert_eq!(error.reason, Reason::StorageCorrupt);
            assert_eq!(error.message, message);
        }
    }

    #[tokio::test]
    async fn a_describe_answer_needs_a_known_status_and_a_bounded_offset() {
        let (store, runtime) = runtime();
        let describe = || {
            runtime.describe_stream(DescribeStreamRequest {
                r#ref: stream_ref(),
                ..Default::default()
            })
        };
        // A Stream with no Record has no offset.
        for status in [
            StreamStatus::Open,
            StreamStatus::Closed,
            StreamStatus::Expired,
        ] {
            for last_offset in [vec![], vec![1; MAX_OFFSET_BYTES]] {
                *store.described.lock() = DescribeStreamResponse {
                    status: status as i32,
                    last_offset: last_offset.clone(),
                };
                assert_eq!(describe().await.unwrap().last_offset, last_offset);
            }
        }
        let bad = [
            (
                StreamStatus::Unspecified as i32,
                0,
                "The Store answered a describe with no status".to_string(),
            ),
            (
                99,
                0,
                "The Store answered a describe with the unknown status 99".to_string(),
            ),
            (
                StreamStatus::Open as i32,
                MAX_OFFSET_BYTES + 1,
                format!(
                    "The Store gave a last offset of {} bytes, more than {MAX_OFFSET_BYTES}",
                    MAX_OFFSET_BYTES + 1
                ),
            ),
        ];
        for (status, length, message) in bad {
            *store.described.lock() = DescribeStreamResponse {
                status,
                last_offset: vec![1; length],
            };
            let error = describe().await.unwrap_err();
            assert_eq!(error.reason, Reason::StorageCorrupt);
            assert_eq!(error.message, message);
        }
    }
}
