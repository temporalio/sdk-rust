//! The Stream runtime, a plain object that lang's bridge builds per client.

use crate::{
    MAX_RECORDS, MAX_WAIT, Reason, StreamError, StreamResult, StreamStore,
    ids::{MAX_OFFSET_BYTES, check_producer, check_required, check_stream_id},
    metrics::Metrics,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        Producer, ReadRecordsRequest, ReadRecordsResponse, StreamIdConflictPolicy, StreamRef,
        StreamStatus, producer::Kind,
    },
    record::{check_bounds, check_records},
};
use prost::Message;
use std::{sync::Arc, time::Duration};
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
    offset_bound(&response.last_offset, "a last offset")?;
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

/// Refuses an offset that a caller gives with more than [MAX_OFFSET_BYTES].
fn caller_offset(offset: &[u8]) -> StreamResult<()> {
    if offset.len() > MAX_OFFSET_BYTES {
        return Err(StreamError::invalid(format!(
            "The offset has {} bytes, more than {MAX_OFFSET_BYTES}",
            offset.len()
        )));
    }
    Ok(())
}

/// Refuses an offset that a Store gives with more than [MAX_OFFSET_BYTES]. An empty one passes.
/// `what` is the offset's place in the message of the error.
fn offset_bound(offset: &[u8], what: &str) -> StreamResult<()> {
    if offset.len() > MAX_OFFSET_BYTES {
        return Err(StreamError::storage_corrupt(format!(
            "The Store gave {what} of {} bytes, more than {MAX_OFFSET_BYTES}",
            offset.len()
        )));
    }
    Ok(())
}

/// Refuses an offset that a Store gives for a Record, empty or with more than [MAX_OFFSET_BYTES].
/// A Worker records the offsets in History and resumes a read from one, and an empty one is the
/// start.
fn store_offset(offset: &[u8]) -> StreamResult<()> {
    if offset.is_empty() {
        return Err(StreamError::storage_corrupt(
            "The Store gave a Record with no offset",
        ));
    }
    offset_bound(offset, "an offset")
}

/// A proto Duration as a `Duration`, `None` when it is unset, zero or negative.
fn duration(value: Option<prost_types::Duration>) -> Option<Duration> {
    let value = value?;
    let duration = Duration::new(value.seconds.max(0) as u64, value.nanos.max(0) as u32);
    (!duration.is_zero()).then_some(duration)
}

/// The byte cap of a page, from a request's `max_bytes`. Zero is no cap.
fn max_bytes(value: i64) -> StreamResult<usize> {
    match value {
        0 => Ok(usize::MAX),
        n if n < 0 => Err(StreamError::invalid("max_bytes must be 0 or more")),
        n => Ok(usize::try_from(n).unwrap_or(usize::MAX)),
    }
}

/// Reads one page, with the caps and the longest wait of a read.
async fn read_page(
    store: &dyn StreamStore,
    metrics: &Metrics,
    mut request: ReadRecordsRequest,
) -> StreamResult<ReadRecordsResponse> {
    caller_offset(&request.from_offset)?;
    let from_offset = request.from_offset.clone();
    let max_bytes = max_bytes(request.max_bytes)?;
    let max = match request.max_records {
        n if n <= 0 => MAX_RECORDS,
        n => (n as usize).min(MAX_RECORDS),
    };
    request.max_records = max as i32;
    request.wait = duration(request.wait)
        .map(|wait| wait.min(MAX_WAIT))
        .and_then(|wait| prost_types::Duration::try_from(wait).ok());
    let response = {
        // The Store holds a read for up to its `wait`. The guard counts the read down again when
        // the Store answers and when the read's future is dropped.
        let _waiting = metrics.read_waiting();
        store.read_records(request).await?
    };
    fit_page(response, &from_offset, max, max_bytes)
}

/// The page cut to `max` Records and `max_bytes`, with its first Record always. A Worker bounds
/// what one read holds by these caps, so a Store that gives more must not pass them. A cut page
/// ends at its last Record and is not done.
fn fit_page(
    mut page: ReadRecordsResponse,
    from_offset: &[u8],
    max: usize,
    max_bytes: usize,
) -> StreamResult<ReadRecordsResponse> {
    for record in &page.records {
        store_offset(&record.offset)?;
    }
    offset_bound(&page.last_offset, "a last offset")?;
    check_cursor(&page, from_offset)?;
    let mut bytes = 0;
    let mut keep = 0;
    for record in &page.records {
        let size = record.encoded_len();
        if keep >= max || (keep > 0 && bytes + size > max_bytes) {
            break;
        }
        bytes += size;
        keep += 1;
    }
    if keep < page.records.len() {
        page.records.truncate(keep);
        page.last_offset = page.records[keep - 1].offset.clone();
        page.done = false;
    }
    Ok(page)
}

/// Refuses a page whose offsets don't rise after `from_offset`, or whose `last_offset` is lower
/// than its last Record. Offsets compare as bytes, and an empty `from_offset` is the start. A
/// Worker resumes a read from `last_offset`, so a bad cursor makes a read that never moves on.
/// A `last_offset` above the last Record is fine, since a Store may skip entries that are not
/// Records, and the page that ends a closed Stream may cover them.
fn check_cursor(page: &ReadRecordsResponse, from_offset: &[u8]) -> StreamResult<()> {
    let mut newest = from_offset;
    for (at, record) in page.records.iter().enumerate() {
        if record.offset.as_slice() <= newest {
            return Err(StreamError::storage_corrupt(if at == 0 {
                "The Store gave a first Record with an offset that is not higher than the \
                 from_offset of the read"
                    .to_string()
            } else {
                format!(
                    "The Store gave Record {at} with an offset that is not higher than the one \
                     before it"
                )
            }));
        }
        newest = &record.offset;
    }
    if page.last_offset.as_slice() < newest {
        return Err(StreamError::storage_corrupt(if page.records.is_empty() {
            "The Store gave an empty page with a last_offset that is lower than the from_offset \
             of the read"
        } else {
            "The Store gave a last_offset that is lower than the offset of the last Record"
        }));
    }
    Ok(())
}

/// Appends a batch that the runtime checked, and checks the offsets of the answer.
async fn append_batch(
    store: &dyn StreamStore,
    request: AppendRecordsRequest,
) -> StreamResult<AppendRecordsResponse> {
    let empty = request.records.is_empty();
    let response = store.append_records(request).await?;
    if empty {
        // A Store may give empty offsets for an empty batch, but not long ones.
        offset_bound(&response.first_offset, "an offset")?;
        offset_bound(&response.last_offset, "an offset")?;
    } else {
        store_offset(&response.first_offset)?;
        store_offset(&response.last_offset)?;
        if response.first_offset > response.last_offset {
            return Err(StreamError::storage_corrupt(
                "The Store gave a first offset that is higher than the last offset of the batch",
            ));
        }
    }
    Ok(response)
}

impl StreamRuntime {
    /// Counts a write that the Store refused for a lower epoch, an append or a close.
    fn count_fenced<T>(&self, result: &StreamResult<T>) {
        if let Err(error) = result
            && error.reason == Reason::Fenced
        {
            self.metrics.fenced();
        }
    }

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
        let appended = append_batch(self.store.as_ref(), request).await;
        self.count_fenced(&appended);
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
        read_page(self.store.as_ref(), &self.metrics, request).await
    }

    /// Closes a Stream. Its readers drain what is left, and later appends fail.
    pub async fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> StreamResult<CloseStreamResponse> {
        stream_id(request.r#ref.as_ref())?;
        check_close_producer(request.producer.as_ref())?;
        let closed = self.store.close_stream(request).await;
        self.count_fenced(&closed);
        closed
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
        proto::{AppendRecord, ClientProducer, StreamRecord, WorkflowProducer, WorkflowTaskEpoch},
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

    fn stored(offset: &[u8], bytes: usize) -> StreamRecord {
        StreamRecord {
            body: Some(Payload {
                data: vec![0; bytes],
                ..Default::default()
            }),
            offset: offset.to_vec(),
            ..Default::default()
        }
    }

    fn workflow(epoch: Option<WorkflowTaskEpoch>) -> Option<Producer> {
        Some(Producer {
            id: "wf:r".to_string(),
            kind: Some(Kind::Workflow(WorkflowProducer {
                run_id: "r".to_string(),
                epoch,
            })),
        })
    }

    fn append_as(producer: Option<Producer>) -> AppendRecordsRequest {
        AppendRecordsRequest {
            r#ref: stream_ref(),
            producer,
            sequence: 1,
            records: vec![AppendRecord {
                body: Some(Payload::default()),
            }],
            ..Default::default()
        }
    }

    fn epoch() -> WorkflowTaskEpoch {
        WorkflowTaskEpoch {
            started_event_id: 3,
            attempt: 1,
            ..Default::default()
        }
    }

    /// A page of Records with these offsets, as a Store gives it.
    fn page(offsets: &[&str], last_offset: &str, done: bool) -> ReadRecordsResponse {
        ReadRecordsResponse {
            records: offsets
                .iter()
                .map(|offset| stored(offset.as_bytes(), 1))
                .collect(),
            last_offset: last_offset.as_bytes().to_vec(),
            done,
        }
    }

    /// What the runtime gives for a read after `from_offset` when the Store answers `answer`.
    async fn read_after(
        from_offset: &str,
        answer: ReadRecordsResponse,
    ) -> StreamResult<ReadRecordsResponse> {
        let (store, runtime) = runtime();
        *store.page.lock() = answer;
        runtime
            .read_records(ReadRecordsRequest {
                r#ref: stream_ref(),
                from_offset: from_offset.as_bytes().to_vec(),
                ..Default::default()
            })
            .await
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
    async fn only_a_fenced_append_or_close_counts_as_fenced() {
        let (store, runtime, meter) = metered();
        let close = || CloseStreamRequest {
            r#ref: stream_ref(),
            producer: client(),
            ..Default::default()
        };
        *store.error.lock() = Some(StreamError::new(Reason::Closed, "closed"));
        runtime
            .append_records(append(1, vec![record()]))
            .await
            .unwrap_err();
        runtime.close_stream(close()).await.unwrap_err();
        assert_eq!(meter.fenced.load(Ordering::SeqCst), 0);
        *store.error.lock() = Some(StreamError::new(Reason::Fenced, "fenced"));
        let error = runtime
            .append_records(append(1, vec![record()]))
            .await
            .unwrap_err();
        assert_eq!(error.reason, Reason::Fenced);
        assert_eq!(meter.fenced.load(Ordering::SeqCst), 1);
        let error = runtime.close_stream(close()).await.unwrap_err();
        assert_eq!(error.reason, Reason::Fenced);
        assert_eq!(meter.fenced.load(Ordering::SeqCst), 2);
        // A close that the runtime refuses before the Store is not a fenced close.
        let bad = CloseStreamRequest {
            producer: None,
            ..close()
        };
        runtime.close_stream(bad).await.unwrap_err();
        assert_eq!(meter.fenced.load(Ordering::SeqCst), 2);
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

    #[tokio::test]
    async fn a_read_goes_to_the_store_with_its_caps_and_the_longest_wait() {
        let (store, runtime) = runtime();
        *store.page.lock() = ReadRecordsResponse {
            records: vec![stored(b"a", 1)],
            last_offset: b"a".to_vec(),
            done: true,
        };
        let page = runtime
            .read_records(ReadRecordsRequest {
                r#ref: stream_ref(),
                from_offset: vec![9; 20],
                wait: Some(prost_types::Duration {
                    seconds: 3600,
                    nanos: 0,
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(page.done);
        let sent = store.reads.lock()[0].clone();
        // An offset is opaque to the runtime, so one of any length up to the bound passes.
        assert_eq!(sent.from_offset, vec![9; 20]);
        assert_eq!(sent.max_records, MAX_RECORDS as i32);
        assert_eq!(duration(sent.wait), Some(MAX_WAIT));
    }

    #[tokio::test]
    async fn a_page_over_its_caps_is_cut_and_not_done() {
        let (store, runtime) = runtime();
        *store.page.lock() = ReadRecordsResponse {
            records: vec![stored(b"a", 10), stored(b"b", 10), stored(b"c", 10)],
            last_offset: b"c".to_vec(),
            done: true,
        };
        let read = |max_records, max_bytes| ReadRecordsRequest {
            r#ref: stream_ref(),
            max_records,
            max_bytes,
            ..Default::default()
        };
        let page = runtime.read_records(read(2, 0)).await.unwrap();
        assert_eq!((page.records.len(), page.done), (2, false));
        assert_eq!(page.last_offset, b"b".to_vec());
        // The first Record comes over the byte cap too, so a read always moves on.
        let page = runtime.read_records(read(0, 1)).await.unwrap();
        assert_eq!((page.records.len(), page.done), (1, false));
        assert_eq!(page.last_offset, b"a".to_vec());
        let page = runtime.read_records(read(0, 0)).await.unwrap();
        assert_eq!((page.records.len(), page.done), (3, true));
    }

    #[tokio::test]
    async fn a_bad_offset_of_the_store_is_corrupt() {
        let (store, runtime) = runtime();
        for offset in [vec![], vec![1; MAX_OFFSET_BYTES + 1]] {
            *store.page.lock() = ReadRecordsResponse {
                records: vec![stored(&offset, 1)],
                ..Default::default()
            };
            let request = ReadRecordsRequest {
                r#ref: stream_ref(),
                ..Default::default()
            };
            let error = runtime.read_records(request).await.unwrap_err();
            assert_eq!(error.reason, Reason::StorageCorrupt);
            *store.appended.lock() = AppendRecordsResponse {
                first_offset: offset.clone(),
                last_offset: offset,
            };
            let epoch = WorkflowTaskEpoch {
                started_event_id: 3,
                attempt: 1,
                ..Default::default()
            };
            let error = runtime
                .append_records(append_as(workflow(Some(epoch))))
                .await
                .unwrap_err();
            assert_eq!(error.reason, Reason::StorageCorrupt);
        }
    }

    #[tokio::test]
    async fn a_bad_request_never_reaches_the_store() {
        let (store, runtime) = runtime();
        let bad_reads = [
            ReadRecordsRequest::default(),
            ReadRecordsRequest {
                r#ref: stream_ref(),
                from_offset: vec![1; MAX_OFFSET_BYTES + 1],
                ..Default::default()
            },
            ReadRecordsRequest {
                r#ref: stream_ref(),
                max_bytes: -1,
                ..Default::default()
            },
        ];
        for request in bad_reads {
            let error = runtime.read_records(request).await.unwrap_err();
            assert_eq!(error.reason, Reason::Invalid);
        }
        // A Workflow writes with its Workflow Task as the epoch, so the fence orders its writes.
        let error = runtime
            .append_records(append_as(workflow(None)))
            .await
            .unwrap_err();
        assert_eq!(error.reason, Reason::Invalid);
        assert!(store.reads.lock().is_empty());
        assert!(store.appends.lock().is_empty());
    }

    #[tokio::test]
    async fn an_append_gets_the_producer_with_its_epoch_and_the_error_of_the_store() {
        let (store, runtime) = runtime();
        let epoch = WorkflowTaskEpoch {
            started_event_id: 3,
            attempt: 1,
            ..Default::default()
        };
        *store.appended.lock() = AppendRecordsResponse {
            first_offset: b"a".to_vec(),
            last_offset: b"a".to_vec(),
        };
        let appended = runtime
            .append_records(append_as(workflow(Some(epoch))))
            .await
            .unwrap();
        assert_eq!(appended.last_offset, b"a".to_vec());
        assert_eq!(store.appends.lock()[0].producer, workflow(Some(epoch)));
        *store.error.lock() = Some(StreamError::new(Reason::Closed, "closed"));
        let error = runtime
            .append_records(append_as(workflow(Some(epoch))))
            .await
            .unwrap_err();
        assert_eq!(
            (error.reason, error.message.as_str()),
            (Reason::Closed, "closed")
        );
    }

    #[tokio::test]
    async fn a_page_with_a_bad_cursor_is_corrupt() {
        let first = "The Store gave a first Record with an offset that is not higher than the \
                     from_offset of the read";
        let record_at = |at| {
            format!(
                "The Store gave Record {at} with an offset that is not higher than the one \
                 before it"
            )
        };
        let last = "The Store gave a last_offset that is lower than the offset of the last Record";
        let empty = "The Store gave an empty page with a last_offset that is lower than the \
                     from_offset of the read";
        let cases = [
            // The Records are not after the offset of the read, and the cursor is empty.
            ("m", page(&["b", "a", "a"], "", false), first.to_string()),
            ("a", page(&["a"], "a", false), first.to_string()),
            ("b", page(&["a", "c"], "c", false), first.to_string()),
            ("", page(&["b", "a"], "b", false), record_at(1)),
            ("", page(&["a", "a"], "a", false), record_at(1)),
            ("", page(&["a", "b", "b"], "b", false), record_at(2)),
            ("a", page(&["b"], "", false), last.to_string()),
            ("", page(&["b", "c"], "b", true), last.to_string()),
            ("m", page(&[], "", false), empty.to_string()),
            ("m", page(&[], "a", true), empty.to_string()),
        ];
        for (from_offset, answer, message) in cases {
            let error = read_after(from_offset, answer.clone()).await.unwrap_err();
            assert_eq!(error.reason, Reason::StorageCorrupt, "{answer:?}");
            assert_eq!(error.message, message, "after {from_offset:?}, {answer:?}");
        }
    }

    #[tokio::test]
    async fn a_page_with_a_good_cursor_passes_as_it_is() {
        let cases = [
            ("", page(&["a", "b", "c"], "c", false)),
            ("a", page(&["b", "c"], "c", false)),
            // A Store may skip entries that are not Records, so the page that ends a closed Stream
            // may cover them. Its `last_offset` is then above its last Record.
            ("a", page(&["b"], "z", true)),
            ("", page(&[], "", false)),
            ("m", page(&[], "m", false)),
            ("m", page(&[], "z", true)),
        ];
        for (from_offset, answer) in cases {
            let page = read_after(from_offset, answer.clone()).await.unwrap();
            assert_eq!(page, answer, "after {from_offset:?}");
        }
    }

    #[tokio::test]
    async fn an_append_answer_with_a_first_offset_above_the_last_is_corrupt() {
        let (store, runtime) = runtime();
        for (first, last, good) in [("a", "a", true), ("a", "c", true), ("c", "a", false)] {
            *store.appended.lock() = AppendRecordsResponse {
                first_offset: first.as_bytes().to_vec(),
                last_offset: last.as_bytes().to_vec(),
            };
            let result = runtime
                .append_records(append_as(workflow(Some(epoch()))))
                .await;
            if good {
                assert_eq!(result.unwrap().last_offset, last.as_bytes().to_vec());
                continue;
            }
            let error = result.unwrap_err();
            assert_eq!(error.reason, Reason::StorageCorrupt);
            assert_eq!(
                error.message,
                "The Store gave a first offset that is higher than the last offset of the batch"
            );
        }
    }

    #[tokio::test]
    async fn the_offsets_of_an_empty_batch_stay_within_the_bound() {
        let (store, runtime) = runtime();
        let empty = || AppendRecordsRequest {
            records: vec![],
            ..append_as(workflow(Some(epoch())))
        };
        for (first, last) in [(0, 0), (MAX_OFFSET_BYTES, MAX_OFFSET_BYTES)] {
            *store.appended.lock() = AppendRecordsResponse {
                first_offset: vec![1; first],
                last_offset: vec![1; last],
            };
            runtime.append_records(empty()).await.unwrap();
        }
        let too_long = MAX_OFFSET_BYTES + 1;
        for (first, last) in [(too_long, 0), (0, too_long)] {
            *store.appended.lock() = AppendRecordsResponse {
                first_offset: vec![1; first],
                last_offset: vec![1; last],
            };
            let error = runtime.append_records(empty()).await.unwrap_err();
            assert_eq!(error.reason, Reason::StorageCorrupt);
            assert_eq!(
                error.message,
                format!("The Store gave an offset of {too_long} bytes, more than 1024")
            );
        }
    }
}
