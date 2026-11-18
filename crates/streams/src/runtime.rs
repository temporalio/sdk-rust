//! The Stream runtime, which checks Stream calls and passes them to a Store.

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
    record::{BatchLimits, check_records},
};
use prost::Message;
use std::{sync::Arc, time::Duration};
use temporalio_common::telemetry::metrics::TemporalMeter;

/// The highest batch `sequence`. A Store that keeps numbers as 64-bit floats holds an integer
/// exactly up to this value.
pub const MAX_SEQUENCE: i64 = (1 << 53) - 1;

/// The Stream runtime on one Store. A bridge builds one for each client that has a Stream Store,
/// and the Workers of that client share it.
///
/// The runtime checks each request before the Store sees it, and each answer after, with the same
/// rules for every Store.
///
/// - Stream ids and Producer ids are opaque text of 1 to 4096 bytes.
/// - A Workflow Producer writes with its Workflow Task as the epoch, so a Store can fence a write
///   from an older task.
/// - A batch must fit the [`BatchLimits`] of the Store, measured on the stored Records.
/// - A read returns at most [`MAX_RECORDS`] Records, and the runtime asks the Store to wait at most
///   [`MAX_WAIT`]. The runtime cuts a page that is over the caps of its request.
/// - The offsets of a page rise, start after `from_offset`, and end at or before `last_offset`.
/// - A create answer has the ref of its Stream, and a describe answer has a known status.
///
/// An answer that breaks these rules fails with [`Reason::StorageCorrupt`]. The runtime ignores a
/// request's `namespace`, since a Store has no namespaces.
pub struct StreamRuntime {
    store: Arc<dyn StreamStore>,
    limits: BatchLimits,
    metrics: Metrics,
}

impl std::fmt::Debug for StreamRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamRuntime").finish_non_exhaustive()
    }
}

/// Refuses a conflict policy value that the proto doesn't define. The Store applies the policy, and
/// treats an unspecified one as `Fail`, as a Workflow start does.
fn check_conflict_policy(value: i32) -> StreamResult<()> {
    StreamIdConflictPolicy::try_from(value)
        .map(drop)
        .map_err(|_| StreamError::invalid(format!("The conflict policy {value} is unknown")))
}

/// Refuses a request with no Stream ref, or with an empty or too long Stream id.
fn check_stream_ref(stream_ref: Option<&StreamRef>) -> StreamResult<()> {
    let id = stream_ref
        .map(|stream_ref| stream_ref.id.as_str())
        .unwrap_or_default();
    check_stream_id(id)
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

/// Refuses a describe answer with a long `last_offset`, no status, or an unknown status. An empty
/// `last_offset` is fine, since a Stream may have no Records.
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
/// Task as the epoch, as for an append.
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

/// Checks an append before the Store sees it, and returns its Producer.
fn append_producer(request: &AppendRecordsRequest) -> StreamResult<&Producer> {
    check_stream_ref(request.r#ref.as_ref())?;
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

/// Refuses an offset from a caller with more than [`MAX_OFFSET_BYTES`].
fn caller_offset(offset: &[u8]) -> StreamResult<()> {
    if offset.len() > MAX_OFFSET_BYTES {
        return Err(StreamError::invalid(format!(
            "The offset has {} bytes, more than {MAX_OFFSET_BYTES}",
            offset.len()
        )));
    }
    Ok(())
}

/// Refuses an offset from the Store with more than [`MAX_OFFSET_BYTES`]. An empty one passes.
/// `what` describes the offset in the error message.
fn offset_bound(offset: &[u8], what: &str) -> StreamResult<()> {
    if offset.len() > MAX_OFFSET_BYTES {
        return Err(StreamError::storage_corrupt(format!(
            "The Store gave {what} of {} bytes, more than {MAX_OFFSET_BYTES}",
            offset.len()
        )));
    }
    Ok(())
}

/// Refuses a Record offset from the Store that is empty or has more than [`MAX_OFFSET_BYTES`]. A
/// Worker resumes a read from a Record's offset, and an empty offset is the start of the Stream.
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

/// Reads one page, with the caps of the request and a wait of at most [`MAX_WAIT`].
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
        // The Store can hold the read for up to its `wait`.
        let _waiting = metrics.read_waiting();
        store.read_records(request).await?
    };
    fit_page(response, &from_offset, max, max_bytes)
}

/// Cuts the page to `max` Records and `max_bytes`, but always keeps the first Record so a read
/// moves on. A Worker bounds what one read holds by these caps, so a Store that gives more can't
/// pass them. A cut page ends at its last kept Record and isn't done.
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
/// Worker resumes a read from `last_offset`, so a bad cursor makes a read that never moves on. A
/// `last_offset` above the last Record is fine, since a Store may skip entries that aren't Records.
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
    /// Counts an append or a close that the Store fenced.
    fn count_fenced<T>(&self, result: &StreamResult<T>) {
        if let Err(error) = result
            && error.reason == Reason::Fenced
        {
            self.metrics.fenced();
        }
    }

    /// Builds the runtime on `store`. It records metrics on `meter` if set.
    pub fn new(store: Arc<dyn StreamStore>, meter: Option<TemporalMeter>) -> Arc<Self> {
        let meter = meter.unwrap_or_else(TemporalMeter::no_op);
        Arc::new(Self {
            limits: store.limits(),
            store,
            metrics: Metrics::new(&meter),
        })
    }

    /// Creates a Stream, or gets the open one with the `UseExisting` conflict policy.
    pub async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        check_stream_ref(request.stream_ref.as_ref())?;
        check_conflict_policy(request.id_conflict_policy)?;
        let id = request
            .stream_ref
            .as_ref()
            .map(|stream_ref| stream_ref.id.clone())
            .unwrap_or_default();
        let created = self.store.create_stream(request).await?;
        check_created(&created, &id)?;
        Ok(created)
    }

    /// Appends one batch as one Producer. A Workflow Producer appends with its Workflow Task as the
    /// epoch, so the Store can fence a write from an older task.
    pub async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        let producer = append_producer(&request)?;
        // A batch over the limits of the Store fails here, so the Store never sees it.
        self.limits.check(producer, &request.records)?;
        let appended = append_batch(self.store.as_ref(), request).await;
        self.count_fenced(&appended);
        appended
    }

    /// Reads one page after `from_offset`. If the Store has no Records to return, it waits up to
    /// the request's `wait`, and `stream_blocking_reads_waiting` counts the read for that time.
    pub async fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> StreamResult<ReadRecordsResponse> {
        check_stream_ref(request.r#ref.as_ref())?;
        read_page(self.store.as_ref(), &self.metrics, request).await
    }

    /// Closes a Stream. Readers still get the Records it has, and later appends fail.
    pub async fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> StreamResult<CloseStreamResponse> {
        check_stream_ref(request.r#ref.as_ref())?;
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
        check_stream_ref(request.r#ref.as_ref())?;
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
        test_store::{Scripted, runtime, runtime_with_limits},
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

    /// A runtime on a new [`Scripted`] Store, with a meter that the test reads.
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
        let cases = [
            (
                "a create with no ref",
                runtime.create_stream(bad_create(None, 0)).await.map(drop),
            ),
            (
                "a create with an unknown conflict policy",
                runtime
                    .create_stream(bad_create(stream_ref(), 99))
                    .await
                    .map(drop),
            ),
            (
                "an append with a negative sequence",
                runtime
                    .append_records(append(-1, vec![record()]))
                    .await
                    .map(drop),
            ),
            (
                "an append over the highest sequence",
                runtime
                    .append_records(append(MAX_SEQUENCE + 1, vec![record()]))
                    .await
                    .map(drop),
            ),
            (
                "an append with a Record that has no body",
                runtime
                    .append_records(append(1, vec![AppendRecord::default()]))
                    .await
                    .map(drop),
            ),
            (
                "an append from a Workflow with no epoch",
                runtime
                    .append_records(append_as(workflow(None)))
                    .await
                    .map(drop),
            ),
            (
                "a close with no Producer",
                runtime
                    .close_stream(CloseStreamRequest {
                        r#ref: stream_ref(),
                        producer: None,
                        ..Default::default()
                    })
                    .await
                    .map(drop),
            ),
            (
                "a close from a Workflow with no epoch",
                runtime
                    .close_stream(CloseStreamRequest {
                        r#ref: stream_ref(),
                        producer: workflow(None),
                        ..Default::default()
                    })
                    .await
                    .map(drop),
            ),
            (
                "a read with no ref",
                runtime
                    .read_records(ReadRecordsRequest::default())
                    .await
                    .map(drop),
            ),
            (
                "a read from a long offset",
                runtime
                    .read_records(ReadRecordsRequest {
                        from_offset: vec![1; MAX_OFFSET_BYTES + 1],
                        ..read()
                    })
                    .await
                    .map(drop),
            ),
            (
                "a read with a negative max_bytes",
                runtime
                    .read_records(ReadRecordsRequest {
                        max_bytes: -1,
                        ..read()
                    })
                    .await
                    .map(drop),
            ),
            (
                "a describe with no ref",
                runtime
                    .describe_stream(DescribeStreamRequest::default())
                    .await
                    .map(drop),
            ),
        ];
        for (case, result) in cases {
            let error = result.expect_err(case);
            assert_eq!(error.reason, Reason::Invalid, "{case}: {}", error.message);
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
            let error = runtime
                .append_records(append_as(workflow(Some(epoch()))))
                .await
                .unwrap_err();
            assert_eq!(error.reason, Reason::StorageCorrupt);
        }
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
            // A read after `m` that gets Records below it and an empty `last_offset`.
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
        assert_eq!(store.appends.lock().len(), 2);
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

    #[tokio::test]
    async fn a_long_last_offset_of_a_read_is_corrupt() {
        // The Record is fine and `last_offset` is above it, so only the bound can refuse the page.
        let page = |last_offset: usize| ReadRecordsResponse {
            records: vec![stored(b"a", 1)],
            last_offset: vec![0xff; last_offset],
            done: true,
        };
        read_after("", page(MAX_OFFSET_BYTES)).await.unwrap();
        let error = read_after("", page(MAX_OFFSET_BYTES + 1))
            .await
            .unwrap_err();
        assert_eq!(error.reason, Reason::StorageCorrupt);
        assert_eq!(
            error.message,
            "The Store gave a last offset of 1025 bytes, more than 1024"
        );
    }

    #[tokio::test]
    async fn the_max_records_of_a_read_is_1000_or_less_and_zero_or_less_is_1000() {
        let (store, runtime) = runtime();
        // The Store gives 1001 Records, one more than the highest cap.
        *store.page.lock() = ReadRecordsResponse {
            records: (1u16..=1001).map(|n| stored(&n.to_be_bytes(), 1)).collect(),
            last_offset: 1001u16.to_be_bytes().to_vec(),
            done: true,
        };
        for (asked, sent, kept) in [
            (0, 1000, 1000),
            (1, 1, 1),
            (1000, 1000, 1000),
            (1001, 1000, 1000),
            (-1, 1000, 1000),
        ] {
            let page = runtime
                .read_records(ReadRecordsRequest {
                    max_records: asked,
                    ..read()
                })
                .await
                .unwrap();
            assert_eq!(store.reads.lock().last().unwrap().max_records, sent);
            assert_eq!(page.records.len(), kept, "max_records {asked}");
            // The page is cut, so it is not done and it ends at its last Record.
            assert!(!page.done);
            assert_eq!(page.last_offset, (kept as u16).to_be_bytes().to_vec());
        }
    }

    #[tokio::test]
    async fn a_batch_over_the_limits_of_its_store_fails_before_the_store() {
        let limits = BatchLimits::default().with_max_batch_records(2).unwrap();
        let (store, runtime) = runtime_with_limits(limits);
        let error = runtime
            .append_records(append(1, vec![record(); 3]))
            .await
            .unwrap_err();
        assert_eq!(error.reason, Reason::TooLarge);
        assert_eq!(error.message, "The batch has 3 Records, more than 2");
        assert!(store.appends.lock().is_empty());
        *store.appended.lock() = AppendRecordsResponse {
            first_offset: b"a".to_vec(),
            last_offset: b"b".to_vec(),
        };
        runtime
            .append_records(append(1, vec![record(); 2]))
            .await
            .unwrap();
        assert_eq!(store.appends.lock().len(), 1);
    }
}
