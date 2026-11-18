//! A Stream Store that keeps its Streams in this process, for the tests of Core. It is a test
//! double. It has a rule of the Stream service only where a test of Core needs that rule.

use parking_lot::Mutex;
use prost::Message;
use std::{collections::HashMap, time::Duration};
use temporalio_streams::{
    Reason, StreamError, StreamResult, StreamStore,
    epoch::{EPOCH_KEY_LEN, ProducerKind, epoch_key},
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest,
        CloseStreamResponse, CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest,
        DescribeStreamResponse, Producer, ReadRecordsRequest, ReadRecordsResponse, StreamRecord,
        StreamRef, StreamStatus,
    },
};
use tokio::{sync::Notify, time::Instant};

/// How long a closed Stream stays readable, unless a test sets another time.
const RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The distance between two Records of one Stream, so that no offset is the next number of another.
const STEP: u64 = 16;
/// The distance between the first offsets of two Streams of one Store.
const BASE: u64 = 1 << 20;

/// The last batch of one Producer, with the epoch that wrote it.
struct Written {
    epoch: [u8; EPOCH_KEY_LEN],
    sequence: i64,
    records: Vec<AppendRecord>,
    first_offset: Vec<u8>,
    last_offset: Vec<u8>,
}

struct Stream {
    /// The offset before the first Record. A Stream gets its own, so that no two Streams of a Store
    /// have the same offset.
    base: u64,
    /// The Records in order. The Record at index `i` has the offset `base + (i + 1) * STEP`.
    records: Vec<StreamRecord>,
    closed_at: Option<Instant>,
    producers: HashMap<(ProducerKind, String), Written>,
}

/// A Store whose Streams live as long as the value. An offset has 8 bytes. It rises with gaps in
/// one Stream, and each Stream has a base of its own.
///
/// It has only the rules that a test of Core needs. A read from an offset past the end waits, with
/// no `OffsetTooHigh`, and a describe never answers `Expired`. A create has no conflict policy and
/// no request id, and the Retention counts on Tokio's clock.
pub(super) struct TestStreamStore {
    retention: Duration,
    streams: Mutex<HashMap<String, Stream>>,
    /// Wakes the reads that wait, when a Stream gets Records or closes.
    changed: Notify,
}

impl Default for TestStreamStore {
    fn default() -> Self {
        Self::with_retention(RETENTION)
    }
}

fn wire(offset: u64) -> Vec<u8> {
    offset.to_be_bytes().to_vec()
}

/// The number of an offset that a read gives. The empty offset is the start, zero.
fn number(offset: &[u8]) -> StreamResult<u64> {
    if offset.len() > 8 {
        return Err(StreamError::invalid("the offset has more than 8 bytes"));
    }
    let mut bytes = [0; 8];
    bytes[8 - offset.len()..].copy_from_slice(offset);
    Ok(u64::from_be_bytes(bytes))
}

fn id_of(stream_ref: Option<&StreamRef>) -> &str {
    stream_ref.map_or("", |stream_ref| stream_ref.id.as_str())
}

fn producer_key(producer: Option<&Producer>) -> StreamResult<(ProducerKind, String)> {
    let producer = producer.ok_or_else(|| StreamError::invalid("the request has no Producer"))?;
    Ok((ProducerKind::try_from(producer)?, producer.id.clone()))
}

fn fenced() -> StreamError {
    StreamError::new(
        Reason::Fenced,
        "the epoch is lower than the newest one of the Producer",
    )
}

impl TestStreamStore {
    /// A Store where a closed Stream expires `retention` after its close.
    pub(super) fn with_retention(retention: Duration) -> Self {
        Self {
            retention,
            streams: Mutex::default(),
            changed: Notify::new(),
        }
    }

    /// The Stream `id`, or the reason that no call can use it.
    fn live<'a>(
        &self,
        streams: &'a mut HashMap<String, Stream>,
        id: &str,
    ) -> StreamResult<&'a mut Stream> {
        let stream = streams
            .get_mut(id)
            .ok_or_else(|| StreamError::new(Reason::NotFound, "nobody created the Stream"))?;
        if stream
            .closed_at
            .is_some_and(|closed_at| closed_at + self.retention <= Instant::now())
        {
            return Err(StreamError::new(Reason::Expired, "the Stream expired"));
        }
        Ok(stream)
    }
}

impl Stream {
    fn new(base: u64) -> Self {
        Self {
            base,
            records: vec![],
            closed_at: None,
            producers: HashMap::new(),
        }
    }

    fn offset(&self, index: usize) -> u64 {
        self.base + (index as u64 + 1) * STEP
    }

    /// The offset of the newest Record, or the empty offset.
    fn newest(&self) -> Vec<u8> {
        match self.records.len() {
            0 => vec![],
            len => wire(self.offset(len - 1)),
        }
    }

    /// The page of at most `max` Records and `max_bytes` after `from`, with its first Record
    /// always. It is done once a closed Stream gave its last Record.
    fn page(&self, request: &ReadRecordsRequest, from: u64) -> ReadRecordsResponse {
        let max = request.max_records.max(1) as usize;
        let max_bytes = match request.max_bytes {
            0 => usize::MAX,
            bytes => usize::try_from(bytes).unwrap_or(usize::MAX),
        };
        // The Records at or before `from` come first, and an offset past the end skips them all.
        let start = ((from.saturating_sub(self.base) / STEP) as usize).min(self.records.len());
        let mut records = vec![];
        let mut bytes = 0;
        for record in self.records[start..].iter().take(max) {
            let size = record.encoded_len();
            if !records.is_empty() && bytes + size > max_bytes {
                break;
            }
            bytes += size;
            records.push(record.clone());
        }
        let last_offset = records.last().map_or_else(
            || request.from_offset.clone(),
            |record| record.offset.clone(),
        );
        ReadRecordsResponse {
            done: self.closed_at.is_some() && start + records.len() >= self.records.len(),
            records,
            last_offset,
        }
    }
}

#[async_trait::async_trait]
impl StreamStore for TestStreamStore {
    async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        let created = {
            let mut streams = self.streams.lock();
            let id = id_of(request.stream_ref.as_ref());
            let created = !streams.contains_key(id);
            let base = (streams.len() as u64 + 1) * BASE;
            streams
                .entry(id.to_string())
                .or_insert_with(|| Stream::new(base));
            created
        };
        Ok(CreateStreamResponse {
            r#ref: request.stream_ref,
            created,
        })
    }

    async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        let producer = request.producer.as_ref();
        let key = producer_key(producer)?;
        let epoch = epoch_key(producer.and_then(|producer| producer.kind.as_ref()));
        let appended = {
            let mut streams = self.streams.lock();
            let stream = self.live(&mut streams, id_of(request.r#ref.as_ref()))?;
            let stored = stream.producers.get(&key);
            // The dedupe state is of the newest epoch only, which restarts the numbers.
            let previous = stored.filter(|stored| stored.epoch == epoch);
            let sequence = match request.sequence {
                0 => previous.map_or(1, |previous| previous.sequence + 1),
                sequence => sequence,
            };
            if let Some(previous) = previous {
                if sequence == previous.sequence {
                    if previous.records != request.records {
                        return Err(StreamError::new(
                            Reason::SeqDivergent,
                            format!(
                                "sequence {sequence} has other content than the batch the Store has"
                            ),
                        ));
                    }
                    return Ok(AppendRecordsResponse {
                        first_offset: previous.first_offset.clone(),
                        last_offset: previous.last_offset.clone(),
                    });
                }
                if sequence < previous.sequence {
                    return Err(StreamError::new(
                        Reason::SequenceTooLow,
                        format!(
                            "sequence {sequence} is below the last one, {}",
                            previous.sequence
                        ),
                    ));
                }
            }
            if stream.closed_at.is_some() {
                return Err(StreamError::new(Reason::Closed, "the Stream is closed"));
            }
            if stored.is_some_and(|stored| epoch < stored.epoch) {
                return Err(fenced());
            }
            let first = stream.records.len();
            for record in &request.records {
                let offset = wire(stream.offset(stream.records.len()));
                stream.records.push(StreamRecord {
                    producer: request.producer.clone(),
                    body: record.body.clone(),
                    offset,
                });
            }
            // An empty batch has no offset, which is the empty wire value.
            let (first_offset, last_offset) = if stream.records.len() == first {
                (vec![], vec![])
            } else {
                (wire(stream.offset(first)), stream.newest())
            };
            stream.producers.insert(
                key,
                Written {
                    epoch,
                    sequence,
                    records: request.records,
                    first_offset: first_offset.clone(),
                    last_offset: last_offset.clone(),
                },
            );
            AppendRecordsResponse {
                first_offset,
                last_offset,
            }
        };
        self.changed.notify_waiters();
        Ok(appended)
    }

    async fn read_records(&self, request: ReadRecordsRequest) -> StreamResult<ReadRecordsResponse> {
        let from = number(&request.from_offset)?;
        let wait = request
            .wait
            .and_then(|wait| Duration::try_from(wait).ok())
            .unwrap_or_default();
        let deadline = Instant::now() + wait;
        loop {
            // Listen before the check, so that an append between the two still wakes this read.
            let changed = self.changed.notified();
            {
                let mut streams = self.streams.lock();
                let stream = self.live(&mut streams, id_of(request.r#ref.as_ref()))?;
                let page = stream.page(&request, from);
                if !page.records.is_empty() || page.done {
                    return Ok(page);
                }
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Ok(ReadRecordsResponse {
                    last_offset: request.from_offset,
                    ..Default::default()
                });
            }
        }
    }

    async fn close_stream(&self, request: CloseStreamRequest) -> StreamResult<CloseStreamResponse> {
        let producer = request.producer.as_ref();
        let key = producer_key(producer)?;
        let epoch = epoch_key(producer.and_then(|producer| producer.kind.as_ref()));
        {
            let mut streams = self.streams.lock();
            let stream = self.live(&mut streams, id_of(request.r#ref.as_ref()))?;
            // A second close succeeds with no change.
            if stream.closed_at.is_some() {
                return Ok(CloseStreamResponse {});
            }
            if stream
                .producers
                .get(&key)
                .is_some_and(|stored| epoch < stored.epoch)
            {
                return Err(fenced());
            }
            stream.closed_at = Some(Instant::now());
        }
        self.changed.notify_waiters();
        Ok(CloseStreamResponse {})
    }

    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        let streams = self.streams.lock();
        let stream = streams
            .get(id_of(request.r#ref.as_ref()))
            .ok_or_else(|| StreamError::new(Reason::NotFound, "nobody created the Stream"))?;
        let status = if stream.closed_at.is_some() {
            StreamStatus::Closed
        } else {
            StreamStatus::Open
        };
        Ok(DescribeStreamResponse {
            status: status as i32,
            last_offset: stream.newest(),
        })
    }
}
