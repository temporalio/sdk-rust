//! The one Stream Store of Core's tests. It keeps the rules of [StreamStore], and a test sets its
//! faults.

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use temporalio_streams::{
    Reason, StreamError, StreamResult, StreamStore,
    epoch::{EPOCH_KEY_LEN, epoch_key},
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest,
        CloseStreamResponse, CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest,
        DescribeStreamResponse, ReadRecordsRequest, ReadRecordsResponse, StreamRecord, StreamRef,
        StreamStatus,
    },
};
use tokio::sync::watch;

/// A write of the Store, which a fault can hit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Op {
    Append,
    Close,
}

/// A write that the Store got, in the order that it came.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Request {
    Append(AppendRecordsRequest),
    Close(CloseStreamRequest),
}

/// The last batch of a Producer, with its epoch.
struct Written {
    epoch: [u8; EPOCH_KEY_LEN],
    sequence: i64,
    records: Vec<AppendRecord>,
    offsets: (Vec<u8>, Vec<u8>),
}

struct Stream {
    /// Each Stream has its own offsets, 16 apart, so an offset counts no Records.
    base: u64,
    records: Vec<StreamRecord>,
    closed: bool,
    producers: HashMap<String, Written>,
}

impl Stream {
    fn offset(&self, index: usize) -> Vec<u8> {
        (self.base + (index as u64 + 1) * 16).to_be_bytes().to_vec()
    }
}

#[derive(Default)]
struct State {
    streams: HashMap<String, Stream>,
    requests: Vec<Request>,
    held: HashSet<Op>,
    failures: HashMap<Op, (StreamError, usize)>,
    lost_answers: HashMap<Op, usize>,
}

/// A Store in memory. As the Redis Store does, it fences a lower epoch of a Producer, writes the
/// last batch of an epoch once, and refuses a lower sequence of an epoch.
#[derive(Default)]
pub(super) struct MemoryStore {
    state: Mutex<State>,
    released: watch::Sender<u64>,
}

impl MemoryStore {
    /// Each call of `op` waits for [Self::release].
    pub(super) fn hold(&self, op: Op) {
        self.state.lock().held.insert(op);
    }

    /// Lets each held call go on, and ends the holds.
    pub(super) fn release(&self) {
        self.state.lock().held.clear();
        self.released.send_modify(|count| *count += 1);
    }

    /// The next `times` calls of `op` fail with `error` before they write.
    pub(super) fn fail(&self, op: Op, error: StreamError, times: usize) {
        self.state.lock().failures.insert(op, (error, times));
    }

    /// The next `times` calls of `op` write, and then fail with `StorageUnavailable`.
    pub(super) fn lose_answers(&self, op: Op, times: usize) {
        self.state.lock().lost_answers.insert(op, times);
    }

    pub(super) fn requests(&self) -> Vec<Request> {
        self.state.lock().requests.clone()
    }

    /// The Records of the Stream `id`, in order.
    pub(super) fn records(&self, id: &str) -> Vec<StreamRecord> {
        let state = self.state.lock();
        state
            .streams
            .get(id)
            .map(|s| s.records.clone())
            .unwrap_or_default()
    }

    pub(super) fn is_closed(&self, id: &str) -> bool {
        let state = self.state.lock();
        state.streams.get(id).is_some_and(|stream| stream.closed)
    }

    /// Takes the request, and then holds or fails it as the faults say.
    async fn arrive(&self, op: Op, request: Request) -> StreamResult<()> {
        let held = {
            let mut state = self.state.lock();
            state.requests.push(request);
            state.held.contains(&op).then(|| self.released.subscribe())
        };
        if let Some(mut released) = held {
            let _ = released.changed().await;
        }
        let mut state = self.state.lock();
        match state.failures.get_mut(&op) {
            Some((error, times)) if *times > 0 => {
                *times -= 1;
                Err(error.clone())
            }
            _ => Ok(()),
        }
    }

    fn answer<T>(&self, op: Op, answer: StreamResult<T>) -> StreamResult<T> {
        let mut state = self.state.lock();
        let lost = state.lost_answers.entry(op).or_default();
        if answer.is_ok() && *lost > 0 {
            *lost -= 1;
            return Err(StreamError::storage_unavailable("The test lost the answer"));
        }
        answer
    }
}

fn id(stream: Option<&StreamRef>) -> String {
    stream.map(|stream| stream.id.clone()).unwrap_or_default()
}

fn not_found() -> StreamError {
    StreamError::new(Reason::NotFound, "Nobody created the Stream")
}

fn fenced() -> StreamError {
    StreamError::new(Reason::Fenced, "A higher epoch of the Producer wrote")
}

impl State {
    fn append(&mut self, request: AppendRecordsRequest) -> StreamResult<AppendRecordsResponse> {
        let stream = (self.streams.get_mut(&id(request.r#ref.as_ref()))).ok_or_else(not_found)?;
        let producer = request.producer.expect("The runtime checks the Producer");
        let epoch = epoch_key(producer.kind.as_ref());
        if let Some(last) = stream.producers.get(&producer.id) {
            if epoch == last.epoch && request.sequence != 0 {
                if request.sequence == last.sequence && request.records == last.records {
                    let (first_offset, last_offset) = last.offsets.clone();
                    return Ok(AppendRecordsResponse {
                        first_offset,
                        last_offset,
                    });
                }
                if request.sequence == last.sequence {
                    return Err(StreamError::new(Reason::SeqDivergent, "Other content"));
                }
                if request.sequence < last.sequence {
                    return Err(StreamError::new(Reason::SequenceTooLow, "A lower sequence"));
                }
            }
            if epoch < last.epoch {
                return Err(fenced());
            }
        }
        if stream.closed {
            return Err(StreamError::new(Reason::Closed, "The Stream is closed"));
        }
        let first = stream.records.len();
        for record in &request.records {
            let offset = stream.offset(stream.records.len());
            stream.records.push(StreamRecord {
                producer: Some(producer.clone()),
                body: record.body.clone(),
                offset,
            });
        }
        let offsets = match stream.records.len() {
            last if last == first => (vec![], vec![]),
            last => (stream.offset(first), stream.offset(last - 1)),
        };
        let written = Written {
            epoch,
            sequence: request.sequence,
            records: request.records,
            offsets: offsets.clone(),
        };
        stream.producers.insert(producer.id, written);
        Ok(AppendRecordsResponse {
            first_offset: offsets.0,
            last_offset: offsets.1,
        })
    }

    fn close(&mut self, request: &CloseStreamRequest) -> StreamResult<CloseStreamResponse> {
        let stream = (self.streams.get_mut(&id(request.r#ref.as_ref()))).ok_or_else(not_found)?;
        let producer = request.producer.as_ref().expect("The runtime checks it");
        let epoch = epoch_key(producer.kind.as_ref());
        if stream.closed {
            return Ok(CloseStreamResponse {});
        }
        if (stream.producers.get(&producer.id)).is_some_and(|last| epoch < last.epoch) {
            return Err(fenced());
        }
        stream.closed = true;
        Ok(CloseStreamResponse {})
    }
}

#[async_trait::async_trait]
impl StreamStore for MemoryStore {
    async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        let mut state = self.state.lock();
        let base = (state.streams.len() as u64 + 1) << 20;
        let created = !state.streams.contains_key(&id(request.stream_ref.as_ref()));
        state
            .streams
            .entry(id(request.stream_ref.as_ref()))
            .or_insert_with(|| Stream {
                base,
                records: vec![],
                closed: false,
                producers: HashMap::new(),
            });
        Ok(CreateStreamResponse {
            r#ref: request.stream_ref,
            created,
        })
    }

    async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        self.arrive(Op::Append, Request::Append(request.clone()))
            .await?;
        let appended = self.state.lock().append(request);
        self.answer(Op::Append, appended)
    }

    async fn read_records(&self, request: ReadRecordsRequest) -> StreamResult<ReadRecordsResponse> {
        let state = self.state.lock();
        let stream = (state.streams.get(&id(request.r#ref.as_ref()))).ok_or_else(not_found)?;
        let after: Vec<&StreamRecord> = (stream.records.iter())
            .filter(|record| record.offset > request.from_offset)
            .collect();
        let records: Vec<StreamRecord> = (after.iter())
            .take(request.max_records.max(1) as usize)
            .map(|record| (*record).clone())
            .collect();
        Ok(ReadRecordsResponse {
            done: stream.closed && records.len() == after.len(),
            last_offset: records
                .last()
                .map_or(request.from_offset, |r| r.offset.clone()),
            records,
        })
    }

    async fn close_stream(&self, request: CloseStreamRequest) -> StreamResult<CloseStreamResponse> {
        self.arrive(Op::Close, Request::Close(request.clone()))
            .await?;
        let closed = self.state.lock().close(&request);
        self.answer(Op::Close, closed)
    }

    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        let state = self.state.lock();
        let stream = (state.streams.get(&id(request.r#ref.as_ref()))).ok_or_else(not_found)?;
        let status = if stream.closed {
            StreamStatus::Closed
        } else {
            StreamStatus::Open
        };
        Ok(DescribeStreamResponse {
            status: status as i32,
            last_offset: stream
                .records
                .last()
                .map(|r| r.offset.clone())
                .unwrap_or_default(),
        })
    }
}
