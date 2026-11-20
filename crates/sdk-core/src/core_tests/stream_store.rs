//! The one Stream Store of Core's tests. It does what [`StreamStore`] requires, and each test sets
//! its faults.

use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use temporalio_streams::{
    AnyError, AnyReason, AppendError, AppendReason, CloseError, CloseReason, CreateError,
    DescribeError, DescribeReason, ReadError, ReadReason, Reason, StreamError, StreamStore,
    epoch::{EPOCH_KEY_LEN, epoch_key},
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest,
        CloseStreamResponse, CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest,
        DescribeStreamResponse, ReadRecordsRequest, ReadRecordsResponse, StreamRecord, StreamRef,
        StreamStatus,
    },
};
use tokio::sync::watch;

/// A call of the Store, which a fault can hit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Op {
    Append,
    Close,
    Read,
}

/// A call that the Store got, in the order that it came.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Request {
    Append(AppendRecordsRequest),
    Close(CloseStreamRequest),
    Read(ReadRecordsRequest),
}

/// The last batch of a Producer, with its epoch.
struct Written {
    epoch: [u8; EPOCH_KEY_LEN],
    sequence: i64,
    records: Vec<AppendRecord>,
    offsets: (Vec<u8>, Vec<u8>),
}

struct Stream {
    /// Each Stream has its own offsets, 16 apart, so an offset doesn't tell how many Records came
    /// before it.
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
    failures: HashMap<Op, (AnyError, usize)>,
    lost_answers: HashMap<Op, usize>,
}

/// A Store in memory. Like the Redis Store, it fences a lower epoch of a Producer, writes the last
/// batch of an epoch only once, and refuses a lower sequence within an epoch. A read with no
/// Record to give waits for one, or for a close, up to its `wait`.
#[derive(Default)]
pub(super) struct MemoryStore {
    state: Mutex<State>,
    released: watch::Sender<u64>,
    /// Counts the writes, so that a read that waits wakes at each one.
    written: watch::Sender<u64>,
}

impl MemoryStore {
    /// Makes each call of `op` wait for [`Self::release`].
    pub(super) fn hold(&self, op: Op) {
        self.state.lock().held.insert(op);
    }

    /// Lets each held call go on, and ends the holds.
    pub(super) fn release(&self) {
        self.state.lock().held.clear();
        self.released.send_modify(|count| *count += 1);
    }

    /// Makes the next `times` calls of `op` fail with `error` before they write.
    pub(super) fn fail(&self, op: Op, error: AnyError, times: usize) {
        self.state.lock().failures.insert(op, (error, times));
    }

    /// Makes the next `times` calls of `op` write, and then fail with `StorageUnavailable`.
    pub(super) fn lose_answers(&self, op: Op, times: usize) {
        self.state.lock().lost_answers.insert(op, times);
    }

    pub(super) fn requests(&self) -> Vec<Request> {
        self.state.lock().requests.clone()
    }

    /// Returns the Records of the Stream `id`, in order.
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

    /// Removes the Stream `id`, as its Retention does, so that a replay finds its Records gone.
    pub(super) fn forget(&self, id: &str) {
        self.state.lock().streams.remove(id);
    }

    /// Returns the reads that the Store got.
    pub(super) fn reads(&self) -> Vec<ReadRecordsRequest> {
        (self.requests().into_iter())
            .filter_map(|request| match request {
                Request::Read(read) => Some(read),
                _ => None,
            })
            .collect()
    }

    /// Takes the request, and then holds or fails it as the faults say.
    async fn arrive(&self, op: Op, request: Request) -> Result<(), AnyError> {
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

    fn answer<T, R>(&self, op: Op, answer: Result<T, StreamError<R>>) -> Result<T, StreamError<R>>
    where
        R: From<AnyReason>,
    {
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

fn not_found<R>(reason: R) -> StreamError<Reason<R>> {
    StreamError::new(Reason::Op(reason), "Nobody created the Stream")
}

fn fenced<R>(reason: R) -> StreamError<Reason<R>> {
    StreamError::new(Reason::Op(reason), "A higher epoch of the Producer wrote")
}

impl State {
    fn append(
        &mut self,
        request: AppendRecordsRequest,
    ) -> Result<AppendRecordsResponse, AppendError> {
        let stream = self.streams.get_mut(&id(request.r#ref.as_ref()));
        let stream = stream.ok_or_else(|| not_found(AppendReason::NotFound))?;
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
                    let divergent = Reason::Op(AppendReason::SeqDivergent);
                    return Err(StreamError::new(divergent, "Other content"));
                }
                if request.sequence < last.sequence {
                    let too_low = Reason::Op(AppendReason::SequenceTooLow);
                    return Err(StreamError::new(too_low, "A lower sequence"));
                }
            }
            if epoch < last.epoch {
                return Err(fenced(AppendReason::Fenced));
            }
        }
        if stream.closed {
            let closed = Reason::Op(AppendReason::Closed);
            return Err(StreamError::new(closed, "The Stream is closed"));
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

    fn close(&mut self, request: &CloseStreamRequest) -> Result<CloseStreamResponse, CloseError> {
        let stream = self.streams.get_mut(&id(request.r#ref.as_ref()));
        let stream = stream.ok_or_else(|| not_found(CloseReason::NotFound))?;
        let producer = request.producer.as_ref().expect("The runtime checks it");
        let epoch = epoch_key(producer.kind.as_ref());
        if stream.closed {
            return Ok(CloseStreamResponse {});
        }
        if (stream.producers.get(&producer.id)).is_some_and(|last| epoch < last.epoch) {
            return Err(fenced(CloseReason::Fenced));
        }
        stream.closed = true;
        Ok(CloseStreamResponse {})
    }
}

impl StreamStore for MemoryStore {
    fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> BoxFuture<'_, Result<CreateStreamResponse, CreateError>> {
        Box::pin(async move {
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
        })
    }

    fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> BoxFuture<'_, Result<AppendRecordsResponse, AppendError>> {
        Box::pin(async move {
            self.arrive(Op::Append, Request::Append(request.clone()))
                .await?;
            let appended = self.state.lock().append(request);
            self.written.send_modify(|count| *count += 1);
            self.answer(Op::Append, appended)
        })
    }

    fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> BoxFuture<'_, Result<ReadRecordsResponse, ReadError>> {
        Box::pin(async move {
            self.arrive(Op::Read, Request::Read(request.clone()))
                .await?;
            let wait = (request.wait).map_or(Duration::ZERO, |wait| wait.try_into().unwrap());
            let until = tokio::time::Instant::now() + wait;
            let mut written = self.written.subscribe();
            loop {
                let page = {
                    let state = self.state.lock();
                    let stream = state.streams.get(&id(request.r#ref.as_ref()));
                    let stream = stream.ok_or_else(|| not_found(ReadReason::NotFound))?;
                    let after: Vec<&StreamRecord> = (stream.records.iter())
                        .filter(|record| record.offset > request.from_offset)
                        .collect();
                    let records: Vec<StreamRecord> = (after.iter())
                        .take(request.max_records.max(1) as usize)
                        .map(|record| (*record).clone())
                        .collect();
                    ReadRecordsResponse {
                        done: stream.closed && records.len() == after.len(),
                        last_offset: (records.last())
                            .map_or(request.from_offset.clone(), |r| r.offset.clone()),
                        records,
                    }
                };
                if !page.records.is_empty() || page.done {
                    return Ok(page);
                }
                let changed = tokio::time::timeout_at(until, written.changed()).await;
                if changed.is_err() {
                    return Ok(page);
                }
            }
        })
    }

    fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> BoxFuture<'_, Result<CloseStreamResponse, CloseError>> {
        Box::pin(async move {
            self.arrive(Op::Close, Request::Close(request.clone()))
                .await?;
            let closed = self.state.lock().close(&request);
            self.written.send_modify(|count| *count += 1);
            self.answer(Op::Close, closed)
        })
    }

    fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> BoxFuture<'_, Result<DescribeStreamResponse, DescribeError>> {
        Box::pin(async move {
            let state = self.state.lock();
            let stream = state.streams.get(&id(request.r#ref.as_ref()));
            let stream = stream.ok_or_else(|| not_found(DescribeReason::NotFound))?;
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
        })
    }
}
