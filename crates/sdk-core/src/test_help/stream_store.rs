//! The one Stream Store of Core's tests. It does what [`StreamStore`] requires, and each test sets
//! its faults.

use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::atomic::{AtomicUsize, Ordering},
};
use temporalio_streams::{
    AnyError, AnyReason, AppendError, AppendReason, CloseError, CloseReason, CreateError,
    DescribeError, DescribeReason, ReadError, ReadReason, StreamError, StreamStore,
    attempt::{ATTEMPT_KEY_LEN, attempt_key, producer_key},
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CloseStreamRequest, CloseStreamResponse,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        ReadRecordsRequest, ReadRecordsResponse, StreamRecord, StreamRef, StreamStatus,
    },
};
use tokio::sync::watch;

/// A write of the Store, which a fault can hit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Op {
    Append,
    Close,
}

/// A write that the Store got, in the order that it came.
#[derive(Clone, Debug, PartialEq)]
pub enum Request {
    /// An append, with the request id of its call.
    Append(AppendRecordsRequest, String),
    Close(CloseStreamRequest),
}

/// A batch that the Store remembers, with the key of its attempt.
struct Written {
    key: [u8; ATTEMPT_KEY_LEN],
    request_id: String,
    offsets: (Vec<u8>, Vec<u8>),
}

struct Stream {
    /// Each Stream has its own offsets, 16 apart, so an offset doesn't tell how many Records came
    /// before it.
    base: u64,
    records: Vec<StreamRecord>,
    closed: bool,
    /// The last batches of each Producer, by its key, oldest first.
    producers: HashMap<Vec<u8>, VecDeque<Written>>,
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
    held_streams: HashSet<String>,
    failures: HashMap<Op, (AnyError, usize)>,
    panics: HashSet<Op>,
    lost_answers: HashMap<Op, usize>,
    delays: usize,
    late: Vec<(AppendRecordsRequest, String)>,
}

/// A Store in memory. It fences an earlier attempt of a Producer, and writes nothing for a resend
/// of one of the last `window` batches of a Producer of a Stream.
pub struct MemoryStore {
    state: Mutex<State>,
    released: watch::Sender<u64>,
    /// The number of the last batches of each Producer of a Stream that the Store remembers.
    pub window: AtomicUsize,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            released: watch::Sender::default(),
            window: AtomicUsize::new(4),
        }
    }
}

impl MemoryStore {
    /// Makes each call of `op` wait for [`Self::release`].
    pub fn hold(&self, op: Op) {
        self.state.lock().held.insert(op);
    }

    /// Lets each held call go on, and ends the holds.
    pub fn release(&self) {
        self.state.lock().held.clear();
        self.released.send_modify(|count| *count += 1);
    }

    /// Makes each write to the Stream `id` wait for [`Self::release_stream`].
    pub fn hold_stream(&self, id: &str) {
        self.state.lock().held_streams.insert(id.to_string());
    }

    /// Lets each held write to the Stream `id` go on.
    pub fn release_stream(&self, id: &str) {
        self.state.lock().held_streams.remove(id);
        self.released.send_modify(|count| *count += 1);
    }

    /// Makes the next `times` calls of `op` fail with `error` before they write.
    pub fn fail(&self, op: Op, error: AnyError, times: usize) {
        self.state.lock().failures.insert(op, (error, times));
    }

    /// Makes each call of `op` panic, as a bug in the code of a Store does.
    pub fn panic(&self, op: Op) {
        self.state.lock().panics.insert(op);
    }

    /// Makes the next `times` calls of `op` write, and then fail with `StoreUnavailable`.
    pub fn lose_answers(&self, op: Op, times: usize) {
        self.state.lock().lost_answers.insert(op, times);
    }

    /// Makes the next `times` appends get no answer, and keeps them for [`Self::write_late`].
    pub fn delay_appends(&self, times: usize) {
        self.state.lock().delays = times;
    }

    /// Writes the delayed append with `request_id`, as a Store does that finishes a write after
    /// its caller gave up.
    pub fn write_late(&self, request_id: &str) {
        let mut state = self.state.lock();
        let at = (state.late.iter()).position(|(_, late_id)| late_id == request_id);
        let (request, request_id) = state.late.remove(at.expect("The append was delayed"));
        let _ = state.append(request, request_id, self.window.load(Ordering::Relaxed));
    }

    pub fn requests(&self) -> Vec<Request> {
        self.state.lock().requests.clone()
    }

    /// Returns the Records of the Stream `id`, in order.
    pub fn records(&self, id: &str) -> Vec<StreamRecord> {
        let state = self.state.lock();
        state
            .streams
            .get(id)
            .map(|s| s.records.clone())
            .unwrap_or_default()
    }

    pub fn is_closed(&self, id: &str) -> bool {
        let state = self.state.lock();
        state.streams.get(id).is_some_and(|stream| stream.closed)
    }

    /// Takes the request, and then holds or fails it as the faults say.
    async fn arrive(&self, op: Op, request: Request) -> Result<(), AnyError> {
        let id = match &request {
            Request::Append(append, _) => stream_id(append.r#ref.as_ref()),
            Request::Close(close) => stream_id(close.r#ref.as_ref()),
        }
        .to_string();
        let mut released = self.released.subscribe();
        let held = {
            let mut state = self.state.lock();
            state.requests.push(request);
            if state.panics.contains(&op) {
                panic!("The Store has a bug");
            }
            state.held.contains(&op)
        };
        if held {
            let _ = released.changed().await;
        }
        while self.state.lock().held_streams.contains(&id) {
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
            return Err(StreamError::store_unavailable("The test lost the answer"));
        }
        answer
    }
}

const NOT_CREATED: &str = "Nobody created the Stream";

/// Returns the Stream id of the ref of a request.
fn stream_id(stream_ref: Option<&StreamRef>) -> &str {
    &stream_ref.expect("A request has a ref").id
}
const FENCED: &str = "A later attempt of the Producer wrote";

impl State {
    fn append(
        &mut self,
        request: AppendRecordsRequest,
        request_id: String,
        window: usize,
    ) -> Result<AppendRecordsResponse, AppendError> {
        let stream = self.streams.get_mut(stream_id(request.r#ref.as_ref()));
        let stream =
            stream.ok_or_else(|| AppendError::operation(AppendReason::NotFound, NOT_CREATED))?;
        let producer = request.producer.expect("The runtime checks the Producer");
        let id = producer_key(&producer).expect("The runtime checks the kind");
        let key = attempt_key(producer.kind.as_ref());
        if let Some(batches) = stream.producers.get(&id) {
            let resent = batches.iter().find(|batch| {
                key == batch.key && !request_id.is_empty() && request_id == batch.request_id
            });
            if let Some(batch) = resent {
                let (first_offset, last_offset) = batch.offsets.clone();
                return Ok(AppendRecordsResponse {
                    first_offset,
                    last_offset,
                });
            }
            if batches.back().is_some_and(|last| key < last.key) {
                return Err(AppendError::operation(AppendReason::Fenced, FENCED));
            }
        }
        if stream.closed {
            let reason = AppendReason::Closed;
            return Err(AppendError::operation(reason, "The Stream is closed"));
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
        let offsets = (
            stream.offset(first),
            stream.offset(stream.records.len() - 1),
        );
        let written = Written {
            key,
            request_id,
            offsets: offsets.clone(),
        };
        let batches = stream.producers.entry(id).or_default();
        batches.push_back(written);
        if batches.len() > window {
            batches.pop_front();
        }
        Ok(AppendRecordsResponse {
            first_offset: offsets.0,
            last_offset: offsets.1,
        })
    }

    fn close(&mut self, request: &CloseStreamRequest) -> Result<CloseStreamResponse, CloseError> {
        let stream = self.streams.get_mut(stream_id(request.r#ref.as_ref()));
        let stream =
            stream.ok_or_else(|| CloseError::operation(CloseReason::NotFound, NOT_CREATED))?;
        let producer = request.producer.as_ref().expect("The runtime checks it");
        let id = producer_key(producer).expect("The runtime checks the kind");
        let key = attempt_key(producer.kind.as_ref());
        if stream.closed {
            return Ok(CloseStreamResponse {});
        }
        let last = stream.producers.get(&id).and_then(VecDeque::back);
        if last.is_some_and(|last| key < last.key) {
            return Err(CloseError::operation(CloseReason::Fenced, FENCED));
        }
        stream.closed = true;
        Ok(CloseStreamResponse {})
    }
}

impl StreamStore for MemoryStore {
    fn create_stream(
        &self,
        request: CreateStreamRequest,
        _: String,
    ) -> BoxFuture<'_, Result<CreateStreamResponse, CreateError>> {
        Box::pin(async move {
            let mut state = self.state.lock();
            let base = (state.streams.len() as u64 + 1) << 20;
            let id = stream_id(request.stream_ref.as_ref()).to_string();
            let created = !state.streams.contains_key(&id);
            state.streams.entry(id).or_insert_with(|| Stream {
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
        request_id: String,
    ) -> BoxFuture<'_, Result<AppendRecordsResponse, AppendError>> {
        Box::pin(async move {
            let arrived = Request::Append(request.clone(), request_id.clone());
            self.arrive(Op::Append, arrived).await?;
            let delayed = {
                let mut state = self.state.lock();
                let delayed = state.delays > 0;
                if delayed {
                    state.delays -= 1;
                    state.late.push((request.clone(), request_id.clone()));
                }
                delayed
            };
            if delayed {
                return std::future::pending().await;
            }
            let window = self.window.load(Ordering::Relaxed);
            let appended = self.state.lock().append(request, request_id, window);
            self.answer(Op::Append, appended)
        })
    }

    fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> BoxFuture<'_, Result<ReadRecordsResponse, ReadError>> {
        Box::pin(async move {
            let state = self.state.lock();
            let stream = state.streams.get(stream_id(request.r#ref.as_ref()));
            let stream =
                stream.ok_or_else(|| ReadError::operation(ReadReason::NotFound, NOT_CREATED))?;
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
            self.answer(Op::Close, closed)
        })
    }

    fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> BoxFuture<'_, Result<DescribeStreamResponse, DescribeError>> {
        Box::pin(async move {
            let state = self.state.lock();
            let stream = state.streams.get(stream_id(request.r#ref.as_ref()));
            let reason = DescribeReason::NotFound;
            let stream = stream.ok_or_else(|| DescribeError::operation(reason, NOT_CREATED))?;
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
