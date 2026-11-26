//! The Stream Store of Core's tests. It implements [`StreamStore`], and each test sets its
//! faults.

use futures_util::future::BoxFuture;
use parking_lot::{Condvar, Mutex};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};
use temporalio_streams::{
    AnyError, AnyReason, AppendError, AppendReason, CreateError, DescribeError, ReadError,
    ReadReason, StreamError, StreamStore,
    attempt::{ATTEMPT_KEY_LEN, attempt_key, producer_key},
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, CreateStreamRequest, CreateStreamResponse,
        DescribeStreamRequest, DescribeStreamResponse, ReadRecordsRequest, ReadRecordsResponse,
        StreamRecord,
    },
};
use tokio::sync::watch;

/// A kind of Store call that a test can set a fault on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Op {
    Append,
    Read,
}

/// A call that the Store got, in the order that it arrived.
#[derive(Clone, Debug, PartialEq)]
pub enum Request {
    /// An append, with the request id of its call.
    Append(AppendRecordsRequest, String),
    Read(ReadRecordsRequest),
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
    /// The held calls of each kind that may go on, one for each.
    passes: HashMap<Op, usize>,
    held_streams: HashSet<String>,
    failures: HashMap<Op, (AnyError, usize)>,
    lost_answers: HashMap<Op, usize>,
    /// Whether each read that arrives waits at the gate.
    gating: bool,
    /// The reads that wait at the gate, by their place in `requests`.
    gated: HashSet<usize>,
    /// The number of reads that the gate let go.
    let_go: usize,
    /// Whether each read keeps its thread until [`MemoryStore::unblock`].
    blocking: bool,
}

/// The number of the last batches of each Producer of a Stream that the Store remembers.
const WINDOW: usize = 4;

/// A Store in memory. It fences an earlier attempt of a Producer, and writes nothing for a resend
/// of one of the last `WINDOW` batches of a Producer of a Stream. A read with no Record to give
/// waits for one up to its `wait`.
#[derive(Default)]
pub struct MemoryStore {
    state: Mutex<State>,
    released: watch::Sender<u64>,
    /// Counts the writes, so that a read that waits wakes at each one.
    written: watch::Sender<u64>,
    unblocked: Condvar,
}

impl MemoryStore {
    /// Makes each call of `op` wait for [`Self::release`].
    pub fn hold(&self, op: Op) {
        self.state.lock().held.insert(op);
    }

    /// Lets `count` held calls of `op` go on, each that arrives while the hold lasts. A call that
    /// stops while it waits takes none.
    pub fn pass(&self, op: Op, count: usize) {
        *self.state.lock().passes.entry(op).or_default() += count;
        self.released.send_modify(|count| *count += 1);
    }

    /// Lets each held call go on, and ends the holds.
    pub fn release(&self) {
        let mut state = self.state.lock();
        state.held.clear();
        state.passes.clear();
        drop(state);
        self.released.send_modify(|count| *count += 1);
    }

    /// Makes each call of the Stream `id` wait for [`Self::release_stream`].
    pub fn hold_stream(&self, id: &str) {
        self.state.lock().held_streams.insert(id.to_string());
    }

    /// Lets each held call of the Stream `id` go on.
    pub fn release_stream(&self, id: &str) {
        self.state.lock().held_streams.remove(id);
        self.released.send_modify(|count| *count += 1);
    }

    /// Makes the next `times` calls of `op` fail with `error` before they write.
    pub fn fail(&self, op: Op, error: AnyError, times: usize) {
        self.state.lock().failures.insert(op, (error, times));
    }

    /// Returns the number of calls of `op` that [`Self::fail`] still fails.
    pub fn failures_left(&self, op: Op) -> usize {
        self.state
            .lock()
            .failures
            .get(&op)
            .map_or(0, |(_, times)| *times)
    }

    /// Makes the next `times` calls of `op` write, and then fail with `StoreUnavailable`.
    pub fn lose_answers(&self, op: Op, times: usize) {
        self.state.lock().lost_answers.insert(op, times);
    }

    /// Makes each read that arrives while `on` is true wait for [`Self::let_go`], so that a test
    /// decides the order in which reads of one Stream end.
    pub fn gate_reads(&self, on: bool) {
        self.state.lock().gating = on;
    }

    /// Lets the reads at the places `reads` of [`Self::requests`] pass the gate.
    pub fn let_go(&self, reads: std::ops::Range<usize>) {
        (self.state.lock().gated).retain(|at| !reads.contains(at));
        self.released.send_modify(|count| *count += 1);
    }

    /// Makes each read keep its thread until [`Self::unblock`], as a Store with a client that
    /// blocks does.
    pub fn block_reads(&self) {
        self.state.lock().blocking = true;
    }

    pub fn unblock(&self) {
        self.state.lock().blocking = false;
        self.unblocked.notify_all();
    }

    /// Returns the number of reads that passed the gate.
    pub fn reads_let_go(&self) -> usize {
        self.state.lock().let_go
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

    /// Removes the Stream `id`, as its Retention does, so that a replay finds its Records gone.
    pub fn forget(&self, id: &str) {
        self.state.lock().streams.remove(id);
    }

    /// Returns the reads that the Store got.
    pub fn reads(&self) -> Vec<ReadRecordsRequest> {
        (self.requests().into_iter())
            .filter_map(|request| match request {
                Request::Read(read) => Some(read),
                Request::Append(..) => None,
            })
            .collect()
    }

    /// Takes the request, and then holds or fails it as the faults say.
    async fn arrive(&self, op: Op, request: Request) -> Result<(), AnyError> {
        let id = match &request {
            Request::Append(append, _) => append.stream_id.clone(),
            Request::Read(read) => read.stream_id.clone(),
        };
        let mut released = self.released.subscribe();
        let (held, gated) = {
            let mut state = self.state.lock();
            state.requests.push(request);
            let at = state.requests.len() - 1;
            let gated = (state.gating && op == Op::Read).then_some(at);
            state.gated.extend(gated);
            (state.held.contains(&op), gated)
        };
        if held {
            loop {
                {
                    let mut state = self.state.lock();
                    if !state.held.contains(&op) {
                        break;
                    }
                    let passes = state.passes.entry(op).or_default();
                    if *passes > 0 {
                        *passes -= 1;
                        break;
                    }
                }
                let _ = released.changed().await;
            }
        }
        if let Some(at) = gated {
            while self.state.lock().gated.contains(&at) {
                let _ = released.changed().await;
            }
            self.state.lock().let_go += 1;
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
const FENCED: &str = "A later attempt of the Producer wrote";

impl State {
    fn append(
        &mut self,
        request: AppendRecordsRequest,
        request_id: String,
    ) -> Result<AppendRecordsResponse, AppendError> {
        let stream = self.streams.get_mut(&request.stream_id);
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
        if batches.len() > WINDOW {
            batches.pop_front();
        }
        Ok(AppendRecordsResponse {
            first_offset: offsets.0,
            last_offset: offsets.1,
        })
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
            let created = !state.streams.contains_key(&request.stream_id);
            state
                .streams
                .entry(request.stream_id)
                .or_insert_with(|| Stream {
                    base,
                    records: vec![],
                    producers: HashMap::new(),
                });
            Ok(CreateStreamResponse { created })
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
            let appended = self.state.lock().append(request, request_id);
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
            {
                let mut state = self.state.lock();
                while state.blocking {
                    self.unblocked.wait(&mut state);
                }
            }
            let wait = (request.wait).map_or(Duration::ZERO, |wait| wait.try_into().unwrap());
            let until = tokio::time::Instant::now() + wait;
            let mut written = self.written.subscribe();
            loop {
                let page = {
                    let state = self.state.lock();
                    let stream = state.streams.get(&request.stream_id);
                    let reason = ReadReason::NotFound;
                    let stream = stream.ok_or_else(|| ReadError::operation(reason, NOT_CREATED))?;
                    let records: Vec<StreamRecord> = (stream.records.iter())
                        .filter(|record| record.offset > request.from_offset)
                        .take(request.max_records.max(1) as usize)
                        .cloned()
                        .collect();
                    ReadRecordsResponse {
                        last_offset: (records.last())
                            .map_or(request.from_offset.clone(), |r| r.offset.clone()),
                        records,
                    }
                };
                if !page.records.is_empty() {
                    return Ok(page);
                }
                let changed = tokio::time::timeout_at(until, written.changed()).await;
                if changed.is_err() {
                    return Ok(page);
                }
            }
        })
    }

    fn describe_stream(
        &self,
        _: DescribeStreamRequest,
    ) -> BoxFuture<'_, Result<DescribeStreamResponse, DescribeError>> {
        unimplemented!("No test describes a Stream of this Store")
    }
}
