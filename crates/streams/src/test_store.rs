#[cfg(feature = "workflow")]
use crate::proto::StreamRecord;
use crate::{
    StreamError, StreamResult, StreamRuntime, StreamStore,
    proto::{
        ActivityEpoch, ActivityProducer, AppendRecordsRequest, AppendRecordsResponse,
        ClientProducer, CloseStreamRequest, CloseStreamResponse, CreateStreamRequest,
        CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse, LocalActivityEpoch,
        LocalActivityProducer, Producer, ReadRecordsRequest, ReadRecordsResponse, WorkflowProducer,
        WorkflowTaskEpoch, producer::Kind,
    },
    record::BatchLimits,
};
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use prost_types::Timestamp;
#[cfg(feature = "workflow")]
use rand::RngExt;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Request {
    Create(CreateStreamRequest),
    Append(AppendRecordsRequest),
    Read(ReadRecordsRequest),
    Close(CloseStreamRequest),
    Describe(DescribeStreamRequest),
}

#[derive(Default)]
pub(crate) struct Answers {
    pub(crate) created: CreateStreamResponse,
    pub(crate) appended: AppendRecordsResponse,
    pub(crate) page: ReadRecordsResponse,
    pub(crate) described: DescribeStreamResponse,
}

#[derive(Default)]
pub(crate) struct Scripted {
    pub(crate) answers: Mutex<Answers>,
    requests: Mutex<Vec<Request>>,
    /// While set, appends and closes get no answer.
    pub(crate) silent: AtomicBool,
    /// The number of appends and closes, from now on, that fail with `StorageUnavailable`.
    pub(crate) unavailable: Mutex<usize>,
}

impl Scripted {
    pub(crate) fn requests(&self) -> Vec<Request> {
        self.requests.lock().clone()
    }

    async fn write(&self) -> StreamResult<()> {
        if self.silent.load(Ordering::Acquire) {
            std::future::pending::<()>().await;
        }
        let mut unavailable = self.unavailable.lock();
        if *unavailable > 0 {
            *unavailable -= 1;
            return Err(StreamError::storage_unavailable("The Store is down"));
        }
        Ok(())
    }
}

impl StreamStore for Scripted {
    fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> BoxFuture<'_, StreamResult<CreateStreamResponse>> {
        Box::pin(async move {
            self.requests.lock().push(Request::Create(request));
            Ok(self.answers.lock().created.clone())
        })
    }

    fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> BoxFuture<'_, StreamResult<AppendRecordsResponse>> {
        Box::pin(async move {
            self.requests.lock().push(Request::Append(request));
            self.write().await?;
            Ok(self.answers.lock().appended.clone())
        })
    }

    fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> BoxFuture<'_, StreamResult<ReadRecordsResponse>> {
        Box::pin(async move {
            self.requests.lock().push(Request::Read(request));
            Ok(self.answers.lock().page.clone())
        })
    }

    fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> BoxFuture<'_, StreamResult<CloseStreamResponse>> {
        Box::pin(async move {
            self.requests.lock().push(Request::Close(request));
            self.write().await?;
            Ok(CloseStreamResponse {})
        })
    }

    fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> BoxFuture<'_, StreamResult<DescribeStreamResponse>> {
        Box::pin(async move {
            self.requests.lock().push(Request::Describe(request));
            Ok(self.answers.lock().described.clone())
        })
    }
}

/// One entry of a model Stream, which is a Record or an entry that a read passes but that isn't
/// one.
#[cfg(feature = "workflow")]
#[derive(Clone, Debug)]
pub(crate) struct Entry {
    pub(crate) offset: Vec<u8>,
    /// When the entry comes, after the Store starts.
    pub(crate) at: std::time::Duration,
    pub(crate) record: Option<StreamRecord>,
}

/// What a model Store does with the next read.
#[cfg(feature = "workflow")]
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Fault {
    /// The read fails with this error.
    Fails(StreamError),
    /// The Store gives no answer.
    Silent,
}

/// A Store with one Stream whose reads get pages of any size, entries that aren't Records, and
/// faults at random. A read with no entry to give waits for one, up to its `wait`.
#[cfg(feature = "workflow")]
pub(crate) struct ModelStore {
    pub(crate) model: Mutex<Model>,
    start: tokio::time::Instant,
}

#[cfg(feature = "workflow")]
pub(crate) struct Model {
    pub(crate) rng: rand::rngs::StdRng,
    pub(crate) entries: Vec<Entry>,
    /// The Stream is closed once its last entry came.
    pub(crate) closed: bool,
    /// Returns the fault of each read, if it has one. The test checks each request in it too.
    #[allow(clippy::type_complexity)]
    pub(crate) fault:
        Box<dyn FnMut(&mut rand::rngs::StdRng, &ReadRecordsRequest) -> Option<Fault> + Send>,
    /// The reads that run now, and the most that ran at the same time.
    pub(crate) reading: (usize, usize),
    /// The offset that an answer with no Records moved to, where the next read must start.
    pub(crate) moved_to: Option<Vec<u8>>,
    pub(crate) violations: Vec<String>,
}

#[cfg(feature = "workflow")]
impl ModelStore {
    pub(crate) fn new(model: Model) -> Self {
        Self {
            model: Mutex::new(model),
            start: tokio::time::Instant::now(),
        }
    }

    /// Returns the Records of the Stream after `offset`.
    pub(crate) fn records_after(&self, offset: &[u8]) -> Vec<StreamRecord> {
        self.came_after(offset, None)
    }

    /// Returns the Records of the Stream after `offset` that came by now.
    pub(crate) fn records_came_after(&self, offset: &[u8]) -> Vec<StreamRecord> {
        self.came_after(offset, Some(tokio::time::Instant::now()))
    }

    fn came_after(&self, offset: &[u8], by: Option<tokio::time::Instant>) -> Vec<StreamRecord> {
        let model = self.model.lock();
        (model.entries.iter())
            .filter(|entry| entry.offset.as_slice() > offset)
            .filter(|entry| by.is_none_or(|by| self.start + entry.at <= by))
            .filter_map(|entry| entry.record.clone())
            .collect()
    }

    async fn read(
        &self,
        request: ReadRecordsRequest,
        reading: &mut Reading<'_>,
    ) -> StreamResult<ReadRecordsResponse> {
        let fault = {
            let mut model = self.model.lock();
            let Model {
                rng,
                fault,
                moved_to,
                violations,
                ..
            } = &mut *model;
            if moved_to
                .as_ref()
                .is_some_and(|to| *to != request.from_offset)
            {
                violations.push("A read didn't start where an empty answer moved to".to_string());
            }
            fault(rng, &request)
        };
        match fault {
            Some(Fault::Fails(error)) => return Err(error),
            Some(Fault::Silent) => {
                reading.waits = false;
                std::future::pending().await
            }
            None => {}
        }
        let wait = request.wait.map_or(std::time::Duration::ZERO, |wait| {
            std::time::Duration::try_from(wait).expect("The runtime checks the wait")
        });
        let until = tokio::time::Instant::now() + wait;
        let after = |entry: &&Entry| entry.offset.as_slice() > request.from_offset.as_slice();
        // The read waits until an entry after its offset comes, or its wait ends.
        loop {
            let next = (self.model.lock().entries.iter().find(after)).map(|entry| entry.at);
            match next.map(|at| self.start + at) {
                Some(at) if at <= tokio::time::Instant::now() => break,
                Some(at) if at < until => tokio::time::sleep_until(at).await,
                _ => {
                    tokio::time::sleep_until(until).await;
                    break;
                }
            }
        }
        let mut model = self.model.lock();
        let Model {
            rng,
            entries,
            closed,
            moved_to,
            ..
        } = &mut *model;
        let now = tokio::time::Instant::now();
        let later: Vec<&Entry> = entries.iter().filter(after).collect();
        let came = (later.iter())
            .filter(|entry| self.start + entry.at <= now)
            .count();
        // A page of any number of the entries that came, with at most `max_records` Records.
        let mut take = if came == 0 {
            0
        } else {
            rng.random_range(1..=came)
        };
        let max_records = request.max_records.max(1) as usize;
        if let Some((at, _)) = (later.iter().enumerate())
            .filter(|(_, entry)| entry.record.is_some())
            .nth(max_records)
        {
            take = take.min(at);
        }
        let page = &later[..take];
        let records: Vec<StreamRecord> = (page.iter())
            .filter_map(|entry| entry.record.clone())
            .collect();
        let last_offset = page
            .last()
            .map_or(request.from_offset, |e| e.offset.clone());
        *moved_to = records.is_empty().then(|| last_offset.clone());
        Ok(ReadRecordsResponse {
            records,
            last_offset,
            done: *closed && take == later.len(),
        })
    }
}

/// Counts a read of a model Store while it runs. A caller that stops waiting for an answer that
/// comes within the read's wait is a violation.
#[cfg(feature = "workflow")]
struct Reading<'a> {
    model: &'a Mutex<Model>,
    waits: bool,
    answered: bool,
}

#[cfg(feature = "workflow")]
impl Drop for Reading<'_> {
    fn drop(&mut self) {
        let mut model = self.model.lock();
        model.reading.0 -= 1;
        if self.waits && !self.answered {
            let violation = "A caller stopped waiting for an answer within its wait".to_string();
            model.violations.push(violation);
        }
    }
}

#[cfg(feature = "workflow")]
impl StreamStore for ModelStore {
    fn create_stream(
        &self,
        _: CreateStreamRequest,
    ) -> BoxFuture<'_, StreamResult<CreateStreamResponse>> {
        unimplemented!("A model Store only reads")
    }

    fn append_records(
        &self,
        _: AppendRecordsRequest,
    ) -> BoxFuture<'_, StreamResult<AppendRecordsResponse>> {
        unimplemented!("A model Store only reads")
    }

    fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> BoxFuture<'_, StreamResult<ReadRecordsResponse>> {
        Box::pin(async move {
            {
                let mut model = self.model.lock();
                model.reading.0 += 1;
                model.reading.1 = model.reading.0.max(model.reading.1);
            }
            let mut reading = Reading {
                model: &self.model,
                waits: true,
                answered: false,
            };
            let answer = self.read(request, &mut reading).await;
            reading.answered = true;
            answer
        })
    }

    fn close_stream(
        &self,
        _: CloseStreamRequest,
    ) -> BoxFuture<'_, StreamResult<CloseStreamResponse>> {
        unimplemented!("A model Store only reads")
    }

    fn describe_stream(
        &self,
        _: DescribeStreamRequest,
    ) -> BoxFuture<'_, StreamResult<DescribeStreamResponse>> {
        unimplemented!("A model Store only reads")
    }
}

pub(crate) fn runtime() -> (Arc<Scripted>, StreamRuntime) {
    let store = Arc::new(Scripted::default());
    (
        store.clone(),
        StreamRuntime::new(store, BatchLimits::default()),
    )
}

pub(crate) fn task(started_event_id: i64, attempt: i32, seconds: i64) -> WorkflowTaskEpoch {
    WorkflowTaskEpoch {
        started_event_id,
        attempt,
        started_time: Some(Timestamp { seconds, nanos: 0 }),
    }
}

pub(crate) fn workflow(epoch: Option<WorkflowTaskEpoch>) -> Producer {
    Producer {
        id: "wf:r".to_string(),
        kind: Some(Kind::Workflow(WorkflowProducer {
            run_id: "r".to_string(),
            epoch,
        })),
    }
}

pub(crate) fn local_activity(task: WorkflowTaskEpoch, attempt: i32) -> Producer {
    Producer {
        id: "la:r:k".to_string(),
        kind: Some(Kind::LocalActivity(LocalActivityProducer {
            run_id: "r".to_string(),
            epoch: Some(LocalActivityEpoch {
                task: Some(task),
                attempt,
            }),
        })),
    }
}

fn activity_epoch(seconds: i64, nanos: i32, attempt: i32) -> Option<ActivityEpoch> {
    Some(ActivityEpoch {
        attempt_scheduled_time: Some(Timestamp { seconds, nanos }),
        attempt,
    })
}

pub(crate) fn activity(seconds: i64, nanos: i32, attempt: i32) -> Producer {
    Producer {
        id: "act:r:a:s".to_string(),
        kind: Some(Kind::WorkflowActivity(ActivityProducer {
            run_id: "r".to_string(),
            epoch: activity_epoch(seconds, nanos, attempt),
        })),
    }
}

pub(crate) fn standalone_activity(seconds: i64, nanos: i32, attempt: i32) -> Producer {
    Producer {
        id: "sa:a".to_string(),
        kind: Some(Kind::StandaloneActivity(ActivityProducer {
            run_id: String::new(),
            epoch: activity_epoch(seconds, nanos, attempt),
        })),
    }
}

pub(crate) fn client() -> Producer {
    Producer {
        id: "client:n:p".to_string(),
        kind: Some(Kind::Client(ClientProducer {})),
    }
}
