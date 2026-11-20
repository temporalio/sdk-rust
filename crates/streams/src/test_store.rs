use crate::{
    AnyError, AppendError, CloseError, CreateError, DescribeError, ReadError, StreamRuntime,
    StreamStore,
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

    async fn write(&self) -> Result<(), AnyError> {
        if self.silent.load(Ordering::Acquire) {
            std::future::pending::<()>().await;
        }
        let mut unavailable = self.unavailable.lock();
        if *unavailable > 0 {
            *unavailable -= 1;
            return Err(AnyError::storage_unavailable("The Store is down"));
        }
        Ok(())
    }
}

impl StreamStore for Scripted {
    fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> BoxFuture<'_, Result<CreateStreamResponse, CreateError>> {
        Box::pin(async move {
            self.requests.lock().push(Request::Create(request));
            Ok(self.answers.lock().created.clone())
        })
    }

    fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> BoxFuture<'_, Result<AppendRecordsResponse, AppendError>> {
        Box::pin(async move {
            self.requests.lock().push(Request::Append(request));
            self.write().await?;
            Ok(self.answers.lock().appended.clone())
        })
    }

    fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> BoxFuture<'_, Result<ReadRecordsResponse, ReadError>> {
        Box::pin(async move {
            self.requests.lock().push(Request::Read(request));
            Ok(self.answers.lock().page.clone())
        })
    }

    fn close_stream(
        &self,
        request: CloseStreamRequest,
    ) -> BoxFuture<'_, Result<CloseStreamResponse, CloseError>> {
        Box::pin(async move {
            self.requests.lock().push(Request::Close(request));
            self.write().await?;
            Ok(CloseStreamResponse {})
        })
    }

    fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> BoxFuture<'_, Result<DescribeStreamResponse, DescribeError>> {
        Box::pin(async move {
            self.requests.lock().push(Request::Describe(request));
            Ok(self.answers.lock().described.clone())
        })
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
