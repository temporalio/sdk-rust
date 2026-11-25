use crate::{
    AppendError, CreateError, DescribeError, ReadError, StreamRuntime, StreamStore,
    proto::{
        ActivityAttempt, AppendRecordsRequest, AppendRecordsResponse, ClientProducer,
        CreateStreamRequest, CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse,
        LocalActivityAttempt, LocalActivityProducer, Producer, ReadRecordsRequest,
        ReadRecordsResponse, StandaloneActivityProducer, WorkflowActivityProducer,
        WorkflowProducer, WorkflowTaskAttempt, producer::Kind,
    },
    record::StoreLimits,
};
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use prost_types::Timestamp;
use std::sync::Arc;

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
}

impl StreamStore for Scripted {
    fn create_stream(
        &self,
        _: CreateStreamRequest,
        _: String,
    ) -> BoxFuture<'_, Result<CreateStreamResponse, CreateError>> {
        Box::pin(async move { Ok(self.answers.lock().created) })
    }

    fn append_records(
        &self,
        _: AppendRecordsRequest,
        _: String,
    ) -> BoxFuture<'_, Result<AppendRecordsResponse, AppendError>> {
        Box::pin(async move { Ok(self.answers.lock().appended.clone()) })
    }

    fn read_records(
        &self,
        _: ReadRecordsRequest,
    ) -> BoxFuture<'_, Result<ReadRecordsResponse, ReadError>> {
        Box::pin(async move { Ok(self.answers.lock().page.clone()) })
    }

    fn describe_stream(
        &self,
        _: DescribeStreamRequest,
    ) -> BoxFuture<'_, Result<DescribeStreamResponse, DescribeError>> {
        Box::pin(async move { Ok(self.answers.lock().described.clone()) })
    }
}

pub(crate) fn runtime() -> (Arc<Scripted>, StreamRuntime) {
    let store = Arc::new(Scripted::default());
    (
        store.clone(),
        StreamRuntime::new(store, StoreLimits::default()),
    )
}

pub(crate) fn task(started_event_id: i64, attempt: i32, seconds: i64) -> WorkflowTaskAttempt {
    WorkflowTaskAttempt {
        started_event_id,
        attempt,
        started_time: Some(Timestamp { seconds, nanos: 0 }),
    }
}

/// Sets the text ids of `producer` to a namespace, a Workflow id, a run id, and an Activity id or
/// the id of a client. A kind takes the ids that it has.
pub(crate) fn set_ids(producer: &mut Producer, ids: [&str; 4]) {
    let [namespace, workflow_id, run_id, other] = ids.map(str::to_string);
    match producer.kind.as_mut().expect("The Producer has a kind") {
        Kind::Workflow(of) => {
            (of.namespace, of.workflow_id, of.run_id) = (namespace, workflow_id, run_id);
        }
        Kind::LocalActivity(of) => {
            (of.namespace, of.workflow_id, of.run_id) = (namespace, workflow_id, run_id);
            of.activity_id = other;
        }
        Kind::WorkflowActivity(of) => {
            (of.namespace, of.workflow_id, of.run_id) = (namespace, workflow_id, run_id);
            of.activity_id = other;
        }
        Kind::StandaloneActivity(of) => {
            (of.namespace, of.activity_id, of.run_id) = (namespace, other, run_id);
        }
        Kind::Client(of) => (of.namespace, of.id) = (namespace, other),
    }
}

/// Returns a Producer of `kind` with each of its text ids set.
pub(crate) fn producer(kind: Kind) -> Producer {
    let mut producer = Producer { kind: Some(kind) };
    set_ids(&mut producer, ["n", "w", "r", "a"]);
    producer
}

pub(crate) fn workflow(attempt: Option<WorkflowTaskAttempt>) -> Producer {
    producer(Kind::Workflow(WorkflowProducer {
        attempt,
        ..Default::default()
    }))
}

pub(crate) fn local_activity(task: WorkflowTaskAttempt, attempt_number: i32) -> Producer {
    producer(Kind::LocalActivity(LocalActivityProducer {
        attempt: Some(LocalActivityAttempt {
            task: Some(task),
            attempt: attempt_number,
        }),
        ..Default::default()
    }))
}

fn activity_attempt(seconds: i64, nanos: i32, attempt_number: i32) -> Option<ActivityAttempt> {
    Some(ActivityAttempt {
        attempt_scheduled_time: Some(Timestamp { seconds, nanos }),
        attempt: attempt_number,
    })
}

pub(crate) fn activity(seconds: i64, nanos: i32, attempt_number: i32) -> Producer {
    producer(Kind::WorkflowActivity(WorkflowActivityProducer {
        attempt: activity_attempt(seconds, nanos, attempt_number),
        ..Default::default()
    }))
}

/// Returns `producer`, which is a Producer of a Workflow's Activity, with the given scheduled event
/// id.
pub(crate) fn with_scheduled_event_id(mut producer: Producer, id: i64) -> Producer {
    match producer.kind.as_mut() {
        Some(Kind::WorkflowActivity(of)) => of.scheduled_event_id = id,
        kind => panic!("{kind:?} isn't a Workflow's Activity"),
    }
    producer
}

pub(crate) fn standalone_activity(seconds: i64, nanos: i32, attempt_number: i32) -> Producer {
    producer(Kind::StandaloneActivity(StandaloneActivityProducer {
        attempt: activity_attempt(seconds, nanos, attempt_number),
        ..Default::default()
    }))
}

pub(crate) fn client() -> Producer {
    producer(Kind::Client(ClientProducer::default()))
}
