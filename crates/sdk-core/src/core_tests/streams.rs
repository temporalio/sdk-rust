//! Direct appends and closes of a Stream, on a test Store. A test drives the Worker with a mock
//! history.

use super::stream_store::TestStreamStore;
use crate::{
    StreamRuntimes, Worker, WorkerConfig,
    replay::{HistoryForReplay, ReplayWorkerInput, TestHistoryBuilder},
    test_help::{
        CounterRecordingMeter, MockPollCfg, ResponseType, WorkerExt, WorkerTestHelpers,
        build_mock_pollers, hist_to_poll_resp, mock_worker, test_worker_cfg,
    },
    worker::{
        MAX_FAILURE_MESSAGE_BYTES, MAX_MARKER_ENTRY_BYTES, MAX_STORE_NAME_BYTES, StreamTestHooks,
        client::WorkflowTaskCompletion,
    },
};
use futures_util::FutureExt;
use parking_lot::Mutex;
use prost::Message;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::{
    protos::{
        constants::STREAM_MARKER_NAME,
        coresdk::{
            external_data::{StreamAppendEntry, StreamCloseEntry, StreamMarkerData},
            workflow_activation::{
                ResolveStreamAppend, ResolveStreamClose, WorkflowActivation, resolve_stream_append,
                workflow_activation_job,
            },
            workflow_commands::{
                AppendToStream, CloseStream, CompleteWorkflowExecution, workflow_command,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            command::v1::command::Attributes,
            common::v1::{Payload, Payloads},
            enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
            failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
            history::v1::MarkerRecordedEventAttributes,
            workflowservice::v1::RespondWorkflowTaskCompletedResponse,
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_streams::{
    Reason, StreamError, StreamResult, StreamRuntime, StreamStore,
    ids::MAX_STREAM_ID_BYTES,
    proto::{
        AppendRecord, AppendRecordsRequest, AppendRecordsResponse, ClientProducer,
        CloseStreamRequest, CloseStreamResponse, CreateStreamRequest, CreateStreamResponse,
        DescribeStreamRequest, DescribeStreamResponse, Producer, ReadRecordsRequest,
        ReadRecordsResponse, StreamRecord, StreamRef, StreamStatus, WorkflowProducer,
        WorkflowTaskEpoch, producer::Kind,
    },
};
use tokio::sync::{Notify, Semaphore};

const STORE: &str = "mem";

fn runtime() -> Arc<StreamRuntime> {
    StreamRuntime::new(Arc::new(TestStreamStore::default()), None)
}

fn stream(id: &str) -> StreamRef {
    StreamRef {
        id: id.to_string(),
        store: STORE.to_string(),
    }
}

fn client() -> Producer {
    Producer {
        id: "client:test".to_string(),
        kind: Some(Kind::Client(ClientProducer {})),
    }
}

fn body(text: &str) -> Payload {
    Payload {
        data: text.as_bytes().to_vec(),
        ..Default::default()
    }
}

async fn create(runtime: &StreamRuntime, id: &str) {
    runtime
        .create_stream(CreateStreamRequest {
            stream_ref: Some(stream(id)),
            ..Default::default()
        })
        .await
        .unwrap();
}

async fn close(runtime: &StreamRuntime, id: &str) {
    runtime
        .close_stream(CloseStreamRequest {
            r#ref: Some(stream(id)),
            producer: Some(client()),
            ..Default::default()
        })
        .await
        .unwrap();
}

async fn all_records(runtime: &StreamRuntime, id: &str) -> Vec<StreamRecord> {
    runtime
        .read_records(ReadRecordsRequest {
            r#ref: Some(stream(id)),
            ..Default::default()
        })
        .await
        .unwrap()
        .records
}

fn texts(records: &[StreamRecord]) -> Vec<String> {
    records
        .iter()
        .map(|record| String::from_utf8(record.body.clone().unwrap().data).unwrap())
        .collect()
}

fn workflow_producer(run_id: &str) -> Producer {
    Producer {
        id: format!("wf:{run_id}"),
        kind: Some(Kind::Workflow(WorkflowProducer {
            run_id: run_id.to_string(),
            epoch: None,
        })),
    }
}

fn append_cmd(seq: u32, id: &str, run_id: &str, texts: &[&str]) -> AppendToStream {
    AppendToStream {
        seq,
        stream: Some(stream(id)),
        bodies: texts.iter().map(|text| body(text)).collect(),
        producer: Some(workflow_producer(run_id)),
        sequence: 0,
    }
}

/// Runs a Worker on the history, with the runtime and a cache of one run, and keeps every
/// completion that it sends.
fn worker(
    t: TestHistoryBuilder,
    responses: impl IntoIterator<Item = ResponseType>,
    runtime: &Arc<StreamRuntime>,
    expected_fails: usize,
) -> (Worker, Arc<Mutex<Vec<WorkflowTaskCompletion>>>) {
    let runtimes = StreamRuntimes::new([(STORE.to_string(), runtime.clone())]);
    let run = start(t, responses, runtimes, 1, expected_fails);
    (run.worker, run.sent)
}

/// A Worker on a mock history, with what it sent to the server.
struct Run {
    worker: Worker,
    sent: Arc<Mutex<Vec<WorkflowTaskCompletion>>>,
    /// The cause and the message of each failed Workflow Task.
    failed: Arc<Mutex<Vec<(WorkflowTaskFailedCause, String)>>>,
    /// The History that a mock server of [served] wrote, which a test can replay.
    history: Option<Arc<Mutex<TestHistoryBuilder>>>,
}

/// A Worker whose mock poller stays alive. The end of a poller begins a shutdown, so a test shuts
/// the Worker down when it asks.
fn start(
    t: TestHistoryBuilder,
    responses: impl IntoIterator<Item = ResponseType>,
    runtimes: StreamRuntimes,
    cache: usize,
    expected_fails: usize,
) -> Run {
    start_with(t, responses, runtimes, cache, expected_fails, true)
}

/// As [start]. With `interminable`, the mock poller doesn't end when it gave every task, so the
/// Worker doesn't shut down before it handles the last one.
fn start_with(
    t: TestHistoryBuilder,
    responses: impl IntoIterator<Item = ResponseType>,
    runtimes: StreamRuntimes,
    cache: usize,
    expected_fails: usize,
    interminable: bool,
) -> Run {
    start_metered(
        t,
        responses,
        runtimes,
        cache,
        expected_fails,
        interminable,
        None,
    )
}

/// As [start_with], with `meter` as the Worker's meter if it is given.
fn start_metered(
    t: TestHistoryBuilder,
    responses: impl IntoIterator<Item = ResponseType>,
    runtimes: StreamRuntimes,
    cache: usize,
    expected_fails: usize,
    interminable: bool,
    meter: Option<&Arc<CounterRecordingMeter>>,
) -> Run {
    start_configured(
        t,
        responses,
        runtimes,
        (cache, expected_fails, interminable),
        meter,
        |_| {},
    )
}

/// As [start_metered], with `configure` setting the Worker's config last. `(cache,
/// expected_fails, interminable)` are as for [start_with].
fn start_configured(
    t: TestHistoryBuilder,
    responses: impl IntoIterator<Item = ResponseType>,
    runtimes: StreamRuntimes,
    (cache, expected_fails, interminable): (usize, usize, bool),
    meter: Option<&Arc<CounterRecordingMeter>>,
    configure: impl FnOnce(&mut WorkerConfig),
) -> Run {
    let sent = Arc::new(Mutex::new(vec![]));
    let failed = Arc::new(Mutex::new(vec![]));
    let mut cfg = MockPollCfg::from_resps(t, responses);
    cfg.num_expected_fails = expected_fails;
    cfg.make_poll_stream_interminable = interminable;
    let kept = sent.clone();
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        kept.lock().push(completion.clone());
        Ok(Default::default())
    }));
    cfg.num_expected_completions = Some((0..).into());
    let kept = failed.clone();
    cfg.expect_fail_wft_matcher = Box::new(move |_, cause, failure| {
        let message = failure
            .as_ref()
            .map(|failure| failure.message.clone())
            .unwrap_or_default();
        kept.lock().push((*cause, message));
        true
    });
    let mut mock = build_mock_pollers(cfg);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = cache;
        wc.stream_runtimes = runtimes;
        configure(wc);
    });
    if let Some(meter) = meter {
        mock.set_temporal_meter(meter.clone().into_temporal_meter());
    }
    Run {
        worker: mock_worker(mock),
        sent,
        failed,
        history: None,
    }
}

/// A Worker on a mock server that gives each task that a completion forces inline, with the events
/// that the completion wrote, as a real server does. So History has the real marker of each task.
/// `t` is the history up to the first task. `observe` sees each completion as the server takes it,
/// and `configure` sets the Worker's config after its cache and runtimes.
fn served(
    t: TestHistoryBuilder,
    runtimes: StreamRuntimes,
    meter: Option<&Arc<CounterRecordingMeter>>,
    mut observe: impl FnMut(&WorkflowTaskCompletion) + Send + 'static,
    configure: impl FnOnce(&mut WorkerConfig),
) -> Run {
    let sent = Arc::new(Mutex::new(vec![]));
    let failed = Arc::new(Mutex::new(vec![]));
    let written = Arc::new(Mutex::new(t.clone()));
    let kept_history = written.clone();
    let mut cfg = MockPollCfg::from_resps(t, [ResponseType::AllHistory]);
    cfg.make_poll_stream_interminable = true;
    cfg.num_expected_completions = Some((0..).into());
    let kept = sent.clone();
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        observe(completion);
        kept.lock().push(completion.clone());
        let mut history = kept_history.lock();
        history.add_workflow_task_completed();
        let completed = history.current_event_id();
        for command in &completion.commands {
            match &command.attributes {
                Some(Attributes::RecordMarkerCommandAttributes(marker)) => {
                    history.add(MarkerRecordedEventAttributes {
                        marker_name: marker.marker_name.clone(),
                        details: marker.details.clone(),
                        workflow_task_completed_event_id: completed,
                        ..Default::default()
                    });
                }
                Some(Attributes::StartTimerCommandAttributes(timer)) => {
                    history.add_timer_started(timer.timer_id.clone());
                }
                Some(Attributes::CompleteWorkflowExecutionCommandAttributes(_)) => {
                    history.add_workflow_execution_completed();
                }
                _ => {}
            }
        }
        let workflow_task = completion.force_create_new_workflow_task.then(|| {
            history.add_workflow_task_scheduled_and_started();
            let mut task = hist_to_poll_resp(&history, "fake_wf_id", ResponseType::AllHistory).resp;
            task.attempt = 1;
            task
        });
        Ok(RespondWorkflowTaskCompletedResponse {
            workflow_task,
            ..Default::default()
        })
    }));
    let kept = failed.clone();
    cfg.expect_fail_wft_matcher = Box::new(move |_, cause, failure| {
        let message = failure
            .as_ref()
            .map(|failure| failure.message.clone())
            .unwrap_or_default();
        kept.lock().push((*cause, message));
        true
    });
    let mut mock = build_mock_pollers(cfg);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 1;
        wc.stream_runtimes = runtimes;
        configure(wc);
    });
    if let Some(meter) = meter {
        mock.set_temporal_meter(meter.clone().into_temporal_meter());
    }
    Run {
        worker: mock_worker(mock),
        sent,
        failed,
        history: Some(written),
    }
}

async fn complete(
    worker: &Worker,
    activation: &WorkflowActivation,
    commands: Vec<workflow_command::Variant>,
) {
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            activation.run_id.clone(),
            commands,
        ))
        .await
        .unwrap();
}

fn appends(activation: &WorkflowActivation) -> Vec<ResolveStreamAppend> {
    activation
        .jobs
        .iter()
        .filter_map(|job| match &job.variant {
            Some(workflow_activation_job::Variant::ResolveStreamAppend(append)) => {
                Some(append.clone())
            }
            _ => None,
        })
        .collect()
}

/// The data of the `core_stream` marker of a completion, if it has one.
fn marker(completion: &WorkflowTaskCompletion) -> Option<StreamMarkerData> {
    completion
        .commands
        .iter()
        .find_map(|command| match command.attributes.as_ref()? {
            Attributes::RecordMarkerCommandAttributes(marker)
                if marker.marker_name == STREAM_MARKER_NAME =>
            {
                let payload = &marker.details["data"].payloads[0];
                Some(StreamMarkerData::decode(payload.data.as_slice()).unwrap())
            }
            _ => None,
        })
}

fn command_types(completion: &WorkflowTaskCompletion) -> Vec<CommandType> {
    completion
        .commands
        .iter()
        .map(|command| command.command_type())
        .collect()
}

/// An append writes the bodies as the Workflow Producer, with the Workflow Task as its epoch, and
/// gives the offsets. The marker records them.
#[tokio::test]
async fn append_records_offsets() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let (worker, sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![append_cmd(1, "s", &run_id, &["a", "b"]).into()],
    )
    .await;
    let result = worker.poll_workflow_activation().await.unwrap();
    let append = &appends(&result)[0];
    let Some(resolve_stream_append::Status::Appended(appended)) = &append.status else {
        panic!("The append failed: {append:?}");
    };
    worker.complete_execution(&result.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let records = all_records(&runtime, "s").await;
    assert_eq!(texts(&records), ["a", "b"]);
    assert_eq!(appended.first_offset, records[0].offset);
    assert_eq!(appended.last_offset, records[1].offset);
    let producer = records[0].producer.clone().unwrap();
    assert_eq!(producer.id, format!("wf:{run_id}"));
    let Some(Kind::Workflow(workflow)) = producer.kind else {
        panic!("The Producer is not a Workflow: {producer:?}");
    };
    let epoch = workflow.epoch.unwrap();
    assert_eq!((epoch.started_event_id, epoch.attempt), (3, 1));
    assert!(epoch.started_time.is_some());

    let entry = &marker(&sent.lock()[0]).unwrap().appends[0];
    assert_eq!((entry.seq, entry.activation), (1, 1));
    assert_eq!(entry.first_offset, records[0].offset);
    assert_eq!(entry.last_offset, records[1].offset);
    assert!(entry.failure.is_none());
}

/// Appends to one Stream go one at a time, in call order.
#[tokio::test]
async fn appends_to_one_stream_go_in_order() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let (worker, _sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    let calls = (1..=5).map(|seq| append_cmd(seq, "s", &run_id, &[&seq.to_string()]).into());
    complete(&worker, &first, calls.collect()).await;
    let mut resolved = vec![];
    loop {
        let result = worker.poll_workflow_activation().await.unwrap();
        resolved.extend(appends(&result).into_iter().map(|append| append.seq));
        if resolved.len() == 5 {
            worker.complete_execution(&result.run_id).await;
            break;
        }
        complete(&worker, &result, vec![]).await;
    }
    worker.drain_pollers_and_shutdown().await;
    assert_eq!(resolved, [1, 2, 3, 4, 5]);
    assert_eq!(
        texts(&all_records(&runtime, "s").await),
        ["1", "2", "3", "4", "5"]
    );
}

/// An append that the fence refuses gives a failure to Workflow code, and the marker records it.
#[tokio::test]
async fn refused_append_gives_a_failure() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    // A write of a later Workflow Task of the run raises the fence over this task.
    let mut later = workflow_producer(&run_id);
    if let Some(Kind::Workflow(workflow)) = later.kind.as_mut() {
        workflow.epoch = Some(WorkflowTaskEpoch {
            started_event_id: 100,
            attempt: 1,
            started_time: None,
        });
    }
    runtime
        .append_records(AppendRecordsRequest {
            r#ref: Some(stream("s")),
            producer: Some(later),
            records: vec![AppendRecord {
                body: Some(body("later")),
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let (worker, sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![append_cmd(1, "s", &run_id, &["stale"]).into()],
    )
    .await;
    let result = worker.poll_workflow_activation().await.unwrap();
    let Some(resolve_stream_append::Status::Failed(failure)) = &appends(&result)[0].status else {
        panic!("The append should fail");
    };
    let Some(FailureInfo::ApplicationFailureInfo(info)) = &failure.failure_info else {
        panic!("The failure has no application info: {failure:?}");
    };
    assert_eq!(info.r#type, "stream_fenced");
    assert!(info.non_retryable);
    worker.complete_execution(&result.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    assert_eq!(texts(&all_records(&runtime, "s").await), ["later"]);
    let entry = &marker(&sent.lock()[0]).unwrap().appends[0];
    assert_eq!(entry.failure.as_ref(), Some(failure));
    assert!(entry.first_offset.is_empty());
}

/// An append in a task that fails is written again by the next attempt, with a higher epoch.
#[tokio::test]
async fn failed_task_appends_again_with_a_higher_epoch() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let responses = [1, 1].map(ResponseType::ToTaskNum);
    let (worker, _sent) = worker(t, responses, &runtime, 1);

    for attempt in 1..=2 {
        let first = worker.poll_workflow_activation().await.unwrap();
        complete(
            &worker,
            &first,
            vec![append_cmd(1, "s", &run_id, &["x"]).into()],
        )
        .await;
        let result = worker.poll_workflow_activation().await.unwrap();
        assert!(matches!(
            appends(&result)[0].status,
            Some(resolve_stream_append::Status::Appended(_))
        ));
        if attempt == 2 {
            worker.complete_execution(&result.run_id).await;
            break;
        }
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::fail(
                result.run_id.clone(),
                Failure {
                    message: "Workflow code failed".to_string(),
                    ..Default::default()
                },
                None,
            ))
            .await
            .unwrap();
        let evict = worker.poll_workflow_activation().await.unwrap();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(evict.run_id))
            .await
            .unwrap();
    }
    worker.drain_pollers_and_shutdown().await;

    let records = all_records(&runtime, "s").await;
    assert_eq!(texts(&records), ["x", "x"]);
    let attempts: Vec<_> = records
        .iter()
        .map(|record| match &record.producer.as_ref().unwrap().kind {
            Some(Kind::Workflow(workflow)) => workflow.epoch.as_ref().unwrap().attempt,
            other => panic!("The Producer is not a Workflow: {other:?}"),
        })
        .collect();
    assert_eq!(attempts, [1, 2]);
}

/// An append needs the Workflow's Producer. With none, the Workflow Task fails.
#[tokio::test]
async fn append_without_a_workflow_producer_fails_the_task() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let mut cfg = MockPollCfg::from_resps(t, [ResponseType::AllHistory]);
    cfg.num_expected_fails = 1;
    cfg.expect_fail_wft_matcher = Box::new(|_, _, failure| {
        failure
            .as_ref()
            .is_some_and(|failure| failure.message.contains("not of kind Workflow"))
    });
    let mut mock = build_mock_pollers(cfg);
    let runtimes = StreamRuntimes::new([(STORE.to_string(), runtime.clone())]);
    mock.worker_cfg(|wc| wc.stream_runtimes = runtimes);
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    let append = AppendToStream {
        producer: Some(client()),
        ..append_cmd(1, "s", "r", &["x"])
    };
    complete(&worker, &first, vec![append.into()]).await;
    worker.drain_pollers_and_shutdown().await;
    assert!(all_records(&runtime, "s").await.is_empty());
}

// Replays. A test builds the marker that Core would have written.

/// A `core_stream` marker event with `data`.
fn stream_marker_event(
    t: &mut TestHistoryBuilder,
    completed_event_id: i64,
    data: &StreamMarkerData,
) {
    let payload = Payload {
        metadata: HashMap::from([("encoding".to_string(), b"binary/protobuf".to_vec())]),
        data: data.encode_to_vec(),
        ..Default::default()
    };
    t.add(MarkerRecordedEventAttributes {
        marker_name: STREAM_MARKER_NAME.to_string(),
        details: HashMap::from([(
            "data".to_string(),
            Payloads {
                payloads: vec![payload],
            },
        )]),
        workflow_task_completed_event_id: completed_event_id,
        ..Default::default()
    });
}

/// A history whose first Workflow Task recorded `data`. A signal starts the second task, so that
/// Workflow code gets an activation in it.
fn replayed_history(data: &StreamMarkerData) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    stream_marker_event(&mut t, 4, data);
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_and_started();
    t
}

fn runtimes(runtime: &Arc<StreamRuntime>) -> StreamRuntimes {
    StreamRuntimes::new([(STORE.to_string(), runtime.clone())])
}

fn signaled(activation: &WorkflowActivation) -> bool {
    activation.jobs.iter().any(|job| {
        matches!(
            job.variant,
            Some(workflow_activation_job::Variant::SignalWorkflow(_))
        )
    })
}

/// A replayed append gives the result that history has, and writes nothing.
#[tokio::test]
async fn append_replay_calls_no_store() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let appended = StreamAppendEntry {
        seq: 1,
        stream_index: 0,
        first_offset: 7u64.to_be_bytes().to_vec(),
        last_offset: 8u64.to_be_bytes().to_vec(),
        failure: None,
        activation: 1,
    };
    let t = replayed_history(&StreamMarkerData {
        reads: vec![],
        appends: vec![appended.clone()],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let run_id = t.get_orig_run_id().to_string();
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 0);

    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(
        &run.worker,
        &first,
        vec![append_cmd(1, "s", &run_id, &["x", "y"]).into()],
    )
    .await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    let Some(resolve_stream_append::Status::Appended(result)) = &appends(&replayed)[0].status
    else {
        panic!("The replayed append has no offsets");
    };
    assert_eq!(
        (result.first_offset.clone(), result.last_offset.clone()),
        (appended.first_offset, appended.last_offset)
    );
    complete(&run.worker, &replayed, vec![]).await;
    let live = run.worker.poll_workflow_activation().await.unwrap();
    run.worker.complete_execution(&live.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(all_records(&runtime, "s").await.len(), 0);
}

/// A non-zero `sequence` of an append goes to the Store as given, so a lower one is refused.
#[tokio::test]
async fn append_sends_the_sequence_that_lang_gives() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let (worker, _sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    let with = |seq, sequence, text| AppendToStream {
        sequence,
        ..append_cmd(seq, "s", &run_id, &[text])
    };
    complete(
        &worker,
        &first,
        vec![with(1, 10, "a").into(), with(2, 3, "b").into()],
    )
    .await;
    let mut results = vec![];
    loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        results.extend(appends(&next));
        if results.len() == 2 {
            worker.complete_execution(&next.run_id).await;
            break;
        }
        complete(&worker, &next, vec![]).await;
    }
    worker.drain_pollers_and_shutdown().await;
    assert!(matches!(
        results[0].status,
        Some(resolve_stream_append::Status::Appended(_))
    ));
    let Some(resolve_stream_append::Status::Failed(failure)) = &results[1].status else {
        panic!("The lower sequence should fail");
    };
    assert!(
        failure.message.starts_with("stream_sequence_too_low"),
        "{}",
        failure.message
    );
    assert_eq!(texts(&all_records(&runtime, "s").await), ["a"]);
}

/// A replay Worker on the history of one Workflow, with `runtimes`.
fn replay_worker(t: TestHistoryBuilder, runtimes: StreamRuntimes) -> Worker {
    let cfg = test_worker_cfg().stream_runtimes(runtimes).build().unwrap();
    crate::init_replay_worker(ReplayWorkerInput::new(
        cfg,
        futures_util::stream::iter([HistoryForReplay::from(t)]),
    ))
    .unwrap()
}

/// Polls every activation until the replay Worker ends the history, completes each with no
/// command, and returns their jobs. The end of the history ends the wait, so a slow machine can't
/// cut it short. The long bound only guards against a hang.
async fn activations_until_the_end(worker: &Worker) -> Vec<String> {
    let mut jobs = vec![];
    loop {
        let next = tokio::time::timeout(Duration::from_secs(60), worker.poll_workflow_activation())
            .await
            .expect("the replay Worker didn't end the history");
        let Ok(next) = next else {
            return jobs;
        };
        jobs.extend(next.jobs.iter().filter_map(|job| {
            job.variant.as_ref().map(|v| match v {
                workflow_activation_job::Variant::RemoveFromCache(evict) => {
                    format!("RemoveFromCache({:?})", evict.reason())
                }
                v => format!("{v}"),
            })
        }));
        let _ = worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(next.run_id))
            .await;
    }
}

/// True when a job evicts the run for a failure. The eviction that ends a replay doesn't count.
fn has_failure_eviction(jobs: &[String]) -> bool {
    jobs.iter()
        .any(|job| job.starts_with("RemoveFromCache") && job != "RemoveFromCache(LangRequested)")
}

/// A replay Worker never calls a Store for a call that history leaves open. The call stays open,
/// as a Local Activity does there.
#[tokio::test]
async fn a_replay_worker_never_writes_to_a_store() {
    let runtime = runtime();
    create(&runtime, "out").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let worker = replay_worker(t, runtimes(&runtime));
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![append_cmd(1, "out", &run_id, &["from-a-replay"]).into()],
    )
    .await;
    let jobs = activations_until_the_end(&worker).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), worker.drain_pollers_and_shutdown()).await;
    assert!(
        !jobs
            .iter()
            .any(|job| job.starts_with("ResolveStreamAppend")),
        "{jobs:?}"
    );
    assert!(all_records(&runtime, "out").await.is_empty());
}

/// A Stream of no Store name, as lang sends it when it has no Stream config to pick one from.
fn unnamed(id: &str) -> Option<StreamRef> {
    Some(StreamRef {
        id: id.to_string(),
        store: String::new(),
    })
}

/// In a replay, an append with no Store name takes the Store of its entry in history.
#[tokio::test]
async fn a_replayed_append_with_no_store_name_takes_the_store_of_history() {
    let appended = StreamAppendEntry {
        seq: 1,
        stream_index: 0,
        first_offset: 7u64.to_be_bytes().to_vec(),
        last_offset: 7u64.to_be_bytes().to_vec(),
        failure: None,
        activation: 1,
    };
    let t = replayed_history(&StreamMarkerData {
        reads: vec![],
        appends: vec![appended],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let run_id = t.get_orig_run_id().to_string();
    let worker = replay_worker(t, StreamRuntimes::default());
    let first = worker.poll_workflow_activation().await.unwrap();
    let append = AppendToStream {
        stream: unnamed("s"),
        ..append_cmd(1, "s", &run_id, &["x"])
    };
    complete(&worker, &first, vec![append.into()]).await;
    let jobs = activations_until_the_end(&worker).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), worker.drain_pollers_and_shutdown()).await;
    assert!(
        jobs.iter().any(|job| job == "ResolveStreamAppend(1)"),
        "{jobs:?}"
    );
    assert!(
        jobs.iter().any(|job| job.starts_with("SignalWorkflow")),
        "{jobs:?}"
    );
    assert!(!has_failure_eviction(&jobs), "{jobs:?}");
}

// Closes. A close takes effect only after History has the Workflow Task that asked for it.

fn close_cmd(seq: u32, id: &str, run_id: &str) -> CloseStream {
    CloseStream {
        seq,
        stream: Some(stream(id)),
        producer: Some(workflow_producer(run_id)),
    }
}

fn stream_is_closed(runtime: &StreamRuntime, id: &str) -> bool {
    // The test Store answers immediately, so this works also inside a mock's sync closure.
    let described = runtime
        .describe_stream(DescribeStreamRequest {
            r#ref: Some(stream(id)),
            ..Default::default()
        })
        .now_or_never()
        .expect("the test Store answers immediately")
        .unwrap();
    described.status == StreamStatus::Closed as i32
}

/// Waits for the Stream to close. A read that waits on the Store wakes at the close, so the wait
/// ends with the close and not at a time. The long wait only guards against a hang.
async fn wait_closed(runtime: &StreamRuntime, id: &str) {
    let mut from_offset = vec![];
    loop {
        let read = runtime
            .read_records(ReadRecordsRequest {
                r#ref: Some(stream(id)),
                from_offset: from_offset.clone(),
                wait: Some(Duration::from_secs(60).try_into().unwrap()),
                ..Default::default()
            })
            .await
            .unwrap();
        if read.done {
            return;
        }
        assert!(
            !read.records.is_empty() || read.last_offset != from_offset,
            "no close came in 60 s"
        );
        from_offset = read.last_offset;
    }
}

/// Polls an activation, runs `then` once the heartbeat has completed the task, and returns the
/// activation. Core sends the heartbeat only while a poll waits, so the poll starts first.
async fn poll_after_heartbeat<F: Future<Output = ()>>(
    worker: &Worker,
    sent: &Mutex<Vec<WorkflowTaskCompletion>>,
    then: impl FnOnce() -> F,
) -> WorkflowActivation {
    let next = worker.poll_workflow_activation();
    tokio::pin!(next);
    tokio::select! {
        _ = until("the heartbeat", || sent.lock().len() == 1) => {}
        early = &mut next => panic!("an activation came before the heartbeat: {early:?}"),
    }
    then().await;
    next.await.unwrap()
}

/// Waits until `condition` holds. It ends with the condition and not at a time, so a slow machine
/// can't fail it. The long bound only guards against a hang.
async fn until(what: &str, condition: impl Fn() -> bool) {
    for _ in 0..6000 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{what} didn't happen in 60 s");
}

fn closes(activation: &WorkflowActivation) -> Vec<ResolveStreamClose> {
    activation
        .jobs
        .iter()
        .filter_map(|job| match &job.variant {
            Some(workflow_activation_job::Variant::ResolveStreamClose(close)) => {
                Some(close.clone())
            }
            _ => None,
        })
        .collect()
}

/// Polls until an activation has a close result, completing the others with no command.
async fn until_closed(worker: &Worker) -> (WorkflowActivation, ResolveStreamClose) {
    loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        if let Some(close) = closes(&next).pop() {
            return (next, close);
        }
        complete(worker, &next, vec![]).await;
    }
}

fn task_epoch(started_event_id: i64) -> WorkflowTaskEpoch {
    WorkflowTaskEpoch {
        started_event_id,
        attempt: 1,
        started_time: None,
    }
}

/// An awaited close spans two Workflow Tasks. The first records the intent and calls no Store.
/// The Stream closes after the server took that task, and the second task gets the result.
#[tokio::test]
async fn an_awaited_close_spans_two_tasks() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let open_at_completion = Arc::new(Mutex::new(vec![]));
    let (seen, store) = (open_at_completion.clone(), runtime.clone());
    let observe = move |_: &WorkflowTaskCompletion| {
        seen.lock().push(!stream_is_closed(&store, "s"));
    };
    let run = served(t, runtimes(&runtime), None, observe, |_| {});
    let (worker, sent) = (run.worker, run.sent);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![close_cmd(1, "s", &run_id).into()]).await;
    let (next, close) = until_closed(&worker).await;
    assert!(close.failed.is_none());
    assert!(stream_is_closed(&runtime, "s"));
    worker.complete_execution(&next.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let sent = sent.lock();
    assert_eq!(*open_at_completion.lock(), [true, false]);
    assert!(sent[0].force_create_new_workflow_task);
    assert_eq!(command_types(&sent[0]), [CommandType::RecordMarker]);
    let intent = &marker(&sent[0]).unwrap().closes[0];
    assert_eq!((intent.seq, intent.resolved), (1, false));
    let epoch = intent.intent_epoch.unwrap();
    assert_eq!((epoch.started_event_id, epoch.attempt), (3, 1));
    let result = &marker(&sent[1]).unwrap().closes[0];
    assert_eq!(
        (result.seq, result.resolved, result.activation),
        (1, true, 0)
    );
    assert!(result.failure.is_none() && result.intent_epoch.is_none());
}

/// A Workflow Task that fails after its close command records no intent, so the Stream stays
/// open and the retry can append.
#[tokio::test]
async fn a_failed_task_after_a_close_leaves_the_stream_open() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let (worker, _sent) = worker(t, [1, 1].map(ResponseType::ToTaskNum), &runtime, 1);

    let first = worker.poll_workflow_activation().await.unwrap();
    // The append holds the task open, so the close waits in it.
    complete(
        &worker,
        &first,
        vec![
            append_cmd(1, "s", &run_id, &["a"]).into(),
            close_cmd(1, "s", &run_id).into(),
        ],
    )
    .await;
    let appended = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(appends(&appended).len(), 1);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::fail(
            appended.run_id.clone(),
            Failure {
                message: "Workflow code failed".to_string(),
                ..Default::default()
            },
            None,
        ))
        .await
        .unwrap();
    let evict = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(evict.run_id))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!stream_is_closed(&runtime, "s"));

    let retry = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &retry,
        vec![append_cmd(1, "s", &run_id, &["b"]).into()],
    )
    .await;
    let appended = worker.poll_workflow_activation().await.unwrap();
    assert!(matches!(
        appends(&appended)[0].status,
        Some(resolve_stream_append::Status::Appended(_))
    ));
    worker.complete_execution(&appended.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    assert_eq!(texts(&all_records(&runtime, "s").await), ["a", "b"]);
    assert!(!stream_is_closed(&runtime, "s"));
}

/// The appends of the task of the intent are in the Stream before the close, and an append of
/// the next task gets `stream_closed`.
#[tokio::test]
async fn appends_of_the_intent_task_come_before_the_close() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let run = served(t, runtimes(&runtime), None, |_| {}, |_| {});
    let (worker, sent) = (run.worker, run.sent);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![
            append_cmd(1, "s", &run_id, &["a"]).into(),
            close_cmd(1, "s", &run_id).into(),
        ],
    )
    .await;
    let appended = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(appends(&appended).len(), 1);
    complete(&worker, &appended, vec![]).await;
    let (closed, close) = until_closed(&worker).await;
    assert!(close.failed.is_none());
    complete(
        &worker,
        &closed,
        vec![append_cmd(2, "s", &run_id, &["b"]).into()],
    )
    .await;
    let refused = worker.poll_workflow_activation().await.unwrap();
    let Some(resolve_stream_append::Status::Failed(failure)) = &appends(&refused)[0].status else {
        panic!("The append after the close should fail");
    };
    assert!(
        failure.message.starts_with("stream_closed"),
        "{}",
        failure.message
    );
    worker.complete_execution(&refused.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    assert_eq!(texts(&all_records(&runtime, "s").await), ["a"]);
    let first_marker = marker(&sent.lock()[0]).unwrap();
    assert_eq!(first_marker.appends.len(), 1);
    assert_eq!(first_marker.closes.len(), 1);
}

/// A close of a Stream that is closed already is a success.
#[tokio::test]
async fn a_close_of_a_closed_stream_succeeds() {
    let runtime = runtime();
    create(&runtime, "s").await;
    close(&runtime, "s").await;
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let worker = served(t, runtimes(&runtime), None, |_| {}, |_| {}).worker;
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![close_cmd(1, "s", &run_id).into()]).await;
    let (next, close) = until_closed(&worker).await;
    assert!(close.failed.is_none(), "{:?}", close.failed);
    worker.complete_execution(&next.run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

/// A close of an older epoch than the fence of the Producer gives a failure.
#[tokio::test]
async fn a_refused_close_gives_a_failure() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let mut later = workflow_producer(&run_id);
    if let Some(Kind::Workflow(workflow)) = later.kind.as_mut() {
        workflow.epoch = Some(task_epoch(100));
    }
    runtime
        .append_records(AppendRecordsRequest {
            r#ref: Some(stream("s")),
            producer: Some(later),
            records: vec![AppendRecord {
                body: Some(body("later")),
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let run = served(t, runtimes(&runtime), None, |_| {}, |_| {});
    let (worker, sent) = (run.worker, run.sent);
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![close_cmd(1, "s", &run_id).into()]).await;
    let (next, close) = until_closed(&worker).await;
    let failure = close.failed.expect("the close should fail");
    assert!(
        failure.message.starts_with("stream_fenced"),
        "{}",
        failure.message
    );
    worker.complete_execution(&next.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    assert!(!stream_is_closed(&runtime, "s"));
    let result = &marker(&sent.lock()[1]).unwrap().closes[0];
    assert_eq!(result.failure.as_ref(), Some(&failure));
}

/// A close fails the Workflow Task when it has no Producer, or when the Worker has no runtime
/// for its Store.
#[tokio::test]
async fn a_close_with_no_producer_or_no_runtime_fails_the_task() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let no_producer = CloseStream {
        producer: None,
        ..close_cmd(1, "s", "r")
    };
    let elsewhere = CloseStream {
        stream: Some(StreamRef {
            id: "s".to_string(),
            store: "elsewhere".to_string(),
        }),
        ..close_cmd(1, "s", "r")
    };
    for (command, expected) in [
        (no_producer, "Stream close 1 has no Producer"),
        (
            elsewhere,
            "This Worker has no Stream runtime for the Store `elsewhere`",
        ),
    ] {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_workflow_task_scheduled_and_started();
        let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 1);
        let first = run.worker.poll_workflow_activation().await.unwrap();
        complete(&run.worker, &first, vec![command.into()]).await;
        run.worker.drain_pollers_and_shutdown().await;
        let failed = run.failed.lock();
        assert!(failed[0].1.contains(expected), "{}", failed[0].1);
    }
    assert!(!stream_is_closed(&runtime, "s"));
}

/// A Workflow that completes in the task of its close, without awaiting it, still closes the
/// Stream after the server took that task.
#[tokio::test]
async fn a_workflow_that_completes_with_its_close_still_closes_the_stream() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    // The mock poller stays alive, so that the Worker shuts down only when the test asks.
    let run = start_with(
        t,
        [ResponseType::AllHistory],
        runtimes(&runtime),
        1,
        0,
        true,
    );
    let (worker, sent) = (run.worker, run.sent);
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![
            close_cmd(1, "s", &run_id).into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    )
    .await;
    wait_closed(&runtime, "s").await;
    worker.drain_pollers_and_shutdown().await;
    let sent = sent.lock();
    assert_eq!(
        command_types(&sent[0]),
        [
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );
    assert!(!sent[0].force_create_new_workflow_task);
}

fn intent_entry(seq: u32) -> StreamCloseEntry {
    StreamCloseEntry {
        seq,
        intent_epoch: Some(task_epoch(3)),
        ..Default::default()
    }
}

fn close_result_entry(seq: u32, failure: Option<Failure>) -> StreamCloseEntry {
    StreamCloseEntry {
        seq,
        resolved: true,
        failure,
        activation: 0,
        ..Default::default()
    }
}

fn closes_only(closes: Vec<StreamCloseEntry>) -> StreamMarkerData {
    StreamMarkerData {
        reads: vec![],
        appends: vec![],
        closes,
        streams: vec![stream("s")],
    }
}

/// A history whose first task recorded `intent`, whose second one, if any, gave `result`, and
/// whose last task a signal starts.
fn close_history(intent: StreamCloseEntry, result: Option<StreamCloseEntry>) -> TestHistoryBuilder {
    close_history_started(
        |t| {
            t.add_by_type(EventType::WorkflowExecutionStarted);
        },
        intent,
        result,
    )
}

/// As [close_history], with `started` adding the first event of the history.
fn close_history_started(
    started: impl FnOnce(&mut TestHistoryBuilder),
    intent: StreamCloseEntry,
    result: Option<StreamCloseEntry>,
) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    started(&mut t);
    t.add_full_wf_task();
    stream_marker_event(&mut t, 4, &closes_only(vec![intent]));
    if let Some(result) = result {
        t.add_full_wf_task();
        stream_marker_event(&mut t, 8, &closes_only(vec![result]));
    }
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_and_started();
    t
}

/// A Worker that is gone after History recorded the intent of a close. A new Worker replays the
/// intent, sends the close with the epoch of the intent's task, and gives the result.
#[tokio::test]
async fn a_new_worker_sends_a_recorded_close() {
    let closes = Arc::new(Mutex::new(vec![]));
    let store = WatchedStore {
        inner: TestStreamStore::default(),
        closes: closes.clone(),
    };
    let runtime = StreamRuntime::new(Arc::new(store), None);
    create(&runtime, "s").await;
    let t = close_history(intent_entry(1), None);
    let run_id = t.get_orig_run_id().to_string();
    let (worker, sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);
    let first = worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    complete(&worker, &first, vec![close_cmd(1, "s", &run_id).into()]).await;
    let (next, close) = until_closed(&worker).await;
    assert!(close.failed.is_none(), "{:?}", close.failed);
    assert!(stream_is_closed(&runtime, "s"));
    worker.complete_execution(&next.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    let sent = sent.lock();
    let data = marker(&sent[0]).unwrap();
    assert_eq!(data.closes.len(), 1);
    assert!(data.closes[0].resolved);
    let Some(Kind::Workflow(sent_by)) = closes.lock()[0].kind.clone() else {
        panic!("the close had no Workflow Producer");
    };
    assert_eq!(
        sent_by.epoch,
        Some(task_epoch(3)),
        "the close has another epoch"
    );
}

/// A close that a new Worker sends again goes in front of an append that Workflow code issued in
/// a later task, so the append gets `stream_closed`, as on the first Worker.
#[tokio::test]
async fn a_resent_close_goes_before_a_later_append() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    stream_marker_event(&mut t, 4, &closes_only(vec![intent_entry(1)]));
    t.add_we_signaled("go", vec![]);
    // The task of the append completed with the append in flight, so it has no marker.
    t.add_full_wf_task();
    t.add_we_signaled("again", vec![]);
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let (worker, _sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![close_cmd(1, "s", &run_id).into()]).await;
    let go = worker.poll_workflow_activation().await.unwrap();
    assert!(signaled(&go) && go.is_replaying);
    complete(
        &worker,
        &go,
        vec![append_cmd(1, "s", &run_id, &["late"]).into()],
    )
    .await;
    let (mut appended, mut closed) = (None, None);
    let last = loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        appended = appended.or(appends(&next).pop());
        closed = closed.or(closes(&next).pop());
        if appended.is_some() && closed.is_some() {
            break next;
        }
        complete(&worker, &next, vec![]).await;
    };
    worker.complete_execution(&last.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    let records = texts(&all_records(&runtime, "s").await);
    assert!(
        matches!(
            appended.unwrap().status,
            Some(resolve_stream_append::Status::Failed(ref failure))
                if failure.message.starts_with("stream_closed")
        ),
        "the append wasn't refused, and the Store has {records:?}"
    );
    assert!(closed.unwrap().failed.is_none());
    assert!(records.is_empty() && stream_is_closed(&runtime, "s"));
}

/// A replay of a History with an intent and its result calls no Store, and gives the result in
/// the activation that the marker has.
#[tokio::test]
async fn a_replay_of_a_close_and_its_result_calls_no_store() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let t = close_history(intent_entry(1), Some(close_result_entry(1, None)));
    let run_id = t.get_orig_run_id().to_string();
    let (worker, _sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![close_cmd(1, "s", &run_id).into()]).await;
    let replayed = worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    assert_eq!(closes(&replayed).len(), 1);
    assert_eq!(replayed.jobs.len(), 1);
    complete(&worker, &replayed, vec![]).await;
    let live = worker.poll_workflow_activation().await.unwrap();
    assert!(signaled(&live) && closes(&live).is_empty());
    worker.complete_execution(&live.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    assert!(
        !stream_is_closed(&runtime, "s"),
        "the replay closed the Stream"
    );
}

/// A replay Worker with an intent and no result sends nothing, and the close stays open there.
#[tokio::test]
async fn a_replay_worker_sends_no_recorded_close() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let t = close_history(intent_entry(1), None);
    let run_id = t.get_orig_run_id().to_string();
    let worker = replay_worker(t, runtimes(&runtime));
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![close_cmd(1, "s", &run_id).into()]).await;
    let jobs = activations_until_the_end(&worker).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), worker.drain_pollers_and_shutdown()).await;
    assert!(
        jobs.iter().any(|job| job.starts_with("SignalWorkflow")),
        "{jobs:?}"
    );
    assert!(
        !jobs.iter().any(|job| job.starts_with("ResolveStreamClose")),
        "{jobs:?}"
    );
    assert!(!stream_is_closed(&runtime, "s"));
}

/// A refused close replays its failure from History, with no Store call.
#[tokio::test]
async fn a_refused_close_replays_its_failure() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let failure = Failure {
        message: "stream_fenced: the epoch is lower than the newest one of the Producer"
            .to_string(),
        ..Default::default()
    };
    let t = close_history(
        intent_entry(1),
        Some(close_result_entry(1, Some(failure.clone()))),
    );
    let run_id = t.get_orig_run_id().to_string();
    let (worker, _sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![close_cmd(1, "s", &run_id).into()]).await;
    let replayed = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(closes(&replayed)[0].failed.as_ref(), Some(&failure));
    complete(&worker, &replayed, vec![]).await;
    let live = worker.poll_workflow_activation().await.unwrap();
    worker.complete_execution(&live.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    assert!(!stream_is_closed(&runtime, "s"));
}

/// Replays a History with the intent of close 1 of Stream `s`, with Workflow code that sends
/// `commands` in its first activation. Returns the cause and message of the failed task.
async fn replay_close_history(
    commands: impl FnOnce(&str) -> Vec<workflow_command::Variant>,
) -> (WorkflowTaskFailedCause, String) {
    let runtime = runtime();
    create(&runtime, "s").await;
    let t = close_history(intent_entry(1), None);
    let run_id = t.get_orig_run_id().to_string();
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 1);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, commands(&run_id)).await;
    run.worker.drain_pollers_and_shutdown().await;
    let failed = run.failed.lock();
    assert_eq!(failed.len(), 1, "the replay should fail");
    failed[0].clone()
}

/// An intent in History with no close in Workflow code is a non-determinism error.
#[tokio::test]
async fn a_close_entry_with_no_call_is_nondeterministic() {
    let (cause, message) = replay_close_history(|_| vec![]).await;
    assert_eq!(cause, WorkflowTaskFailedCause::NonDeterministicError);
    assert!(
        message.contains("History has a Stream result of close 1 that Workflow code never got"),
        "{message}"
    );
}

/// A close of another Stream than History has is a non-determinism error.
#[tokio::test]
async fn a_close_of_another_stream_is_nondeterministic() {
    let (cause, message) =
        replay_close_history(|run_id| vec![close_cmd(1, "other", run_id).into()]).await;
    assert_eq!(cause, WorkflowTaskFailedCause::NonDeterministicError);
    assert!(
        message.contains("Stream close 1 closes Stream `other`"),
        "{message}"
    );
}

/// A replayed close with no Store name takes the Store of its entry, so a replay Worker with no
/// runtime replays it.
#[tokio::test]
async fn a_replayed_close_with_no_store_name_takes_the_store_of_history() {
    let t = close_history(intent_entry(1), Some(close_result_entry(1, None)));
    let run_id = t.get_orig_run_id().to_string();
    let worker = replay_worker(t, StreamRuntimes::default());
    let first = worker.poll_workflow_activation().await.unwrap();
    let close = CloseStream {
        stream: unnamed("s"),
        ..close_cmd(1, "s", &run_id)
    };
    complete(&worker, &first, vec![close.into()]).await;
    let jobs = activations_until_the_end(&worker).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), worker.drain_pollers_and_shutdown()).await;
    assert!(
        jobs.iter().any(|job| job == "ResolveStreamClose(1)"),
        "{jobs:?}"
    );
    assert!(
        jobs.iter().any(|job| job.starts_with("SignalWorkflow")),
        "{jobs:?}"
    );
    assert!(!has_failure_eviction(&jobs), "{jobs:?}");
}

/// A Worker with a meter, on a history of one Workflow Task, whose poller stays alive.
fn metered(
    t: TestHistoryBuilder,
    responses: impl IntoIterator<Item = ResponseType>,
    runtimes: StreamRuntimes,
) -> (Run, Arc<CounterRecordingMeter>) {
    let meter = Arc::new(CounterRecordingMeter::default());
    let run = start_metered(t, responses, runtimes, 1, 0, true, Some(&meter));
    (run, meter)
}

fn one_task_history() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t
}

/// The labels of the Store `mem`.
const MEM: &[(&str, &str)] = &[("stream_store", STORE)];

/// The total of each counter, in the order of `names`, for the Store `mem`.
fn totals(meter: &CounterRecordingMeter, names: &[&str]) -> Vec<u64> {
    names
        .iter()
        .map(|name| meter.counter_total(name, MEM))
        .collect()
}

/// Polls and completes activations with no command until every Stream call of the run has its
/// result, and returns the last activation.
async fn until_resolved(worker: &Worker, appends: usize) -> WorkflowActivation {
    let mut appended = 0;
    loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        appended += self::appends(&next).len();
        if appended == appends {
            return next;
        }
        complete(worker, &next, vec![]).await;
    }
}

/// An append counts once when Core sends it, and its latency gets one value.
#[tokio::test]
async fn an_append_counts_its_start_and_its_latency() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let (run, meter) = metered(t, [ResponseType::AllHistory], runtimes(&runtime));
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(
        &run.worker,
        &first,
        vec![append_cmd(1, "s", &run_id, &["a"]).into()],
    )
    .await;
    let last = until_resolved(&run.worker, 1).await;
    run.worker.complete_execution(&last.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(
        totals(&meter, &["stream_append_total", "stream_append_failed"]),
        [1, 0]
    );
    assert_eq!(meter.duration_count("stream_append_latency", MEM), 1);
}

/// A close counts once when Core sends it, after the server took the task of its intent, and its
/// latency gets one value.
#[tokio::test]
async fn a_close_counts_its_start_and_its_latency() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let meter = Arc::new(CounterRecordingMeter::default());
    let run = served(t, runtimes(&runtime), Some(&meter), |_| {}, |_| {});
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![close_cmd(1, "s", &run_id).into()]).await;
    let (next, close) = until_closed(&run.worker).await;
    assert!(close.failed.is_none());
    run.worker.complete_execution(&next.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(
        totals(&meter, &["stream_close_total", "stream_close_failed"]),
        [1, 0]
    );
    assert_eq!(meter.duration_count("stream_close_latency", MEM), 1);
}

/// An append and a close that fail each count as failed.
#[tokio::test]
async fn each_failed_call_counts_as_failed() {
    let runtime = runtime();
    create(&runtime, "closed").await;
    close(&runtime, "closed").await;
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let (run, meter) = metered(t, [ResponseType::AllHistory], runtimes(&runtime));
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(
        &run.worker,
        &first,
        vec![append_cmd(1, "closed", &run_id, &["a"]).into()],
    )
    .await;
    let last = until_resolved(&run.worker, 1).await;
    run.worker.complete_execution(&last.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(
        totals(&meter, &["stream_append_total", "stream_append_failed"]),
        [1, 1]
    );

    // A write of a later Workflow Task of the run raises the fence over the task of the close.
    create(&runtime, "fenced").await;
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let mut later = workflow_producer(&run_id);
    if let Some(Kind::Workflow(workflow)) = later.kind.as_mut() {
        workflow.epoch = Some(task_epoch(100));
    }
    runtime
        .append_records(AppendRecordsRequest {
            r#ref: Some(stream("fenced")),
            producer: Some(later),
            records: vec![AppendRecord {
                body: Some(body("later")),
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let meter = Arc::new(CounterRecordingMeter::default());
    let run = served(t, runtimes(&runtime), Some(&meter), |_| {}, |_| {});
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(
        &run.worker,
        &first,
        vec![close_cmd(1, "fenced", &run_id).into()],
    )
    .await;
    let (next, close) = until_closed(&run.worker).await;
    assert!(close.failed.is_some());
    run.worker.complete_execution(&next.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(
        totals(&meter, &["stream_close_total", "stream_close_failed"]),
        [1, 1]
    );
}

/// An append and a close that a replay gives from History count nothing.
#[tokio::test]
async fn replayed_calls_count_nothing() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let meter = Arc::new(CounterRecordingMeter::default());

    let appended = StreamAppendEntry {
        seq: 1,
        stream_index: 0,
        first_offset: 7u64.to_be_bytes().to_vec(),
        last_offset: 8u64.to_be_bytes().to_vec(),
        failure: None,
        activation: 1,
    };
    let append_history = replayed_history(&StreamMarkerData {
        reads: vec![],
        appends: vec![appended],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let append_run = append_history.get_orig_run_id().to_string();
    let close_events = close_history(intent_entry(1), Some(close_result_entry(1, None)));
    let close_run = close_events.get_orig_run_id().to_string();
    for (t, command) in [
        (
            append_history,
            append_cmd(1, "s", &append_run, &["x"]).into(),
        ),
        (close_events, close_cmd(1, "s", &close_run).into()),
    ] {
        let run = start_metered(
            t,
            [ResponseType::AllHistory],
            runtimes(&runtime),
            1,
            0,
            true,
            Some(&meter),
        );
        let first = run.worker.poll_workflow_activation().await.unwrap();
        complete(&run.worker, &first, vec![command]).await;
        let mut next = run.worker.poll_workflow_activation().await.unwrap();
        while next.is_replaying {
            complete(&run.worker, &next, vec![]).await;
            next = run.worker.poll_workflow_activation().await.unwrap();
        }
        assert!(signaled(&next));
        run.worker.complete_execution(&next.run_id).await;
        run.worker.drain_pollers_and_shutdown().await;
    }
    // The meter took the metrics of the two Workers.
    assert_eq!(meter.counter_total("worker_start", &[]), 2);
    let names = ["stream_append_total", "stream_close_total"];
    assert_eq!(totals(&meter, &names), [0; 2]);
    assert!(
        names
            .iter()
            .all(|name| meter.counter_labels(name).is_empty()),
        "a replay counted a call"
    );
}

/// The Stream metrics have the attributes of the run's Local Activity metrics but the Activity
/// type, plus the Store of the call, and never a Stream id, a Producer id or an offset.
#[tokio::test]
async fn stream_metrics_have_the_store_of_the_call() {
    let mem = runtime();
    create(&mem, "s").await;
    let other = StreamRuntime::new(Arc::new(TestStreamStore::default()), None);
    let in_other = StreamRef {
        id: "s".to_string(),
        store: "other".to_string(),
    };
    other
        .create_stream(CreateStreamRequest {
            stream_ref: Some(in_other.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let runtimes = StreamRuntimes::new([
        (STORE.to_string(), mem.clone()),
        ("other".to_string(), other.clone()),
    ]);
    let (run, meter) = metered(t, [ResponseType::AllHistory], runtimes);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let to_other = AppendToStream {
        stream: Some(in_other),
        ..append_cmd(2, "s", &run_id, &["b"])
    };
    complete(
        &run.worker,
        &first,
        vec![append_cmd(1, "s", &run_id, &["a"]).into(), to_other.into()],
    )
    .await;
    let last = until_resolved(&run.worker, 2).await;
    run.worker.complete_execution(&last.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    for store in [STORE, "other"] {
        let labels = [("stream_store", store)];
        assert_eq!(
            meter.counter_total("stream_append_total", &labels),
            1,
            "{store}"
        );
        assert_eq!(
            meter.duration_count("stream_append_latency", &labels),
            1,
            "{store}"
        );
    }
    let expected = HashSet::from(["namespace", "task_queue", "workflow_type", "stream_store"]);
    for labels in meter.counter_labels("stream_append_total") {
        let keys: HashSet<&str> = labels.keys().map(String::as_str).collect();
        assert_eq!(keys, expected, "{labels:?}");
        assert_eq!(labels["workflow_type"], "default_wf_type", "{labels:?}");
    }
}

/// A test Store that holds each close, and each append if `appends` is set, until the test
/// lets it go.
struct HeldCloseStore {
    inner: TestStreamStore,
    /// Told when a held call reaches the Store.
    held: Arc<Notify>,
    /// One permit lets one held call go.
    release: Arc<Semaphore>,
    appends: bool,
}

impl HeldCloseStore {
    async fn hold(&self) {
        self.held.notify_one();
        self.release
            .acquire()
            .await
            .expect("the test never closes the semaphore")
            .forget();
    }
}

#[async_trait::async_trait]
impl StreamStore for HeldCloseStore {
    async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        self.inner.create_stream(request).await
    }

    async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        if self.appends {
            self.hold().await;
        }
        self.inner.append_records(request).await
    }

    async fn read_records(&self, request: ReadRecordsRequest) -> StreamResult<ReadRecordsResponse> {
        self.inner.read_records(request).await
    }

    async fn close_stream(&self, request: CloseStreamRequest) -> StreamResult<CloseStreamResponse> {
        self.hold().await;
        self.inner.close_stream(request).await
    }

    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        self.inner.describe_stream(request).await
    }
}

/// A Store runtime that holds each close, with what tells the test about it.
struct Held {
    runtime: Arc<StreamRuntime>,
    held: Arc<Notify>,
    release: Arc<Semaphore>,
    /// Told each time a graceful shutdown waits for a close in flight.
    shutdown_waits: Arc<Notify>,
}

impl Held {
    async fn new() -> Self {
        Self::holding(false).await
    }

    /// A Store that holds each close, and each append if `appends` is set.
    async fn holding(appends: bool) -> Self {
        let held = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let store = HeldCloseStore {
            inner: TestStreamStore::default(),
            held: held.clone(),
            release: release.clone(),
            appends,
        };
        let runtime = StreamRuntime::new(Arc::new(store), None);
        create(&runtime, "s").await;
        Self {
            runtime,
            held,
            release,
            shutdown_waits: Arc::new(Notify::new()),
        }
    }

    fn runtimes(&self) -> StreamRuntimes {
        runtimes(&self.runtime)
    }

    /// A Worker on `t` that runs on this Store. The test hooks tell the test each time a graceful
    /// shutdown waits for a close in flight, with `close_bound` in place of the real bound.
    fn start(
        &self,
        t: TestHistoryBuilder,
        close_bound: Option<Duration>,
        meter: Option<&Arc<CounterRecordingMeter>>,
    ) -> Run {
        let hooks = StreamTestHooks {
            close_bound,
            shutdown_waits: Some(self.shutdown_waits.clone()),
            ..Default::default()
        };
        start_configured(
            t,
            [ResponseType::AllHistory],
            self.runtimes(),
            (1, 0, true),
            meter,
            |wc| wc.stream_test_hooks = hooks,
        )
    }
}

/// Runs a graceful shutdown of `worker` while the Store holds its close. The shutdown must wait
/// for the close, and complete once the test lets the close go.
async fn shutdown_waits_for_the_held_close(worker: Worker, held: &Held) {
    let mut shutdown = Box::pin(worker.drain_pollers_and_shutdown());
    tokio::select! {
        _ = &mut shutdown => panic!("the shutdown completed while the Store held the close"),
        _ = held.shutdown_waits.notified() => {}
    }
    assert!(!stream_is_closed(&held.runtime, "s"));
    held.release.add_permits(1);
    shutdown.await;
    assert!(stream_is_closed(&held.runtime, "s"));
}

/// A Workflow that completes in the task of a close that it doesn't await: a graceful shutdown
/// waits until the Store answered the close.
#[tokio::test]
async fn a_shutdown_waits_for_a_close_of_a_completed_workflow() {
    let held = Held::new().await;
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let run = held.start(t, None, None);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(
        &run.worker,
        &first,
        vec![
            close_cmd(1, "s", &run_id).into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    )
    .await;
    held.held.notified().await;
    shutdown_waits_for_the_held_close(run.worker, &held).await;
    let sent = run.sent.lock();
    assert_eq!(
        command_types(&sent[0]),
        [
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );
}

/// An awaited close whose result has no Workflow Task yet: a graceful shutdown waits until the
/// Store answered the close.
#[tokio::test]
async fn a_shutdown_waits_for_an_awaited_close() {
    let held = Held::new().await;
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let run = held.start(t, None, None);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![close_cmd(1, "s", &run_id).into()]).await;
    held.held.notified().await;
    shutdown_waits_for_the_held_close(run.worker, &held).await;
    let sent = run.sent.lock();
    assert!(sent[0].force_create_new_workflow_task);
    assert!(!marker(&sent[0]).unwrap().closes[0].resolved);
}

/// A Store that never answers a close doesn't hold a graceful shutdown past the bound. The close
/// is given up, and the Stream stays open.
#[tokio::test]
async fn a_shutdown_gives_up_a_close_past_its_bound() {
    let held = Held::new().await;
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    // A short bound keeps the test quick. The two tests before this one check the wait.
    let meter = Arc::new(CounterRecordingMeter::default());
    let run = held.start(t, Some(Duration::from_secs(1)), Some(&meter));
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(
        &run.worker,
        &first,
        vec![
            close_cmd(1, "s", &run_id).into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    )
    .await;
    held.held.notified().await;
    run.worker.drain_pollers_and_shutdown().await;
    assert!(!stream_is_closed(&held.runtime, "s"));
    assert_eq!(
        totals(&meter, &["stream_close_total", "stream_close_abandoned"]),
        [1, 1]
    );
}

/// A task with several appends to two Streams writes each Stream ref once in its marker, and the
/// replay of that marker gives each append its own result.
#[tokio::test]
async fn a_marker_has_each_stream_once() {
    let runtime = runtime();
    create(&runtime, "s").await;
    create(&runtime, "t").await;
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let run = start_with(
        t,
        [ResponseType::AllHistory],
        runtimes(&runtime),
        1,
        0,
        true,
    );
    let commands = |run_id: &str| -> Vec<workflow_command::Variant> {
        vec![
            append_cmd(1, "s", run_id, &["a"]).into(),
            append_cmd(2, "t", run_id, &["b"]).into(),
            append_cmd(3, "s", run_id, &["c"]).into(),
            append_cmd(4, "t", run_id, &["d"]).into(),
        ]
    };
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, commands(&run_id)).await;
    let (live, last) = all_appends(&run.worker, 4).await;
    run.worker.complete_execution(&last.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    let data = marker(run.sent.lock().last().unwrap()).unwrap();
    let mut ids: Vec<_> = data.streams.iter().map(|s| s.id.as_str()).collect();
    ids.sort();
    assert_eq!(ids, ["s", "t"]);
    assert!(data.streams.iter().all(|s| s.store == STORE));
    let mut entries: Vec<_> = data
        .appends
        .iter()
        .map(|entry| {
            (
                entry.seq,
                data.streams[entry.stream_index as usize].id.as_str(),
            )
        })
        .collect();
    entries.sort();
    assert_eq!(entries, [(1, "s"), (2, "t"), (3, "s"), (4, "t")]);

    let replayed = replayed_history(&data);
    let run_id = replayed.get_orig_run_id().to_string();
    let run = start_with(
        replayed,
        [ResponseType::AllHistory],
        runtimes(&runtime),
        1,
        0,
        true,
    );
    let first = run.worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    complete(&run.worker, &first, commands(&run_id)).await;
    let (replayed, last) = all_appends(&run.worker, 4).await;
    assert_eq!(replayed, live);
    complete(&run.worker, &last, vec![]).await;
    let signal = run.worker.poll_workflow_activation().await.unwrap();
    assert!(signaled(&signal) && !signal.is_replaying);
    run.worker.complete_execution(&signal.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(texts(&all_records(&runtime, "s").await), ["a", "c"]);
}

/// The result of each append, by seq.
type AppendResults = Vec<(u32, Option<resolve_stream_append::Status>)>;

/// Polls until `count` appends have their results. Returns each result by seq, and the
/// activation of the last one, which the caller completes.
async fn all_appends(worker: &Worker, count: usize) -> (AppendResults, WorkflowActivation) {
    let mut results = vec![];
    loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        results.extend(appends(&next).into_iter().map(|a| (a.seq, a.status)));
        if results.len() == count {
            results.sort_by_key(|(seq, _)| *seq);
            return (results, next);
        }
        complete(worker, &next, vec![]).await;
    }
}

/// A test Store that keeps the Producer of each close.
struct WatchedStore {
    inner: TestStreamStore,
    closes: Arc<Mutex<Vec<Producer>>>,
}

#[async_trait::async_trait]
impl StreamStore for WatchedStore {
    async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        self.inner.create_stream(request).await
    }

    async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        self.inner.append_records(request).await
    }

    async fn read_records(&self, request: ReadRecordsRequest) -> StreamResult<ReadRecordsResponse> {
        self.inner.read_records(request).await
    }

    async fn close_stream(&self, request: CloseStreamRequest) -> StreamResult<CloseStreamResponse> {
        self.closes.lock().extend(request.producer.clone());
        self.inner.close_stream(request).await
    }

    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        self.inner.describe_stream(request).await
    }
}

/// A cache-full eviction of a run whose append is in flight stops the append, and the Worker
/// takes the task of the run that needed the cache slot.
#[tokio::test]
async fn a_cache_full_eviction_stops_a_call_in_flight() {
    let held = Held::holding(true).await;
    let first = one_task_history();
    let first_run = first.get_orig_run_id().to_string();
    let second = one_task_history();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let gate = async move {
        released.await.ok();
    }
    .boxed();
    let mut mock = crate::test_help::build_multihist_mock_sg(
        [
            crate::test_help::FakeWfResponses {
                wf_id: "first".to_string(),
                hist: first,
                response_batches: vec![ResponseType::AllHistory],
            },
            crate::test_help::FakeWfResponses {
                wf_id: "second".to_string(),
                hist: second,
                response_batches: vec![ResponseType::UntilResolved(gate, 1)],
            },
        ],
        false,
        0,
    );
    mock.make_wft_stream_interminable();
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 1;
        wc.stream_runtimes = held.runtimes();
    });
    let worker = mock_worker(mock);
    let start = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(start.run_id, first_run);
    complete(
        &worker,
        &start,
        vec![append_cmd(1, "s", &first_run, &["a"]).into()],
    )
    .await;
    // The append waits in the Store, and holds the first run's task open.
    held.held.notified().await;
    release.send(()).unwrap();
    let evicted = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(evicted.run_id, first_run);
    assert!(evicted.is_only_eviction(), "{:?}", evicted.jobs);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(evicted.run_id))
        .await
        .unwrap();
    let other = worker.poll_workflow_activation().await.unwrap();
    assert_ne!(other.run_id, first_run);
    worker.complete_execution(&other.run_id).await;
    // The append's task is gone, so letting the Store go on writes nothing.
    held.release.add_permits(1);
    worker.drain_pollers_and_shutdown().await;
    assert!(all_records(&held.runtime, "s").await.is_empty());
}

/// What makes a [FlakyStore] fail, and what reached it.
#[derive(Default)]
struct Flaky {
    /// The next appends that fail with `StorageUnavailable`. A test sets `usize::MAX` to fail every
    /// append, and zero to turn the failures off.
    fail_appends: AtomicUsize,
    /// The next closes that fail with `StorageUnavailable`, set as `fail_appends` is.
    fail_closes: AtomicUsize,
    /// True if a failed append writes its batch first, as when only the Store's answer is lost.
    lose_answers: AtomicBool,
    /// The appends that reached the Store.
    appends: AtomicUsize,
    /// The closes that reached the Store.
    closes: AtomicUsize,
}

impl Flaky {
    /// Takes one failure from `left`, if it has one.
    fn fails(left: &AtomicUsize) -> StreamResult<()> {
        let mut now = left.load(Ordering::Acquire);
        while now > 0 {
            match left.compare_exchange_weak(now, now - 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    return Err(StreamError::new(
                        Reason::StorageUnavailable,
                        "the Store is down",
                    ));
                }
                Err(actual) => now = actual,
            }
        }
        Ok(())
    }
}

/// A test Store whose next appends or closes fail with `StorageUnavailable`.
struct FlakyStore {
    inner: TestStreamStore,
    flaky: Arc<Flaky>,
}

#[async_trait::async_trait]
impl StreamStore for FlakyStore {
    async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        self.inner.create_stream(request).await
    }

    async fn append_records(
        &self,
        request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        self.flaky.appends.fetch_add(1, Ordering::AcqRel);
        if self.flaky.lose_answers.load(Ordering::Acquire) {
            let appended = self.inner.append_records(request).await?;
            Flaky::fails(&self.flaky.fail_appends)?;
            return Ok(appended);
        }
        Flaky::fails(&self.flaky.fail_appends)?;
        self.inner.append_records(request).await
    }

    async fn read_records(&self, request: ReadRecordsRequest) -> StreamResult<ReadRecordsResponse> {
        self.inner.read_records(request).await
    }

    async fn close_stream(&self, request: CloseStreamRequest) -> StreamResult<CloseStreamResponse> {
        self.flaky.closes.fetch_add(1, Ordering::AcqRel);
        Flaky::fails(&self.flaky.fail_closes)?;
        self.inner.close_stream(request).await
    }

    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        self.inner.describe_stream(request).await
    }
}

/// A runtime on a [FlakyStore] with Stream `s`, and what makes the Store fail.
async fn flaky_runtime() -> (Arc<StreamRuntime>, Arc<Flaky>) {
    let flaky = Arc::new(Flaky::default());
    let store = FlakyStore {
        inner: TestStreamStore::default(),
        flaky: flaky.clone(),
    };
    let runtime = StreamRuntime::new(Arc::new(store), None);
    create(&runtime, "s").await;
    (runtime, flaky)
}

/// The failure of an append that failed.
fn append_failure(append: &ResolveStreamAppend) -> Failure {
    let Some(resolve_stream_append::Status::Failed(failure)) = &append.status else {
        panic!("The append should fail: {append:?}");
    };
    failure.clone()
}

fn failure_info(failure: &Failure) -> &ApplicationFailureInfo {
    let Some(FailureInfo::ApplicationFailureInfo(info)) = &failure.failure_info else {
        panic!("The failure has no application info: {failure:?}");
    };
    info
}

/// What an append of `a` and `b` gave on a Store whose first two answers were lost.
struct OutageAppend {
    runtime: Arc<StreamRuntime>,
    flaky: Arc<Flaky>,
    meter: Arc<CounterRecordingMeter>,
    offsets: (Vec<u8>, Vec<u8>),
    marker: StreamMarkerData,
}

/// Appends `a` and `b` on a Store that writes the first two tries and loses their answers.
async fn append_through_an_outage() -> OutageAppend {
    let (runtime, flaky) = flaky_runtime().await;
    flaky.lose_answers.store(true, Ordering::Release);
    flaky.fail_appends.store(2, Ordering::Release);
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let (run, meter) = metered(t, [ResponseType::AllHistory], runtimes(&runtime));
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(
        &run.worker,
        &first,
        vec![append_cmd(1, "s", &run_id, &["a", "b"]).into()],
    )
    .await;
    let result = run.worker.poll_workflow_activation().await.unwrap();
    let Some(resolve_stream_append::Status::Appended(appended)) = &appends(&result)[0].status
    else {
        panic!("The append failed: {:?}", appends(&result));
    };
    let offsets = (appended.first_offset.clone(), appended.last_offset.clone());
    run.worker.complete_execution(&result.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    let marker = marker(&run.sent.lock()[0]).unwrap();
    OutageAppend {
        runtime,
        flaky,
        meter,
        offsets,
        marker,
    }
}

/// An append whose first two tries reach the Store and lose their answers goes on. The Store
/// takes the resends once, Workflow code gets the offsets of the first write, and the marker and
/// the metrics have one append that didn't fail.
#[tokio::test]
async fn an_append_retries_a_store_outage() {
    let done = append_through_an_outage().await;
    assert_eq!(done.flaky.appends.load(Ordering::Acquire), 3);
    let records = all_records(&done.runtime, "s").await;
    assert_eq!(texts(&records), ["a", "b"]);
    assert_eq!(
        done.offsets,
        (records[0].offset.clone(), records[1].offset.clone())
    );
    assert_eq!(done.marker.appends.len(), 1);
    let entry = &done.marker.appends[0];
    assert_eq!(
        (entry.first_offset.clone(), entry.last_offset.clone()),
        done.offsets
    );
    assert!(entry.failure.is_none());
    assert_eq!(
        totals(
            &done.meter,
            &["stream_append_total", "stream_append_failed"]
        ),
        [1, 0]
    );
}

/// A replay of the marker of an append that the Store answered after an outage gives its offsets
/// and makes no append.
#[tokio::test]
async fn a_replay_of_a_retried_append_calls_no_store() {
    let done = append_through_an_outage().await;
    let before = done.flaky.appends.load(Ordering::Acquire);
    let t = replayed_history(&done.marker);
    let run_id = t.get_orig_run_id().to_string();
    let run = start(t, [ResponseType::AllHistory], runtimes(&done.runtime), 1, 0);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(
        &run.worker,
        &first,
        vec![append_cmd(1, "s", &run_id, &["a", "b"]).into()],
    )
    .await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    let Some(resolve_stream_append::Status::Appended(result)) = &appends(&replayed)[0].status
    else {
        panic!("The replayed append has no offsets");
    };
    assert_eq!(
        (result.first_offset.clone(), result.last_offset.clone()),
        done.offsets
    );
    complete(&run.worker, &replayed, vec![]).await;
    let live = run.worker.poll_workflow_activation().await.unwrap();
    run.worker.complete_execution(&live.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(done.flaky.appends.load(Ordering::Acquire), before);
    assert_eq!(all_records(&done.runtime, "s").await.len(), 2);
}

/// An append whose Store stays down for 40 s doesn't resolve, since a write has no bound in time.
/// Each failed try wrote its batch and lost the answer. Once the Store answers, the append
/// succeeds and the Store has one copy of its Records. Tokio's paused clock moves the time.
#[tokio::test]
async fn an_append_has_no_retry_bound() {
    let (runtime, flaky) = flaky_runtime().await;
    flaky.lose_answers.store(true, Ordering::Release);
    flaky.fail_appends.store(usize::MAX, Ordering::Release);
    // The task timeout is long, so that no heartbeat comes while the Store is down.
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(3600));
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let (worker, sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![append_cmd(1, "s", &run_id, &["a"]).into()],
    )
    .await;
    tokio::time::pause();
    let outage = Duration::from_secs(40);
    let started = tokio::time::Instant::now();
    let result = {
        let next = worker.poll_workflow_activation();
        tokio::pin!(next);
        tokio::select! {
            early = &mut next => panic!("The append resolved while the Store was down: {early:?}"),
            _ = tokio::time::sleep(outage) => {}
        }
        assert!(started.elapsed() >= outage);
        assert!(flaky.appends.load(Ordering::Acquire) > 10);
        flaky.fail_appends.store(0, Ordering::Release);
        next.await.unwrap()
    };
    let Some(resolve_stream_append::Status::Appended(appended)) = &appends(&result)[0].status
    else {
        panic!("The append failed: {:?}", appends(&result));
    };
    let offsets = (appended.first_offset.clone(), appended.last_offset.clone());
    worker.complete_execution(&result.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    let records = all_records(&runtime, "s").await;
    assert_eq!(texts(&records), ["a"]);
    assert_eq!(
        offsets,
        (records[0].offset.clone(), records[0].offset.clone())
    );
    let entry = &marker(&sent.lock()[0]).unwrap().appends[0];
    assert!(entry.failure.is_none());
}

/// Polls the activation that a failed Workflow Task causes, which must be an eviction, and
/// completes it. The wait has a bound, so that a task that didn't fail makes the test fail and not
/// hang.
async fn complete_eviction_of_failed_task(worker: &Worker) {
    let evicted = tokio::time::timeout(Duration::from_secs(30), worker.poll_workflow_activation())
        .await
        .expect("The task didn't fail at its heartbeat")
        .unwrap();
    assert!(evicted.is_only_eviction(), "{:?}", evicted.jobs);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(evicted.run_id))
        .await
        .unwrap();
}

/// A Workflow Task whose append is in a Store outage at the heartbeat fails. Core forces no task,
/// Workflow code gets no result of the append, and the Store gets no more tries after the failure.
#[tokio::test]
async fn an_append_in_an_outage_fails_its_task_at_the_heartbeat() {
    let (runtime, flaky) = flaky_runtime().await;
    flaky.fail_appends.store(usize::MAX, Ordering::Release);
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(300));
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 1);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(
        &run.worker,
        &first,
        vec![append_cmd(1, "s", &run_id, &["a"]).into()],
    )
    .await;
    // No result of the append comes before the eviction.
    complete_eviction_of_failed_task(&run.worker).await;
    until("the failed task", || run.failed.lock().len() == 1).await;
    let tries = flaky.appends.load(Ordering::Acquire);
    assert!(tries > 1, "{tries}");
    // The next wait of a try that went on is 400 ms, so another try would come inside this time.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        flaky.appends.load(Ordering::Acquire),
        tries,
        "The append went on after its task failed"
    );
    run.worker.drain_pollers_and_shutdown().await;

    let failed = run.failed.lock();
    assert_eq!(failed.len(), 1);
    assert_eq!(
        failed[0].0,
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
    );
    assert_eq!(
        failed[0].1,
        format!(
            "StreamStoreUnavailable: append of Stream \"s\" on Store \"{STORE}\": \
             stream_storage_unavailable: the Store is down"
        )
    );
    assert!(
        run.sent.lock().is_empty(),
        "A completion went to the server: {:?}",
        run.sent.lock()
    );
}

/// A graceful shutdown doesn't stop the tries of an append in a Store outage. The task fails at
/// its heartbeat, as at any other time, so Workflow code never gets the outage as an error of the
/// append, and no completion goes to the server. The shutdown ends with that failed task, a few
/// seconds at most after the task started. The heartbeat timer runs during a shutdown, with a
/// Local Activity manager and without one.
#[rstest::rstest]
#[case::with_a_local_activity_manager(false)]
#[case::with_no_local_activity_manager(true)]
#[tokio::test]
async fn a_shutdown_in_an_outage_fails_the_task_of_an_append(#[case] workflow_only: bool) {
    let (runtime, flaky) = flaky_runtime().await;
    flaky.fail_appends.store(usize::MAX, Ordering::Release);
    // The heartbeat comes at 800 ms, long after the shutdown starts.
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(1));
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let run = start_configured(
        t,
        [ResponseType::AllHistory],
        runtimes(&runtime),
        (1, 1, true),
        None,
        |wc| {
            if workflow_only {
                wc.task_types = WorkerTaskTypes::workflow_only();
            }
        },
    );
    let task_started = tokio::time::Instant::now();
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(
        &run.worker,
        &first,
        vec![append_cmd(1, "s", &run_id, &["a"]).into()],
    )
    .await;
    until("a failed try of the append", || {
        flaky.appends.load(Ordering::Acquire) >= 1
    })
    .await;
    run.worker.initiate_shutdown();
    assert!(
        run.failed.lock().is_empty(),
        "The task failed before the shutdown started"
    );
    // The drain panics on an activation that is not an eviction, so a result of the append that
    // reached Workflow code fails the test.
    tokio::time::timeout(
        Duration::from_secs(10),
        run.worker.drain_pollers_and_shutdown(),
    )
    .await
    .expect("The shutdown didn't end within 10 s");
    assert!(
        task_started.elapsed() < Duration::from_secs(5),
        "{:?}",
        task_started.elapsed()
    );

    let failed = run.failed.lock();
    assert_eq!(failed.len(), 1);
    assert_eq!(
        failed[0].0,
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
    );
    assert_eq!(
        failed[0].1,
        format!(
            "StreamStoreUnavailable: append of Stream \"s\" on Store \"{STORE}\": \
             stream_storage_unavailable: the Store is down"
        )
    );
    assert!(
        run.sent.lock().is_empty(),
        "A completion went to the server: {:?}",
        run.sent.lock()
    );
}

/// The server tries the task that an outage failed again. Once the Store works, the append runs
/// again with the epoch of the new task, and Workflow code gets its offsets.
#[tokio::test]
async fn a_task_that_an_outage_failed_runs_its_append_again() {
    let (runtime, flaky) = flaky_runtime().await;
    flaky.fail_appends.store(usize::MAX, Ordering::Release);
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(300));
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let responses = [1, 1].map(ResponseType::ToTaskNum);
    let (worker, sent) = worker(t, responses, &runtime, 1);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![append_cmd(1, "s", &run_id, &["x"]).into()],
    )
    .await;
    complete_eviction_of_failed_task(&worker).await;
    flaky.fail_appends.store(0, Ordering::Release);
    let again = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &again,
        vec![append_cmd(1, "s", &run_id, &["x"]).into()],
    )
    .await;
    let result = worker.poll_workflow_activation().await.unwrap();
    assert!(matches!(
        appends(&result)[0].status,
        Some(resolve_stream_append::Status::Appended(_))
    ));
    worker.complete_execution(&result.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let records = all_records(&runtime, "s").await;
    assert_eq!(texts(&records), ["x"]);
    let Some(Kind::Workflow(workflow)) = &records[0].producer.as_ref().unwrap().kind else {
        panic!("The Producer is not a Workflow");
    };
    assert_eq!(workflow.epoch.as_ref().unwrap().attempt, 2);
    assert!(
        sent.lock()
            .iter()
            .all(|sent| !sent.force_create_new_workflow_task)
    );
}

/// A task whose append the Store holds, with no answer and no error, still completes at its
/// heartbeat with a new task forced. A write that is only in flight never fails a task.
#[tokio::test]
async fn an_append_in_flight_at_the_heartbeat_does_not_fail_its_task() {
    let held = Held::holding(true).await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(300));
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let run = served(t, held.runtimes(), None, |_| {}, |_| {});
    let (worker, sent) = (run.worker, run.sent);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![append_cmd(1, "s", &run_id, &["a"]).into()],
    )
    .await;
    held.held.notified().await;
    let result = poll_after_heartbeat(&worker, &sent, || async {
        held.release.add_permits(1);
    })
    .await;
    let Some(resolve_stream_append::Status::Appended(appended)) = &appends(&result)[0].status
    else {
        panic!("The append failed: {:?}", appends(&result));
    };
    let records = all_records(&held.runtime, "s").await;
    assert_eq!(texts(&records), ["a"]);
    assert_eq!(appended.first_offset, records[0].offset);
    worker.complete_execution(&result.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    assert!(sent.lock()[0].force_create_new_workflow_task);
    assert!(run.failed.lock().is_empty());
}

/// A Workflow Task that waits for a close whose Store fails every try fails at its heartbeat, on a
/// Worker with no Local Activity manager too. The close stops with its run, so the manager has no
/// close in flight, and the Store gets no more tries.
#[tokio::test]
async fn a_close_in_an_outage_fails_its_task() {
    let (runtime, flaky) = flaky_runtime().await;
    flaky.fail_closes.store(usize::MAX, Ordering::Release);
    let t = close_history_started(
        |t| t.add_wfe_started_with_wft_timeout(Duration::from_millis(300)),
        intent_entry(1),
        None,
    );
    let run_id = t.get_orig_run_id().to_string();
    let waits = Arc::new(Notify::new());
    let hooks = StreamTestHooks {
        close_bound: Some(Duration::from_secs(1)),
        shutdown_waits: Some(waits.clone()),
        ..Default::default()
    };
    let run = start_configured(
        t,
        [ResponseType::AllHistory],
        runtimes(&runtime),
        (1, 1, true),
        None,
        |wc| {
            wc.task_types = WorkerTaskTypes::workflow_only();
            wc.stream_test_hooks = hooks;
        },
    );
    let first = run.worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    complete(&run.worker, &first, vec![close_cmd(1, "s", &run_id).into()]).await;
    // The signal starts the live task, which sends the close that History recorded.
    let live = run.worker.poll_workflow_activation().await.unwrap();
    assert!(signaled(&live) && !live.is_replaying);
    complete(&run.worker, &live, vec![]).await;
    complete_eviction_of_failed_task(&run.worker).await;
    until("the failed task", || run.failed.lock().len() == 1).await;
    let tries = flaky.closes.load(Ordering::Acquire);
    assert!(tries > 1, "{tries}");
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        flaky.closes.load(Ordering::Acquire),
        tries,
        "The close went on after its task failed"
    );
    run.worker.drain_pollers_and_shutdown().await;
    assert!(
        waits.notified().now_or_never().is_none(),
        "The shutdown waited for a close in flight"
    );

    let failed = run.failed.lock();
    assert_eq!(failed.len(), 1);
    assert_eq!(
        failed[0].0,
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
    );
    assert_eq!(
        failed[0].1,
        format!(
            "StreamStoreUnavailable: close of Stream \"s\" on Store \"{STORE}\": \
             stream_storage_unavailable: the Store is down"
        )
    );
    assert!(run.sent.lock().is_empty());
    assert!(!stream_is_closed(&runtime, "s"));
}

/// An append to a closed Stream resolves on its first try, with no retry.
#[tokio::test]
async fn a_closed_stream_fails_an_append_on_the_first_try() {
    let (runtime, flaky) = flaky_runtime().await;
    close(&runtime, "s").await;
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let (worker, _sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![append_cmd(1, "s", &run_id, &["a"]).into()],
    )
    .await;
    let result = worker.poll_workflow_activation().await.unwrap();
    let failure = append_failure(&appends(&result)[0]);
    assert_eq!(failure_info(&failure).r#type, "stream_closed");
    assert_eq!(flaky.appends.load(Ordering::Acquire), 1);
    worker.complete_execution(&result.run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

/// An append that goes to the Store with sequence zero isn't tried again, since the Store keeps no
/// dedupe for such a batch. Its first failure reaches Workflow code.
#[tokio::test]
async fn an_append_with_sequence_zero_is_not_retried() {
    let (runtime, flaky) = flaky_runtime().await;
    flaky.fail_appends.store(1, Ordering::Release);
    let t = one_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let (worker, _sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![append_cmd(0, "s", &run_id, &["a"]).into()],
    )
    .await;
    let result = worker.poll_workflow_activation().await.unwrap();
    let failure = append_failure(&appends(&result)[0]);
    assert_eq!(failure_info(&failure).r#type, "stream_storage_unavailable");
    assert_eq!(flaky.appends.load(Ordering::Acquire), 1);
    worker.complete_execution(&result.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    assert!(all_records(&runtime, "s").await.is_empty());
}

/// Checks that `failure` is the `stream_invalid` refusal of an input over `limit`.
fn assert_refused(failure: Option<&Failure>, what: &str, limit: usize) {
    let failure = failure.expect("the call has no failure");
    assert_eq!(failure_info(failure).r#type, "stream_invalid");
    let tail = format!("bytes, more than the limit of {limit}");
    assert!(
        failure
            .message
            .starts_with(&format!("stream_invalid: {what} has "))
            && failure.message.ends_with(&tail),
        "{}",
        failure.message
    );
}

/// Runs a Workflow on a test Store that issues `commands` in its first activation and ends in
/// the next. Returns that activation, the completion of the task and the counts of Store calls.
async fn refused_calls(
    commands: impl FnOnce(&str) -> Vec<workflow_command::Variant>,
) -> (WorkflowActivation, WorkflowTaskCompletion, Vec<u64>) {
    let runtime = runtime();
    create(&runtime, "s").await;
    let meter = Arc::new(CounterRecordingMeter::default());
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(3600));
    t.add_workflow_task_scheduled_and_started();
    let run = served(t, runtimes(&runtime), Some(&meter), |_| {}, |_| {});
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, commands(&first.run_id)).await;
    let refused = run.worker.poll_workflow_activation().await.unwrap();
    run.worker.complete_execution(&refused.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert!(run.failed.lock().is_empty(), "{:?}", run.failed.lock());
    let sent = run.sent.lock()[0].clone();
    let names = ["stream_append_total", "stream_close_total"];
    let calls = names
        .iter()
        .map(|name| meter.counter_total(name, &[]))
        .collect();
    (refused, sent, calls)
}

/// An append and a close of a Stream id over its limit are refused with `stream_invalid` in the
/// next activation, with no Store call and nothing in the marker.
#[tokio::test]
async fn a_stream_id_over_its_limit_is_refused() {
    let long = StreamRef {
        id: "x".repeat(MAX_STREAM_ID_BYTES + 1),
        ..stream("s")
    };
    let (refused, sent, calls) = refused_calls(|run_id| {
        vec![
            AppendToStream {
                stream: Some(long.clone()),
                ..append_cmd(1, "s", run_id, &["a"])
            }
            .into(),
            CloseStream {
                stream: Some(long.clone()),
                ..close_cmd(1, "s", run_id)
            }
            .into(),
        ]
    })
    .await;
    let append = appends(&refused).pop().unwrap();
    let Some(resolve_stream_append::Status::Failed(failed)) = &append.status else {
        panic!("{append:?}");
    };
    assert_refused(Some(failed), "The Stream id", MAX_STREAM_ID_BYTES);
    let close = closes(&refused).pop().unwrap();
    assert_refused(close.failed.as_ref(), "The Stream id", MAX_STREAM_ID_BYTES);
    assert!(marker(&sent).is_none(), "{:?}", marker(&sent));
    assert_eq!(calls, [0, 0]);
}

/// An append to a Store name over its limit is refused before the Worker looks for a runtime of
/// that name, so the Workflow Task doesn't fail.
#[tokio::test]
async fn a_store_name_over_its_limit_is_refused() {
    let long = StreamRef {
        store: "m".repeat(MAX_STORE_NAME_BYTES + 1),
        ..stream("s")
    };
    let (refused, sent, calls) = refused_calls(|run_id| {
        vec![
            AppendToStream {
                stream: Some(long.clone()),
                ..append_cmd(1, "s", run_id, &["a"])
            }
            .into(),
        ]
    })
    .await;
    let append = appends(&refused).pop().unwrap();
    let Some(resolve_stream_append::Status::Failed(failed)) = &append.status else {
        panic!("{append:?}");
    };
    assert_refused(Some(failed), "The Store name", MAX_STORE_NAME_BYTES);
    assert!(marker(&sent).is_none(), "{:?}", marker(&sent));
    assert_eq!(calls, [0, 0]);
}

/// The seqs of the append entries of a completion's marker.
fn marker_appends(completion: &WorkflowTaskCompletion) -> Vec<u32> {
    marker(completion).map_or(vec![], |data| {
        data.appends.iter().map(|entry| entry.seq).collect()
    })
}

/// Polls `worker` until Workflow code got the results of appends 1 to `count`, each once, and
/// completes the Workflow then. Returns the results of each activation.
async fn take_appends(worker: &Worker, count: u32) -> Vec<Vec<u32>> {
    let mut got = HashSet::new();
    let mut activations = vec![];
    loop {
        let next = tokio::time::timeout(Duration::from_secs(60), worker.poll_workflow_activation())
            .await
            .expect("no activation came")
            .unwrap();
        let seqs: Vec<u32> = appends(&next).iter().map(|append| append.seq).collect();
        for seq in &seqs {
            assert!(got.insert(*seq), "append {seq} came twice");
        }
        activations.push(seqs);
        if got.len() == count as usize {
            worker.complete_execution(&next.run_id).await;
            return activations;
        }
        complete(worker, &next, vec![]).await;
    }
}

/// A task with more append results than its marker has room for completes with a marker under the
/// entry bound and a forced new task, which takes the rest. Each append is in one marker, and a
/// replay of the tasks gives each result in the activation of the first run.
#[tokio::test]
async fn many_appends_keep_each_marker_under_the_entry_bound() {
    const COUNT: u32 = 7000;
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(3600));
    t.add_workflow_task_scheduled_and_started();
    let run = served(t, runtimes(&runtime), None, |_| {}, |_| {});
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let writes = |run_id: &str| -> Vec<workflow_command::Variant> {
        (1..=COUNT)
            .map(|seq| append_cmd(seq, "s", run_id, &["r"]).into())
            .collect()
    };
    complete(&run.worker, &first, writes(&first.run_id)).await;
    let live = take_appends(&run.worker, COUNT).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert!(run.failed.lock().is_empty(), "{:?}", run.failed.lock());

    let sent = run.sent.lock().clone();
    let with_appends: Vec<_> = sent
        .iter()
        .filter(|completion| !marker_appends(completion).is_empty())
        .collect();
    assert!(with_appends.len() >= 2, "{} tasks", with_appends.len());
    for completion in &with_appends[..with_appends.len() - 1] {
        assert!(completion.force_create_new_workflow_task);
    }
    for completion in &with_appends {
        let size = marker(completion).unwrap().encoded_len();
        assert!(size < MAX_MARKER_ENTRY_BYTES, "{size}");
    }
    let mut recorded: Vec<u32> = with_appends
        .iter()
        .flat_map(|completion| marker_appends(completion))
        .collect();
    recorded.sort();
    assert_eq!(recorded, (1..=COUNT).collect::<Vec<_>>());

    let history = run.history.unwrap().lock().clone();
    let replay = replay_worker(history, StreamRuntimes::default());
    let first = replay.poll_workflow_activation().await.unwrap();
    complete(&replay, &first, writes(&first.run_id)).await;
    let replayed = take_appends(&replay, COUNT).await;
    assert_eq!(replayed, live);
    let ended = activations_until_the_end(&replay).await;
    assert!(!has_failure_eviction(&ended), "{ended:?}");
}

/// A Store that fails each append with a long message.
struct WordyStore(TestStreamStore);

#[async_trait::async_trait]
impl StreamStore for WordyStore {
    async fn create_stream(
        &self,
        request: CreateStreamRequest,
    ) -> StreamResult<CreateStreamResponse> {
        self.0.create_stream(request).await
    }

    async fn append_records(
        &self,
        _request: AppendRecordsRequest,
    ) -> StreamResult<AppendRecordsResponse> {
        // A sign of three bytes, so that the cut must find a character boundary.
        Err(StreamError::new(
            Reason::Fenced,
            "\u{20ac}".repeat(100 * 1024),
        ))
    }

    async fn read_records(&self, request: ReadRecordsRequest) -> StreamResult<ReadRecordsResponse> {
        self.0.read_records(request).await
    }

    async fn close_stream(&self, request: CloseStreamRequest) -> StreamResult<CloseStreamResponse> {
        self.0.close_stream(request).await
    }

    async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> StreamResult<DescribeStreamResponse> {
        self.0.describe_stream(request).await
    }
}

/// A Store failure with a message of 300 KiB gives Workflow code the message cut at its bound,
/// with its reason, and the marker records the same small entry.
#[tokio::test]
async fn a_long_store_failure_message_is_cut() {
    let store = WordyStore(TestStreamStore::default());
    let runtime = StreamRuntime::new(Arc::new(store), None);
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(3600));
    t.add_workflow_task_scheduled_and_started();
    let run = served(t, runtimes(&runtime), None, |_| {}, |_| {});
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let append = append_cmd(1, "s", &first.run_id, &["a"]);
    complete(&run.worker, &first, vec![append.into()]).await;
    let next = run.worker.poll_workflow_activation().await.unwrap();
    let got = append_failure(&appends(&next)[0]);
    run.worker.complete_execution(&next.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert!(
        got.message.len() <= MAX_FAILURE_MESSAGE_BYTES,
        "{}",
        got.message.len()
    );
    assert!(got.message.starts_with("stream_fenced: \u{20ac}"));
    assert!(got.message.ends_with("\u{20ac}..."));
    let info = failure_info(&got);
    assert_eq!(info.r#type, "stream_fenced");
    assert!(info.non_retryable);
    let entry = marker(&run.sent.lock()[0]).unwrap().appends.remove(0);
    assert_eq!(entry.failure.as_ref(), Some(&got));
    assert!(entry.encoded_len() < 8 * 1024, "{}", entry.encoded_len());
}
