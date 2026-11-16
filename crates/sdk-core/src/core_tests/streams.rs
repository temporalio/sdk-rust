//! Direct reads and appends of a Stream, on the memory Store. A test drives the Worker with a mock
//! history, and adds Records to the Store between its polls.

use crate::{
    StreamRuntimes, Worker, WorkflowErrorType,
    replay::{HistoryForReplay, ReplayWorkerInput, TestHistoryBuilder},
    test_help::{
        CounterRecordingMeter, MockPollCfg, PollWFTRespExt, ResponseType, WorkerExt,
        WorkerTestHelpers, build_mock_pollers, hist_to_poll_resp, mock_worker,
        schedule_local_activity_cmd, test_worker_cfg,
    },
    worker::client::WorkflowTaskCompletion,
};
use futures_util::FutureExt;
use parking_lot::Mutex;
use prost::Message;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime},
};
use temporalio_common::protos::{
    constants::STREAM_MARKER_NAME,
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        common::StreamReadStop,
        external_data::{
            StreamAppendEntry, StreamCloseEntry, StreamMarkerData, StreamReadEnded, StreamReadEntry,
        },
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, ResolveStreamRead, WorkflowActivation,
            resolve_stream_append, workflow_activation_job,
        },
        workflow_commands::{
            ActivityCancellationType, AppendToStream, CloseStream, CloseStreamRead,
            CompleteWorkflowExecution, StartStreamRead, StartTimer, UpdateResponse,
            update_response, workflow_command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::command::Attributes,
        common::v1::{Payload, Payloads},
        enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
        failure::v1::{Failure, failure::FailureInfo},
        history::v1::MarkerRecordedEventAttributes,
        workflowservice::v1::RespondWorkflowTaskCompletedResponse,
    },
};
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_streams::{
    MemoryStreamOptions, MemoryStreamRuntime, StreamResult, StreamRuntime,
    digest::records_digest,
    memory::MemoryStore,
    proto::{
        AppendRecord, AppendRecordsRequest, ClientProducer, CloseStreamRequest,
        CreateStreamRequest, DescribeStreamRequest, Producer, ReadRecordsRequest, StreamRecord,
        StreamRef, StreamStatus, WorkflowProducer, WorkflowTaskEpoch, producer::Kind,
    },
    store::{
        Appended, Batch, Capabilities, Closed, Create, Created, DEFAULT_RETENTION, Entry, Store,
        StreamState,
    },
};
use tokio::sync::{Notify, Semaphore};

const STORE: &str = "mem";

fn runtime() -> Arc<StreamRuntime> {
    MemoryStreamRuntime::new(STORE, MemoryStreamOptions::default(), None)
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

/// Appends `texts` as a client, as a Producer outside the Workflow would.
async fn put(runtime: &StreamRuntime, id: &str, texts: &[&str]) {
    runtime
        .append_records(AppendRecordsRequest {
            r#ref: Some(stream(id)),
            producer: Some(client()),
            records: texts
                .iter()
                .map(|text| AppendRecord {
                    body: Some(body(text)),
                })
                .collect(),
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

fn read_cmd(seq: u32, id: &str) -> StartStreamRead {
    StartStreamRead {
        seq,
        stream: Some(stream(id)),
        idle_timeout: Some(Duration::from_secs(30).try_into().unwrap()),
        ..Default::default()
    }
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

/// A `core_stream` marker event, as the server records the marker that Core sent.
fn marker_event(t: &mut TestHistoryBuilder, completed_event_id: i64) {
    t.add(MarkerRecordedEventAttributes {
        marker_name: STREAM_MARKER_NAME.to_string(),
        workflow_task_completed_event_id: completed_event_id,
        ..Default::default()
    });
}

/// Runs a Worker on the history, with the runtime and a cache of one run, and keeps every
/// completion that it sends.
fn worker(
    t: TestHistoryBuilder,
    responses: impl IntoIterator<Item = ResponseType>,
    runtime: &Arc<StreamRuntime>,
    expected_fails: usize,
) -> (Worker, Arc<Mutex<Vec<WorkflowTaskCompletion>>>) {
    let runtimes = unbatched([(STORE.to_string(), runtime.clone())]);
    let run = start(t, responses, runtimes, 1, expected_fails);
    (run.worker, run.sent)
}

/// A Worker on a mock history, with what it sent to the server.
struct Run {
    worker: Worker,
    sent: Arc<Mutex<Vec<WorkflowTaskCompletion>>>,
    /// The cause and the message of each failed Workflow Task.
    failed: Arc<Mutex<Vec<(WorkflowTaskFailedCause, String)>>>,
}

/// A Worker whose mock poller stays alive. The end of a poller begins a shutdown, which ends a
/// task that only reads hold open, so a test shuts the Worker down when it asks.
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
    });
    if let Some(meter) = meter {
        mock.set_temporal_meter(meter.clone().into_temporal_meter());
    }
    Run {
        worker: mock_worker(mock),
        sent,
        failed,
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

fn reads(activation: &WorkflowActivation) -> Vec<ResolveStreamRead> {
    activation
        .jobs
        .iter()
        .filter_map(|job| match &job.variant {
            Some(workflow_activation_job::Variant::ResolveStreamRead(read)) => Some(read.clone()),
            _ => None,
        })
        .collect()
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

fn stop(read: &ResolveStreamRead) -> StreamReadStop {
    read.end.as_ref().unwrap().stop()
}

/// A read runs through a heartbeat and a task that a timer completes early, and ends with its
/// Stream. Each task records one marker of what Workflow code got in it.
///
/// A test drives a read over a heartbeat like this. History starts with a short Workflow Task
/// timeout, and each task after the first starts with the marker event of the task before. The
/// test adds Records to the Store, polls the page, and completes with no command. It then polls
/// until Core completes the task as a heartbeat at 80 % of the task timeout, and adds the next
/// Records, which come in the next task.
#[tokio::test]
async fn read_spans_three_tasks() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(500));
    t.add_full_wf_task();
    marker_event(&mut t, 4);
    t.add_full_wf_task();
    t.add_by_type(EventType::TimerStarted);
    marker_event(&mut t, 8);
    t.add_workflow_task_scheduled_and_started();
    let responses = [1, 2, 3].map(ResponseType::ToTaskNum);
    let (worker, sent) = worker(t, responses, &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    put(&runtime, "s", &["a"]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["a"]);
    complete(&worker, &page, vec![]).await;

    // The heartbeat comes at 400 ms, and completes the first task with the marker of its page.
    let page = poll_after_heartbeat(&worker, &sent, || put(&runtime, "s", &["b", "c"])).await;
    assert_eq!(texts(&reads(&page)[0].records), ["b", "c"]);
    let timer = StartTimer {
        seq: 1,
        start_to_fire_timeout: Some(Duration::from_secs(60).try_into().unwrap()),
    };
    complete(&worker, &page, vec![timer.into()]).await;

    put(&runtime, "s", &["d"]).await;
    close(&runtime, "s").await;
    let mut got = vec![];
    let last = loop {
        let page = worker.poll_workflow_activation().await.unwrap();
        let read = reads(&page).pop().unwrap();
        got.extend(texts(&read.records));
        if read.end.is_some() {
            assert_eq!(stop(&read), StreamReadStop::Done);
            assert!(read.end.as_ref().unwrap().done);
            break page;
        }
        complete(&worker, &page, vec![]).await;
    };
    assert_eq!(got, ["d"]);
    complete(
        &worker,
        &last,
        vec![CompleteWorkflowExecution { result: None }.into()],
    )
    .await;
    worker.drain_pollers_and_shutdown().await;

    let records = all_records(&runtime, "s").await;
    let offset = |at: usize| records[at].offset.clone();
    let sent = sent.lock();
    assert_eq!(sent.len(), 3);
    assert!(sent[0].force_create_new_workflow_task);
    assert!(sent[1].force_create_new_workflow_task);
    assert!(!sent[2].force_create_new_workflow_task);
    assert_eq!(command_types(&sent[0]), [CommandType::RecordMarker]);
    assert_eq!(
        command_types(&sent[1]),
        [CommandType::StartTimer, CommandType::RecordMarker]
    );
    assert_eq!(
        command_types(&sent[2]),
        [
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );

    let data = marker(&sent[0]).unwrap();
    let entry = &data.reads[0];
    assert_eq!(data.streams, [stream("s")]);
    assert_eq!((entry.seq, entry.stream_index, entry.count), (1, 0, 1));
    assert!(entry.from_offset.is_empty() && entry.after_offset.is_empty());
    assert_eq!(entry.last_offset, offset(0));
    assert_eq!(entry.digest, records_digest(&records[..1]).to_vec());
    assert!(entry.records.is_empty());
    // The page came in the second activation of the task, after the one that started the read.
    assert_eq!(
        (entry.page_counts.clone(), entry.page_activations.clone()),
        (vec![1], vec![1])
    );
    assert!(entry.end.is_none());

    let entry = &marker(&sent[1]).unwrap().reads[0];
    assert_eq!(
        (entry.after_offset.clone(), entry.last_offset.clone()),
        (offset(0), offset(2))
    );
    assert_eq!(entry.digest, records_digest(&records[1..3]).to_vec());
    assert_eq!(
        (entry.page_counts.clone(), entry.page_activations.clone()),
        (vec![2], vec![0])
    );

    let entry = &marker(&sent[2]).unwrap().reads[0];
    assert_eq!((entry.after_offset.clone(), entry.count), (offset(2), 1));
    let end = entry.end.as_ref().unwrap();
    assert_eq!(
        (end.last_offset.clone(), end.stop(), end.done),
        (offset(3), StreamReadStop::Done, true)
    );
}

/// A Worker with no Local Activities still heartbeats a task that a read holds open.
#[tokio::test]
async fn heartbeat_runs_without_local_activities() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(300));
    t.add_full_wf_task();
    marker_event(&mut t, 4);
    t.add_workflow_task_scheduled_and_started();
    let responses = [1, 2].map(ResponseType::ToTaskNum);
    let sent = Arc::new(Mutex::new(vec![]));
    let mut cfg = MockPollCfg::from_resps(t, responses);
    // The poller stays alive, as the end of a poller begins a shutdown, which stops the read.
    cfg.make_poll_stream_interminable = true;
    let kept = sent.clone();
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        kept.lock().push(completion.clone());
        Ok(Default::default())
    }));
    let mut mock = build_mock_pollers(cfg);
    let runtimes = unbatched([(STORE.to_string(), runtime.clone())]);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 1;
        wc.task_types = WorkerTaskTypes::workflow_only();
        wc.stream_runtimes = runtimes;
    });
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    put(&runtime, "s", &["a"]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &page, vec![]).await;
    let page = poll_after_heartbeat(&worker, &sent, || put(&runtime, "s", &["b"])).await;
    assert_eq!(texts(&reads(&page)[0].records), ["b"]);
    worker.complete_execution(&page.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let sent = sent.lock();
    assert_eq!(sent.len(), 2);
    assert!(sent[0].force_create_new_workflow_task);
    assert_eq!(marker(&sent[0]).unwrap().reads[0].count, 1);
    assert_eq!(marker(&sent[1]).unwrap().reads[0].count, 1);
}

/// With its Records in History, a read's marker holds them and no digest.
#[tokio::test]
async fn marker_with_records_in_history_has_no_digest() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b", "c"]).await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let (worker, sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        max_records: 2,
        records_in_history: true,
        ..read_cmd(1, "s")
    };
    complete(&worker, &first, vec![read.into()]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    let read = &reads(&page)[0];
    assert_eq!(texts(&read.records), ["a", "b"]);
    assert_eq!(stop(read), StreamReadStop::MaxRecords);
    worker.complete_execution(&page.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let sent = sent.lock();
    let entry = &marker(&sent[0]).unwrap().reads[0];
    assert_eq!(texts(&entry.records), ["a", "b"]);
    assert!(entry.digest.is_empty());
    assert_eq!(entry.count, 2);
    assert_eq!(
        entry.end.as_ref().unwrap().last_offset,
        read.records[1].offset
    );
}

/// Two reads give their pages in one activation, by seq, and the marker has an entry for each.
#[tokio::test]
async fn two_reads_keep_their_pages_and_activations() {
    let runtime = runtime();
    for id in ["x", "y"] {
        create(&runtime, id).await;
    }
    put(&runtime, "y", &["y1"]).await;
    put(&runtime, "x", &["x1"]).await;
    close(&runtime, "x").await;
    close(&runtime, "y").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let (worker, sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![read_cmd(2, "y").into(), read_cmd(1, "x").into()],
    )
    .await;
    // Each read may end in its own activation, but the jobs of one activation go by seq.
    let mut ended = vec![];
    while ended.len() < 2 {
        let page = worker.poll_workflow_activation().await.unwrap();
        let seqs: Vec<_> = reads(&page).iter().map(|read| read.seq).collect();
        let mut sorted = seqs.clone();
        sorted.sort();
        assert_eq!(seqs, sorted);
        for read in reads(&page) {
            assert_eq!(stop(&read), StreamReadStop::Done);
            ended.push((read.seq, texts(&read.records)));
        }
        if ended.len() < 2 {
            complete(&worker, &page, vec![]).await;
        } else {
            worker.complete_execution(&page.run_id).await;
        }
    }
    ended.sort();
    assert_eq!(
        ended,
        [(1, vec!["x1".to_string()]), (2, vec!["y1".to_string()])]
    );
    worker.drain_pollers_and_shutdown().await;

    let data = marker(&sent.lock()[0]).unwrap();
    let mut entries: Vec<_> = data
        .reads
        .iter()
        .map(|entry| {
            let stream = &data.streams[entry.stream_index as usize];
            (entry.seq, stream.id.clone(), entry.count)
        })
        .collect();
    entries.sort();
    assert_eq!(entries, [(1, "x".to_string(), 1), (2, "y".to_string(), 1)]);
}

/// A read next to a Local Activity in one task. The Local Activity marker comes first, and the
/// Stream marker records the activation that got the page.
#[tokio::test]
async fn read_next_to_local_activity_keeps_activations() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let (worker, sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    let la = schedule_local_activity_cmd(
        1,
        "1",
        ActivityCancellationType::TryCancel,
        Duration::from_secs(60),
    );
    complete(&worker, &first, vec![la, read_cmd(1, "s").into()]).await;
    let task = worker.poll_activity_task().await.unwrap();
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    put(&runtime, "s", &["a"]).await;
    let (mut la_at, mut page_at) = (None, None);
    let mut activation = 0;
    loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        activation += 1;
        for job in &next.jobs {
            match &job.variant {
                Some(workflow_activation_job::Variant::ResolveActivity(_)) => {
                    la_at = Some(activation)
                }
                Some(workflow_activation_job::Variant::ResolveStreamRead(_)) => {
                    page_at = Some(activation)
                }
                _ => {}
            }
        }
        if la_at.is_some() && page_at.is_some() {
            worker.complete_execution(&next.run_id).await;
            break;
        }
        complete(&worker, &next, vec![]).await;
    }
    worker.drain_pollers_and_shutdown().await;

    let sent = sent.lock();
    assert_eq!(sent.len(), 1);
    assert_eq!(
        command_types(&sent[0]),
        [
            CommandType::RecordMarker,
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );
    assert!(marker(&sent[0]).is_some());
    let entry = &marker(&sent[0]).unwrap().reads[0];
    assert_eq!(entry.page_activations, [page_at.unwrap()]);
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

/// A command for the server completes the task at once while a read runs. The read goes on, and
/// its next page waits for the next task.
#[tokio::test]
async fn server_command_completes_the_task_while_a_read_runs() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_by_type(EventType::TimerStarted);
    t.add_workflow_task_scheduled_and_started();
    let responses = [1, 2].map(ResponseType::ToTaskNum);
    let (worker, sent) = worker(t, responses, &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    let timer = StartTimer {
        seq: 1,
        start_to_fire_timeout: Some(Duration::from_secs(60).try_into().unwrap()),
    };
    complete(&worker, &first, vec![read_cmd(1, "s").into(), timer.into()]).await;
    put(&runtime, "s", &["a"]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["a"]);
    worker.complete_execution(&page.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let sent = sent.lock();
    assert_eq!(command_types(&sent[0]), [CommandType::StartTimer]);
    assert!(sent[0].force_create_new_workflow_task);
    // The page is the first activation of the second task.
    let entry = &marker(&sent[1]).unwrap().reads[0];
    assert_eq!(entry.page_activations, [0]);
}

/// A failed task writes no marker and stops its read. The next attempt reads again from the start.
#[tokio::test]
async fn failed_task_drops_read_results() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let responses = [1, 1].map(ResponseType::ToTaskNum);
    let (worker, sent) = worker(t, responses, &runtime, 1);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["a"]);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::fail(
            page.run_id.clone(),
            Failure {
                message: "Workflow code failed".to_string(),
                ..Default::default()
            },
            None,
        ))
        .await
        .unwrap();
    put(&runtime, "s", &["b"]).await;
    let evict = worker.poll_workflow_activation().await.unwrap();
    assert!(matches!(
        evict.jobs[0].variant,
        Some(workflow_activation_job::Variant::RemoveFromCache(_))
    ));
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(evict.run_id))
        .await
        .unwrap();

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["a", "b"]);
    worker.complete_execution(&page.run_id).await;
    worker.drain_pollers_and_shutdown().await;

    let sent = sent.lock();
    assert_eq!(sent.len(), 1);
    let entry = &marker(&sent[0]).unwrap().reads[0];
    assert!(entry.after_offset.is_empty());
    assert_eq!(entry.count, 2);
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

/// A close stops a read. Workflow code gets no result for it, and the marker has no end.
#[tokio::test]
async fn close_stops_the_read() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let (worker, sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["a"]);
    // The close stops the read, so the task completes with nothing outstanding.
    complete(
        &worker,
        &page,
        vec![
            CloseStreamRead { seq: 1 }.into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    )
    .await;
    put(&runtime, "s", &["b"]).await;
    worker.drain_pollers_and_shutdown().await;

    let entry = &marker(&sent.lock()[0]).unwrap().reads[0];
    assert_eq!(entry.count, 1);
    assert!(entry.end.is_none());
}

/// A read ends at the first of its stops.
#[tokio::test]
async fn read_stops_at_each_cap() {
    let runtime = runtime();
    for id in ["done", "records", "bytes", "idle", "timeout"] {
        create(&runtime, id).await;
    }
    put(&runtime, "done", &["a"]).await;
    close(&runtime, "done").await;
    put(&runtime, "records", &["a", "b", "c"]).await;
    put(&runtime, "bytes", &["a", "b"]).await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let (worker, _sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let short = Some(Duration::from_millis(200).try_into().unwrap());
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![
            read_cmd(1, "done").into(),
            StartStreamRead {
                max_records: 2,
                ..read_cmd(2, "records")
            }
            .into(),
            // The first Record is over the cap, and the read still takes it.
            StartStreamRead {
                max_bytes: 1,
                ..read_cmd(3, "bytes")
            }
            .into(),
            StartStreamRead {
                idle_timeout: short,
                ..read_cmd(4, "idle")
            }
            .into(),
            StartStreamRead {
                timeout: short,
                ..read_cmd(5, "timeout")
            }
            .into(),
        ],
    )
    .await;
    let mut ends = vec![];
    loop {
        let page = worker.poll_workflow_activation().await.unwrap();
        for read in reads(&page) {
            if read.end.is_some() {
                ends.push((read.seq, stop(&read), texts(&read.records)));
            }
        }
        if ends.len() == 5 {
            worker.complete_execution(&page.run_id).await;
            break;
        }
        complete(&worker, &page, vec![]).await;
    }
    worker.drain_pollers_and_shutdown().await;
    ends.sort_by_key(|end| end.0);
    let owned = |texts: &[&str]| {
        texts
            .iter()
            .map(|text| text.to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        ends,
        [
            (1, StreamReadStop::Done, owned(&["a"])),
            (2, StreamReadStop::MaxRecords, owned(&["a", "b"])),
            (3, StreamReadStop::MaxBytes, owned(&["a"])),
            (4, StreamReadStop::Idle, owned(&[])),
            (5, StreamReadStop::Timeout, owned(&[])),
        ]
    );
}

/// A call on a Store with no runtime fails the Workflow Task, and writes nothing.
#[tokio::test]
async fn call_on_unknown_store_fails() {
    let runtime = runtime();
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let mut cfg = MockPollCfg::from_resps(t, [ResponseType::AllHistory]);
    cfg.num_expected_fails = 1;
    cfg.expect_fail_wft_matcher = Box::new(|_, cause, failure| {
        matches!(
            cause,
            WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
        ) && failure.as_ref().is_some_and(|failure| {
            failure.message == "This Worker has no Stream runtime for the Store `elsewhere`"
        })
    });
    let mut mock = build_mock_pollers(cfg);
    let runtimes = unbatched([(STORE.to_string(), runtime)]);
    mock.worker_cfg(|wc| wc.stream_runtimes = runtimes);
    let worker = mock_worker(mock);

    let first = worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        stream: Some(StreamRef {
            id: "s".to_string(),
            store: "elsewhere".to_string(),
        }),
        ..read_cmd(1, "s")
    };
    complete(&worker, &first, vec![read.into()]).await;
    worker.drain_pollers_and_shutdown().await;
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
    let runtimes = unbatched([(STORE.to_string(), runtime.clone())]);
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

// Replays. A test builds the marker that Core would have written, from the Records in the Store.

/// One page of a read entry: its Records and its activation in the Workflow Task.
struct Page<'a> {
    records: &'a [StreamRecord],
    activation: u32,
}

/// The marker entry of read `seq` of Stream `id`, with its pages and its end.
fn read_entry(
    seq: u32,
    after_offset: Vec<u8>,
    pages: &[Page<'_>],
    end: Option<(StreamReadStop, u32)>,
    records_in_history: bool,
) -> StreamReadEntry {
    let records: Vec<StreamRecord> = pages
        .iter()
        .flat_map(|page| page.records.iter().cloned())
        .collect();
    let last_offset = records
        .last()
        .map(|record| record.offset.clone())
        .unwrap_or_default();
    let mut before = 0;
    let page_activations = pages
        .iter()
        .enumerate()
        .map(|(at, page)| {
            let gap = if at == 0 {
                page.activation
            } else {
                page.activation - before
            };
            before = page.activation;
            gap
        })
        .collect();
    StreamReadEntry {
        seq,
        stream_index: 0,
        from_offset: vec![],
        end: end.map(|(stop, activation)| StreamReadEnded {
            last_offset: if last_offset.is_empty() {
                after_offset.clone()
            } else {
                last_offset.clone()
            },
            stop: stop as i32,
            done: stop == StreamReadStop::Done,
            failure: None,
            activation,
        }),
        after_offset,
        last_offset,
        count: records.len() as u32,
        digest: if records_in_history || records.is_empty() {
            vec![]
        } else {
            records_digest(&records).to_vec()
        },
        page_counts: pages.iter().map(|page| page.records.len() as u32).collect(),
        page_activations,
        records: if records_in_history { records } else { vec![] },
    }
}

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

/// A history whose first Workflow Task started read 1 of Stream `s` and recorded `data`. A signal
/// starts the second task, so that Workflow code gets an activation in it.
fn replayed_read_history(data: &StreamMarkerData) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    stream_marker_event(&mut t, 4, data);
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_and_started();
    t
}

fn runtimes(runtime: &Arc<StreamRuntime>) -> StreamRuntimes {
    unbatched([(STORE.to_string(), runtime.clone())])
}

/// Runtimes with no batch window, so that a read gives each batch as it comes and a test can check
/// exact pages. The tests of the window set their own.
fn unbatched(runtimes: impl IntoIterator<Item = (String, Arc<StreamRuntime>)>) -> StreamRuntimes {
    StreamRuntimes::new(runtimes).with_read_batch_window(Duration::ZERO)
}

fn signaled(activation: &WorkflowActivation) -> bool {
    activation.jobs.iter().any(|job| {
        matches!(
            job.variant,
            Some(workflow_activation_job::Variant::SignalWorkflow(_))
        )
    })
}

/// A replay reads the Records that the marker counts from the Store, and gives them in the
/// activation that the marker has.
#[tokio::test]
async fn replay_reads_records_from_the_store() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    close(&runtime, "s").await;
    let records = all_records(&runtime, "s").await;
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], Some((StreamReadStop::Done, 1)), false);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 0);

    let first = run.worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    complete(&run.worker, &first, vec![read_cmd(1, "s").into()]).await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    let read = &reads(&replayed)[0];
    assert_eq!(texts(&read.records), ["a", "b"]);
    assert_eq!(read.records, records);
    assert_eq!(stop(read), StreamReadStop::Done);
    complete(&run.worker, &replayed, vec![]).await;
    let live = run.worker.poll_workflow_activation().await.unwrap();
    assert!(signaled(&live) && reads(&live).is_empty());
    run.worker.complete_execution(&live.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert!(marker(&run.sent.lock()[0]).is_none());
}

/// A marker with its Records needs no Store, so a Worker with no runtime replays it.
#[tokio::test]
async fn replay_from_history_calls_no_store() {
    let records: Vec<_> = ["a", "b"]
        .iter()
        .enumerate()
        .map(|(at, text)| StreamRecord {
            producer: Some(client()),
            body: Some(body(text)),
            offset: (at as u64 + 1).to_be_bytes().to_vec(),
        })
        .collect();
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], Some((StreamReadStop::Done, 1)), true);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let run = start(
        t,
        [ResponseType::AllHistory],
        StreamRuntimes::default(),
        1,
        0,
    );

    let first = run.worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        records_in_history: true,
        ..read_cmd(1, "s")
    };
    complete(&run.worker, &first, vec![read.into()]).await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(reads(&replayed)[0].records, records);
    complete(&run.worker, &replayed, vec![]).await;
    let live = run.worker.poll_workflow_activation().await.unwrap();
    run.worker.complete_execution(&live.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
}

/// Replays a marker of read 1 of Stream `s` on `runtimes`, and returns the message of the failed
/// Workflow Task.
async fn failed_replay(data: StreamMarkerData, runtimes: StreamRuntimes) -> String {
    let t = replayed_read_history(&data);
    // The Records are read after the poll, so the poller must not end before the task fails.
    let run = start_with(t, [ResponseType::AllHistory], runtimes, 1, 1, true);
    let polled = tokio::time::timeout(
        Duration::from_secs(2),
        run.worker.poll_workflow_activation(),
    )
    .await;
    assert!(
        polled.is_err(),
        "The task should fail before any activation"
    );
    run.worker.drain_pollers_and_shutdown().await;
    let failed = run.failed.lock();
    assert_eq!(failed.len(), 1);
    assert_eq!(
        failed[0].0,
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
    );
    failed[0].1.clone()
}

/// The marker of a read of `records` from the start of Stream `s`, and the range in its messages.
fn one_page(records: &[StreamRecord]) -> (StreamMarkerData, String) {
    let page = Page {
        records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], None, false);
    let range = format!(
        "Stream `s` in Store `mem`, the Records after the start through offset {}",
        records
            .last()
            .unwrap()
            .offset
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    (
        StreamMarkerData {
            reads: vec![entry],
            appends: vec![],
            closes: vec![],
            streams: vec![stream("s")],
        },
        range,
    )
}

/// The Records that a marker counts are gone after the Retention of their closed Stream.
#[tokio::test]
async fn replay_fails_when_records_are_gone() {
    let options = MemoryStreamOptions {
        retention: Some(Duration::from_millis(100)),
        ..Default::default()
    };
    let runtime = MemoryStreamRuntime::new(STORE, options, None);
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let (data, range) = one_page(&all_records(&runtime, "s").await);
    close(&runtime, "s").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let message = failed_replay(data, runtimes(&runtime)).await;
    assert!(
        message.starts_with(&format!("StreamReplayError: {range}: stream_expired")),
        "{message}"
    );
}

#[tokio::test]
async fn replay_fails_on_digest_mismatch() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let (mut data, range) = one_page(&all_records(&runtime, "s").await);
    data.reads[0].digest[0] ^= 1;
    let message = failed_replay(data, runtimes(&runtime)).await;
    assert_eq!(
        message,
        format!("StreamReplayError: {range}: the digest of the Records differs")
    );
}

#[tokio::test]
async fn replay_fails_with_no_runtime() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let (data, range) = one_page(&all_records(&runtime, "s").await);
    let message = failed_replay(data, StreamRuntimes::default()).await;
    assert_eq!(
        message,
        format!(
            "StreamReplayError: {range}: this Worker has no Stream runtime for the Store `mem`"
        )
    );
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
    let t = replayed_read_history(&StreamMarkerData {
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

/// A read that history leaves open goes on live from its last offset, with what is left of its
/// caps.
#[tokio::test]
async fn open_read_resumes_after_replay() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let records = all_records(&runtime, "s").await;
    put(&runtime, "s", &["c", "d"]).await;
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], None, false);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 0);

    let first = run.worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        max_records: 3,
        ..read_cmd(1, "s")
    };
    complete(&run.worker, &first, vec![read.into()]).await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&replayed)[0].records), ["a", "b"]);
    complete(&run.worker, &replayed, vec![]).await;
    let resumed = loop {
        let live = run.worker.poll_workflow_activation().await.unwrap();
        if let Some(read) = reads(&live).pop() {
            break (live, read);
        }
        complete(&run.worker, &live, vec![]).await;
    };
    assert_eq!(texts(&resumed.1.records), ["c"]);
    assert_eq!(stop(&resumed.1), StreamReadStop::MaxRecords);
    run.worker.complete_execution(&resumed.0.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;

    let entry = &marker(&run.sent.lock()[0]).unwrap().reads[0];
    assert_eq!(entry.after_offset, records[1].offset);
    assert_eq!(entry.count, 1);
}

/// A resumed read keeps the deadline of its start, which history dates, so a read that started
/// an hour ago with a timeout of a minute ends at once.
#[tokio::test]
async fn resumed_read_keeps_its_deadline() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let records = all_records(&runtime, "s").await;
    let page = Page {
        records: &records[..1],
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], None, false);
    let mut t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let an_hour_ago = SystemTime::now() - Duration::from_secs(3600);
    for event_id in 1..=5 {
        t.modify_event(event_id, |event| {
            event.event_time = Some(an_hour_ago.into())
        });
    }
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 0);

    let first = run.worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        timeout: Some(Duration::from_secs(60).try_into().unwrap()),
        ..read_cmd(1, "s")
    };
    complete(&run.worker, &first, vec![read.into()]).await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&replayed)[0].records), ["a"]);
    complete(&run.worker, &replayed, vec![]).await;
    let ended = loop {
        let live = run.worker.poll_workflow_activation().await.unwrap();
        if let Some(read) = reads(&live).pop() {
            break (live, read);
        }
        complete(&run.worker, &live, vec![]).await;
    };
    assert!(ended.1.records.is_empty());
    assert_eq!(stop(&ended.1), StreamReadStop::Timeout);
    assert_eq!(ended.1.end.as_ref().unwrap().last_offset, records[0].offset);
    run.worker.complete_execution(&ended.0.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
}

/// A close in a replay ends the read, so it doesn't go on live.
#[tokio::test]
async fn close_ends_a_read_in_replay() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let records = all_records(&runtime, "s").await;
    put(&runtime, "s", &["b"]).await;
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], None, false);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 0);

    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![read_cmd(1, "s").into()]).await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&replayed)[0].records), ["a"]);
    complete(
        &run.worker,
        &replayed,
        vec![CloseStreamRead { seq: 1 }.into()],
    )
    .await;
    let live = run.worker.poll_workflow_activation().await.unwrap();
    assert!(signaled(&live) && reads(&live).is_empty());
    // A read that went on would hold the task open, and the completion would wait for it.
    run.worker.complete_execution(&live.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    let sent = run.sent.lock();
    assert_eq!(sent.len(), 1);
    assert!(marker(&sent[0]).is_none());
}

/// A read of another Stream than history has is a non-determinism error.
#[tokio::test]
async fn other_stream_id_is_nondeterministic() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let (data, _) = one_page(&all_records(&runtime, "s").await);
    let t = replayed_read_history(&data);
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 1);

    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![read_cmd(1, "other").into()]).await;
    run.worker.drain_pollers_and_shutdown().await;
    let failed = run.failed.lock();
    assert_eq!(failed[0].0, WorkflowTaskFailedCause::NonDeterministicError);
    assert!(
        failed[0].1.contains("Stream read 1 reads Stream `other`"),
        "{}",
        failed[0].1
    );
}

/// A result in history of a call that Workflow code never makes is a non-determinism error.
#[tokio::test]
async fn entry_with_no_call_is_nondeterministic() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let (data, _) = one_page(&all_records(&runtime, "s").await);
    let t = replayed_read_history(&data);
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 1);

    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![]).await;
    run.worker.drain_pollers_and_shutdown().await;
    let failed = run.failed.lock();
    assert_eq!(failed[0].0, WorkflowTaskFailedCause::NonDeterministicError);
    assert!(
        failed[0]
            .1
            .contains("History has a Stream result of read 1 that Workflow code never got"),
        "{}",
        failed[0].1
    );
}

/// A Worker with no cache replays every earlier Workflow Task, and still runs a read and an
/// append to the end of the Workflow. The append is written once.
#[tokio::test]
async fn worker_with_no_cache_runs_reads_and_appends() {
    let runtime = runtime();
    create(&runtime, "s").await;
    create(&runtime, "out").await;
    put(&runtime, "s", &["a", "b"]).await;
    close(&runtime, "s").await;
    let records = all_records(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer = t.add_timer_started("1".to_string());
    t.add_full_wf_task();
    t.add_timer_started("2".to_string());
    let page = Page {
        records: &records,
        activation: 0,
    };
    let entry = read_entry(1, vec![], &[page], Some((StreamReadStop::Done, 0)), false);
    stream_marker_event(
        &mut t,
        8,
        &StreamMarkerData {
            reads: vec![entry],
            appends: vec![],
            closes: vec![],
            streams: vec![stream("s")],
        },
    );
    t.add_timer_fired(timer, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let responses = [1, 2, 3].map(ResponseType::ToTaskNum);
    let run = start(t, responses, runtimes(&runtime), 0, 0);

    let timer = |seq| StartTimer {
        seq,
        start_to_fire_timeout: Some(Duration::from_secs(60).try_into().unwrap()),
    };
    loop {
        let next = run.worker.poll_workflow_activation().await.unwrap();
        let mut commands: Vec<workflow_command::Variant> = vec![];
        let mut done = false;
        for job in &next.jobs {
            match &job.variant {
                Some(workflow_activation_job::Variant::InitializeWorkflow(_)) => {
                    commands.extend([read_cmd(1, "s").into(), timer(1).into()]);
                }
                Some(workflow_activation_job::Variant::ResolveStreamRead(read)) => {
                    assert_eq!(texts(&read.records), ["a", "b"]);
                    commands.push(timer(2).into());
                }
                Some(workflow_activation_job::Variant::FireTimer(_)) => {
                    commands.push(append_cmd(1, "out", &run_id, &["a", "b"]).into());
                }
                Some(workflow_activation_job::Variant::ResolveStreamAppend(_)) => {
                    commands.push(CompleteWorkflowExecution { result: None }.into());
                    done = true;
                }
                _ => {}
            }
        }
        complete(&run.worker, &next, commands).await;
        if done {
            break;
        }
    }
    run.worker.drain_pollers_and_shutdown().await;

    assert_eq!(texts(&all_records(&runtime, "out").await), ["a", "b"]);
    let sent = run.sent.lock();
    assert_eq!(sent.len(), 3);
    assert_eq!(command_types(&sent[0]), [CommandType::StartTimer]);
    assert_eq!(
        command_types(&sent[1]),
        [CommandType::StartTimer, CommandType::RecordMarker]
    );
    assert_eq!(marker(&sent[1]).unwrap().reads[0], {
        let page = Page {
            records: &records,
            activation: 0,
        };
        read_entry(1, vec![], &[page], Some((StreamReadStop::Done, 0)), false)
    });
    assert_eq!(
        command_types(&sent[2]),
        [
            CommandType::RecordMarker,
            CommandType::CompleteWorkflowExecution
        ]
    );
}

/// The state of the Workflow of [replay_matches_live_activations], which lang would keep.
#[derive(Default)]
struct TwoReads {
    appends: u32,
    open_appends: u32,
    ended_reads: u32,
    local_activity_done: bool,
}

impl TwoReads {
    /// The commands for an activation. Each page of a read gets an append of its Records.
    fn activate(
        &mut self,
        activation: &WorkflowActivation,
        run_id: &str,
    ) -> (Vec<workflow_command::Variant>, bool) {
        let mut commands = vec![];
        for job in &activation.jobs {
            match &job.variant {
                Some(workflow_activation_job::Variant::InitializeWorkflow(_)) => {
                    commands.push(schedule_local_activity_cmd(
                        1,
                        "1",
                        ActivityCancellationType::TryCancel,
                        Duration::from_secs(60),
                    ));
                    commands.push(read_cmd(1, "x").into());
                    commands.push(read_cmd(2, "y").into());
                }
                Some(workflow_activation_job::Variant::ResolveStreamRead(read)) => {
                    if !read.records.is_empty() {
                        self.appends += 1;
                        self.open_appends += 1;
                        let texts = texts(&read.records);
                        let texts: Vec<_> = texts.iter().map(String::as_str).collect();
                        commands.push(append_cmd(self.appends, "out", run_id, &texts).into());
                    }
                    if read.end.is_some() {
                        self.ended_reads += 1;
                    }
                }
                Some(workflow_activation_job::Variant::ResolveStreamAppend(_)) => {
                    self.open_appends -= 1;
                }
                Some(workflow_activation_job::Variant::ResolveActivity(_)) => {
                    self.local_activity_done = true;
                }
                _ => {}
            }
        }
        let done = self.ended_reads == 2 && self.open_appends == 0 && self.local_activity_done;
        if done {
            commands.push(CompleteWorkflowExecution { result: None }.into());
        }
        (commands, done)
    }
}

/// A replay gives every page, end and append result in the activation of its first run, next to
/// a Local Activity result, and writes nothing.
#[tokio::test]
async fn replay_matches_live_activations() {
    let runtime = runtime();
    for id in ["x", "y", "out"] {
        create(&runtime, id).await;
    }
    put(&runtime, "x", &["x1", "x2"]).await;
    put(&runtime, "y", &["y1"]).await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let run_id = t.get_orig_run_id().to_string();
    let replay = t.clone();
    let (worker, sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);

    let mut live = vec![];
    let mut workflow = TwoReads::default();
    let first = worker.poll_workflow_activation().await.unwrap();
    let (commands, _) = workflow.activate(&first, &run_id);
    live.push(first.jobs.clone());
    complete(&worker, &first, commands).await;
    let task = worker.poll_activity_task().await.unwrap();
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    // A second page of `x` comes while the first one is out with Workflow code.
    put(&runtime, "x", &["x3"]).await;
    close(&runtime, "x").await;
    close(&runtime, "y").await;
    loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        let (commands, done) = workflow.activate(&next, &run_id);
        live.push(next.jobs.clone());
        complete(&worker, &next, commands).await;
        if done {
            break;
        }
    }
    worker.drain_pollers_and_shutdown().await;
    let written = all_records(&runtime, "out").await.len();
    assert_eq!(written, 4);

    let mut replay = replay;
    replay.add_workflow_task_completed();
    let completion = sent.lock()[0].clone();
    for command in completion.commands {
        match command.attributes {
            Some(Attributes::RecordMarkerCommandAttributes(marker)) => {
                replay.add(MarkerRecordedEventAttributes {
                    marker_name: marker.marker_name,
                    details: marker.details,
                    workflow_task_completed_event_id: 4,
                    header: marker.header,
                    failure: marker.failure,
                });
            }
            Some(Attributes::CompleteWorkflowExecutionCommandAttributes(_)) => {
                replay.add_workflow_execution_completed();
            }
            other => panic!("The task sent an unexpected command: {other:?}"),
        }
    }
    let run = start(replay, [ResponseType::AllHistory], runtimes(&runtime), 1, 0);
    let mut replayed = vec![];
    let mut workflow = TwoReads::default();
    loop {
        let next = run.worker.poll_workflow_activation().await.unwrap();
        assert!(next.is_replaying);
        let (commands, done) = workflow.activate(&next, &run_id);
        replayed.push(next.jobs.clone());
        complete(&run.worker, &next, commands).await;
        if done {
            break;
        }
    }
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(replayed, live);
    assert_eq!(all_records(&runtime, "out").await.len(), written);
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

/// Replays a history whose only Workflow Task has a page of read 1 of Stream `s` and completes the
/// Workflow. Workflow code sends `commands` and the completion in its first activation. Returns the
/// cause and message of the failed Workflow Task.
async fn replay_last_task(
    commands: Vec<workflow_command::Variant>,
) -> (WorkflowTaskFailedCause, String) {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let (data, _) = one_page(&all_records(&runtime, "s").await);
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    stream_marker_event(&mut t, 4, &data);
    t.add_workflow_execution_completed();
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 1);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let mut commands = commands;
    commands.push(CompleteWorkflowExecution { result: None }.into());
    complete(&run.worker, &first, commands).await;
    run.worker.drain_pollers_and_shutdown().await;
    let failed = run.failed.lock();
    assert_eq!(failed.len(), 1, "the replay should fail");
    failed[0].clone()
}

/// A result in history with no call is a non-determinism error, also in the task that completes
/// the Workflow.
#[tokio::test]
async fn a_result_with_no_call_in_the_last_task_is_nondeterministic() {
    let (cause, message) = replay_last_task(vec![]).await;
    assert_eq!(cause, WorkflowTaskFailedCause::NonDeterministicError);
    assert!(
        message.contains("History has a Stream result of read 1 that Workflow code never got"),
        "{message}"
    );
}

/// A read of another Stream, or from another offset, is a non-determinism error, also in the task
/// that completes the Workflow.
#[tokio::test]
async fn a_changed_read_in_the_last_task_is_nondeterministic() {
    let (cause, message) = replay_last_task(vec![read_cmd(1, "other").into()]).await;
    assert_eq!(cause, WorkflowTaskFailedCause::NonDeterministicError);
    assert!(
        message.contains("Stream read 1 reads Stream `other`"),
        "{message}"
    );
    let later = StartStreamRead {
        from_offset: 1u64.to_be_bytes().to_vec(),
        ..read_cmd(1, "s")
    };
    let (cause, message) = replay_last_task(vec![later.into()]).await;
    assert_eq!(cause, WorkflowTaskFailedCause::NonDeterministicError);
    assert!(message.contains("or from another offset"), "{message}");
}

/// With non-determinism set to fail the Workflow, a changed read in the last task fails the
/// Workflow. The results that history holds don't fail that forced completion again.
#[tokio::test]
async fn a_changed_read_can_fail_the_workflow() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let (data, _) = one_page(&all_records(&runtime, "s").await);
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_completed();
    stream_marker_event(&mut t, 4, &data);
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_and_started();
    let sent = Arc::new(Mutex::new(vec![]));
    let mut cfg = MockPollCfg::from_resps(t, [ResponseType::AllHistory]);
    let kept = sent.clone();
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        kept.lock().push(completion.clone());
        Ok(Default::default())
    }));
    let mut mock = build_mock_pollers(cfg);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 1;
        wc.stream_runtimes = runtimes(&runtime);
        wc.workflow_failure_errors = [WorkflowErrorType::Nondeterminism].into();
    });
    let worker = mock_worker(mock);
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "other").into()]).await;
    worker.drain_pollers_and_shutdown().await;
    let sent = sent.lock();
    assert_eq!(
        command_types(&sent[0]),
        [CommandType::FailWorkflowExecution]
    );
}

/// A read whose cap falls on the last Record of a closed Stream ends `Done`, so that Workflow code
/// knows that the Stream is finished.
#[tokio::test]
async fn a_cap_on_the_last_record_of_a_closed_stream_ends_done() {
    let runtime = runtime();
    for id in ["records", "bytes"] {
        create(&runtime, id).await;
        put(&runtime, id, &["a", "b"]).await;
        close(&runtime, id).await;
    }
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let (worker, _sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);
    let first = worker.poll_workflow_activation().await.unwrap();
    let records = StartStreamRead {
        max_records: 2,
        ..read_cmd(1, "records")
    };
    // Room for both Records and not a byte more.
    let both: u64 = all_records(&runtime, "bytes")
        .await
        .iter()
        .map(|record| record.encoded_len() as u64)
        .sum();
    let bytes = StartStreamRead {
        max_bytes: both,
        ..read_cmd(2, "bytes")
    };
    complete(&worker, &first, vec![records.into(), bytes.into()]).await;
    let mut ends = vec![];
    while ends.len() < 2 {
        let next = worker.poll_workflow_activation().await.unwrap();
        for read in reads(&next) {
            if let Some(end) = read.end {
                ends.push((read.seq, end.stop(), end.done));
            }
        }
        if ends.len() == 2 {
            worker.complete_execution(&next.run_id).await;
        } else {
            complete(&worker, &next, vec![]).await;
        }
    }
    worker.drain_pollers_and_shutdown().await;
    ends.sort_by_key(|end| end.0);
    assert_eq!(
        ends,
        [
            (1, StreamReadStop::Done, true),
            (2, StreamReadStop::Done, true)
        ]
    );
}

fn read_with_idle(seq: u32, id: &str, idle: Duration) -> StartStreamRead {
    StartStreamRead {
        idle_timeout: Some(idle.try_into().unwrap()),
        ..read_cmd(seq, id)
    }
}

fn timer_cmd(seq: u32) -> workflow_command::Variant {
    StartTimer {
        seq,
        start_to_fire_timeout: Some(Duration::from_secs(60).try_into().unwrap()),
    }
    .into()
}

/// A read with no idle timeout ends after 1 minute with no Record. On a quiet Stream it holds its
/// task open past a heartbeat, and then ends with `Idle`. Tokio's paused clock moves the minute.
#[tokio::test]
async fn a_read_with_no_idle_timeout_ends_after_a_minute() {
    let waits = Arc::new(AtomicUsize::new(0));
    let store = WatchedStore {
        inner: MemoryStore::new(DEFAULT_RETENTION, None),
        gave: Default::default(),
        waits: waits.clone(),
        closes: Default::default(),
    };
    let runtime = StreamRuntime::new(STORE, Arc::new(store), None);
    create(&runtime, "s").await;
    // The heartbeats come at 40 s and 80 s, so the minute ends between them.
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(50));
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_and_started();
    let sent = Arc::new(Mutex::new(vec![]));
    let mut cfg = MockPollCfg::from_resps(t, [1, 2].map(ResponseType::ToTaskNum));
    cfg.make_poll_stream_interminable = true;
    let kept = sent.clone();
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        kept.lock().push(completion.clone());
        Ok(Default::default())
    }));
    let mut mock = build_mock_pollers(cfg);
    // With no Local Activity manager, the Stream manager keeps the heartbeat on Tokio's clock.
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 1;
        wc.task_types = WorkerTaskTypes::workflow_only();
        wc.stream_runtimes = runtimes(&runtime);
    });
    let worker = mock_worker(mock);
    let first = worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        idle_timeout: None,
        ..read_cmd(1, "s")
    };
    complete(&worker, &first, vec![read.into()]).await;
    // The read's minute counts on this test's clock from its first wait on the Store.
    until("the read's wait on the Store", || {
        waits.load(Ordering::Acquire) == 1
    })
    .await;
    let ended = {
        let next = worker.poll_workflow_activation();
        tokio::pin!(next);
        tokio::select! {
            _ = async {
                move_clock(Duration::from_secs(40)).await;
                until("the heartbeat", || sent.lock().len() == 1).await;
            } => {}
            early = &mut next => panic!("an activation came before the heartbeat: {early:?}"),
        }
        assert!(sent.lock()[0].force_create_new_workflow_task);
        let ended = tokio::select! {
            ended = tokio::time::timeout(Duration::from_secs(30), &mut next) => ended,
            _ = move_clock(Duration::from_secs(20)) => {
                tokio::time::timeout(Duration::from_secs(30), &mut next).await
            }
        };
        ended
    };
    let ended = ended
        .expect("the read didn't end after its minute")
        .unwrap();
    let read = &reads(&ended)[0];
    assert!(read.records.is_empty());
    assert_eq!(stop(read), StreamReadStop::Idle);
    worker.complete_execution(&ended.run_id).await;
    worker.drain_pollers_and_shutdown().await;
}

/// A read whose timeout passed before Core could start it still looks at the Store once, and
/// gives the Records that are there. The history dates its Workflow Task an hour ago.
#[tokio::test]
async fn a_read_past_its_deadline_gives_what_is_there() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(30));
    t.add_workflow_task_scheduled_and_started();
    let an_hour_ago = SystemTime::now() - Duration::from_secs(3600);
    for event_id in 1..=3 {
        t.modify_event(event_id, |event| {
            event.event_time = Some(an_hour_ago.into())
        });
    }
    let (worker, _sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);
    let first = worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        timeout: Some(Duration::from_secs(60).try_into().unwrap()),
        ..read_cmd(1, "s")
    };
    complete(&worker, &first, vec![read.into()]).await;
    let mut got = vec![];
    let end = loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        let read = reads(&next).pop().unwrap();
        got.extend(texts(&read.records));
        if let Some(end) = read.end.clone() {
            worker.complete_execution(&next.run_id).await;
            break end;
        }
        complete(&worker, &next, vec![]).await;
    };
    worker.drain_pollers_and_shutdown().await;
    assert_eq!(got, ["a", "b"]);
    assert_eq!(end.stop(), StreamReadStop::Timeout);
    assert!(!end.done);
}

/// A read with a zero idle timeout doesn't wait for the Store. On an empty Stream it ends with
/// `Idle` immediately, and on a Stream that has Records it gives them.
#[tokio::test]
async fn a_read_with_a_zero_idle_timeout_gives_what_is_there() {
    let runtime = runtime();
    create(&runtime, "empty").await;
    create(&runtime, "full").await;
    put(&runtime, "full", &["a", "b"]).await;
    for (id, expected) in [("empty", vec![]), ("full", vec!["a", "b"])] {
        let mut t = TestHistoryBuilder::default();
        t.add_wfe_started_with_wft_timeout(Duration::from_secs(30));
        t.add_workflow_task_scheduled_and_started();
        let (worker, _sent) = worker(t, [ResponseType::AllHistory], &runtime, 0);
        let first = worker.poll_workflow_activation().await.unwrap();
        complete(
            &worker,
            &first,
            vec![read_with_idle(1, id, Duration::ZERO).into()],
        )
        .await;
        let mut got = vec![];
        let end = loop {
            let next = worker.poll_workflow_activation().await.unwrap();
            let read = reads(&next).pop().unwrap();
            got.extend(texts(&read.records));
            if let Some(end) = read.end.clone() {
                worker.complete_execution(&next.run_id).await;
                break end;
            }
            complete(&worker, &next, vec![]).await;
        };
        worker.drain_pollers_and_shutdown().await;
        assert_eq!(got, expected, "{id}");
        assert_eq!(end.stop(), StreamReadStop::Idle, "{id}");
        assert!(!end.done, "{id}");
    }
}

/// The idle time doesn't count while a read pauses for Workflow code to take its Records. A read
/// that pauses at its `read_ahead_bytes` reads the Records that came during the pause.
#[tokio::test]
async fn idle_time_skips_a_paused_read() {
    let (runtime, served) = counted_runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(30));
    t.add_workflow_task_scheduled_and_started();
    let runtimes = runtimes(&runtime).with_read_ahead_bytes(SMALL_READ_AHEAD);
    let run = start(t, [ResponseType::AllHistory], runtimes, 1, 0);
    let worker = run.worker;
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![read_with_idle(1, "s", Duration::from_secs(1)).into()],
    )
    .await;
    put(&runtime, "s", &["a"]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    let mut got = reads(&page)[0].records.len();
    let big = "x".repeat(1 << 10);
    for _ in 0..4 {
        put(&runtime, "s", &[big.as_str()]).await;
    }
    // Four Records of 1 KiB pass the bound, so the read pauses once the Store gave them.
    until("the read of four Records", || {
        served.load(Ordering::Acquire) >= 5
    })
    .await;
    for _ in 0..2 {
        put(&runtime, "s", &[big.as_str()]).await;
    }
    // Workflow code holds the activation for 2 s, so the read pauses past its idle time.
    tokio::time::sleep(Duration::from_millis(1700)).await;
    complete(&worker, &page, vec![]).await;
    let end = loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        let read = reads(&next).pop().unwrap();
        got += read.records.len();
        if let Some(end) = read.end.clone() {
            worker.complete_execution(&next.run_id).await;
            break end;
        }
        complete(&worker, &next, vec![]).await;
    };
    worker.drain_pollers_and_shutdown().await;
    assert_eq!(end.stop(), StreamReadStop::Idle);
    assert_eq!(got, all_records(&runtime, "s").await.len());
}

/// The idle time doesn't count while a page waits for the next Workflow Task. Records that come
/// in that wait join the page.
#[tokio::test]
async fn idle_time_skips_a_page_that_waits_for_a_task() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_timer_started("1".to_string());
    t.add_workflow_task_scheduled_and_started();
    // The second task comes only after "b" is in, so the page can't go out with "a" alone.
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let next_task = async move {
        released.await.ok();
    }
    .boxed();
    let responses = [
        ResponseType::ToTaskNum(1),
        ResponseType::UntilResolved(next_task, 2),
    ];
    let (worker, sent) = worker(t, responses, &runtime, 0);
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![
            read_with_idle(1, "s", Duration::from_millis(300)).into(),
            timer_cmd(1),
        ],
    )
    .await;
    put(&runtime, "s", &["a"]).await;
    // Longer than the idle time, which must not end a read whose page waits for a task.
    tokio::time::sleep(Duration::from_millis(700)).await;
    put(&runtime, "s", &["b"]).await;
    release.send(()).unwrap();
    let page = worker.poll_workflow_activation().await.unwrap();
    let read = &reads(&page)[0];
    assert_eq!(texts(&read.records), ["a", "b"]);
    assert!(read.end.is_none(), "the read ended while its page waited");
    worker.complete_execution(&page.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    assert!(sent.lock()[0].force_create_new_workflow_task);
}

/// No read task outlives its Worker, also one that paused because its run holds its
/// `read_ahead_bytes` that no Workflow Task took.
#[tokio::test]
async fn no_read_outlives_its_worker() {
    let (runtime, served) = counted_runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    let runtimes = runtimes(&runtime).with_read_ahead_bytes(SMALL_READ_AHEAD);
    // The mock poller stays alive, or its end would shut the Worker down before the read pauses.
    let run = start_with(t, [ResponseType::AllHistory], runtimes, 1, 0, true);
    let (worker, sent) = (run.worker, run.sent);
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(
        &worker,
        &first,
        vec![read_with_idle(1, "s", Duration::from_secs(1)).into()],
    )
    .await;
    put(&runtime, "s", &["a"]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    // The timer completes the task immediately, so the read's later pages wait for a task that
    // never comes.
    complete(&worker, &page, vec![timer_cmd(1)]).await;
    assert_eq!(sent.lock().len(), 1);
    let big = "x".repeat(1 << 10);
    for _ in 0..5 {
        put(&runtime, "s", &[big.as_str()]).await;
    }
    // Four Records of 1 KiB pass the bound, so the read pauses once the Store gave them.
    until("the read of four Records", || {
        served.load(Ordering::Acquire) >= 5
    })
    .await;
    worker.drain_pollers_and_shutdown().await;
    // A paused read would hold the runtime for good, so this wait would not end.
    until("the end of every Stream task", || {
        Arc::strong_count(&runtime) == 1
    })
    .await;
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

/// A replay Worker needs no runtime for a read that history leaves open, when its marker holds
/// the Records.
#[tokio::test]
async fn a_replay_worker_needs_no_runtime_for_an_open_read() {
    let records = vec![StreamRecord {
        producer: Some(client()),
        body: Some(body("a")),
        offset: 1u64.to_be_bytes().to_vec(),
    }];
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], None, true);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let worker = replay_worker(t, StreamRuntimes::default());
    let first = worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        records_in_history: true,
        ..read_cmd(1, "s")
    };
    complete(&worker, &first, vec![read.into()]).await;
    let jobs = activations_until_the_end(&worker).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), worker.drain_pollers_and_shutdown()).await;
    assert!(
        jobs.iter().any(|job| job == "ResolveStreamRead(1)"),
        "{jobs:?}"
    );
    assert!(
        jobs.iter().any(|job| job.starts_with("SignalWorkflow")),
        "{jobs:?}"
    );
    assert!(!has_failure_eviction(&jobs), "{jobs:?}");
}

/// A replay Worker still reads the Records of a marker that has none from the Store.
#[tokio::test]
async fn a_replay_worker_reads_a_marker_from_the_store() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let records = all_records(&runtime, "s").await;
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], Some((StreamReadStop::Done, 1)), false);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let worker = replay_worker(t, runtimes(&runtime));
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    let replayed = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(reads(&replayed)[0].records, records);
    complete(&worker, &replayed, vec![]).await;
    let _ = activations_until_the_end(&worker).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), worker.drain_pollers_and_shutdown()).await;
}

/// A cached run that gets a full history doesn't read the markers that it has already. Here the
/// marker in history names a Store that the Worker has no runtime for, so a read of it would fail
/// the task.
#[tokio::test]
async fn a_cached_run_reads_no_marker_of_a_full_history() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let elsewhere = read_entry(
        1,
        vec![],
        &[Page {
            records: &[StreamRecord {
                offset: 1u64.to_be_bytes().to_vec(),
                ..Default::default()
            }],
            activation: 1,
        }],
        None,
        false,
    );
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(500));
    t.add_full_wf_task();
    stream_marker_event(
        &mut t,
        4,
        &StreamMarkerData {
            reads: vec![elsewhere],
            appends: vec![],
            closes: vec![],
            streams: vec![StreamRef {
                id: "s".to_string(),
                store: "elsewhere".to_string(),
            }],
        },
    );
    t.add_we_signaled("go", vec![]);
    t.add_workflow_task_scheduled_and_started();
    // Both tasks come as a full history, as from the normal queue after a sticky timeout.
    let responses = [1, 2].map(ResponseType::ToTaskNum);
    let run = start(t, responses, runtimes(&runtime), 1, 0);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![read_cmd(1, "s").into()]).await;
    put(&runtime, "s", &["a"]).await;
    let page = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &page, vec![]).await;
    // The heartbeat completes the first task, and the second one comes.
    let signal = run.worker.poll_workflow_activation().await.unwrap();
    assert!(signaled(&signal));
    run.worker.complete_execution(&signal.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert!(run.failed.lock().is_empty(), "{:?}", run.failed.lock());
    assert_eq!(run.sent.lock().len(), 2);
}

/// Replays the history of a marker of read 1 of Stream `s` on a replay Worker, and returns the
/// message of the eviction that ends it. The replay Worker must end the history, and not feed it
/// again.
async fn replay_worker_eviction(data: StreamMarkerData, runtimes: StreamRuntimes) -> String {
    let worker = replay_worker(replayed_read_history(&data), runtimes);
    let evicted = tokio::time::timeout(Duration::from_secs(60), worker.poll_workflow_activation())
        .await
        .expect("the replay Worker gave no activation")
        .unwrap();
    let Some(workflow_activation_job::Variant::RemoveFromCache(evict)) =
        evicted.jobs.first().and_then(|job| job.variant.clone())
    else {
        panic!("Expected an eviction, got {:?}", evicted.jobs);
    };
    assert_eq!(evicted.jobs.len(), 1);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(evicted.run_id))
        .await
        .unwrap();
    let next = tokio::time::timeout(Duration::from_secs(60), worker.poll_workflow_activation())
        .await
        .expect("the replay Worker didn't end the history");
    assert!(
        next.is_err(),
        "the replay Worker fed the history again: {next:?}"
    );
    let _ = tokio::time::timeout(Duration::from_secs(5), worker.drain_pollers_and_shutdown()).await;
    evict.message
}

/// A replay Worker with no runtime for the Store of a marker evicts the run with the
/// `StreamReplayError`, and ends the history.
#[tokio::test]
async fn a_replay_worker_with_no_runtime_ends_the_history() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let (data, range) = one_page(&all_records(&runtime, "s").await);
    let message = replay_worker_eviction(data, StreamRuntimes::default()).await;
    assert!(
        message.contains(&format!(
            "StreamReplayError: {range}: this Worker has no Stream runtime for the Store `mem`"
        )),
        "{message}"
    );
}

/// A replay Worker whose Store lost the Records of a marker evicts the run with the
/// `StreamReplayError`, and ends the history.
#[tokio::test]
async fn a_replay_worker_whose_records_are_gone_ends_the_history() {
    let options = MemoryStreamOptions {
        retention: Some(Duration::from_millis(100)),
        ..Default::default()
    };
    let runtime = MemoryStreamRuntime::new(STORE, options, None);
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let (data, range) = one_page(&all_records(&runtime, "s").await);
    close(&runtime, "s").await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let message = replay_worker_eviction(data, runtimes(&runtime)).await;
    assert!(
        message.contains(&format!("StreamReplayError: {range}: stream_expired")),
        "{message}"
    );
}

/// A Stream of no Store name, as lang sends it when it has no Stream config to pick one from.
fn unnamed(id: &str) -> Option<StreamRef> {
    Some(StreamRef {
        id: id.to_string(),
        store: String::new(),
    })
}

/// In a replay, a read with no Store name takes the Store of its entry in history, so a replay
/// Worker with no runtime replays a marker that holds its Records.
#[tokio::test]
async fn a_replayed_read_with_no_store_name_takes_the_store_of_history() {
    let records = vec![StreamRecord {
        producer: Some(client()),
        body: Some(body("a")),
        offset: 1u64.to_be_bytes().to_vec(),
    }];
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], Some((StreamReadStop::Done, 1)), true);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let worker = replay_worker(t, StreamRuntimes::default());
    let first = worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        stream: unnamed("s"),
        records_in_history: true,
        ..read_cmd(1, "s")
    };
    complete(&worker, &first, vec![read.into()]).await;
    let jobs = activations_until_the_end(&worker).await;
    let _ = tokio::time::timeout(Duration::from_secs(5), worker.drain_pollers_and_shutdown()).await;
    assert!(
        jobs.iter().any(|job| job == "ResolveStreamRead(1)"),
        "{jobs:?}"
    );
    assert!(
        jobs.iter().any(|job| job.starts_with("SignalWorkflow")),
        "{jobs:?}"
    );
    assert!(!has_failure_eviction(&jobs), "{jobs:?}");
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
    let t = replayed_read_history(&StreamMarkerData {
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

/// A replayed call that names another Store than history is still a non-determinism error.
#[tokio::test]
async fn a_replayed_read_of_another_store_is_nondeterministic() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    let (data, _) = one_page(&all_records(&runtime, "s").await);
    let t = replayed_read_history(&data);
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 1);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let read = StartStreamRead {
        stream: Some(StreamRef {
            id: "s".to_string(),
            store: "other".to_string(),
        }),
        ..read_cmd(1, "s")
    };
    complete(&run.worker, &first, vec![read.into()]).await;
    run.worker.drain_pollers_and_shutdown().await;
    let failed = run.failed.lock();
    assert_eq!(failed[0].0, WorkflowTaskFailedCause::NonDeterministicError);
    assert!(failed[0].1.contains("in Store `other`"), "{}", failed[0].1);
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
    // The memory Store answers at once, so this works also inside a mock's sync closure.
    let described = runtime
        .describe_stream(DescribeStreamRequest {
            r#ref: Some(stream(id)),
            ..Default::default()
        })
        .now_or_never()
        .expect("the memory Store answers at once")
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

/// A history whose first task completed with a marker event, and whose second one started.
fn two_task_history() -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    marker_event(&mut t, 4);
    t.add_workflow_task_scheduled_and_started();
    t
}

/// An awaited close spans two Workflow Tasks. The first records the intent and calls no Store.
/// The Stream closes after the server took that task, and the second task gets the result.
#[tokio::test]
async fn an_awaited_close_spans_two_tasks() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let t = two_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let open_at_completion = Arc::new(Mutex::new(vec![]));
    let sent = Arc::new(Mutex::new(vec![]));
    let mut cfg = MockPollCfg::from_resps(t, [1, 2].map(ResponseType::ToTaskNum));
    let (kept, seen, store) = (sent.clone(), open_at_completion.clone(), runtime.clone());
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        seen.lock().push(!stream_is_closed(&store, "s"));
        kept.lock().push(completion.clone());
        Ok(Default::default())
    }));
    let mut mock = build_mock_pollers(cfg);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 1;
        wc.stream_runtimes = runtimes(&runtime);
    });
    let worker = mock_worker(mock);

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
    let t = two_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let (worker, sent) = worker(t, [1, 2].map(ResponseType::ToTaskNum), &runtime, 0);

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
    let t = two_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let (worker, _sent) = worker(t, [1, 2].map(ResponseType::ToTaskNum), &runtime, 0);
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
    let t = two_task_history();
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
    let (worker, sent) = worker(t, [1, 2].map(ResponseType::ToTaskNum), &runtime, 0);
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
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
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
        inner: MemoryStore::new(DEFAULT_RETENTION, None),
        gave: Default::default(),
        waits: Default::default(),
        closes: closes.clone(),
    };
    let runtime = StreamRuntime::new(STORE, Arc::new(store), None);
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
    commands: Vec<workflow_command::Variant>,
) -> (WorkflowTaskFailedCause, String) {
    let runtime = runtime();
    create(&runtime, "s").await;
    let t = close_history(intent_entry(1), None);
    let run = start(t, [ResponseType::AllHistory], runtimes(&runtime), 1, 1);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, commands).await;
    run.worker.drain_pollers_and_shutdown().await;
    let failed = run.failed.lock();
    assert_eq!(failed.len(), 1, "the replay should fail");
    failed[0].clone()
}

/// An intent in History with no close in Workflow code is a non-determinism error.
#[tokio::test]
async fn a_close_entry_with_no_call_is_nondeterministic() {
    let (cause, message) = replay_close_history(vec![]).await;
    assert_eq!(cause, WorkflowTaskFailedCause::NonDeterministicError);
    assert!(
        message.contains("History has a Stream result of close 1 that Workflow code never got"),
        "{message}"
    );
}

/// A close of another Stream than History has is a non-determinism error.
#[tokio::test]
async fn a_close_of_another_stream_is_nondeterministic() {
    let (cause, message) = replay_close_history(vec![close_cmd(1, "other", "r").into()]).await;
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
async fn until_resolved(worker: &Worker, reads: usize, appends: usize) -> WorkflowActivation {
    let (mut ended, mut appended) = (0, 0);
    loop {
        let next = worker.poll_workflow_activation().await.unwrap();
        ended += self::reads(&next)
            .iter()
            .filter(|read| read.end.is_some())
            .count();
        appended += self::appends(&next).len();
        if ended == reads && appended == appends {
            return next;
        }
        complete(worker, &next, vec![]).await;
    }
}

/// A read counts once when Core starts it, and counts each Record that Workflow code got.
#[tokio::test]
async fn a_read_counts_its_start_and_its_records() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    close(&runtime, "s").await;
    let (run, meter) = metered(
        one_task_history(),
        [ResponseType::AllHistory],
        runtimes(&runtime),
    );
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![read_cmd(1, "s").into()]).await;
    let last = until_resolved(&run.worker, 1, 0).await;
    run.worker.complete_execution(&last.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(
        totals(
            &meter,
            &[
                "stream_read_total",
                "stream_read_records",
                "stream_read_failed"
            ]
        ),
        [1, 2, 0]
    );
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
    let last = until_resolved(&run.worker, 0, 1).await;
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
    let t = two_task_history();
    let run_id = t.get_orig_run_id().to_string();
    let (run, meter) = metered(t, [1, 2].map(ResponseType::ToTaskNum), runtimes(&runtime));
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

/// A replay that reads the Records of a marker from the Store counts the entry and its latency,
/// and counts no read.
#[tokio::test]
async fn a_replay_counts_its_read_of_a_marker_entry() {
    let runtime = runtime();
    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let (data, _) = one_page(&all_records(&runtime, "s").await);
    let (run, meter) = metered(
        replayed_read_history(&data),
        [ResponseType::AllHistory],
        runtimes(&runtime),
    );
    let first = run.worker.poll_workflow_activation().await.unwrap();
    assert!(first.is_replaying);
    complete(&run.worker, &first, vec![read_cmd(1, "s").into()]).await;
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&replayed)[0].records), ["a", "b"]);
    complete(&run.worker, &replayed, vec![]).await;
    let live = run.worker.poll_workflow_activation().await.unwrap();
    // The read goes on live, which counts as a start, with no Record yet.
    complete(&run.worker, &live, vec![]).await;
    put(&runtime, "s", &["c"]).await;
    let page = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["c"]);
    run.worker.complete_execution(&page.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(
        totals(
            &meter,
            &[
                "stream_replay_read_total",
                "stream_replay_read_failed",
                "stream_read_total",
                "stream_read_records",
            ]
        ),
        [1, 0, 1, 1]
    );
    assert_eq!(meter.duration_count("stream_replay_read_latency", MEM), 1);
}

/// A read, an append, a close and a replay's read of a marker entry that fail each count as
/// failed.
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
        vec![
            read_cmd(1, "missing").into(),
            append_cmd(1, "closed", &run_id, &["a"]).into(),
        ],
    )
    .await;
    let last = until_resolved(&run.worker, 1, 1).await;
    run.worker.complete_execution(&last.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(
        totals(
            &meter,
            &[
                "stream_read_total",
                "stream_read_failed",
                "stream_append_total",
                "stream_append_failed",
            ]
        ),
        [1, 1, 1, 1]
    );

    // A write of a later Workflow Task of the run raises the fence over the task of the close.
    create(&runtime, "fenced").await;
    let t = two_task_history();
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
    let (run, meter) = metered(t, [1, 2].map(ResponseType::ToTaskNum), runtimes(&runtime));
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

    create(&runtime, "s").await;
    put(&runtime, "s", &["a", "b"]).await;
    let (mut data, _) = one_page(&all_records(&runtime, "s").await);
    data.reads[0].digest[0] ^= 1;
    let meter = Arc::new(CounterRecordingMeter::default());
    let run = start_metered(
        replayed_read_history(&data),
        [ResponseType::AllHistory],
        runtimes(&runtime),
        1,
        1,
        true,
        Some(&meter),
    );
    // The task fails before any activation, so the poll ends when the failure is in.
    tokio::select! {
        _ = until("the failed task", || run.failed.lock().len() == 1) => {}
        polled = run.worker.poll_workflow_activation() => panic!("an activation came: {polled:?}"),
    }
    run.worker.drain_pollers_and_shutdown().await;
    assert_eq!(
        totals(
            &meter,
            &["stream_replay_read_total", "stream_replay_read_failed"]
        ),
        [1, 1]
    );
    assert_eq!(meter.duration_count("stream_replay_read_latency", MEM), 1);
}

/// A read, an append and a close that a replay gives from History count nothing, and neither does
/// a replay of a marker that holds its Records.
#[tokio::test]
async fn replayed_calls_count_nothing() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let meter = Arc::new(CounterRecordingMeter::default());

    put(&runtime, "s", &["a"]).await;
    let records = all_records(&runtime, "s").await;
    let page = Page {
        records: &records,
        activation: 1,
    };
    let entry = read_entry(1, vec![], &[page], Some((StreamReadStop::Done, 1)), true);
    let read_history = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let read = StartStreamRead {
        records_in_history: true,
        ..read_cmd(1, "s")
    };
    let appended = StreamAppendEntry {
        seq: 1,
        stream_index: 0,
        first_offset: 7u64.to_be_bytes().to_vec(),
        last_offset: 8u64.to_be_bytes().to_vec(),
        failure: None,
        activation: 1,
    };
    let append_history = replayed_read_history(&StreamMarkerData {
        reads: vec![],
        appends: vec![appended],
        closes: vec![],
        streams: vec![stream("s")],
    });
    let append_run = append_history.get_orig_run_id().to_string();
    let close_events = close_history(intent_entry(1), Some(close_result_entry(1, None)));
    let close_run = close_events.get_orig_run_id().to_string();
    for (t, command) in [
        (read_history, workflow_command::Variant::from(read)),
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
    // The meter took the metrics of the three Workers.
    assert_eq!(meter.counter_total("worker_start", &[]), 3);
    let names = [
        "stream_read_total",
        "stream_read_records",
        "stream_append_total",
        "stream_close_total",
        "stream_replay_read_total",
    ];
    assert_eq!(totals(&meter, &names), [0; 5]);
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
    let other = MemoryStreamRuntime::new("other", MemoryStreamOptions::default(), None);
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
    let runtimes = unbatched([
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
    let last = until_resolved(&run.worker, 0, 2).await;
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

/// A memory Store that holds each close until the test lets it go.
struct HeldCloseStore {
    inner: MemoryStore,
    /// Told when a close reaches the Store.
    held: Arc<Notify>,
    /// One permit lets one close go.
    release: Arc<Semaphore>,
}

#[async_trait::async_trait]
impl Store for HeldCloseStore {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn create(&self, stream_ref: &StreamRef, create: &Create<'_>) -> StreamResult<Created> {
        self.inner.create(stream_ref, create).await
    }

    async fn append(&self, stream_ref: &StreamRef, batch: &Batch<'_>) -> StreamResult<Appended> {
        self.inner.append(stream_ref, batch).await
    }

    async fn read(
        &self,
        stream_ref: &StreamRef,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)> {
        self.inner.read(stream_ref, from, count).await
    }

    async fn wait_after(
        &self,
        stream_ref: &StreamRef,
        after: i64,
        deadline: tokio::time::Instant,
    ) -> StreamResult<bool> {
        self.inner.wait_after(stream_ref, after, deadline).await
    }

    async fn close(&self, stream_ref: &StreamRef, producer: &Producer) -> StreamResult<Closed> {
        self.held.notify_one();
        self.release
            .acquire()
            .await
            .expect("the test never closes the semaphore")
            .forget();
        self.inner.close(stream_ref, producer).await
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
        let held = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let store = HeldCloseStore {
            inner: MemoryStore::new(DEFAULT_RETENTION, None),
            held: held.clone(),
            release: release.clone(),
        };
        let runtime = StreamRuntime::new(STORE, Arc::new(store), None);
        create(&runtime, "s").await;
        Self {
            runtime,
            held,
            release,
            shutdown_waits: Arc::new(Notify::new()),
        }
    }

    fn runtimes(&self) -> StreamRuntimes {
        runtimes(&self.runtime).with_shutdown_waits(self.shutdown_waits.clone())
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
    let run = start_with(t, [ResponseType::AllHistory], held.runtimes(), 1, 0, true);
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
    let run = start_with(t, [ResponseType::AllHistory], held.runtimes(), 1, 0, true);
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
    let runtimes = held.runtimes().with_close_bound(Duration::from_secs(1));
    let run = start_with(t, [ResponseType::AllHistory], runtimes, 1, 0, true);
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
}

/// A `read_ahead_bytes` that four Records of 1 KiB pass, so that a test pauses a read with little
/// memory.
const SMALL_READ_AHEAD: usize = 4 << 10;

/// A memory Store that counts the entries that its reads gave.
struct CountedStore {
    inner: MemoryStore,
    served: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Store for CountedStore {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn create(&self, stream_ref: &StreamRef, create: &Create<'_>) -> StreamResult<Created> {
        self.inner.create(stream_ref, create).await
    }

    async fn append(&self, stream_ref: &StreamRef, batch: &Batch<'_>) -> StreamResult<Appended> {
        self.inner.append(stream_ref, batch).await
    }

    async fn read(
        &self,
        stream_ref: &StreamRef,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)> {
        let read = self.inner.read(stream_ref, from, count).await?;
        self.served.fetch_add(read.1.len(), Ordering::AcqRel);
        Ok(read)
    }

    async fn wait_after(
        &self,
        stream_ref: &StreamRef,
        after: i64,
        deadline: tokio::time::Instant,
    ) -> StreamResult<bool> {
        self.inner.wait_after(stream_ref, after, deadline).await
    }

    async fn close(&self, stream_ref: &StreamRef, producer: &Producer) -> StreamResult<Closed> {
        self.inner.close(stream_ref, producer).await
    }
}

/// A runtime on a [CountedStore], and the count of the entries that its reads gave.
fn counted_runtime() -> (Arc<StreamRuntime>, Arc<AtomicUsize>) {
    let served = Arc::new(AtomicUsize::new(0));
    let store = CountedStore {
        inner: MemoryStore::new(DEFAULT_RETENTION, None),
        served: served.clone(),
    };
    (StreamRuntime::new(STORE, Arc::new(store), None), served)
}

/// A read with a small `read_ahead_bytes` stops asking the Store while that many bytes of its
/// Records wait for Workflow code, and goes on once an activation takes them. Its first page
/// always comes.
#[tokio::test]
async fn a_read_pauses_at_its_read_ahead_bytes() {
    let (runtime, served) = counted_runtime();
    create(&runtime, "s").await;
    // One byte, so that each page is past the bound until an activation takes it.
    let runtimes = runtimes(&runtime).with_read_ahead_bytes(1);
    let run = start_with(
        one_task_history(),
        [ResponseType::AllHistory],
        runtimes,
        1,
        0,
        true,
    );
    let first = run.worker.poll_workflow_activation().await.unwrap();
    complete(&run.worker, &first, vec![read_cmd(1, "s").into()]).await;
    put(&runtime, "s", &["a"]).await;
    let page = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["a"]);
    // Workflow code holds the page. The read gives "b" to the run, and then waits, so "c" stays
    // in the Store until an activation takes "b".
    put(&runtime, "s", &["b"]).await;
    until("the read of b", || served.load(Ordering::Acquire) >= 2).await;
    put(&runtime, "s", &["c"]).await;
    assert_eq!(served.load(Ordering::Acquire), 2);
    complete(&run.worker, &page, vec![]).await;
    let page = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["b"]);
    complete(&run.worker, &page, vec![]).await;
    let page = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(texts(&reads(&page)[0].records), ["c"]);
    run.worker.complete_execution(&page.run_id).await;
    run.worker.drain_pollers_and_shutdown().await;
}

/// A Worker whose `read_ahead_bytes` is zero is refused when its config is built.
#[test]
fn a_zero_read_ahead_bytes_is_refused() {
    let runtimes = runtimes(&runtime()).with_read_ahead_bytes(0);
    let Err(refused) = test_worker_cfg().stream_runtimes(runtimes).build() else {
        panic!("a zero `read_ahead_bytes` was taken");
    };
    assert_eq!(
        refused,
        "`read_ahead_bytes` of `stream_runtimes` must be > 0"
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

    let replayed = replayed_read_history(&data);
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

/// A memory Store that keeps when each of its reads gave Records, and counts the long polls that
/// wait on it.
struct WatchedStore {
    inner: MemoryStore,
    /// The clock time and the count of each read that gave Records.
    gave: Arc<Mutex<Vec<(tokio::time::Instant, usize)>>>,
    waits: Arc<AtomicUsize>,
    /// The Producer of each close.
    closes: Arc<Mutex<Vec<Producer>>>,
}

#[async_trait::async_trait]
impl Store for WatchedStore {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn create(&self, stream_ref: &StreamRef, create: &Create<'_>) -> StreamResult<Created> {
        self.inner.create(stream_ref, create).await
    }

    async fn append(&self, stream_ref: &StreamRef, batch: &Batch<'_>) -> StreamResult<Appended> {
        self.inner.append(stream_ref, batch).await
    }

    async fn read(
        &self,
        stream_ref: &StreamRef,
        from: i64,
        count: usize,
    ) -> StreamResult<(StreamState, Vec<Entry>)> {
        let read = self.inner.read(stream_ref, from, count).await?;
        if !read.1.is_empty() {
            self.gave
                .lock()
                .push((tokio::time::Instant::now(), read.1.len()));
        }
        Ok(read)
    }

    async fn wait_after(
        &self,
        stream_ref: &StreamRef,
        after: i64,
        deadline: tokio::time::Instant,
    ) -> StreamResult<bool> {
        self.waits.fetch_add(1, Ordering::AcqRel);
        self.inner.wait_after(stream_ref, after, deadline).await
    }

    async fn close(&self, stream_ref: &StreamRef, producer: &Producer) -> StreamResult<Closed> {
        self.closes.lock().push(producer.clone());
        self.inner.close(stream_ref, producer).await
    }
}

/// A Worker with a batch window of `window` on a [WatchedStore] with Stream `s`, whose first
/// Workflow Task is out. Its task timeout is long, so that no heartbeat comes while a test moves
/// the clock.
struct Windowed {
    runtime: Arc<StreamRuntime>,
    gave: Arc<Mutex<Vec<(tokio::time::Instant, usize)>>>,
    waits: Arc<AtomicUsize>,
    run: Run,
    first: WorkflowActivation,
}

impl Windowed {
    async fn new(window: Duration) -> Self {
        let gave = Arc::new(Mutex::new(vec![]));
        let waits = Arc::new(AtomicUsize::new(0));
        let store = WatchedStore {
            inner: MemoryStore::new(DEFAULT_RETENTION, None),
            gave: gave.clone(),
            waits: waits.clone(),
            closes: Default::default(),
        };
        let runtime = StreamRuntime::new(STORE, Arc::new(store), None);
        create(&runtime, "s").await;
        let mut t = TestHistoryBuilder::default();
        t.add_wfe_started_with_wft_timeout(Duration::from_secs(3600));
        t.add_workflow_task_scheduled_and_started();
        let runtimes = StreamRuntimes::new([(STORE.to_string(), runtime.clone())])
            .with_read_batch_window(window);
        let run = start_with(t, [ResponseType::AllHistory], runtimes, 1, 0, true);
        let first = run.worker.poll_workflow_activation().await.unwrap();
        Self {
            runtime,
            gave,
            waits,
            run,
            first,
        }
    }

    /// The next activation, which must come without a wait for a window.
    async fn next(&self) -> WorkflowActivation {
        tokio::time::timeout(
            Duration::from_secs(30),
            self.run.worker.poll_workflow_activation(),
        )
        .await
        .expect("no activation came")
        .unwrap()
    }

    async fn finish(self, last: &WorkflowActivation) {
        self.run.worker.complete_execution(&last.run_id).await;
        self.run.worker.drain_pollers_and_shutdown().await;
    }
}

/// A read whose idle timeout is far past the clock moves of a test with a long window.
fn long_read(seq: u32) -> StartStreamRead {
    StartStreamRead {
        idle_timeout: Some(Duration::from_secs(3600).try_into().unwrap()),
        ..read_cmd(seq, "s")
    }
}

/// Moves Tokio's clock by `by`, so that a long window passes with no real wait.
async fn move_clock(by: Duration) {
    tokio::time::pause();
    tokio::time::advance(by).await;
    tokio::time::resume();
}

const LONG_WINDOW: Duration = Duration::from_secs(60);

/// A read gives its first Record immediately. The Records that come in the window after that
/// batch go in one batch, which the read asks the Store for only once the window passed.
#[tokio::test]
async fn a_read_gives_the_records_of_its_window_in_one_batch() {
    let w = Windowed::new(LONG_WINDOW).await;
    complete(&w.run.worker, &w.first, vec![long_read(1).into()]).await;
    until("the read's wait on the Store", || {
        w.waits.load(Ordering::Acquire) == 1
    })
    .await;
    put(&w.runtime, "s", &["a"]).await;
    let page = w.next().await;
    assert_eq!(texts(&reads(&page)[0].records), ["a"]);
    // The read task gives a batch and goes on in one poll on this thread, so it waits in its
    // window before this test runs again. With no window it would wait on the Store again.
    assert_eq!(
        w.waits.load(Ordering::Acquire),
        1,
        "the read asked the Store again in its window"
    );
    complete(&w.run.worker, &page, vec![]).await;
    put(&w.runtime, "s", &["b"]).await;
    put(&w.runtime, "s", &["c"]).await;
    move_clock(LONG_WINDOW).await;
    let page = w.next().await;
    assert_eq!(texts(&reads(&page)[0].records), ["b", "c"]);
    let gave = w.gave.lock().clone();
    assert_eq!(gave.iter().map(|(_, n)| *n).collect::<Vec<_>>(), [1, 2]);
    assert!(
        gave[1].0 - gave[0].0 >= LONG_WINDOW,
        "the read asked the Store again before its window passed"
    );
    w.finish(&page).await;
}

/// With a zero window, the read waits on the Store again right after each batch, and gives each
/// batch as it comes.
#[tokio::test]
async fn a_zero_window_gives_each_batch_as_it_comes() {
    let w = Windowed::new(Duration::ZERO).await;
    complete(&w.run.worker, &w.first, vec![long_read(1).into()]).await;
    let mut last = None;
    for (at, text) in ["a", "b", "c"].into_iter().enumerate() {
        // The read waits on the Store before the Record comes, so no window holds it.
        until("the read's wait on the Store", || {
            w.waits.load(Ordering::Acquire) > at
        })
        .await;
        put(&w.runtime, "s", &[text]).await;
        let page = w.next().await;
        assert_eq!(texts(&reads(&page)[0].records), [text]);
        if at < 2 {
            complete(&w.run.worker, &page, vec![]).await;
        } else {
            last = Some(page);
        }
    }
    assert_eq!(w.gave.lock().len(), 3);
    w.finish(&last.unwrap()).await;
}

/// A `timeout` that passes in the window ends the read at its deadline, with no look at the
/// Store for the Record that came in the window.
#[tokio::test]
async fn a_timeout_in_the_window_ends_the_read() {
    let w = Windowed::new(LONG_WINDOW).await;
    let read = StartStreamRead {
        timeout: Some(Duration::from_secs(30).try_into().unwrap()),
        ..long_read(1)
    };
    complete(&w.run.worker, &w.first, vec![read.into()]).await;
    put(&w.runtime, "s", &["a"]).await;
    let page = w.next().await;
    assert_eq!(texts(&reads(&page)[0].records), ["a"]);
    complete(&w.run.worker, &page, vec![]).await;
    put(&w.runtime, "s", &["b"]).await;
    move_clock(Duration::from_secs(30)).await;
    let ended = w.next().await;
    let read = &reads(&ended)[0];
    assert!(read.records.is_empty(), "{:?}", texts(&read.records));
    assert_eq!(stop(read), StreamReadStop::Timeout);
    assert_eq!(w.gave.lock().len(), 1);
    w.finish(&ended).await;
}

/// A close of a read in its window stops it. A second read takes the Records that came, and the
/// first never asks the Store again, also once its window passed.
#[tokio::test]
async fn a_close_in_the_window_stops_the_read() {
    let w = Windowed::new(LONG_WINDOW).await;
    complete(&w.run.worker, &w.first, vec![long_read(1).into()]).await;
    put(&w.runtime, "s", &["a"]).await;
    let page = w.next().await;
    let after_a = reads(&page)[0].records[0].offset.clone();
    let second = StartStreamRead {
        from_offset: after_a,
        ..long_read(2)
    };
    complete(
        &w.run.worker,
        &page,
        vec![CloseStreamRead { seq: 1 }.into(), second.into()],
    )
    .await;
    put(&w.runtime, "s", &["b"]).await;
    let page = w.next().await;
    assert_eq!(reads(&page).iter().map(|r| r.seq).collect::<Vec<_>>(), [2]);
    assert_eq!(texts(&reads(&page)[0].records), ["b"]);
    complete(&w.run.worker, &page, vec![]).await;
    // Both windows pass. A read that still ran would look at the Store again here.
    move_clock(LONG_WINDOW).await;
    put(&w.runtime, "s", &["c"]).await;
    let page = w.next().await;
    assert_eq!(reads(&page).iter().map(|r| r.seq).collect::<Vec<_>>(), [2]);
    assert_eq!(texts(&reads(&page)[0].records), ["c"]);
    assert_eq!(
        w.gave.lock().len(),
        3,
        "the closed read asked the Store again"
    );
    w.finish(&page).await;
}

/// A task that a read holds open until its heartbeat counts that hold apart from its duration, so
/// that the duration warning leaves it out.
#[tokio::test]
async fn a_read_holds_its_task_apart_from_the_duration_warning() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(500));
    t.add_full_wf_task();
    marker_event(&mut t, 4);
    t.add_workflow_task_scheduled_and_started();
    let (worker, sent) = worker(t, [1, 2].map(ResponseType::ToTaskNum), &runtime, 0);
    let first = worker.poll_workflow_activation().await.unwrap();
    let run_id = first.run_id.clone();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    put(&runtime, "s", &["a"]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &page, vec![]).await;
    let page = poll_after_heartbeat(&worker, &sent, || put(&runtime, "s", &["b"])).await;
    worker.complete_execution(&page.run_id).await;
    worker.drain_pollers_and_shutdown().await;
    let tasks = crate::worker::TASK_DURATIONS
        .lock()
        .remove(&run_id)
        .expect("the run recorded its tasks");
    let (duration, held) = tasks[0];
    assert!(held > Duration::ZERO, "the read's hold wasn't counted");
    assert!(held <= duration, "{held:?} of {duration:?}");
}

/// A history in which the first Workflow Task, on this Worker, started read 1 and a timer, and a
/// second one, on another Worker, recorded `marker` if it is given. A signal starts each later
/// task, and the third one is the task that comes back to this Worker.
fn foreign_task_history(marker: Option<&StreamMarkerData>) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(3600));
    t.add_full_wf_task();
    t.add_timer_started("1".to_string());
    t.add_we_signaled("go", vec![]);
    t.add_full_wf_task();
    if let Some(marker) = marker {
        stream_marker_event(&mut t, 9, marker);
    }
    t.add_we_signaled("again", vec![]);
    t.add_workflow_task_scheduled_and_started();
    t
}

/// A runtime on a [WatchedStore], and the reads of it that gave Records.
type Watched = (
    Arc<StreamRuntime>,
    Arc<Mutex<Vec<(tokio::time::Instant, usize)>>>,
);

/// A runtime on a [WatchedStore] with Stream `s` and one Record, "a", and the reads that gave
/// Records.
async fn watched_with_a() -> Watched {
    let gave = Arc::new(Mutex::new(vec![]));
    let store = WatchedStore {
        inner: MemoryStore::new(DEFAULT_RETENTION, None),
        gave: gave.clone(),
        waits: Arc::new(AtomicUsize::new(0)),
        closes: Default::default(),
    };
    let runtime = StreamRuntime::new(STORE, Arc::new(store), None);
    create(&runtime, "s").await;
    put(&runtime, "s", &["a"]).await;
    gave.lock().clear();
    (runtime, gave)
}

/// What Workflow code got from a run that came back to its Worker after another Worker ran a task.
#[derive(Debug, Default)]
struct BackAgain {
    /// Each read page, with whether its activation replayed.
    pages: Vec<(bool, Vec<String>)>,
    evictions: Vec<String>,
}

/// Runs the first task of `t` on a Worker whose read takes the Record in the Store with no task
/// in hand, and then gives it the third task. Workflow code starts read 1 and a timer, and ends
/// the Workflow once the second signal came live and it got one Record.
async fn back_again(
    t: TestHistoryBuilder,
    (runtime, gave): Watched,
    records_in_history: bool,
) -> BackAgain {
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    let gate = async move {
        released.await.ok();
    }
    .boxed();
    let responses = [
        ResponseType::ToTaskNum(1),
        ResponseType::UntilResolved(gate, 3),
    ];
    let run = start_with(t, responses, runtimes(&runtime), 1, 0, true);
    let read = StartStreamRead {
        records_in_history,
        idle_timeout: None,
        ..read_cmd(1, "s")
    };
    let mut seen = BackAgain::default();
    let mut gate = Some(release);
    let (mut got, mut again) = (0, false);
    loop {
        let next = tokio::time::timeout(
            Duration::from_secs(30),
            run.worker.poll_workflow_activation(),
        )
        .await
        .unwrap_or_else(|_| panic!("no activation came: {seen:?}"))
        .unwrap();
        let mut first = false;
        for job in next.jobs.iter().filter_map(|job| job.variant.as_ref()) {
            match job {
                workflow_activation_job::Variant::InitializeWorkflow(_) => first = true,
                workflow_activation_job::Variant::RemoveFromCache(evict) => {
                    seen.evictions.push(evict.message.clone());
                }
                workflow_activation_job::Variant::ResolveStreamRead(page) => {
                    got += page.records.len();
                    seen.pages.push((next.is_replaying, texts(&page.records)));
                }
                workflow_activation_job::Variant::SignalWorkflow(signal) => {
                    again |= signal.signal_name == "again" && !next.is_replaying;
                }
                _ => {}
            }
        }
        assert!(
            run.failed.lock().is_empty(),
            "{:?} after {seen:?}",
            run.failed.lock()
        );
        if first {
            complete(&run.worker, &next, vec![read.clone().into(), timer_cmd(1)]).await;
            // The read takes the Record with no task in hand. Its page is in the run by the time
            // the Store gave the Record, since the read sends it in the same poll on this thread.
            if let Some(release) = gate.take() {
                until("the read of the Record", || !gave.lock().is_empty()).await;
                release.send(()).unwrap();
            }
        } else if again && got >= 1 {
            run.worker.complete_execution(&next.run_id).await;
            break;
        } else {
            complete(&run.worker, &next, vec![]).await;
        }
    }
    run.worker.drain_pollers_and_shutdown().await;
    seen
}

/// The marker of a page of `records` that read 1 gave in the second activation of a task.
fn one_page_marker(records: &[StreamRecord], records_in_history: bool) -> StreamMarkerData {
    let page = Page {
        records,
        activation: 1,
    };
    StreamMarkerData {
        reads: vec![read_entry(1, vec![], &[page], None, records_in_history)],
        streams: vec![stream("s")],
        ..Default::default()
    }
}

/// A cached run gets a task with a page that another Worker recorded, with its Records in the
/// Store. Its replay reads them, so Workflow code gets the Record once, in the replayed task.
#[tokio::test]
async fn a_cached_run_replays_a_page_that_another_worker_recorded() {
    let watched = watched_with_a().await;
    let marker = one_page_marker(&all_records(&watched.0, "s").await, false);
    let seen = back_again(foreign_task_history(Some(&marker)), watched, false).await;
    assert_eq!(seen.pages, [(true, vec!["a".to_string()])], "{seen:?}");
}

/// The same with the Records in History, while the cached run's own read took the Record with no
/// task in hand. Workflow code gets it once, and not also from that read.
#[tokio::test]
async fn a_cached_run_drops_its_own_page_for_one_in_history() {
    let watched = watched_with_a().await;
    let marker = one_page_marker(&all_records(&watched.0, "s").await, true);
    let seen = back_again(foreign_task_history(Some(&marker)), watched, true).await;
    assert_eq!(seen.pages, [(true, vec!["a".to_string()])], "{seen:?}");
}

/// Another Worker's task gave read 1 no page. The cached run's own read took the Record with no
/// task in hand, and it must not reach Workflow code in that replayed task, since no marker has
/// it. It comes in a live task instead.
#[tokio::test]
async fn a_cached_run_gives_no_page_that_history_lacks() {
    let seen = back_again(foreign_task_history(None), watched_with_a().await, false).await;
    assert_eq!(seen.pages, [(false, vec!["a".to_string()])], "{seen:?}");
}

/// A graceful shutdown ends a Workflow Task that only a read holds open, also when the server
/// returns the next task inline after each heartbeat, as a real server does. The task completes
/// with no new task, and the read resumes from History on the Worker that takes the run later.
#[tokio::test]
async fn a_shutdown_ends_a_task_that_only_a_read_holds() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(300));
    for _ in 0..400 {
        t.add_full_wf_task();
    }
    t.add_workflow_task_scheduled_and_started();
    let inline = t.clone();
    let sent = Arc::new(Mutex::new(vec![]));
    let mut cfg = MockPollCfg::from_resps(t, [ResponseType::ToTaskNum(1)]);
    cfg.make_poll_stream_interminable = true;
    let kept = sent.clone();
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        let mut kept = kept.lock();
        kept.push(completion.clone());
        // The heartbeat forces a task, which the server returns inline.
        let workflow_task = (completion.force_create_new_workflow_task
            && completion.return_new_workflow_task)
            .then(|| {
                let next = ResponseType::ToTaskNum(kept.len() + 1);
                hist_to_poll_resp(&inline, "fake_wf_id", next).resp
            });
        Ok(RespondWorkflowTaskCompletedResponse {
            workflow_task,
            ..Default::default()
        })
    }));
    let mut mock = build_mock_pollers(cfg);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 1;
        wc.stream_runtimes = runtimes(&runtime);
    });
    let worker = mock_worker(mock);
    let first = worker.poll_workflow_activation().await.unwrap();
    complete(&worker, &first, vec![read_cmd(1, "s").into()]).await;
    // The guard is long next to the heartbeat at 240 ms, so a shutdown that waits on the inline
    // tasks shows here.
    let shutdown =
        tokio::time::timeout(Duration::from_secs(10), worker.drain_pollers_and_shutdown());
    if shutdown.await.is_err() {
        panic!(
            "the shutdown didn't end after 10 s, with {} heartbeat completions",
            sent.lock().len()
        );
    }
    let sent = sent.lock();
    let last = sent.last().unwrap();
    assert!(!last.force_create_new_workflow_task && !last.return_new_workflow_task);
}

/// An Update rejection is a message for the server and no command. With only a read open, the
/// task completes immediately with it, as for a command, and doesn't wait for the heartbeat.
#[tokio::test]
async fn an_update_rejection_next_to_a_read_completes_the_task() {
    let runtime = runtime();
    create(&runtime, "s").await;
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_secs(2));
    t.add_workflow_task_scheduled_and_started();
    let mut task = hist_to_poll_resp(&t, "fake_wf_id", ResponseType::AllHistory).resp;
    task.add_update_request("upd-1", 1);
    let run = start(t, [ResponseType::Raw(task)], runtimes(&runtime), 1, 0);
    let first = run.worker.poll_workflow_activation().await.unwrap();
    let rejection = UpdateResponse {
        protocol_instance_id: "upd-1".to_string(),
        response: Some(update_response::Response::Rejected(Default::default())),
    };
    let started = std::time::Instant::now();
    complete(
        &run.worker,
        &first,
        vec![read_cmd(1, "s").into(), rejection.into()],
    )
    .await;
    if run.sent.lock().is_empty() {
        // Core sends the heartbeat only while a poll waits.
        let next = run.worker.poll_workflow_activation();
        tokio::pin!(next);
        tokio::select! {
            _ = until("the rejection", || !run.sent.lock().is_empty()) => {}
            early = &mut next => panic!("an activation came first: {early:?}"),
        }
        panic!(
            "the rejection reached the server after {:?}, at the heartbeat",
            started.elapsed()
        );
    }
    let sent = run.sent.lock()[0].clone();
    assert_eq!(sent.messages.len(), 1);
    assert!(sent.commands.is_empty());
    run.worker.drain_pollers_and_shutdown().await;
}
