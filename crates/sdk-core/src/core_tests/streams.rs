//! Direct reads and appends of a Stream, on the memory Store. A test drives the Worker with a mock
//! history, and adds Records to the Store between its polls.

use crate::{
    StreamRuntimes, Worker, WorkflowErrorType,
    replay::TestHistoryBuilder,
    test_help::{
        MockPollCfg, ResponseType, WorkerExt, WorkerTestHelpers, build_mock_pollers, mock_worker,
        schedule_local_activity_cmd,
    },
    worker::client::WorkflowTaskCompletion,
};
use parking_lot::Mutex;
use prost::Message;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use temporalio_common::protos::{
    constants::STREAM_MARKER_NAME,
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        common::StreamReadStop,
        external_data::{StreamAppendEntry, StreamMarkerData, StreamReadEnded, StreamReadEntry},
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamRead, WorkflowActivation, resolve_stream_append,
            workflow_activation_job,
        },
        workflow_commands::{
            ActivityCancellationType, AppendToStream, CloseStreamRead, CompleteWorkflowExecution,
            StartStreamRead, StartTimer, workflow_command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::command::Attributes,
        common::v1::{Payload, Payloads},
        enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
        failure::v1::{Failure, failure::FailureInfo},
        history::v1::MarkerRecordedEventAttributes,
    },
};
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_streams::{
    MemoryStreamOptions, MemoryStreamRuntime, StreamRuntime,
    digest::records_digest,
    proto::{
        AppendRecord, AppendRecordsRequest, ClientProducer, CloseStreamRequest,
        CreateStreamRequest, Producer, ReadRecordsRequest, StreamRecord, StreamRef,
        WorkflowProducer, WorkflowTaskEpoch, producer::Kind,
    },
};

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
}

fn start(
    t: TestHistoryBuilder,
    responses: impl IntoIterator<Item = ResponseType>,
    runtimes: StreamRuntimes,
    cache: usize,
    expected_fails: usize,
) -> Run {
    let sent = Arc::new(Mutex::new(vec![]));
    let failed = Arc::new(Mutex::new(vec![]));
    let mut cfg = MockPollCfg::from_resps(t, responses);
    cfg.num_expected_fails = expected_fails;
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
/// test adds Records to the Store, polls the page, and completes with no command. It then sleeps
/// past 80 % of the task timeout, so that Core completes the task as a heartbeat, and adds the
/// next Records, which come in the next task.
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
    tokio::time::sleep(Duration::from_millis(700)).await;
    put(&runtime, "s", &["b", "c"]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
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

    let entry = &marker(&sent[0]).unwrap().reads[0];
    assert_eq!(
        (entry.seq, entry.stream.clone(), entry.count),
        (1, Some(stream("s")), 1)
    );
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
    let kept = sent.clone();
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        kept.lock().push(completion.clone());
        Ok(Default::default())
    }));
    let mut mock = build_mock_pollers(cfg);
    let runtimes = StreamRuntimes::new([(STORE.to_string(), runtime.clone())]);
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
    tokio::time::sleep(Duration::from_millis(400)).await;
    put(&runtime, "s", &["b"]).await;
    let page = worker.poll_workflow_activation().await.unwrap();
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
        .map(|entry| (entry.seq, entry.stream.clone().unwrap().id, entry.count))
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
    let runtimes = StreamRuntimes::new([(STORE.to_string(), runtime)]);
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

// Replays. A test builds the marker that Core would have written, from the Records in the Store.

/// One page of a read entry: its Records and its activation in the Workflow Task.
struct Page<'a> {
    records: &'a [StreamRecord],
    activation: u32,
}

/// The marker entry of read `seq` of Stream `id`, with its pages and its end.
fn read_entry(
    seq: u32,
    id: &str,
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
        stream: Some(stream(id)),
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
    let entry = read_entry(
        1,
        "s",
        vec![],
        &[page],
        Some((StreamReadStop::Done, 1)),
        false,
    );
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
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
    let entry = read_entry(
        1,
        "s",
        vec![],
        &[page],
        Some((StreamReadStop::Done, 1)),
        true,
    );
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
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
    let run = start(t, [ResponseType::AllHistory], runtimes, 1, 1);
    let polled = run.worker.poll_workflow_activation().await;
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
    let entry = read_entry(1, "s", vec![], &[page], None, false);
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
        stream: Some(stream("s")),
        first_offset: 7u64.to_be_bytes().to_vec(),
        last_offset: 8u64.to_be_bytes().to_vec(),
        failure: None,
        activation: 1,
    };
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![],
        appends: vec![appended.clone()],
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
    let entry = read_entry(1, "s", vec![], &[page], None, false);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
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
    let entry = read_entry(1, "s", vec![], &[page], None, false);
    let mut t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
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
    let entry = read_entry(1, "s", vec![], &[page], None, false);
    let t = replayed_read_history(&StreamMarkerData {
        reads: vec![entry],
        appends: vec![],
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
    let entry = read_entry(
        1,
        "s",
        vec![],
        &[page],
        Some((StreamReadStop::Done, 0)),
        false,
    );
    stream_marker_event(
        &mut t,
        8,
        &StreamMarkerData {
            reads: vec![entry],
            appends: vec![],
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
        read_entry(
            1,
            "s",
            vec![],
            &[page],
            Some((StreamReadStop::Done, 0)),
            false,
        )
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
