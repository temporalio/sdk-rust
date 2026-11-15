//! Direct reads and appends of a Stream, on the memory Store. A test drives the Worker with a mock
//! history, and adds Records to the Store between its polls.

use crate::{
    StreamRuntimes, Worker,
    replay::TestHistoryBuilder,
    test_help::{
        MockPollCfg, ResponseType, WorkerExt, WorkerTestHelpers, build_mock_pollers, mock_worker,
        schedule_local_activity_cmd,
    },
    worker::client::WorkflowTaskCompletion,
};
use parking_lot::Mutex;
use prost::Message;
use std::{sync::Arc, time::Duration};
use temporalio_common::protos::{
    constants::STREAM_MARKER_NAME,
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        common::StreamReadStop,
        external_data::StreamMarkerData,
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
        common::v1::Payload,
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
    let sent = Arc::new(Mutex::new(vec![]));
    let mut cfg = MockPollCfg::from_resps(t, responses);
    cfg.num_expected_fails = expected_fails;
    let kept = sent.clone();
    cfg.completion_mock_fn = Some(Box::new(move |completion| {
        kept.lock().push(completion.clone());
        Ok(Default::default())
    }));
    let mut mock = build_mock_pollers(cfg);
    let runtimes = StreamRuntimes::new([(STORE.to_string(), runtime.clone())]);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 1;
        wc.stream_runtimes = runtimes;
    });
    (mock_worker(mock), sent)
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
