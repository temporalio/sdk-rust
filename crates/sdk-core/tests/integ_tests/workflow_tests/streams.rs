use crate::common::{CoreWfStarter, integ_worker_config};
use futures_util::stream;
use std::{collections::HashMap, sync::Arc, time::Duration};
use temporalio_common::protos::{
    coresdk::{
        workflow_activation::{WorkflowActivation, workflow_activation_job::Variant as Job},
        workflow_commands::{
            AppendToStream, CloseStream, CompleteWorkflowExecution, ReadStream, StartTimer,
            workflow_command::Variant as Command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        common::v1::Payload,
        failure::v1::Failure,
        history::v1::history_event::Attributes,
        sdk::v1::{WorkflowTaskStreamCalls, workflow_task_stream_calls::read_result},
    },
};
use temporalio_sdk_core::{
    StreamRuntimes, init_replay_worker, prost_dur,
    replay::{HistoryForReplay, ReplayWorkerInput},
    test_help::{
        WorkerTestHelpers,
        stream_store::{MemoryStore, Request},
    },
};
use temporalio_streams::{
    StreamRuntime, StreamStore,
    proto::{
        AppendRecord, AppendRecordsRequest, ClientProducer, CreateStreamRequest, Producer,
        StreamRef, producer::Kind,
    },
    record::StoreLimits,
};

pub(crate) const STORE: &str = "mem";

pub(crate) fn stream(id: &str) -> StreamRef {
    StreamRef {
        id: id.to_string(),
        store: STORE.to_string(),
    }
}

/// The Workflow of the test. It reads `quiet` and starts a timer. The timer is a command for the
/// server, so the task completes at once and the read gives its page in the next task. The read
/// waits for a Record that never comes, so its page has none. At that page the Workflow reads `in`
/// twice and appends to `out`, at the result of the append it closes `out`, and it completes at the
/// result of the close.
fn workflow(activation: &WorkflowActivation) -> Vec<Command> {
    let mut commands: Vec<Command> = vec![];
    for job in &activation.jobs {
        match &job.variant {
            Some(Job::InitializeWorkflow(_)) => {
                let waits = ReadStream {
                    wait: Some(prost_dur!(from_secs(60))),
                    ..read(1, "quiet")
                };
                commands.push(waits.into());
                // The Workflow completes long before the timer fires.
                let timer = StartTimer {
                    seq: 1,
                    start_to_fire_timeout: Some(prost_dur!(from_secs(60))),
                };
                commands.push(timer.into());
            }
            Some(Job::ResolveStreamRead(page)) if page.seq == 1 => {
                commands.push(read(2, "in").into());
                commands.push(read(3, "in").into());
                let body = Payload::from(b"hello".to_vec());
                let append = AppendToStream {
                    seq: 1,
                    stream: Some(stream("out")),
                    bodies: vec![body],
                };
                commands.push(append.into());
            }
            Some(Job::ResolveStreamAppend(_)) => {
                let close = CloseStream {
                    seq: 1,
                    stream: Some(stream("out")),
                };
                commands.push(close.into());
            }
            Some(Job::ResolveStreamClose(_)) => {
                commands.push(CompleteWorkflowExecution { result: None }.into());
            }
            _ => {}
        }
    }
    commands
}

fn read(seq: u32, id: &str) -> ReadStream {
    ReadStream {
        seq,
        stream: Some(stream(id)),
        max_records: 2,
        ..Default::default()
    }
}

/// Returns the results of Stream calls in `activation`, as text.
fn stream_jobs(activation: &WorkflowActivation) -> Vec<String> {
    let jobs = activation
        .jobs
        .iter()
        .filter_map(|job| job.variant.as_ref());
    jobs.filter(|job| {
        matches!(
            job,
            Job::ResolveStreamAppend(_) | Job::ResolveStreamRead(_) | Job::ResolveStreamClose(_)
        )
    })
    .map(|job| format!("{job:?}"))
    .collect()
}

/// Returns the entries of the Stream calls of a task, as text, sorted. The order of the results of
/// one task depends on when the Store answers.
fn entries(calls: &WorkflowTaskStreamCalls) -> Vec<String> {
    let appends = (calls.append_results.iter()).map(|a| format!("append {}", a.seq));
    let intents = (calls.close_intents.iter()).map(|i| format!("close intent {}", i.seq));
    let closes = (calls.close_results.iter()).map(|c| format!("close {}", c.seq));
    let reads = calls.read_results.iter().map(|read| match &read.result {
        Some(read_result::Result::Page(page)) => {
            format!("read {} with {} Records", read.seq, page.record_count)
        }
        other => format!("read {}: {other:?}", read.seq),
    });
    let mut entries: Vec<String> = appends.chain(intents).chain(closes).chain(reads).collect();
    entries.sort();
    entries
}

/// Returns a Store with the Streams `out`, `in` and `quiet`, and its runtime for a Worker. The
/// Stream `in` has three Records.
pub(crate) async fn store() -> (Arc<MemoryStore>, StreamRuntimes) {
    let store = Arc::new(MemoryStore::default());
    for id in ["out", "in", "quiet"] {
        let create = CreateStreamRequest {
            stream_ref: Some(stream(id)),
            ..Default::default()
        };
        store.create_stream(create, id.to_string()).await.unwrap();
    }
    let client = Kind::Client(ClientProducer {
        namespace: "default".to_string(),
        id: "test".to_string(),
    });
    let bodies: Vec<Payload> = (0..3u8).map(|at| Payload::from(vec![at])).collect();
    let fill = AppendRecordsRequest {
        r#ref: Some(stream("in")),
        producer: Some(Producer { kind: Some(client) }),
        records: (bodies.iter().cloned())
            .map(|body| AppendRecord { body: Some(body) })
            .collect(),
    };
    store.append_records(fill, String::new()).await.unwrap();
    let runtime = Arc::new(StreamRuntime::new(store.clone(), StoreLimits::default()));
    let runtimes = StreamRuntimes::new([(STORE.to_string(), runtime)]);
    (store, runtimes)
}

// The attempt of a Stream call and the Stream calls of a Workflow Task in History depend on what
// the server does with a failed task, with a task that Core forces, and with a completion whose
// Stream calls have no entry, which a mock server can't show.
#[tokio::test]
async fn stream_calls_write_as_the_servers_task_and_replay_from_its_ledgers() {
    let (store, mut runtimes) = store().await;
    // The read of `quiet` ends by its read limit, so this keeps the test short.
    runtimes.settings.read_time = Some(Duration::from_secs(2));
    let mut starter = CoreWfStarter::new("stream_calls_write_as_the_servers_task");
    // A task must not reach its heartbeat, which would split its results over two ledgers.
    starter.workflow_options.task_timeout = Some(Duration::from_secs(60));
    let of_worker = runtimes.clone();
    starter.set_core_cfg_mutator(move |config| config.stream_runtimes = of_worker.clone());
    let core = starter.get_core_worker().await;
    starter.start_wf().await;

    // The first attempt of task 2 fails once its append has a result, and the server sends a second
    // attempt of the task. The failure evicts the run, so that attempt replays it from the start.
    let mut failed = false;
    let mut live = vec![];
    loop {
        let activation = core.poll_workflow_activation().await.unwrap();
        let run_id = activation.run_id.clone();
        if activation.is_only_eviction() {
            let evicted = WorkflowActivationCompletion::empty(run_id);
            core.complete_workflow_activation(evicted).await.unwrap();
            continue;
        }
        let variants = || (activation.jobs.iter()).filter_map(|job| job.variant.as_ref());
        if variants().any(|job| matches!(job, Job::InitializeWorkflow(_))) {
            live.clear();
        }
        if !failed && variants().any(|job| matches!(job, Job::ResolveStreamAppend(_))) {
            failed = true;
            let failure = Failure::application_failure("The test fails this task".into(), false);
            let fails = WorkflowActivationCompletion::fail(run_id, failure, None);
            core.complete_workflow_activation(fails).await.unwrap();
            continue;
        }
        live.extend(stream_jobs(&activation));
        let commands = workflow(&activation);
        let ends = matches!(commands[..], [Command::CompleteWorkflowExecution(_)]);
        let completion = WorkflowActivationCompletion::from_cmds(run_id, commands);
        core.complete_workflow_activation(completion).await.unwrap();
        if ends {
            break;
        }
    }
    core.handle_eviction().await;
    assert_eq!(live.len(), 5, "{live:?}");

    // History has the Stream calls on the completion of each Workflow Task with a result or an
    // intent, and on the completion of task 1, which has no entry but leaves a read with no result.
    // So the server kept the field, also with no entry. Task 1 completed for its timer while the
    // read ran, so its result is in the Stream calls of the next task.
    let history = starter.get_history().await;
    let mut ledgers = vec![];
    // The attempt of each Workflow Task, by the id of its started event.
    let mut attempts = HashMap::new();
    let mut scheduled_attempt = 0;
    for event in &history.events {
        match event.attributes.as_ref() {
            Some(Attributes::WorkflowTaskScheduledEventAttributes(scheduled)) => {
                scheduled_attempt = scheduled.attempt;
            }
            Some(Attributes::WorkflowTaskStartedEventAttributes(_)) => {
                attempts.insert(event.event_id, scheduled_attempt);
            }
            Some(Attributes::WorkflowTaskCompletedEventAttributes(completed)) => {
                let metadata = completed.sdk_metadata.as_ref();
                if let Some(calls) = metadata.and_then(|metadata| metadata.stream_calls.as_ref()) {
                    ledgers.push((event.event_id, entries(calls)));
                }
            }
            // The Workflow has no Local Activity and no patch, and a Stream call adds no event to
            // History, so History has no marker.
            Some(Attributes::MarkerRecordedEventAttributes(marker)) => {
                panic!("History has a marker: {marker:?}");
            }
            _ => {}
        }
    }
    let tasks: Vec<i64> = ledgers.iter().map(|(completed, _)| *completed).collect();
    assert!(tasks.windows(2).all(|pair| pair[0] < pair[1]), "{tasks:?}");
    let recorded: Vec<Vec<String>> = ledgers.into_iter().map(|(_, entries)| entries).collect();
    let expected = [
        vec![],
        vec![
            "append 1",
            "close intent 1",
            "read 1 with 0 Records",
            "read 2 with 2 Records",
            "read 3 with 2 Records",
        ],
        vec!["close 1"],
    ];
    assert_eq!(recorded, expected);

    // Each write has the attempt that the server gave its Workflow Task. The append ran in both
    // attempts of task 2. The close ran in task 3 with the attempt that recorded its intent, the
    // second one of task 2. The server doesn't count a failure of a task on a sticky queue, so
    // both attempts of task 2 can have the same number, and their started events tell them apart.
    let writes: Vec<(i64, i32)> = (store.requests().iter())
        .filter_map(|request| match request {
            Request::Append(append, _) if append.r#ref == Some(stream("out")) => {
                append.producer.as_ref()
            }
            Request::Close(close) => close.producer.as_ref(),
            _ => None,
        })
        .map(|producer| match &producer.kind {
            Some(Kind::Workflow(workflow)) => {
                let attempt = workflow.attempt.expect("A Workflow call has its attempt");
                (attempt.started_event_id, attempt.attempt)
            }
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(writes.len(), 3, "{writes:?}");
    for (started, attempt) in &writes {
        assert_eq!(
            attempts.get(started),
            Some(attempt),
            "{writes:?} {attempts:?}"
        );
    }
    let (first, second, close) = (writes[0].0, writes[1].0, writes[2].0);
    assert!(first < second && second == close, "{writes:?}");

    // A second Worker, which doesn't have the run, replays the History of the server. It reads
    // the Records of the digest from the Store, and gives Workflow code the jobs of the first run.
    let mut config = integ_worker_config(starter.get_task_queue());
    config.stream_runtimes = runtimes;
    let history = HistoryForReplay::new(history, starter.get_wf_id());
    let replayer = init_replay_worker(ReplayWorkerInput::new(config, stream::iter([history])));
    let replayer = replayer.expect("The replay Worker starts");
    let mut replayed = vec![];
    loop {
        let activation = replayer.poll_workflow_activation().await.unwrap();
        let run_id = activation.run_id.clone();
        if activation.is_only_eviction() {
            let evicted = WorkflowActivationCompletion::empty(run_id);
            replayer
                .complete_workflow_activation(evicted)
                .await
                .unwrap();
            break;
        }
        replayed.extend(stream_jobs(&activation));
        let completion = WorkflowActivationCompletion::from_cmds(run_id, workflow(&activation));
        (replayer.complete_workflow_activation(completion).await).unwrap();
    }
    assert_eq!(replayed, live);
}
