//! The Stream reads of Workflow code against a dev server, which writes and pages the History.

use crate::common::CoreWfStarter;
use std::{collections::HashMap, sync::Arc, time::Duration};
use temporalio_client::{NamespacedClient, grpc::WorkflowService};
use temporalio_common::protos::{
    constants::STREAM_READ_MARKER_NAME,
    coresdk::{
        FromJsonPayloadExt,
        external_data::{StreamReadMarkerData, stream_read_result},
        workflow_activation::{
            WorkflowActivation, resolve_stream_read, workflow_activation_job::Variant as Job,
        },
        workflow_commands::{
            CompleteWorkflowExecution, ModifyWorkflowProperties, ReadStream, StartTimer,
            workflow_command::Variant as Command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        common::v1::{Memo, Payload, WorkflowExecution},
        enums::v1::EventType,
        history::v1::{History, history_event::Attributes},
        workflowservice::v1::GetWorkflowExecutionHistoryRequest,
    },
};
use temporalio_sdk_core::{
    StreamRuntimes, prost_dur,
    test_help::stream_store::{MemoryStore, Op},
};
use temporalio_streams::{
    StreamRuntime, StreamStore,
    proto::{
        AppendRecord, AppendRecordsRequest, ClientProducer, CreateStreamRequest, Producer,
        producer::Kind, sdk::StreamRef,
    },
    record::StoreLimits,
};

const STORE: &str = "mem";

fn stream() -> StreamRef {
    StreamRef {
        id: "s".to_string(),
        store: STORE.to_string(),
    }
}

/// Returns a Store with the Stream `s` of `count` Records, and its runtime for a Worker.
async fn store(count: usize) -> (Arc<MemoryStore>, StreamRuntimes) {
    let store = Arc::new(MemoryStore::default());
    let create = CreateStreamRequest {
        stream_id: "s".to_string(),
        ..Default::default()
    };
    store.create_stream(create, "s".to_string()).await.unwrap();
    let producer = Producer {
        kind: Some(Kind::Client(ClientProducer {
            namespace: "default".to_string(),
            id: "test".to_string(),
        })),
    };
    // A batch has at most 1000 Records.
    for start in (0..count).step_by(1000) {
        let records = (start..count.min(start + 1000))
            .map(|at| AppendRecord {
                body: Some(Payload::from(at.to_be_bytes().to_vec())),
            })
            .collect();
        let fill = AppendRecordsRequest {
            stream_id: "s".to_string(),
            producer: Some(producer.clone()),
            records,
        };
        store.append_records(fill, String::new()).await.unwrap();
    }
    let runtime = Arc::new(StreamRuntime::new(store.clone(), StoreLimits::default()));
    let runtimes = StreamRuntimes::new([(STORE.to_string(), runtime)]);
    (store, runtimes)
}

/// Returns the read of one Record of `s` after `from_offset`, with no wait.
fn read(seq: u32, from_offset: &[u8]) -> Command {
    ReadStream {
        seq,
        stream: Some(stream()),
        from_offset: from_offset.to_vec(),
        max_records: 1,
        ..Default::default()
    }
    .into()
}

fn complete() -> Command {
    CompleteWorkflowExecution { result: None }.into()
}

/// Returns each job of `activation` as text, with the number of Records of a page.
fn jobs_of(activation: &WorkflowActivation) -> Vec<String> {
    (activation.jobs.iter())
        .filter_map(|job| job.variant.as_ref())
        .map(|job| match job {
            Job::ResolveStreamRead(read) => match &read.status {
                Some(resolve_stream_read::Status::Page(page)) => {
                    format!("read {} of {}", read.seq, page.records.len())
                }
                _ => format!("read {} failed", read.seq),
            },
            other => format!("{other:?}")
                .split(['(', ' '])
                .next()
                .unwrap()
                .to_string(),
        })
        .collect()
}

/// Returns the next commands of a Workflow that drains `s` one Record at a time. It starts timer 1
/// once a read gives no Record or fails, and completes once the timer fires.
fn drain(activation: &WorkflowActivation) -> Vec<Command> {
    let mut commands = vec![];
    for job in activation
        .jobs
        .iter()
        .filter_map(|job| job.variant.as_ref())
    {
        match job {
            Job::InitializeWorkflow(_) => commands.push(read(1, &[])),
            Job::ResolveStreamRead(result) => match &result.status {
                Some(resolve_stream_read::Status::Page(page)) if !page.records.is_empty() => {
                    commands.push(read(result.seq + 1, &page.last_offset));
                }
                _ => commands.push(
                    StartTimer {
                        seq: 1,
                        start_to_fire_timeout: Some(prost_dur!(from_secs(1))),
                    }
                    .into(),
                ),
            },
            Job::FireTimer(_) => commands.push(complete()),
            _ => {}
        }
    }
    commands
}

/// The Records of the Stream that a replay of a paged History reads.
const PAGED_RECORDS: usize = 400;

/// Returns the next commands of a Workflow that reads `s` one Record at a time, with a timer at
/// each page that never fires in the test. The timer is a command, so each page has a Workflow
/// Task and a batch of History of its own. At the last Record of the Stream, which the data of
/// the Record tells, it starts timer 1, and it completes once timer 1 fires.
fn drain_a_page_in_each_task(activation: &WorkflowActivation) -> Vec<Command> {
    let last = (PAGED_RECORDS - 1).to_be_bytes().to_vec();
    let mut commands = vec![];
    for job in activation
        .jobs
        .iter()
        .filter_map(|job| job.variant.as_ref())
    {
        match job {
            Job::InitializeWorkflow(_) => commands.push(read(1, &[])),
            Job::ResolveStreamRead(result) => {
                let Some(resolve_stream_read::Status::Page(page)) = &result.status else {
                    continue;
                };
                let ends = (page.records.iter())
                    .any(|record| record.body.as_ref().is_some_and(|body| body.data == last));
                let (seq, wait) = match ends {
                    true => (1, 1),
                    false => (1000 + result.seq, 3600),
                };
                let timer = StartTimer {
                    seq,
                    start_to_fire_timeout: Some(prost_dur!(from_secs(wait))),
                };
                commands.push(timer.into());
                if !ends {
                    commands.push(read(result.seq + 1, &page.last_offset));
                }
            }
            Job::FireTimer(fired) if fired.seq == 1 => commands.push(complete()),
            _ => {}
        }
    }
    commands
}

/// Returns the entries of each read marker of `history`, and the event id of the marker.
fn read_markers(history: &History) -> Vec<(i64, StreamReadMarkerData)> {
    (history.events.iter())
        .filter_map(|event| match event.attributes.as_ref()? {
            Attributes::MarkerRecordedEventAttributes(marker)
                if marker.marker_name == STREAM_READ_MARKER_NAME =>
            {
                let data = &marker.details["data"].payloads[0];
                Some((
                    event.event_id,
                    StreamReadMarkerData::from_json_payload(data).unwrap(),
                ))
            }
            _ => None,
        })
        .collect()
}

// A run that leaves the cache replays a long History, which the server pages, and reads the
// Records of each read marker from the Store again.
#[tokio::test]
async fn a_replay_of_a_paged_history_gives_each_page_of_the_first_run() {
    let (store, runtimes) = store(PAGED_RECORDS).await;
    let mut starter = CoreWfStarter::new("a_replay_of_a_paged_history");
    starter.workflow_options.task_timeout = Some(Duration::from_secs(30));
    let of_worker = runtimes.clone();
    starter.set_core_cfg_mutator(move |config| config.stream_runtimes = of_worker.clone());
    let core = starter.get_core_worker().await;
    let run_id = starter.start_wf().await;
    // The first run drains the Stream, and the run leaves the cache once timer 1 is out.
    let (mut lives, mut evictions) = (vec![vec![]], vec![]);
    let drive = async {
        loop {
            let activation = core.poll_workflow_activation().await.unwrap();
            let run_id = activation.run_id.clone();
            if let Some(Job::RemoveFromCache(evict)) =
                activation.jobs.first().and_then(|job| job.variant.as_ref())
            {
                evictions.push(evict.message.clone());
                lives.push(vec![]);
                let evicted = WorkflowActivationCompletion::empty(run_id);
                core.complete_workflow_activation(evicted).await.unwrap();
                continue;
            }
            lives.last_mut().unwrap().push(jobs_of(&activation));
            let commands = drain_a_page_in_each_task(&activation);
            let starts_timer = commands
                .iter()
                .any(|command| matches!(command, Command::StartTimer(timer) if timer.seq == 1));
            let ends = commands
                .iter()
                .any(|command| matches!(command, Command::CompleteWorkflowExecution(_)));
            let completion = WorkflowActivationCompletion::from_cmds(run_id.clone(), commands);
            core.complete_workflow_activation(completion).await.unwrap();
            if ends {
                return;
            }
            if starts_timer && evictions.is_empty() {
                core.request_workflow_eviction(&run_id);
            }
        }
    };
    let finished = tokio::time::timeout(Duration::from_secs(120), drive)
        .await
        .is_ok();
    let history = starter.get_history().await;
    // The server gives the History in pages.
    let mut client = starter.get_core_client().await;
    let request = GetWorkflowExecutionHistoryRequest {
        namespace: client.namespace(),
        execution: Some(WorkflowExecution {
            workflow_id: starter.get_wf_id().to_string(),
            run_id,
        }),
        ..Default::default()
    };
    let first_page =
        WorkflowService::get_workflow_execution_history(&mut client, tonic::Request::new(request))
            .await
            .unwrap()
            .into_inner();
    let first_page_events = first_page.history.map_or(0, |history| history.events.len());
    assert!(
        !first_page.next_page_token.is_empty(),
        "The server gave all {} events in one page",
        history.events.len()
    );
    let markers = read_markers(&history).len();
    let first = lives[0].clone();
    let replayed = lives.get(1).cloned().unwrap_or_default();
    let fired = vec!["FireTimer".to_string()];
    let label = format!(
        "History has {} events in pages of {first_page_events}, and {markers} read markers. \
         Evictions {evictions:?}. The first run had {} activations, and the replay {}. The Store \
         got {} reads",
        history.events.len(),
        first.len(),
        replayed.len(),
        store.reads().len()
    );
    assert!(finished, "The Workflow didn't complete. {label}");
    assert_eq!(replayed, [first, vec![fired]].concat(), "{label}");
}

// A read in the same completion as a command doesn't hold the task. With no later event for the
// Workflow, the Records of the Stream must still reach the read.
#[tokio::test]
async fn a_read_with_a_memo_upsert_in_the_first_task_gets_its_page() {
    let (store, runtimes) = store(3).await;
    let mut starter = CoreWfStarter::new("a_read_with_a_memo_upsert");
    let of_worker = runtimes.clone();
    starter.set_core_cfg_mutator(move |config| config.stream_runtimes = of_worker.clone());
    let core = starter.get_core_worker().await;
    starter.start_wf().await;
    let mut seen = vec![];
    let drive = async {
        loop {
            let activation = core.poll_workflow_activation().await.unwrap();
            let run_id = activation.run_id.clone();
            seen.push(jobs_of(&activation));
            let commands: Vec<Command> = match activation.jobs[0].variant.as_ref() {
                Some(Job::InitializeWorkflow(_)) => {
                    let fields = HashMap::from([("k".to_string(), Payload::from(vec![1]))]);
                    let memo = ModifyWorkflowProperties {
                        upserted_memo: Some(Memo { fields }),
                    };
                    vec![memo.into(), read(1, &[])]
                }
                _ if seen
                    .last()
                    .unwrap()
                    .iter()
                    .any(|job| job.starts_with("read 1")) =>
                {
                    vec![complete()]
                }
                _ => vec![],
            };
            let ends = matches!(commands[..], [Command::CompleteWorkflowExecution(_)]);
            let completion = WorkflowActivationCompletion::from_cmds(run_id, commands);
            core.complete_workflow_activation(completion).await.unwrap();
            if ends {
                return;
            }
        }
    };
    let finished = tokio::time::timeout(Duration::from_secs(20), drive)
        .await
        .is_ok();
    let history = starter.get_history().await;
    let events: Vec<EventType> = history.events.iter().map(|e| e.event_type()).collect();
    assert!(
        finished,
        "The read got no page in 20 s: activations {seen:?}, History {events:?}, {} reads of the \
         Store",
        store.reads().len()
    );
}

// Workflow code drains a Stream one Record at a time. The runtime cut the first page while the
// Store had more, and then the Store gives no answer. A read outage fails the task as an append
// outage does, so Core must not make a new task with an empty marker at each read limit, and
// History must keep no failure of the read.
#[tokio::test]
async fn a_read_outage_after_a_cut_page_fails_the_task_on_the_server() {
    let (store, mut runtimes) = store(4).await;
    runtimes.settings.read_time = Some(Duration::from_millis(250));
    runtimes.settings.try_deadline = Duration::from_secs(2);
    // The Store answers the first read only.
    store.hold(Op::Read);
    store.pass(Op::Read, 1);
    let mut starter = CoreWfStarter::new("a_read_outage_after_a_cut_page");
    let of_worker = runtimes.clone();
    starter.set_core_cfg_mutator(move |config| config.stream_runtimes = of_worker.clone());
    let core = starter.get_core_worker().await;
    starter.start_wf().await;
    let mut seen = vec![];
    let drive = async {
        loop {
            let activation = core.poll_workflow_activation().await.unwrap();
            let run_id = activation.run_id.clone();
            if activation.is_only_eviction() {
                seen.push(vec!["eviction".to_string()]);
                let evicted = WorkflowActivationCompletion::empty(run_id);
                core.complete_workflow_activation(evicted).await.unwrap();
                continue;
            }
            seen.push(jobs_of(&activation));
            // The Workflow completes at a failed read in place of the timer.
            let commands: Vec<Command> = (drain(&activation).into_iter())
                .map(|command| match command {
                    Command::StartTimer(_) => complete(),
                    other => other,
                })
                .collect();
            let ends = matches!(commands[..], [Command::CompleteWorkflowExecution(_)]);
            let completion = WorkflowActivationCompletion::from_cmds(run_id, commands);
            core.complete_workflow_activation(completion).await.unwrap();
            if ends {
                return;
            }
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(5), drive).await;
    let history = starter.get_history().await;
    let markers = read_markers(&history);
    let empty = markers
        .iter()
        .filter(|(_, data)| data.reads.is_empty())
        .count();
    let recorded_failure = (markers.iter().flat_map(|(_, data)| &data.reads))
        .any(|entry| matches!(entry.result, Some(stream_read_result::Result::Failure(_))));
    let failed_tasks = (history.events.iter())
        .filter(|event| event.event_type() == EventType::WorkflowTaskFailed)
        .count();
    let events: Vec<EventType> = history.events.iter().map(|e| e.event_type()).collect();
    assert!(
        empty == 0 && !recorded_failure && failed_tasks > 0,
        "{empty} read markers with no entry, a read marker with a failure: {recorded_failure}, \
         {failed_tasks} failed tasks. Activations {seen:?}, History {events:?}"
    );
}

/// Returns the number of each type of event in `history`.
fn count_of(history: &History, event_type: EventType) -> usize {
    (history.events.iter())
        .filter(|event| event.event_type() == event_type)
        .count()
}

// A read with a wait on a Stream with no Record gets a check before each completion with a timer.
// The check gets no Record, so the read starts again in the task that the timer makes, and the
// Worker asks the server for no other task.
#[tokio::test]
async fn a_read_of_an_idle_stream_makes_no_task_on_the_server() {
    let (store, runtimes) = store(0).await;
    let mut starter = CoreWfStarter::new("a_read_of_an_idle_stream");
    let of_worker = runtimes.clone();
    starter.set_core_cfg_mutator(move |config| config.stream_runtimes = of_worker.clone());
    let core = starter.get_core_worker().await;
    starter.start_wf().await;
    let timer = |seq| -> Command {
        StartTimer {
            seq,
            start_to_fire_timeout: Some(prost_dur!(from_millis(500))),
        }
        .into()
    };
    let mut seen = vec![];
    let drive = async {
        loop {
            let activation = core.poll_workflow_activation().await.unwrap();
            let run_id = activation.run_id.clone();
            seen.push(jobs_of(&activation));
            let commands: Vec<Command> = match activation.jobs[0].variant.as_ref() {
                Some(Job::InitializeWorkflow(_)) => {
                    let read = ReadStream {
                        seq: 1,
                        stream: Some(stream()),
                        wait: Some(prost_dur!(from_secs(60))),
                        ..Default::default()
                    };
                    vec![read.into(), timer(1)]
                }
                Some(Job::FireTimer(fired)) if fired.seq < 3 => vec![timer(fired.seq + 1)],
                _ => vec![complete()],
            };
            let ends = matches!(commands[..], [Command::CompleteWorkflowExecution(_)]);
            let completion = WorkflowActivationCompletion::from_cmds(run_id, commands);
            core.complete_workflow_activation(completion).await.unwrap();
            if ends {
                return;
            }
        }
    };
    let finished = tokio::time::timeout(Duration::from_secs(20), drive)
        .await
        .is_ok();
    let history = starter.get_history().await;
    let events: Vec<EventType> = history.events.iter().map(|e| e.event_type()).collect();
    let label = format!(
        "activations {seen:?}, History {events:?}, {} reads of the Store",
        store.reads().len()
    );
    assert!(finished, "{label}");
    // The first task and the task of each timer.
    assert_eq!(
        count_of(&history, EventType::WorkflowTaskCompleted),
        4,
        "{label}"
    );
    assert_eq!(read_markers(&history), [], "{label}");
}

// Workflow code drains a Stream with no wait, and answers its last page after the read limit of
// the task. The next read is a check, and its page with no Record ends the drain in the same task.
#[tokio::test]
async fn a_drain_that_crosses_the_read_limit_ends_in_its_task_on_the_server() {
    let (store, mut runtimes) = store(2).await;
    runtimes.settings.read_time = Some(Duration::from_millis(250));
    let mut starter = CoreWfStarter::new("a_drain_that_crosses_the_read_limit");
    let of_worker = runtimes.clone();
    starter.set_core_cfg_mutator(move |config| config.stream_runtimes = of_worker.clone());
    let core = starter.get_core_worker().await;
    starter.start_wf().await;
    let mut seen = vec![];
    let drive = async {
        loop {
            let activation = core.poll_workflow_activation().await.unwrap();
            let run_id = activation.run_id.clone();
            let jobs = jobs_of(&activation);
            if jobs.iter().any(|job| job.starts_with("read 2 of")) {
                tokio::time::sleep(Duration::from_millis(600)).await;
            }
            seen.push(jobs);
            // The Workflow completes at the page with no Record in place of the timer.
            let commands: Vec<Command> = (drain(&activation).into_iter())
                .map(|command| match command {
                    Command::StartTimer(_) => complete(),
                    other => other,
                })
                .collect();
            let ends = matches!(commands[..], [Command::CompleteWorkflowExecution(_)]);
            let completion = WorkflowActivationCompletion::from_cmds(run_id, commands);
            core.complete_workflow_activation(completion).await.unwrap();
            if ends {
                return;
            }
        }
    };
    let finished = tokio::time::timeout(Duration::from_secs(20), drive)
        .await
        .is_ok();
    let history = starter.get_history().await;
    let events: Vec<EventType> = history.events.iter().map(|e| e.event_type()).collect();
    let label = format!(
        "activations {seen:?}, History {events:?}, {} reads of the Store",
        store.reads().len()
    );
    assert!(finished, "{label}");
    assert_eq!(
        count_of(&history, EventType::WorkflowTaskCompleted),
        1,
        "{label}"
    );
    let entries: usize = (read_markers(&history).iter())
        .map(|(_, data)| data.reads.len())
        .sum();
    assert_eq!(entries, 3, "{label}");
}
