//! The Stream reads of Workflow code, through a Worker on a mock server. These need the real
//! machinery: the completion of a task while a read runs, the three ways History reaches a replay,
//! a page end, the bound of the Records that a replay holds, and a shutdown.

use super::*;
use crate::worker::WorkflowErrorType;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::collections::HashSet;
use temporalio_common::protos::{
    coresdk::{workflow_activation::ResolveStreamRead, workflow_commands::StartStreamRead},
    temporal::api::failure::v1::failure::FailureInfo,
};
use temporalio_streams::{
    proto::{ClientProducer, CloseStreamRequest, ReadRecordsRequest, StreamRecord},
    workflow::peeked_marker,
};

fn client() -> Option<Producer> {
    Some(Producer {
        id: "client:test".to_string(),
        kind: Some(ProducerKind::Client(ClientProducer {})),
    })
}

/// Appends `count` Records to the Stream `id` as a client. The data of each is its place in the
/// Stream.
async fn fill(store: &MemoryStore, id: &str, count: usize) {
    let had = store.records(id).len();
    let records = (had..had + count)
        .map(|at| AppendRecord {
            body: Some(Payload {
                data: vec![at as u8],
                ..Default::default()
            }),
        })
        .collect();
    let append = AppendRecordsRequest {
        r#ref: Some(stream(id)),
        producer: client(),
        records,
        ..Default::default()
    };
    store.append_records(append).await.unwrap();
}

/// Appends a Record whose body has `size` bytes to the Stream `id` as a client.
async fn append_sized(store: &MemoryStore, id: &str, size: usize) {
    let body = Payload {
        data: vec![0; size],
        ..Default::default()
    };
    let append = AppendRecordsRequest {
        r#ref: Some(stream(id)),
        producer: client(),
        records: vec![AppendRecord { body: Some(body) }],
        ..Default::default()
    };
    store.append_records(append).await.unwrap();
}

async fn close_as_client(store: &MemoryStore, id: &str) {
    let close = CloseStreamRequest {
        r#ref: Some(stream(id)),
        producer: client(),
        ..Default::default()
    };
    store.close_stream(close).await.unwrap();
}

fn read(seq: u32, id: &str, idle: Duration, records_in_history: bool) -> Command {
    StartStreamRead {
        seq,
        stream: Some(stream(id)),
        from_offset: vec![],
        idle_timeout: Some(idle.try_into().unwrap()),
        records_in_history,
    }
    .into()
}

fn jobs_of(activation: &WorkflowActivation) -> Vec<Job> {
    (activation.jobs.iter())
        .map(|job| job.variant.clone().unwrap())
        .collect()
}

/// Returns the pages of reads in `activation`.
fn pages(activation: &WorkflowActivation) -> impl Iterator<Item = &ResolveStreamRead> {
    (activation.jobs.iter()).filter_map(|job| match &job.variant {
        Some(Job::ResolveStreamRead(read)) => Some(read),
        _ => None,
    })
}

fn ends(activation: &WorkflowActivation) -> bool {
    pages(activation).any(|read| read.end.is_some())
}

/// Returns the data of the last Record that `activation` gives, if it gives one.
fn last_data(activation: &WorkflowActivation) -> Option<u8> {
    let last = pages(activation).flat_map(|read| &read.records).last()?;
    last.body.as_ref()?.data.first().copied()
}

/// Returns the Records that each page of `jobs` gave, in order.
fn records_of(jobs: &[Vec<Job>]) -> Vec<StreamRecord> {
    (jobs.iter().flatten())
        .filter_map(|job| match job {
            Job::ResolveStreamRead(read) => Some(read.records.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// What makes the task complete while a read runs.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Completes {
    /// The heartbeat, while the read has a backlog that Workflow code takes slowly.
    AtTheHeartbeat,
    /// A timer that Workflow code starts at the first page, with the Records in History.
    ForATimer,
    /// The rejection of an Update while the read waits.
    ForAnUpdateRejection,
    /// The intent of a close.
    ForACloseIntent,
}

fn pages_of_five(wc: &mut WorkerConfig) {
    wc.stream_runtimes.settings.read_page_records = 5;
}

// A task that only reads hold completes at its heartbeat, and immediately for a command, a
// message or a close intent. Workflow code gets each Record once and in order over the tasks, and
// a replay gives the same jobs.
#[rstest::rstest]
#[case(Completes::AtTheHeartbeat)]
#[case(Completes::ForATimer)]
#[case(Completes::ForAnUpdateRejection)]
#[case(Completes::ForACloseIntent)]
#[tokio::test]
async fn a_task_completes_while_a_read_runs(#[case] completes: Completes) {
    let store = store(&["s", "c"]).await;
    let backlog = if completes == Completes::AtTheHeartbeat {
        1000
    } else {
        12
    };
    fill(&store, "s", backlog).await;
    let mut opts = StartOpts::new(&store);
    opts.configure = pages_of_five;
    if completes == Completes::ForAnUpdateRejection {
        // The task that the timer forces has the Update.
        opts.served = Some(Box::new(move |number, t| {
            let mut task = next_task(t);
            if number == 1 {
                task.add_update_request("u", t.current_event_id() - 1);
            }
            Some(task)
        }));
    }
    let task_timeout = match completes {
        Completes::AtTheHeartbeat => Duration::from_millis(200),
        _ => Duration::from_secs(10),
    };
    let run = start(history(task_timeout), opts);
    let workflow: Workflow = match completes {
        Completes::AtTheHeartbeat => |activation| match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![read(1, "s", Duration::ZERO, false)],
            _ if ends(activation) => vec![complete()],
            _ => vec![],
        },
        Completes::ForATimer => |activation| match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![read(1, "s", Duration::ZERO, true)],
            _ if ends(activation) => vec![complete()],
            _ if last_data(activation) == Some(4) => vec![timer(1)],
            _ => vec![],
        },
        Completes::ForAnUpdateRejection => |activation| match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => {
                vec![read(1, "s", Duration::from_secs(60), false), timer(1)]
            }
            Some(Job::DoUpdate(_)) => {
                let rejected = update_response::Response::Rejected(Default::default());
                vec![update_response(activation, rejected)]
            }
            _ if ends(activation) => vec![complete()],
            _ => vec![],
        },
        Completes::ForACloseIntent => |activation| match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => {
                vec![read(1, "s", Duration::ZERO, false), close(1, "c")]
            }
            _ if ends(activation) => vec![complete()],
            _ => vec![],
        },
    };
    // Workflow code takes its activations slowly, so that the backlog outlasts the heartbeat.
    let pause = match completes {
        Completes::AtTheHeartbeat => Duration::from_millis(2),
        _ => Duration::ZERO,
    };
    let drive = async {
        let mut jobs = vec![];
        while let Ok(activation) = run.worker.poll_workflow_activation().await {
            if activation.is_only_eviction() {
                let empty = WorkflowActivationCompletion::empty(activation.run_id);
                let _ = run.worker.complete_workflow_activation(empty).await;
                break;
            }
            jobs.push(
                activation
                    .jobs
                    .iter()
                    .map(|job| job.variant.clone().unwrap())
                    .collect(),
            );
            tokio::time::sleep(pause).await;
            respond(&run.worker, &activation, workflow).await;
            // Once the rejection went out, the read gets one more Record and the close.
            if let Some(Job::DoUpdate(_)) = &activation.jobs[0].variant {
                fill(&store, "s", 1).await;
                close_as_client(&store, "s").await;
            }
        }
        jobs
    };
    // Each early completion goes out with no wait for a heartbeat at 8 s.
    let live: Vec<Vec<Job>> = tokio::time::timeout(Duration::from_secs(5), drive)
        .await
        .expect("A task waited for its heartbeat");
    let sent = run.sent();
    let early = match completes {
        Completes::ForAnUpdateRejection => &sent[1],
        _ => &sent[0],
    };
    assert!(early.force_create_new_workflow_task, "{early:?}");
    let read_entry = marker(early).and_then(|marker| marker.reads.first().cloned());
    assert!(read_entry.as_ref().is_none_or(|read| read.end.is_none()));
    match completes {
        Completes::AtTheHeartbeat => {
            assert!(read_entry.is_some_and(|read| read.digest.len() == 16))
        }
        Completes::ForATimer => {
            assert_eq!(
                command_types(early)[..2],
                [CommandType::StartTimer, CommandType::RecordMarker]
            );
            assert!(read_entry.is_some_and(|read| read.records.len() == 5));
        }
        Completes::ForAnUpdateRejection => assert_eq!(early.messages.len(), 1),
        Completes::ForACloseIntent => {
            assert_eq!(marker(early).unwrap().close_intents.len(), 1);
        }
    }
    // Workflow code got each Record of the Stream once, in order, over several tasks.
    assert!(sent.len() > 1);
    assert_eq!(records_of(&live), store.records("s"));
    // A cached run that gets its whole History reads nothing for a replay, which would ask for
    // more than a live read does.
    let page_bytes = StreamSettings::default().read_ahead_bytes as i64;
    let reads = store.reads();
    assert!(reads.iter().all(|read| read.max_bytes <= page_bytes));
    run.worker.drain_pollers_and_shutdown_ref().await;
    // The mock server writes no Update, so that History doesn't replay.
    if completes == Completes::ForAnUpdateRejection {
        return;
    }
    // A replay gives the same jobs. With `records_in_history` it reads no Store.
    assert_eq!(run.replay(workflow).await, live);
    let replay_reads = store.reads().len() - reads.len();
    let in_history = completes == Completes::ForATimer;
    assert_eq!(replay_reads == 0, in_history, "{replay_reads}");
}

/// How History reaches a Worker that doesn't have the run.
#[derive(Clone, Copy, Debug)]
enum Path {
    /// The poll has the whole History.
    FullHistory,
    /// The poll has the last task, and Core fetches the History from the start.
    CacheMiss,
    /// The History goes on past the task of the poll, and the replay fetches the page with the
    /// marker.
    LaterPage,
}

/// What a replay finds of the Records of its read.
#[derive(Clone, Copy, Debug)]
enum Replay {
    /// The Stream is gone, and the Worker fails the Workflow for it.
    Gone,
    /// The Stream is gone, and the Worker fails the Workflow only for nondeterminism.
    GoneWithTheNondeterminismOptIn,
    /// The Store gives no answer.
    StoreDown,
    /// The Worker has no runtime for the Store.
    NoRuntime,
}

fn fails_workflow(wc: &mut WorkerConfig) {
    wc.workflow_failure_errors = HashSet::from([WorkflowErrorType::StreamReplay]);
}

fn fails_workflow_for_nondeterminism(wc: &mut WorkerConfig) {
    wc.workflow_failure_errors = HashSet::from([WorkflowErrorType::Nondeterminism]);
}

fn fails_workflow_with_its_store_down(wc: &mut WorkerConfig) {
    fails_workflow(wc);
    fast_tries(wc);
    wc.stream_runtimes.settings.replay_retry_bound = Duration::from_millis(100);
}

fn fails_workflow_with_no_runtime(wc: &mut WorkerConfig) {
    fails_workflow(wc);
    wc.stream_runtimes = StreamRuntimes::default();
}

/// Starts a timer in each of three tasks, reads Stream `s` in the fourth, and completes after a
/// fourth timer.
fn read_in_the_fourth_task(activation: &WorkflowActivation) -> Vec<Command> {
    match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
        Some(Job::FireTimer(fired)) if fired.seq < 3 => vec![timer(fired.seq + 1)],
        Some(Job::FireTimer(fired)) if fired.seq == 3 => vec![read(1, "s", Duration::ZERO, false)],
        Some(Job::FireTimer(_)) => vec![complete()],
        _ if ends(activation) => vec![timer(4)],
        _ => vec![],
    }
}

// A replay whose Records are gone fails the Workflow with the opt-in, on each way that History
// reaches it. A Store that gives no answer, or a Worker with no runtime for it, fails only the
// task, also with the opt-in.
#[rstest::rstest]
#[case(Path::FullHistory, Replay::Gone)]
#[case(Path::CacheMiss, Replay::Gone)]
#[case(Path::LaterPage, Replay::Gone)]
#[case(Path::FullHistory, Replay::GoneWithTheNondeterminismOptIn)]
#[case(Path::CacheMiss, Replay::StoreDown)]
#[case(Path::LaterPage, Replay::NoRuntime)]
#[tokio::test]
async fn the_replay_rule_holds_on_each_history_path(#[case] path: Path, #[case] replay: Replay) {
    let store = store(&["s"]).await;
    fill(&store, "s", 3).await;
    // The first run replays the three timers, and reads in the fourth task.
    let mut t = history(Duration::from_secs(10));
    for id in ["1", "2", "3"] {
        t.add_workflow_task_completed();
        let started = t.add_timer_started(id.to_string());
        t.add_timer_fired(started, id.to_string());
        t.add_workflow_task_scheduled_and_started();
    }
    let first = start(t, StartOpts::new(&store));
    loop {
        let activation = first.worker.poll_workflow_activation().await.unwrap();
        respond(&first.worker, &activation, read_in_the_fourth_task).await;
        if ends(&activation) {
            break;
        }
    }
    first.worker.drain_pollers_and_shutdown_ref().await;
    let mut t = first.history.lock().clone();
    let started = t.current_event_id();
    t.add_timer_fired(started, "4".to_string());
    let whole = next_task(&mut t);
    match replay {
        Replay::Gone | Replay::GoneWithTheNondeterminismOptIn => store.forget("s"),
        Replay::StoreDown => store.fail(
            Op::Read,
            StreamError::storage_unavailable("The Store is down"),
            usize::MAX,
        ),
        Replay::NoRuntime => {}
    }
    let mut opts = StartOpts::new(&store);
    let events = whole.history.clone().unwrap().events;
    opts.responses = vec![ResponseType::Raw(match path {
        Path::FullHistory => whole,
        Path::CacheMiss => {
            opts.pages = vec![page(&events, vec![])];
            hist_to_poll_resp(&t, "fake_wf_id", ResponseType::OneTask(5)).resp
        }
        Path::LaterPage => {
            // The poll is for the second task, and its History goes on past it. The first update
            // ends at the third task, so the replay fetches the fourth task with its marker.
            opts.pages = vec![page(&events[8..14], vec![2]), page(&events[14..], vec![])];
            let mut task = whole;
            task.history = Some(History {
                events: events[..8].to_vec(),
            });
            (task.previous_started_event_id, task.started_event_id) = (3, 8);
            task.next_page_token = vec![1];
            task
        }
    })];
    opts.served = None;
    opts.configure = match replay {
        Replay::Gone => fails_workflow,
        Replay::GoneWithTheNondeterminismOptIn => fails_workflow_for_nondeterminism,
        Replay::StoreDown => fails_workflow_with_its_store_down,
        Replay::NoRuntime => fails_workflow_with_no_runtime,
    };
    let fails_the_workflow = matches!(replay, Replay::Gone);
    opts.fails = usize::from(!fails_the_workflow);
    let second = start(t, opts);
    second.drive(read_in_the_fourth_task).await;
    second.worker.drain_pollers_and_shutdown_ref().await;
    let failed = second.failed.lock().clone();
    if fails_the_workflow {
        assert_eq!(failed, []);
        let sent = second.sent();
        let commands = &sent.last().unwrap().commands;
        let attributes = commands
            .last()
            .and_then(|command| command.attributes.as_ref());
        let Some(Attributes::FailWorkflowExecutionCommandAttributes(fail)) = attributes else {
            panic!("{commands:?}");
        };
        let info = fail.failure.as_ref().and_then(|f| f.failure_info.as_ref());
        assert!(
            matches!(info, Some(FailureInfo::ApplicationFailureInfo(info))
                if info.r#type == "StreamReplayError"),
            "{fail:?}"
        );
    } else {
        assert!(second.sent().is_empty());
        let [(cause, message)] = &failed[..] else {
            panic!("{failed:?}");
        };
        assert_eq!(
            *cause,
            WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
        );
        let reason = match replay {
            Replay::StoreDown => "The Store is unavailable",
            Replay::NoRuntime => "This Worker has no Stream runtime",
            _ => "The Store doesn't have the 3 Records",
        };
        assert!(message.contains(reason), "{message}");
    }
}

/// Who takes the task that the shutdown forces.
#[derive(Clone, Copy, Debug, PartialEq)]
enum After {
    /// Another Worker, which goes on with the read from History.
    AnotherWorker,
    /// The same Worker, through a poll that was still open.
    TheSameWorker,
}

/// Reads Stream `s`, and completes once it got the Record with the data 4.
fn read_until_the_fifth_record(activation: &WorkflowActivation) -> Vec<Command> {
    match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![read(1, "s", Duration::from_secs(60), false)],
        _ if last_data(activation) == Some(4) || ends(activation) => vec![complete()],
        _ => vec![],
    }
}

// A graceful shutdown completes a task that only a read holds, with a new task forced on the
// normal queue. The read stops with no end, and another Worker goes on from History. A task that
// comes back to the Worker starts no read, and goes on to another Worker.
#[rstest::rstest]
#[case(After::AnotherWorker)]
#[case(After::TheSameWorker)]
#[tokio::test]
async fn a_shutdown_ends_a_task_that_only_reads_hold(#[case] after: After) {
    let store = store(&["s"]).await;
    fill(&store, "s", 3).await;
    let t = history(Duration::from_secs(10));
    let mut opts = StartOpts::new(&store);
    opts.served = None;
    if after == After::TheSameWorker {
        let (tasks, receiver) = tokio::sync::mpsc::unbounded_channel();
        tasks.send(task_of(&t)).unwrap();
        opts.tasks = Some(receiver);
        let back: NextTask = Box::new(|number, t| (number == 1).then(|| next_task(t)));
        opts.during_report = Some((tasks, back));
    }
    let run = start(t, opts);
    let workflow = read_until_the_fifth_record;
    let init = run.worker.poll_workflow_activation().await.unwrap();
    respond(&run.worker, &init, workflow).await;
    let page = run.worker.poll_workflow_activation().await.unwrap();
    assert_eq!(last_data(&page), Some(2));
    respond(&run.worker, &page, workflow).await;
    wait_until(|| store.reads().len() >= 2).await;
    let reads = store.reads().len();
    // The shutdown would wait for the heartbeat at 8 s otherwise.
    tokio::time::timeout(
        Duration::from_secs(5),
        run.worker.drain_pollers_and_shutdown_ref(),
    )
    .await
    .expect("The shutdown waited for the read");
    let sent = run.sent();
    for completion in &sent {
        assert!(completion.force_create_new_workflow_task);
        assert!(!completion.return_new_workflow_task && completion.sticky_attributes.is_none());
    }
    let first_marker = marker(&sent[0]).unwrap();
    assert!(first_marker.reads[0].end.is_none(), "{first_marker:?}");
    if after == After::TheSameWorker {
        // The task came back, and its completion forced one more task with no new read.
        assert_eq!(sent.len(), 2);
        assert_eq!(store.reads().len(), reads);
        return;
    }
    assert_eq!(sent.len(), 1);
    fill(&store, "s", 2).await;
    let mut t = run.history.lock().clone();
    let mut opts = StartOpts::new(&store);
    opts.responses = vec![ResponseType::Raw(next_task(&mut t))];
    let next = start(t, opts);
    let jobs = next.drive(workflow).await;
    // The next Worker replays the first page, and reads only the Records after it.
    assert_eq!(records_of(&jobs), store.records("s"));
    next.worker.drain_pollers_and_shutdown_ref().await;
}

/// Starts timer 1, reads Stream `s` at the Signal, and starts timer 3 once the read ends.
fn read_then_timers(activation: &WorkflowActivation) -> Vec<Command> {
    match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
        Some(Job::SignalWorkflow(_)) => vec![read(1, "s", Duration::ZERO, false)],
        Some(Job::FireTimer(fired)) if fired.seq == 3 => vec![timer(4)],
        Some(Job::FireTimer(fired)) if fired.seq == 4 => vec![complete()],
        _ if ends(activation) => vec![timer(3)],
        _ => vec![],
    }
}

// The completion of the task before a page end can be on the next page, with the marker of a
// read whose Records the replay reads again before Workflow code starts a timer.
#[tokio::test]
async fn a_read_replays_the_same_wherever_a_page_ends() {
    let store = store(&["s"]).await;
    fill(&store, "s", 3).await;
    let first = start(history_to_task_3(), StartOpts::new(&store));
    loop {
        let activation = first.worker.poll_workflow_activation().await.unwrap();
        respond(&first.worker, &activation, read_then_timers).await;
        if ends(&activation) {
            break;
        }
    }
    first.worker.drain_pollers_and_shutdown_ref().await;
    let mut t = first.history.lock().clone();
    add_tasks_4_and_5(&mut t);
    let reads = store.reads().len();
    replay_at_each_cut(&store, &t, read_then_timers).await;
    assert!(store.reads().len() > reads);
}

/// Reads Stream `s`, and starts a timer at each page, which completes the task while the read goes
/// on, so each task gives one page. Once the read ends it starts timer 1, and it completes once
/// timer 1 fires.
fn read_with_a_timer_at_each_page(activation: &WorkflowActivation) -> Vec<Command> {
    // The offset of a page's last Record gives its timer a seq of its own.
    let seq = |record: &StreamRecord| u64::from_be_bytes(record.offset[..].try_into().unwrap());
    match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![read(1, "s", Duration::ZERO, false)],
        Some(Job::FireTimer(_)) => vec![complete()],
        _ if ends(activation) => vec![timer(1)],
        _ => (pages(activation).filter_map(|page| page.records.last()))
            .map(|last| timer(seq(last) as u32))
            .collect(),
    }
}

/// Replays `t` from the poll `task`, with the rest of its History on `pages`. Returns the jobs of
/// each activation, and the reads of the replay before each activation.
async fn replay_with_reads(
    store: &Arc<MemoryStore>,
    t: &TestHistoryBuilder,
    task: PollWorkflowTaskQueueResponse,
    pages: Vec<GetWorkflowExecutionHistoryResponse>,
) -> (Vec<Vec<Job>>, Vec<Vec<ReadRecordsRequest>>) {
    let mut opts = StartOpts::new(store);
    opts.responses = vec![ResponseType::Raw(task)];
    opts.pages = pages;
    let run = start(t.clone(), opts);
    let (mut jobs, mut reads, mut seen) = (vec![], vec![], store.reads().len());
    while let Ok(activation) = run.worker.poll_workflow_activation().await {
        let all = store.reads();
        reads.push(all[seen..].to_vec());
        seen = all.len();
        if activation.is_only_eviction() {
            let empty = WorkflowActivationCompletion::empty(activation.run_id);
            let _ = run.worker.complete_workflow_activation(empty).await;
            break;
        }
        jobs.push(jobs_of(&activation));
        respond(&run.worker, &activation, read_with_a_timer_at_each_page).await;
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
    (jobs, reads)
}

// A replay reads the markers of an update in History order, and once the Records that it read are
// 16 MiB or more, the update ends before the task of the next marker. So each update holds less
// than 16 MiB of Records before its last marker. The replay reads the Records of each entry once,
// and gives the jobs of the first run, whether the poll has the whole History or the rest of it
// comes on pages.
#[tokio::test]
async fn a_replay_holds_the_records_of_about_two_tasks() {
    const BOUND: usize = 16 << 20;
    let mut most_cuts = 0;
    for seed in 0..3 {
        let mut rng = StdRng::seed_from_u64(seed);
        let store = store(&["s"]).await;
        // Records of up to 1.5 MiB, with more than two updates of them.
        let total = rng.random_range(34 << 20..=48 << 20);
        let mut filled = 0;
        while filled < total {
            let size = match rng.random_bool(0.9) {
                true => rng.random_range(256 << 10..=1536 << 10),
                false => rng.random_range(1..1000),
            };
            append_sized(&store, "s", size).await;
            filled += size;
        }
        let first = start(history(Duration::from_secs(10)), StartOpts::new(&store));
        let mut live = vec![];
        loop {
            let activation = first.worker.poll_workflow_activation().await.unwrap();
            live.push(jobs_of(&activation));
            respond(&first.worker, &activation, read_with_a_timer_at_each_page).await;
            if ends(&activation) {
                break;
            }
        }
        first.worker.drain_pollers_and_shutdown_ref().await;
        let mut t = first.history.lock().clone();
        let events = t.get_full_history_info().unwrap().into_events();
        let started = (events.iter().rev())
            .find(|event| event.event_type() == EventType::TimerStarted)
            .unwrap();
        t.add_timer_fired(started.event_id, "1".to_string());
        let whole = next_task(&mut t);
        // The offset that the read entry of each marker with Records starts after, and the bytes
        // of its Records.
        let records = store.records("s");
        let markers: Vec<(Vec<u8>, usize)> = (events.iter())
            .filter_map(|event| peeked_marker(event)?.ok())
            .map(|marker| {
                let entry = &marker.reads[0];
                let count: u32 = entry.page_records.iter().sum();
                let bytes = (records.iter())
                    .filter(|record| record.offset > entry.after_offset)
                    .take(count as usize)
                    .map(Message::encoded_len)
                    .sum();
                (entry.after_offset.clone(), bytes)
            })
            .filter(|(_, bytes)| *bytes > 0)
            .collect();
        let events = whole.history.clone().unwrap().events;
        let mut paged = hist_to_poll_resp(&t, "fake_wf_id", ResponseType::ToTaskNum(2)).resp;
        let polled = paged.history.as_ref().unwrap().events.len();
        paged.next_page_token = vec![1];
        let cut = rng.random_range(polled + 1..=events.len());
        let pages = vec![
            page(&events[polled..cut], vec![2]),
            page(&events[cut..], vec![]),
        ];
        for (task, pages) in [(whole, vec![]), (paged, pages)] {
            let whole_history = pages.is_empty();
            let (jobs, reads) = replay_with_reads(&store, &t, task, pages).await;
            assert_eq!(jobs[..live.len()], live[..], "seed {seed}");
            let fired = vec![Job::FireTimer(FireTimer { seq: 1 })];
            assert_eq!(jobs[live.len()..], [fired], "seed {seed}");
            let starts = |reads: &[ReadRecordsRequest], after: &[u8]| {
                (reads.iter())
                    .filter(|read| read.from_offset == after)
                    .count()
            };
            for (after, _) in &markers {
                assert_eq!(starts(&reads.concat(), after), 1, "seed {seed}: {after:?}");
            }
            // The reads before an activation are those of an update. They take the next markers
            // in order, and hold less than the bound before the last of them.
            let (mut next, mut updates) = (0, 0_usize);
            for update in reads.iter().filter(|reads| !reads.is_empty()) {
                let read = (markers[next..].iter())
                    .take_while(|(after, _)| starts(update, after) > 0)
                    .count();
                assert!(read > 0, "seed {seed}: marker {next}");
                let held: usize = markers[next..next + read - 1].iter().map(|(_, b)| b).sum();
                assert!(held < BOUND, "seed {seed}: {held}");
                (next, updates) = (next + read, updates + 1);
            }
            assert_eq!(next, markers.len(), "seed {seed}");
            // With the whole History in the poll, each update after the first comes from a cut.
            if whole_history {
                most_cuts = most_cuts.max(updates.saturating_sub(1));
            }
        }
    }
    // A replay ends more than one update early.
    assert!(most_cuts > 1, "{most_cuts}");
}
