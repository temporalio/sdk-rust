//! The Stream reads of Workflow code, through a Worker on a mock server. These tests need the real
//! machinery of a Worker, such as the completion of a task while a read runs, or a replay.
//! Workflow code reads a Stream with one read for each page, from the `last_offset` of the page
//! before.

use super::*;
use crate::worker::{WorkflowErrorType, streams::peeked_read_marker};
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::collections::{HashSet, VecDeque};
use temporalio_common::protos::{
    constants::{LOCAL_ACTIVITY_MARKER_NAME, PATCH_MARKER_NAME, STREAM_READ_MARKER_NAME},
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        external_data::{
            StreamReadMarkerData, StreamReadPage, StreamReadResult, stream_read_result,
        },
        workflow_activation::{FireTimer, ResolveStreamRead, StreamPage, resolve_stream_read},
        workflow_commands::{self, SetPatchMarker, UpsertWorkflowSearchAttributes},
        workflow_completion::workflow_activation_completion::Status,
    },
    temporal::api::{common::v1::SearchAttributes, failure::v1::failure::FailureInfo},
};
use temporalio_streams::{
    proto::{ClientProducer, ReadRecordsRequest, StreamRecord},
    record::field_len,
};

fn client() -> Option<Producer> {
    Some(Producer {
        kind: Some(ProducerKind::Client(ClientProducer {
            namespace: NAMESPACE.to_string(),
            id: "test".to_string(),
        })),
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
        stream_id: id.to_string(),
        producer: client(),
        records,
    };
    store.append_records(append, String::new()).await.unwrap();
}

/// Appends a Record whose body has `size` bytes to the Stream `id` as a client.
async fn append_sized(store: &MemoryStore, id: &str, size: usize) {
    let body = Payload {
        data: vec![0; size],
        ..Default::default()
    };
    let append = AppendRecordsRequest {
        stream_id: id.to_string(),
        producer: client(),
        records: vec![AppendRecord { body: Some(body) }],
    };
    store.append_records(append, String::new()).await.unwrap();
}

/// Returns the read of the page of the Stream `id` after `from_offset`, which waits `wait` for a
/// Record.
fn read(seq: u32, id: &str, from_offset: &[u8], wait: Duration) -> ReadStream {
    ReadStream {
        seq,
        stream: Some(stream(id)),
        from_offset: from_offset.to_vec(),
        wait: Some(wait.try_into().unwrap()),
        ..Default::default()
    }
}

/// Returns the data of each read marker of `completion`, in order.
fn read_markers(completion: &WorkflowTaskCompletion) -> Vec<StreamReadMarkerData> {
    (completion.commands.iter())
        .filter_map(|command| match command.attributes.as_ref() {
            Some(Attributes::RecordMarkerCommandAttributes(marker))
                if marker.marker_name == STREAM_READ_MARKER_NAME =>
            {
                let data = &marker.details["data"].payloads[0];
                Some(StreamReadMarkerData::from_json_payload(data).unwrap())
            }
            _ => None,
        })
        .collect()
}

fn jobs_of(activation: &WorkflowActivation) -> Vec<Job> {
    (activation.jobs.iter())
        .map(|job| job.variant.clone().unwrap())
        .collect()
}

/// Returns the results of reads in `activation`.
fn results(activation: &WorkflowActivation) -> impl Iterator<Item = &ResolveStreamRead> {
    (activation.jobs.iter()).filter_map(|job| match &job.variant {
        Some(Job::ResolveStreamRead(read)) => Some(read),
        _ => None,
    })
}

fn page_of(read: &ResolveStreamRead) -> Option<&StreamPage> {
    match &read.status {
        Some(resolve_stream_read::Status::Page(page)) => Some(page),
        _ => None,
    }
}

/// Returns the read that goes on after the page of `read`, with the next seq. Returns `None` if
/// the read gave no Record, or the last one of its Stream, so Workflow code stops reading.
fn next_read(read: &ResolveStreamRead, wait: Duration) -> Option<ReadStream> {
    let page = page_of(read).filter(|page| !page.records.is_empty())?;
    Some(self::read(read.seq + 1, "s", &page.last_offset, wait))
}

/// Returns true if `activation` has the result of a read after which Workflow code stops reading.
fn ends(activation: &WorkflowActivation) -> bool {
    results(activation).any(|read| next_read(read, Duration::ZERO).is_none())
}

/// Returns the Records that each page of `jobs` gave, in order.
fn records_of(jobs: &[Vec<Job>]) -> Vec<StreamRecord> {
    (jobs.iter().flatten())
        .filter_map(|job| match job {
            Job::ResolveStreamRead(read) => page_of(read),
            _ => None,
        })
        .flat_map(|page| page.records.clone())
        .collect()
}

/// What keeps a read from an answer.
#[derive(Clone, Copy, Debug, PartialEq)]
enum NoAnswer {
    /// The Store holds the read.
    StoreHolds,
    /// The Stream has no Record when Workflow code starts a timer with the read. The Record comes
    /// once the task completed.
    WithATimer,
}

// At the read limit of its task, a read with no answer gives a page with no Record that ends at
// its `from_offset`, with `wait_cut`. A command for the server waits for one check of a read with
// a wait. A check with no Record parks the read, so the task completes with no new task, and the
// read starts again in the next task, which the timer makes.
#[rstest::rstest]
#[case(NoAnswer::StoreHolds)]
#[case(NoAnswer::WithATimer)]
#[tokio::test]
async fn a_read_with_no_answer_ends_at_its_limit_or_in_the_next_task(#[case] case: NoAnswer) {
    let store = store(&["s"]).await;
    if case == NoAnswer::StoreHolds {
        fill(&store, "s", 1).await;
        store.hold(Op::Read);
    }
    let mut opts = StartOpts::new(&store);
    opts.eager = Some(Box::new(|completion, t| {
        let types = command_types(completion);
        if !types.contains(&CommandType::StartTimer) {
            return None;
        }
        fire_last_timer(t);
        Some(next_task(t))
    }));
    let run = start(history(Duration::from_secs(60)), opts);
    let workflow = move |activation: &WorkflowActivation| -> Vec<Command> {
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => {
                let mut commands = vec![read(1, "s", &[], Duration::from_secs(60)).into()];
                if case == NoAnswer::WithATimer {
                    commands.push(timer(1));
                }
                commands
            }
            Some(Job::FireTimer(_)) => vec![],
            _ => vec![complete()],
        }
    };
    let comes = async {
        if case == NoAnswer::WithATimer {
            wait_until(|| !run.sent().is_empty()).await;
            fill(&store, "s", 1).await;
        }
    };
    // The heartbeat of each task is at 48 s.
    let drive = tokio::time::timeout(Duration::from_secs(30), run.drive(workflow));
    let (live, ()) = tokio::join!(drive, comes);
    let live = live.expect("A read held its task to its heartbeat");
    assert_eq!(
        live.len(),
        2 + usize::from(case == NoAnswer::WithATimer),
        "{live:?}"
    );
    let [Job::ResolveStreamRead(result)] = &live[live.len() - 1][..] else {
        panic!("{live:?}");
    };
    let page = page_of(result).unwrap();
    let sent = run.sent();
    match case {
        NoAnswer::StoreHolds => {
            assert!(page.records.is_empty() && page.last_offset.is_empty());
            assert!(page.wait_cut);
            assert_eq!(sent.len(), 1);
            // The Store's wait leaves room for the answer before the read limit at 250 ms.
            let wait: Duration = (store.reads()[0].wait).unwrap().try_into().unwrap();
            assert!(
                wait > Duration::ZERO && wait < Duration::from_millis(250),
                "{wait:?}"
            );
        }
        NoAnswer::WithATimer => {
            // The task completes at once, so the check takes the place of the read that it
            // didn't send yet. It parks the read, and the completion asks for no new task.
            assert_eq!(command_types(&sent[0]), [CommandType::StartTimer]);
            assert!(!sent[0].force_create_new_workflow_task);
            let waits: Vec<bool> = (store.reads().iter())
                .map(|read| read.wait.is_some())
                .collect();
            assert_eq!(waits, [false, true]);
            assert_eq!(page.records, store.records("s"));
            assert!(!page.wait_cut);
            let [marker] = &read_markers(&sent[1])[..] else {
                panic!("{sent:?}");
            };
            assert_eq!(marker.reads[0].activation_index, 1);
            assert_eq!(sent.len(), 2);
        }
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
    store.release();
    assert_eq!(run.replay(workflow).await, live);
}

/// Where a read meets a Store in an outage.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Outage {
    /// Each try fails with `StoreUnavailable`, and the read limit comes.
    AtTheReadLimit,
    /// The Store holds each read, and the check before the completion of a timer gets no answer
    /// by its deadline.
    AtACheck,
}

// A read outage fails the task as an append outage does, at the read limit and at a check. So
// Workflow code gets no `StoreUnavailable`, History keeps no failure of the read, and the task
// asks for no new task.
#[rstest::rstest]
#[case(Outage::AtTheReadLimit)]
#[case(Outage::AtACheck)]
#[tokio::test]
async fn a_read_outage_at_a_decision_point_fails_the_task(#[case] case: Outage) {
    let store = store(&["s"]).await;
    fill(&store, "s", 1).await;
    match case {
        Outage::AtTheReadLimit => {
            let down = StreamError::store_unavailable("The Store is down");
            store.fail(Op::Read, down, usize::MAX);
        }
        Outage::AtACheck => store.hold(Op::Read),
    }
    let mut opts = StartOpts::new(&store);
    opts.fails = 1;
    opts.configure = Box::new(|wc| {
        wc.stream_runtimes.settings.read_check_deadline = Duration::from_millis(200);
    });
    let run = start(history(Duration::from_secs(60)), opts);
    let workflow = move |activation: &WorkflowActivation| -> Vec<Command> {
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => {
                let mut commands = vec![read(1, "s", &[], Duration::from_secs(60)).into()];
                if case == Outage::AtACheck {
                    commands.push(timer(1));
                }
                commands
            }
            _ => vec![complete()],
        }
    };
    let (_, sequence) = drive_for(&run, Duration::from_secs(5), workflow).await;
    let failed = run.failed.lock().clone();
    let [(cause, message)] = &failed[..] else {
        panic!("{sequence}");
    };
    assert_eq!(
        *cause,
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
    );
    let error = match case {
        Outage::AtTheReadLimit => "The Store is down",
        Outage::AtACheck => "The Store gave no answer in 200ms",
    };
    let expected = format!(
        "StreamStoreUnavailable: read of Stream \"s\" on Store \"{STORE}\": \
         stream_store_unavailable: {error}"
    );
    assert_eq!(*message, expected, "{sequence}");
    assert!(run.sent().is_empty(), "{sequence}");
    run.worker.drain_pollers_and_shutdown_ref().await;
}

/// What a Store does with a read that doesn't wait.
#[derive(Clone, Copy, Debug, PartialEq)]
enum NoWait {
    /// The Store answers after the read limit of the task.
    AnswersLate,
    /// The Store never answers, and each try ends at a try deadline before the read limit.
    NeverAnswers,
}

// Only the Store can say that a Stream has no Record after the offset of a read that doesn't
// wait. So the read limit doesn't end such a read while its try waits for the answer. The read
// then holds the task no longer, and before the completion stops it, it gets a check, whose answer
// the task gives. A read whose last try failed fails the task at the read limit.
#[rstest::rstest]
#[case(NoWait::AnswersLate)]
#[case(NoWait::NeverAnswers)]
#[tokio::test]
async fn a_read_with_no_wait_gets_the_answer_of_the_store(#[case] case: NoWait) {
    let store = store(&["s"]).await;
    fill(&store, "s", 2).await;
    store.hold(Op::Read);
    let mut opts = StartOpts::new(&store);
    opts.fails = usize::from(case == NoWait::NeverAnswers);
    opts.configure = Box::new(move |wc| {
        wc.stream_runtimes.settings.try_deadline = match case {
            NoWait::AnswersLate => Duration::from_millis(300),
            NoWait::NeverAnswers => Duration::from_millis(100),
        };
    });
    let run = start(history(Duration::from_secs(60)), opts);
    let workflow = |activation: &WorkflowActivation| -> Vec<Command> {
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![read(1, "s", &[], Duration::ZERO).into()],
            _ => vec![complete()],
        }
    };
    let (live, sequence) = match case {
        NoWait::AnswersLate => {
            let release = async {
                tokio::time::sleep(Duration::from_millis(400)).await;
                store.release();
            };
            let drive = tokio::time::timeout(Duration::from_secs(30), run.drive(workflow));
            let (live, ()) = tokio::join!(drive, release);
            (live.expect("The read never ended"), String::new())
        }
        NoWait::NeverAnswers => (
            vec![],
            drive_for(&run, Duration::from_secs(5), workflow).await.1,
        ),
    };
    let sent = run.sent();
    let failed = run.failed.lock().clone();
    match case {
        NoWait::AnswersLate => {
            // The first task passed its read limit with no command, and the check before its
            // completion got the page.
            assert_eq!(
                command_types(&sent[0]),
                [
                    CommandType::RecordMarker,
                    CommandType::CompleteWorkflowExecution
                ],
                "{sequence}"
            );
            let [marker] = &read_markers(&sent[0])[..] else {
                panic!("{sequence}");
            };
            let page = recorded_page(&marker.reads[0]).unwrap();
            assert_eq!(page.record_count, 2, "{sequence}");
            let waits: Vec<bool> = (store.reads().iter())
                .map(|read| read.wait.is_some())
                .collect();
            assert_eq!(waits, [false, false], "{sequence}");
            assert_eq!(failed, [], "{sequence}");
        }
        NoWait::NeverAnswers => {
            let [(_, message)] = &failed[..] else {
                panic!("{sequence}");
            };
            assert!(
                message.ends_with("stream_store_unavailable: The Store gave no answer in 100ms"),
                "{message}"
            );
            assert!(sent.is_empty(), "{sequence}");
        }
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
    store.release();
    if case == NoWait::AnswersLate {
        assert_eq!(run.replay(workflow).await, live);
    }
}

/// How History reaches a Worker that doesn't have the run.
#[derive(Clone, Copy, Debug)]
enum Path {
    /// The poll has the whole History.
    FullHistory,
    /// The poll has the last task, and Core fetches the History from the start.
    CacheMiss,
    /// The poll has the whole History, and the replay reads the Records of each marker in a part
    /// of its own, so those of the second read come while a completion waits.
    InALaterPart,
}

/// Fires the timer that the History of `t` started last.
fn fire_last_timer(t: &mut TestHistoryBuilder) {
    let events = t.get_full_history_info().unwrap().into_events();
    let started = (events.iter().rev()).find_map(|event| match &event.attributes {
        Some(history_event::Attributes::TimerStartedEventAttributes(timer)) => {
            Some((event.event_id, timer.timer_id.clone()))
        }
        _ => None,
    });
    let (started, id) = started.unwrap();
    t.add_timer_fired(started, id);
}

/// What a replay finds of the Records of its read.
#[derive(Clone, Copy, Debug)]
enum Replay {
    /// The Stream is gone, and the Worker fails the Workflow for it.
    Gone,
    /// The Stream is gone, and the Worker fails the Workflow only for nondeterminism.
    GoneWithTheNondeterminismOptIn,
    /// The Stream is gone, and the Worker fails the Workflow for it. The task is on its second
    /// attempt.
    GoneOnALaterAttempt,
    /// The Stream is gone, and the Worker fails the Workflow for it. The task is a query.
    GoneForAQuery,
    /// The Store gives no answer.
    StoreDown,
    /// The Store gives no answer. The task is a query.
    StoreDownForAQuery,
    /// The Worker has no runtime for the Store.
    NoRuntime,
    /// The Worker has no runtime for the Store. The task is a query.
    NoRuntimeForAQuery,
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

fn fails_workflow_with_parts_of_one_marker(wc: &mut WorkerConfig) {
    fails_workflow(wc);
    wc.stream_runtimes.settings.max_task_record_bytes = 1;
}

/// Reads Stream `r` in task 2 and Stream `s` in task 4, with a timer in each task, and completes
/// after timer 4.
fn read_in_the_second_and_fourth_tasks(activation: &WorkflowActivation) -> Vec<Command> {
    let got = |seq| results(activation).any(|read| read.seq == seq);
    let read = |seq, id| vec![read(seq, id, &[], Duration::ZERO).into()];
    match &activation.jobs[0].variant {
        Some(Job::QueryWorkflow(_)) => answer(activation).into_iter().collect(),
        Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
        Some(Job::FireTimer(fired)) if fired.seq == 1 => read(1, "r"),
        Some(Job::FireTimer(fired)) if fired.seq == 2 => vec![timer(3)],
        Some(Job::FireTimer(fired)) if fired.seq == 3 => read(2, "s"),
        Some(Job::FireTimer(_)) => vec![complete()],
        _ if got(1) => vec![timer(2)],
        _ if got(2) => vec![timer(4)],
        _ => vec![],
    }
}

// Records that are gone fail the Workflow, with the opt-in, on the first attempt of a task. A
// Store with no answer, or a Worker with no runtime for it, fails only the task, since another
// Worker may still read. A query gets the failure as its answer.
#[rstest::rstest]
#[case(Path::FullHistory, Replay::Gone)]
#[case(Path::InALaterPart, Replay::Gone)]
#[case(Path::FullHistory, Replay::GoneWithTheNondeterminismOptIn)]
#[case(Path::FullHistory, Replay::GoneOnALaterAttempt)]
#[case(Path::FullHistory, Replay::GoneForAQuery)]
#[case(Path::CacheMiss, Replay::StoreDown)]
#[case(Path::FullHistory, Replay::StoreDownForAQuery)]
#[case(Path::FullHistory, Replay::NoRuntime)]
#[case(Path::FullHistory, Replay::NoRuntimeForAQuery)]
#[tokio::test]
async fn the_replay_rule_holds_on_each_history_path(#[case] path: Path, #[case] replay: Replay) {
    let store = store(&["r", "s"]).await;
    fill(&store, "r", 2).await;
    fill(&store, "s", 3).await;
    // The first run reads in two Workers. Each one replays the timers that History fired, and
    // reads in the last task.
    let mut t = history(Duration::from_secs(60));
    t.add_workflow_task_completed();
    let started = t.add_timer_started("1".to_string());
    t.add_timer_fired(started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    for seq in [1, 2] {
        let first = start(t, StartOpts::new(&store));
        loop {
            let activation = first.worker.poll_workflow_activation().await.unwrap();
            respond(
                &first.worker,
                &activation,
                read_in_the_second_and_fourth_tasks,
            )
            .await;
            if results(&activation).any(|read| read.seq == seq) {
                break;
            }
        }
        first.worker.drain_pollers_and_shutdown_ref().await;
        t = first.history.lock().clone();
        fire_last_timer(&mut t);
        if seq == 1 {
            t.add_full_wf_task();
            let started = t.add_timer_started("3".to_string());
            t.add_timer_fired(started, "3".to_string());
            t.add_workflow_task_scheduled_and_started();
        }
    }
    let mut whole = next_task(&mut t);
    let mut opts = StartOpts::new(&store);
    match replay {
        Replay::Gone | Replay::GoneWithTheNondeterminismOptIn => store.forget("s"),
        // The mock poller numbers the attempts of its responses, so this task comes on a channel.
        Replay::GoneOnALaterAttempt => {
            store.forget("s");
            whole.attempt = 2;
            let (tasks, receiver) = tokio::sync::mpsc::unbounded_channel();
            tasks.send(whole.clone()).unwrap();
            opts.tasks = Some(receiver);
        }
        Replay::GoneForAQuery => store.forget("s"),
        Replay::StoreDown | Replay::StoreDownForAQuery => store.fail(
            Op::Read,
            StreamError::store_unavailable("The Store is down"),
            usize::MAX,
        ),
        Replay::NoRuntime | Replay::NoRuntimeForAQuery => {}
    }
    let is_query = matches!(
        replay,
        Replay::GoneForAQuery | Replay::StoreDownForAQuery | Replay::NoRuntimeForAQuery
    );
    if is_query {
        // The server gave out the query before timer 4 fired.
        whole = query_before(&whole, t.current_event_id() - 3);
        opts.queries = 1;
    }
    let events = whole.history.clone().unwrap().events;
    opts.responses = vec![ResponseType::Raw(match path {
        Path::CacheMiss => {
            opts.pages = vec![page(&events)];
            let tasks = t.get_full_history_info().unwrap().wf_task_count();
            hist_to_poll_resp(&t, "fake_wf_id", ResponseType::OneTask(tasks)).resp
        }
        Path::FullHistory | Path::InALaterPart => whole,
    })];
    opts.served = None;
    let configure: fn(&mut WorkerConfig) = match (path, replay) {
        (Path::InALaterPart, _) => fails_workflow_with_parts_of_one_marker,
        (_, Replay::GoneWithTheNondeterminismOptIn) => fails_workflow_for_nondeterminism,
        (_, Replay::StoreDown | Replay::StoreDownForAQuery) => fails_workflow_with_its_store_down,
        (_, Replay::NoRuntime | Replay::NoRuntimeForAQuery) => fails_workflow_with_no_runtime,
        _ => fails_workflow,
    };
    opts.configure = Box::new(configure);
    let fails_the_workflow = matches!(replay, Replay::Gone);
    let reports_nothing = is_query || matches!(replay, Replay::GoneOnALaterAttempt);
    let reason = match replay {
        Replay::StoreDown | Replay::StoreDownForAQuery => "The Store is unavailable",
        Replay::NoRuntime | Replay::NoRuntimeForAQuery => "This Worker has no Stream runtime",
        _ => "The Store doesn't have the 3 Records",
    };
    opts.fails = usize::from(!fails_the_workflow && !reports_nothing);
    let second = start(t, opts);
    second.drive(read_in_the_second_and_fourth_tasks).await;
    second.worker.drain_pollers_and_shutdown_ref().await;
    let failed = second.failed.lock().clone();
    let answers = second.answers.lock().clone();
    if reports_nothing {
        assert!(failed.is_empty() && second.sent().is_empty());
        let failed_with_the_reason =
            |(_, failure): &Answer| failure.as_ref().is_some_and(|m| m.contains(reason));
        assert!(answers.iter().all(failed_with_the_reason), "{answers:?}");
        assert_eq!(answers.len(), usize::from(is_query));
    } else if fails_the_workflow {
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
        assert!(message.contains(reason), "{message}");
    }
}

/// Answers a forced completion with the next task. Answers one that starts a timer other than
/// timer 1 with the firing of that timer and the next task, since Core asks for no new task for a
/// read that it knows of no Record for.
fn page_server() -> Eager {
    Box::new(|completion, t| {
        if completion.force_create_new_workflow_task {
            return Some(next_task(t));
        }
        let timers: Vec<&str> = (completion.commands.iter())
            .filter_map(|command| match &command.attributes {
                Some(Attributes::StartTimerCommandAttributes(timer)) => Some(&*timer.timer_id),
                _ => None,
            })
            .collect();
        // A completion that also starts timer 1 ends the reads, and the test fires timer 1.
        let fires = timers.iter().any(|id| *id != "1") && !timers.contains(&"1");
        fires.then(|| {
            fire_last_timer(t);
            next_task(t)
        })
    })
}

/// Returns true if `activation` fires a timer that isn't the timer of a page. A page timer has the
/// offset of a Record as its seq, which is far above the seq of each other timer.
fn fires_a_last_timer(activation: &WorkflowActivation) -> bool {
    (activation.jobs.iter())
        .any(|job| matches!(&job.variant, Some(Job::FireTimer(fired)) if fired.seq < 1 << 20))
}

/// Returns Workflow code that reads Stream `s` in pages of `page_records` Records. At each page it
/// starts a timer and the next read, so the task completes while that read runs, and each task
/// gives one page. Once a read gives no Record it starts timer 1, and it completes once timer 1
/// fires.
fn read_with_a_timer_at_each_page(
    page_records: usize,
) -> impl Fn(&WorkflowActivation) -> Vec<Command> + Copy {
    // The offset of a page's last Record gives its timer a seq of its own.
    let seq = |offset: &[u8]| u64::from_be_bytes(offset.try_into().unwrap());
    let page = move |read: ReadStream| -> Command {
        ReadStream {
            max_records: page_records as i32,
            ..read
        }
        .into()
    };
    move |activation| match &activation.jobs[0].variant {
        Some(Job::QueryWorkflow(_)) => answer(activation).into_iter().collect(),
        Some(Job::InitializeWorkflow(_)) => vec![page(read(1, "s", &[], Duration::ZERO))],
        _ if fires_a_last_timer(activation) => vec![complete()],
        _ => (results(activation))
            .flat_map(|result| match next_read(result, Duration::ZERO) {
                Some(next) => vec![timer(seq(&next.from_offset) as u32), page(next)],
                None => vec![timer(1)],
            })
            .collect(),
    }
}

/// Runs `read_with_a_timer_at_each_page` until a read gives no Record. Returns the jobs of each
/// activation, and the History that the run wrote, to which it adds the firing of timer 1 and the
/// next task. The next task comes last, with the whole History.
async fn first_run_with_a_timer_at_each_page(
    store: &Arc<MemoryStore>,
    page_records: usize,
) -> (
    Vec<Vec<Job>>,
    TestHistoryBuilder,
    PollWorkflowTaskQueueResponse,
) {
    let mut opts = StartOpts::new(store);
    opts.eager = Some(page_server());
    let first = start(history(Duration::from_secs(60)), opts);
    let workflow = read_with_a_timer_at_each_page(page_records);
    let mut live = vec![];
    loop {
        let activation = first.worker.poll_workflow_activation().await.unwrap();
        live.push(jobs_of(&activation));
        respond(&first.worker, &activation, workflow).await;
        if ends(&activation) {
            break;
        }
    }
    first.worker.drain_pollers_and_shutdown_ref().await;
    let mut t = first.history.lock().clone();
    fire_last_timer(&mut t);
    let whole = next_task(&mut t);
    (live, t, whole)
}

/// Completes `activation` if it only evicts its run, and returns true if it did.
async fn evicted(worker: &Worker, activation: &WorkflowActivation) -> bool {
    let evicts = activation.is_only_eviction();
    if evicts {
        let empty = WorkflowActivationCompletion::empty(activation.run_id.clone());
        let _ = worker.complete_workflow_activation(empty).await;
    }
    evicts
}

fn recorded_page(entry: &StreamReadResult) -> Option<&StreamReadPage> {
    match &entry.result {
        Some(stream_read_result::Result::Page(page)) => Some(page),
        _ => None,
    }
}

/// Returns the offset that the read entry of each marker of `t` with Records starts after, and the
/// bytes of its Records in Stream `s`.
fn markers_of(t: &TestHistoryBuilder, store: &MemoryStore) -> Vec<(Vec<u8>, usize)> {
    let records = store.records("s");
    let events = t.get_full_history_info().unwrap().into_events();
    (events.iter())
        .filter_map(|event| peeked_read_marker(event)?.ok())
        .map(|marker| {
            let entry = &marker.reads[0];
            let count = recorded_page(entry).map_or(0, |page| page.record_count);
            let bytes = (records.iter())
                .filter(|record| record.offset > entry.from_offset)
                .take(count as usize)
                .map(prost::Message::encoded_len)
                .sum();
            (entry.from_offset.clone(), bytes)
        })
        .filter(|(_, bytes)| *bytes > 0)
        .collect()
}

/// Checks the reads of a replay of `markers`. The reads before an activation are those of a part,
/// which reads the next markers in order, and whose Records fit `bound` unless it reads one
/// marker. The replay reads each marker once. Returns the number of markers that each part reads.
fn check_parts(
    seed: u64,
    markers: &[(Vec<u8>, usize)],
    reads: &[Vec<ReadRecordsRequest>],
    bound: usize,
) -> Vec<usize> {
    let starts = |reads: &[ReadRecordsRequest], after: &[u8]| {
        (reads.iter())
            .filter(|read| read.from_offset == after)
            .count()
    };
    for (after, _) in markers {
        assert_eq!(starts(&reads.concat(), after), 1, "seed {seed}: {after:?}");
    }
    let (mut next, mut parts) = (0, vec![]);
    for part in reads.iter().filter(|reads| !reads.is_empty()) {
        let read = (markers[next..].iter())
            .take_while(|(after, _)| starts(part, after) > 0)
            .count();
        assert!(read > 0, "seed {seed}: marker {next}");
        let held: usize = markers[next..next + read].iter().map(|(_, b)| b).sum();
        assert!(read == 1 || held <= bound, "seed {seed}: {held}");
        next += read;
        parts.push(read);
    }
    assert_eq!(next, markers.len(), "seed {seed}");
    parts
}

/// Replays `t` from the poll `task` on a Worker that `configure` sets, with the Workflow code
/// that read it in pages of `page_records`. Returns the jobs of each activation, and the reads of
/// the replay before each activation.
async fn replay_with_reads(
    store: &Arc<MemoryStore>,
    t: &TestHistoryBuilder,
    task: PollWorkflowTaskQueueResponse,
    page_records: usize,
    configure: impl FnOnce(&mut WorkerConfig) + 'static,
) -> (Vec<Vec<Job>>, Vec<Vec<ReadRecordsRequest>>) {
    let mut opts = StartOpts::new(store);
    opts.responses = vec![ResponseType::Raw(task)];
    opts.configure = Box::new(configure);
    let run = start(t.clone(), opts);
    let (mut jobs, mut reads, mut seen) = (vec![], vec![], store.reads().len());
    while let Ok(activation) = run.worker.poll_workflow_activation().await {
        let all = store.reads();
        reads.push(all[seen..].to_vec());
        seen = all.len();
        if evicted(&run.worker, &activation).await {
            break;
        }
        jobs.push(jobs_of(&activation));
        let workflow = read_with_a_timer_at_each_page(page_records);
        respond(&run.worker, &activation, workflow).await;
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
    (jobs, reads)
}

// A replay reads the next part only once Workflow code got the Records of the part before it, but
// for those of one marker, which the machines peeked. So it never holds more than two parts.
#[tokio::test]
async fn a_replay_holds_at_most_two_parts_of_records() {
    const PART: usize = 64 << 10;
    let mut most_parts = 0;
    for seed in 0..8 {
        let _seed = SeedOnPanic(seed);
        let mut rng = StdRng::seed_from_u64(seed);
        let store = store(&["s"]).await;
        // Each task of the first run gives one page, so each marker has the Records of one.
        let page_records = rng.random_range(1..=8);
        for _ in 0..page_records * rng.random_range(5..=40) {
            let size = match rng.random_bool(0.7) {
                true => rng.random_range(1..=2 << 10),
                false => rng.random_range(2 << 10..=24 << 10),
            };
            append_sized(&store, "s", size).await;
        }
        let (live, t, whole) = first_run_with_a_timer_at_each_page(&store, page_records).await;
        let markers = markers_of(&t, &store);
        let parts_of_64_kib =
            |wc: &mut WorkerConfig| wc.stream_runtimes.settings.max_task_record_bytes = PART;
        let (jobs, reads) =
            replay_with_reads(&store, &t, whole, page_records, parts_of_64_kib).await;
        assert_eq!(jobs[..live.len()], live[..], "seed {seed}");
        let fired = vec![Job::FireTimer(FireTimer { seq: 1 })];
        assert_eq!(jobs[live.len()..], [fired], "seed {seed}");
        let parts = check_parts(seed, &markers, &reads, PART);
        most_parts = most_parts.max(parts.len());
        // The bytes that the replay read, and those that Workflow code got, before each activation.
        let size = |record: &StreamRecord| prost::Message::encoded_len(record);
        let (mut read, mut given, mut next) = (0, 0, 0);
        let mut last_two = [0, 0];
        for (part, jobs) in reads.iter().zip(&jobs) {
            if !part.is_empty() {
                let markers = &markers[next..next + part.len()];
                next += part.len();
                let bytes: usize = markers.iter().map(|(_, bytes)| bytes).sum();
                last_two = [last_two[1], bytes];
                read += bytes;
            }
            let held = read - given;
            assert!(held <= last_two[0] + last_two[1], "seed {seed}: {held}");
            given += records_of(std::slice::from_ref(jobs))
                .iter()
                .map(size)
                .sum::<usize>();
        }
    }
    // Some replay reads more than three parts.
    assert!(most_parts > 3, "{most_parts}");
}

/// What Workflow code does at a page of its read, besides the timer that completes the task.
#[derive(Clone, Copy, Debug)]
enum AtAPage {
    Nothing,
    Patch,
    Append,
    LocalActivity,
}

impl AtAPage {
    /// Returns what the Workflow code of `seed` does at the page whose last Record has the offset
    /// `seq`. The first run and each replay decide the same way.
    fn of(seed: u64, seq: u32) -> Self {
        let mut rng = StdRng::seed_from_u64(seed << 32 | u64::from(seq));
        [
            Self::Nothing,
            Self::Patch,
            Self::Append,
            Self::LocalActivity,
        ][rng.random_range(0..4)]
    }
}

/// What the first run of a seed does. The seed decides all of it, and the first run lets one
/// result in at a time, so two runs of a seed write the same History.
#[derive(Clone, Debug)]
struct Plan {
    /// The Workflow waits for a Signal in its second task before it reads, so its first task
    /// completes with no command.
    waits_for_a_signal: bool,
    /// What Workflow code does at each page, by the offset of the page's last Record.
    at_pages: HashMap<u32, AtAPage>,
}

/// Returns the Workflow code of `plan`. It reads Stream `s` in pages of `page_records` Records. At
/// each page it starts a timer and the next read, so the task completes at once while that read
/// runs, and it does what the plan says. Each page has a task of its own. At a page
/// with a Local Activity, the timer and the next read wait for the Activity's result, which would
/// otherwise end the wait of the next task for the read. It starts timer 1 once a read gives no
/// Record, and completes once timer 1 fires.
fn reads_with_peeked_results(
    plan: Plan,
    page_records: usize,
) -> impl FnMut(&WorkflowActivation) -> Vec<Command> {
    let mut notified = HashSet::new();
    let mut deferred: Option<ReadStream> = None;
    let page = move |read: ReadStream| -> Command {
        ReadStream {
            max_records: page_records as i32,
            ..read
        }
        .into()
    };
    move |activation| {
        for job in &activation.jobs {
            if let Some(Job::NotifyHasPatch(patch)) = &job.variant {
                notified.insert(patch.patch_id.clone());
            }
        }
        let mut commands = vec![];
        for job in &activation.jobs {
            match &job.variant {
                Some(Job::InitializeWorkflow(_)) if !plan.waits_for_a_signal => {
                    commands.push(page(read(1, "s", &[], Duration::ZERO)));
                }
                Some(Job::SignalWorkflow(_)) => {
                    commands.push(page(read(1, "s", &[], Duration::ZERO)));
                }
                Some(Job::ResolveActivity(_)) => {
                    if let Some(next) = deferred.take() {
                        let seq = u64::from_be_bytes(next.from_offset[..].try_into().unwrap());
                        commands.push(timer(seq as u32));
                        commands.push(page(next));
                    }
                }
                Some(Job::FireTimer(_)) => commands.push(complete()),
                Some(Job::ResolveStreamRead(result)) => {
                    let Some(next) = next_read(result, Duration::ZERO) else {
                        commands.push(timer(1));
                        continue;
                    };
                    let seq = u64::from_be_bytes(next.from_offset[..].try_into().unwrap()) as u32;
                    let at_page = plan.at_pages[&seq];
                    match at_page {
                        AtAPage::LocalActivity => deferred = Some(next),
                        _ => {
                            commands.push(timer(seq));
                            commands.push(page(next));
                        }
                    }
                    match at_page {
                        AtAPage::Nothing => {}
                        // A replay patches only once History told it of the patch, as lang does.
                        AtAPage::Patch => {
                            let patch_id = format!("p{seq}");
                            if !activation.is_replaying || notified.contains(&patch_id) {
                                let deprecated = false;
                                commands.push(
                                    SetPatchMarker {
                                        patch_id,
                                        deprecated,
                                    }
                                    .into(),
                                );
                            }
                        }
                        AtAPage::Append => commands.push(append(seq, "a")),
                        AtAPage::LocalActivity => commands.push(local_activity(seq)),
                    }
                }
                _ => {}
            }
        }
        commands
    }
}

/// Returns the name of each job of each activation, sorted, without the patches that only a
/// replay gets.
fn job_names(jobs: &[Vec<Job>]) -> Vec<Vec<String>> {
    let name = |job: &Job| match job {
        Job::NotifyHasPatch(_) => None,
        Job::DoUpdate(update) => Some(format!("update {}", update.id)),
        Job::ResolveActivity(result) => Some(format!("local activity {}", result.seq)),
        other => Some(format!("{other:?}")),
    };
    (jobs.iter())
        .map(|jobs| {
            let mut names: Vec<String> = jobs.iter().filter_map(name).collect();
            names.sort();
            names
        })
        .collect()
}

/// Runs the Workflow of `plan` until its read ends, and returns the jobs of each activation, and
/// the History with the firing of timer 1. The Signal that a Workflow may wait for is in the
/// History of its first poll.
///
/// A page, the result of an append and the result of a Local Activity would race each other. So
/// the run waits for the one result that can come next. It lets the Store answer a read, or
/// completes a Local Activity, only when nothing else can come.
async fn first_run_with_peeked_results(
    plan: &Plan,
    store: &Arc<MemoryStore>,
    page_records: usize,
) -> (Vec<Vec<Job>>, TestHistoryBuilder) {
    let mut opts = StartOpts::new(store);
    // Only the Store and the Local Activities end a wait, so that each page has a task.
    opts.configure = Box::new(|wc| wc.stream_runtimes.settings.read_time = None);
    // The server answers each completion with the next task, but the one that starts timer 1.
    opts.eager = Some(Box::new(move |completion, t| {
        let ends = (completion.commands.iter()).any(|command| {
            matches!(&command.attributes,
                Some(Attributes::StartTimerCommandAttributes(timer)) if timer.timer_id == "1")
        });
        (!ends).then(|| next_task(t))
    }));
    let mut t = history(Duration::from_secs(60));
    if plan.waits_for_a_signal {
        t.add_workflow_task_completed();
        t.add_we_signaled("go", vec![]);
        t.add_workflow_task_scheduled_and_started();
    }
    store.hold(Op::Read);
    let run = start(t, opts);
    let mut workflow = reads_with_peeked_results(plan.clone(), page_records);
    let mut live = vec![];
    let mut flags = 0;
    // The appends with no result yet, the Local Activities that the run didn't complete, and
    // whether a read waits for the Store.
    let (mut appends, mut local_activities, mut reading) = (0, 0, false);
    loop {
        let activation = run.worker.poll_workflow_activation().await.unwrap();
        live.push(jobs_of(&activation));
        let commands = workflow(&activation);
        let ended = ends(&activation);
        for job in &live[live.len() - 1] {
            if let Job::ResolveStreamAppend(_) = job {
                appends -= 1;
            }
        }
        for command in &commands {
            match command {
                Command::ReadStream(_) => reading = true,
                Command::AppendToStream(_) => appends += 1,
                Command::ScheduleLocalActivity(_) => local_activities += 1,
                _ => {}
            }
        }
        let mut completion =
            WorkflowActivationCompletion::from_cmds(activation.run_id.clone(), commands);
        // Each page uses a new lang flag, so each completion has a flag that the ones before it
        // don't have.
        if results(&activation).next().is_some()
            && let Some(Status::Successful(success)) = completion.status.as_mut()
        {
            flags += 1;
            success.used_internal_flags.push(flags);
        }
        run.worker
            .complete_workflow_activation(completion)
            .await
            .unwrap();
        if ended {
            break;
        }
        // The Store answers an append immediately.
        if appends > 0 {
            continue;
        }
        if local_activities > 0 {
            local_activities -= 1;
            let task = run.worker.poll_activity_task().await.unwrap();
            let completion = ActivityTaskCompletion {
                task_token: task.task_token,
                result: Some(ActivityExecutionResult::ok(Payload::default())),
            };
            run.worker.complete_activity_task(completion).await.unwrap();
        } else if reading {
            // A read that the completion stopped takes no pass, so the one that goes on gets it.
            reading = false;
            store.pass(Op::Read, 1);
        }
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
    store.release();
    let mut t = run.history.lock().clone();
    fire_last_timer(&mut t);
    (live, t)
}

/// Returns what the machines peek in the completion of the event `completed` of `events`.
fn peeked_in(events: &[HistoryEvent], completed: i64) -> Vec<&'static str> {
    let completion = (events.iter())
        .skip_while(|event| event.event_id <= completed)
        .take_while(|event| event.event_type() != EventType::WorkflowTaskScheduled);
    completion
        .filter_map(|event| match &event.attributes {
            Some(history_event::Attributes::MarkerRecordedEventAttributes(marker)) => {
                match marker.marker_name.as_str() {
                    LOCAL_ACTIVITY_MARKER_NAME => Some("local activity"),
                    PATCH_MARKER_NAME => Some("patch"),
                    STREAM_READ_MARKER_NAME => Some("stream"),
                    _ => None,
                }
            }
            Some(history_event::Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(_)) => {
                Some("update")
            }
            _ => None,
        })
        .collect()
}

/// Turns workflow task chunking on or off for the replay of `t`, by the flags of its first
/// completion. The other flags of that completion stay.
fn set_chunking(t: &mut TestHistoryBuilder, chunking: bool) {
    let flag = CoreInternalFlags::DisableWorkflowTaskChunking as u32;
    let first = (t.get_full_history_info().unwrap().into_events().iter())
        .find_map(|event| match &event.attributes {
            Some(history_event::Attributes::WorkflowTaskCompletedEventAttributes(a)) => {
                a.sdk_metadata.clone()
            }
            _ => None,
        })
        .unwrap_or_default();
    let core: Vec<CoreInternalFlags> = CoreInternalFlags::all_except_too_high()
        .filter(|known| {
            let known = *known as u32;
            match known == flag {
                true => !chunking,
                false => first.core_used_flags.contains(&known),
            }
        })
        .collect();
    t.set_flags_first_wft(&core, &first.lang_used_flags);
}

/// Why a replay never joins a task to the next one.
#[derive(Clone, Copy, Debug, PartialEq)]
enum NeverJoined {
    /// History has the flag that turns chunking off.
    ChunkingOff,
    /// The task has a command event.
    WithACommand,
}

// A replay joins a forced task with no command event to the next task only with chunking on. So a
// forced completion with an open call and no result records no marker with no entry when its task
// has a command event, or when History turns chunking off.
#[rstest::rstest]
#[case(NeverJoined::ChunkingOff)]
#[case(NeverJoined::WithACommand)]
#[tokio::test]
async fn a_forced_completion_that_a_replay_never_joins_has_no_empty_marker(
    #[case] case: NeverJoined,
) {
    let store = store(&["s"]).await;
    let mut t = history(Duration::from_secs(60));
    t.add_workflow_task_completed();
    let started = t.add_timer_started("1".to_string());
    t.add_timer_fired(started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    set_chunking(&mut t, case == NeverJoined::WithACommand);
    let mut opts = StartOpts::new(&store);
    opts.served = None;
    let run = start(t, opts);
    let workflow = move |activation: &WorkflowActivation| -> Vec<Command> {
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
            _ if case == NeverJoined::WithACommand => vec![append(1, "s"), timer(2)],
            _ => vec![append(1, "s")],
        }
    };
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    respond(&run.worker, &replayed, workflow).await;
    let fired = run.worker.poll_workflow_activation().await.unwrap();
    store.hold(Op::Append);
    respond(&run.worker, &fired, workflow).await;
    run.worker.request_workflow_eviction(&fired.run_id);
    assert_eq!(run.drive(|_| vec![]).await.len(), 0);
    let sent = run.sent();
    let [last] = &sent[..] else {
        panic!("{sent:?}");
    };
    assert!(last.force_create_new_workflow_task);
    let types = match case {
        NeverJoined::ChunkingOff => vec![],
        NeverJoined::WithACommand => vec![CommandType::StartTimer],
    };
    assert_eq!(command_types(last), types);
    run.worker.drain_pollers_and_shutdown_ref().await;
    store.release();
}

/// Returns the recorded bytes of each read marker of `events` with Records, and the event id of
/// the completion that has it.
fn recorded_markers(events: &[HistoryEvent]) -> Vec<(u64, i64)> {
    let mut completed = 0;
    (events.iter())
        .filter_map(|event| {
            if event.event_type() == EventType::WorkflowTaskCompleted {
                completed = event.event_id;
            }
            let marker = peeked_read_marker(event)?.ok()?;
            let pages = marker.reads.iter().filter_map(recorded_page);
            let recorded = pages.map(|page| page.record_bytes).sum();
            Some((recorded, completed))
        })
        .filter(|(recorded, _)| *recorded > 0)
        .collect()
}

type Poll = (
    PollWorkflowTaskQueueResponse,
    Vec<GetWorkflowExecutionHistoryResponse>,
);

fn poll_for_last_task(
    t: &TestHistoryBuilder,
    whole: PollWorkflowTaskQueueResponse,
    path: Path,
) -> Poll {
    match path {
        Path::CacheMiss => {
            let events = whole.history.unwrap().events;
            let tasks = t.get_full_history_info().unwrap().wf_task_count();
            let sticky = hist_to_poll_resp(t, "fake_wf_id", ResponseType::OneTask(tasks)).resp;
            (sticky, vec![page(&events)])
        }
        Path::FullHistory | Path::InALaterPart => (whole, vec![]),
    }
}

/// Returns each job of a read in `jobs`, in the order that Workflow code got them.
fn read_jobs(jobs: &[Vec<Job>]) -> Vec<String> {
    let read = |job: &&Job| matches!(job, Job::ResolveStreamRead(_));
    let jobs = jobs.iter().flatten().filter(read);
    jobs.map(|job| format!("{job:?}")).collect()
}

/// What a replay gave Workflow code.
#[derive(Debug, PartialEq)]
struct Replayed {
    /// The jobs of each activation, by name.
    jobs: Vec<Vec<String>>,
    reads: Vec<String>,
    /// The lang flags of each activation.
    flags: Vec<Vec<u32>>,
}

async fn replay_with_bound(
    store: &Arc<MemoryStore>,
    t: &TestHistoryBuilder,
    (task, pages): &Poll,
    bound: usize,
    label: String,
    mut workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
) -> Replayed {
    let mut opts = StartOpts::new(store);
    opts.responses = vec![ResponseType::Raw(task.clone())];
    opts.pages = pages.clone();
    opts.configure = Box::new(move |wc| wc.stream_runtimes.settings.max_task_record_bytes = bound);
    opts.label = label.clone();
    let replay = start(t.clone(), opts);
    let mut flags = vec![];
    let flagged = |activation: &WorkflowActivation| {
        flags.push(activation.available_internal_flags.clone());
        workflow(activation)
    };
    let jobs = tokio::time::timeout(Duration::from_secs(30), replay.drive(flagged))
        .await
        .unwrap_or_else(|_| panic!("{label}: the replay hung"));
    replay.worker.drain_pollers_and_shutdown_ref().await;
    Replayed {
        jobs: job_names(&jobs),
        reads: read_jobs(&jobs),
        flags,
    }
}

// A replay gives the same activations whatever the size of its parts of Records. With chunking
// off they are the activations of the first run. With chunking on it gives the results of a task
// in fewer activations, as for a Local Activity.
#[tokio::test]
async fn a_replay_gives_the_jobs_of_the_first_run_in_both_chunking_modes() {
    let (mut positions, mut peeked) = (HashMap::new(), HashMap::new());
    let mut chunked_after_no_command = 0;
    for seed in 0..40 {
        let _seed = SeedOnPanic(seed);
        let mut rng = StdRng::seed_from_u64(seed);
        let store = store(&["s", "a"]).await;
        let page_records = rng.random_range(1..=3);
        // At least four pages, so that the first part can end after the first, a middle or the
        // next-to-last marker.
        fill(&store, "s", rng.random_range(3 * page_records + 1..=20)).await;
        let records = store.records("s");
        let pages: Vec<u32> = (records.chunks(page_records))
            .map(|page| u64::from_be_bytes(page[page.len() - 1].offset[..].try_into().unwrap()))
            .map(|seq| seq as u32)
            .collect();
        // The seed picks the last marker of the first part, and what the completion of that
        // marker has. It's the marker of page `last`.
        let kind = seed as usize % 3;
        let last = match (seed / 3) % 3 {
            0 => 1,
            1 => rng.random_range(2..pages.len() - 1),
            _ => pages.len() - 1,
        };
        let mut at_pages: HashMap<u32, AtAPage> = (pages.iter())
            .map(|seq| (*seq, AtAPage::of(seed, *seq)))
            .collect();
        let at = [AtAPage::Patch, AtAPage::LocalActivity, AtAPage::Append][kind];
        at_pages.insert(pages[last - 1], at);
        let plan = Plan {
            waits_for_a_signal: seed % 2 == 1,
            at_pages,
        };
        let (live, mut t) = first_run_with_peeked_results(&plan, &store, page_records).await;
        let chunking = (seed / 2) % 2 == 0;
        set_chunking(&mut t, chunking);
        let whole = next_task(&mut t);
        let events = whole.history.clone().unwrap().events;
        let markers = recorded_markers(&events);
        assert_eq!(markers.len(), pages.len(), "seed {seed}: {markers:?}");
        // The first part takes the markers up to `last`, and the replay waits for the next one.
        let bound = markers[..last]
            .iter()
            .map(|(recorded, _)| recorded)
            .sum::<u64>() as usize;
        let position = match last {
            1 => "after the first marker",
            _ if last == markers.len() - 1 => "before the last marker",
            _ => "after a middle marker",
        };
        *positions.entry(position).or_insert(0) += 1;
        for kind in peeked_in(&events, markers[last - 1].1) {
            *peeked.entry(kind).or_insert(0) += 1;
        }
        // The replay waits once it applied the task whose completion is two before the one of
        // the first unread marker. With chunking, a completion with no command joins two tasks.
        let completions: Vec<i64> = (events.iter())
            .filter(|event| event.event_type() == EventType::WorkflowTaskCompleted)
            .map(|event| event.event_id)
            .collect();
        let unread = completions
            .iter()
            .position(|id| *id == markers[last].1)
            .unwrap();
        let no_command = unread >= 2
            && (events.iter())
                .skip_while(|event| event.event_id <= completions[unread - 2])
                .take_while(|event| event.event_type() != EventType::WorkflowTaskScheduled)
                .all(|event| !event.is_command_event());
        if chunking && no_command {
            chunked_after_no_command += 1;
        }
        let path = match rng.random_bool(0.5) {
            true => Path::FullHistory,
            false => Path::CacheMiss,
        };
        let poll = poll_for_last_task(&t, whole, path);
        let mut replays = vec![];
        for bound in [usize::MAX, bound] {
            let label = format!("seed {seed}, bound {bound}");
            let workflow = reads_with_peeked_results(plan.clone(), page_records);
            replays.push(replay_with_bound(&store, &t, &poll, bound, label, workflow).await);
        }
        let [in_one_part, in_parts] = [&replays[0], &replays[1]];
        assert_eq!(in_parts, in_one_part, "seed {seed}");
        let fired = vec![Job::FireTimer(FireTimer { seq: 1 })];
        let first_run = [live, vec![fired]].concat();
        if chunking {
            let sorted = |jobs: &[Vec<String>]| {
                let mut jobs = jobs.concat();
                jobs.sort();
                jobs
            };
            let of_first_run = sorted(&job_names(&first_run));
            assert_eq!(sorted(&in_parts.jobs), of_first_run, "seed {seed}");
            assert_eq!(in_parts.reads, read_jobs(&first_run), "seed {seed}");
        } else {
            assert_eq!(in_parts.jobs, job_names(&first_run), "seed {seed}");
        }
    }
    // The seeds end the first part at each position, with each kind of peeked result in the
    // completion before the wait, and some chunked replays wait after a task with no command.
    assert_eq!(positions.len(), 3, "{positions:?}");
    assert_eq!(peeked.len(), 3, "{peeked:?}");
    assert!(chunked_after_no_command > 0);
}

// The server gives a long History in pages. The poll has the first page and Core fetches the
// others, or Core fetches all of them from the start when the run isn't in the cache. A marker in
// a later page has only its count and its digest too, so the replay must read its Records.
#[tokio::test]
async fn a_replay_reads_the_records_of_a_marker_in_a_later_page_of_history() {
    let store = store(&["s"]).await;
    fill(&store, "s", 6).await;
    // Each task reads one Record, so each marker has the count and the digest of one page.
    let (live, t, whole) = first_run_with_a_timer_at_each_page(&store, 1).await;
    let first_run = [live, vec![vec![Job::FireTimer(FireTimer { seq: 1 })]]].concat();
    let events = whole.history.clone().unwrap().events;
    let markers = recorded_markers(&events).len();
    // A page ends before the completion of task 2 or task 4. The server writes a completion in a
    // batch after the one of the task's started event, and a page holds whole batches.
    let completions: Vec<usize> = (events.iter().enumerate())
        .filter(|(_, event)| event.event_type() == EventType::WorkflowTaskCompleted)
        .map(|(at, _)| at)
        .collect();
    let (at, later) = (completions[1], completions[3]);
    assert!(recorded_markers(&events[later..]).len() >= 2);
    let continued = |events: &[HistoryEvent]| GetWorkflowExecutionHistoryResponse {
        next_page_token: vec![1],
        ..page(events)
    };
    let mut polled = whole.clone();
    polled.history = Some(History {
        events: events[..at].to_vec(),
    });
    polled.next_page_token = vec![1];
    let (sticky, one_page) = poll_for_last_task(&t, whole, Path::CacheMiss);
    let fetched_from_the_start = vec![continued(&events[..later]), page(&events[later..])];
    let fetched_after_the_poll = vec![continued(&events[at..later]), page(&events[later..])];
    let layouts = [
        ("one page", sticky.clone(), one_page),
        ("two pages", sticky, fetched_from_the_start),
        ("three pages", polled, fetched_after_the_poll),
    ];
    let workflow = read_with_a_timer_at_each_page(1);
    for (label, task, pages) in layouts {
        let before = store.reads().len();
        let poll = (task, pages);
        let replayed =
            replay_with_bound(&store, &t, &poll, usize::MAX, label.to_string(), workflow).await;
        assert_eq!(replayed.jobs, job_names(&first_run), "{label}");
        assert_eq!(replayed.reads, read_jobs(&first_run), "{label}");
        // The replay read the Records of each marker from the Store once.
        assert_eq!(store.reads().len() - before, markers, "{label}");
    }
}

/// What the task after the first page has.
#[derive(Clone, Copy, Debug)]
enum NoJob {
    /// A Signal, at which Workflow code upserts a search attribute. History keeps chunking on.
    Upsert,
    /// No event, with chunking off.
    EmptyUnchunked,
}

/// Returns Workflow code that reads Stream `s` one Record at a time. At the first page it appends
/// to Stream `a`, and it reads on once the append has its result. It upserts a search attribute at
/// a Signal, starts timer 1 once a read gives no Record, and completes once timer 1 fires.
fn reads_and_upserts() -> impl FnMut(&WorkflowActivation) -> Vec<Command> {
    let one = |read: ReadStream| -> Command {
        ReadStream {
            max_records: 1,
            ..read
        }
        .into()
    };
    let mut after_first = None;
    move |activation| {
        let mut commands = vec![];
        for job in &activation.jobs {
            match &job.variant {
                Some(Job::InitializeWorkflow(_)) => {
                    commands.push(one(read(1, "s", &[], Duration::ZERO)));
                }
                Some(Job::SignalWorkflow(_)) => {
                    let fields = HashMap::from([("k".to_string(), Payload::default())]);
                    let search_attributes = Some(SearchAttributes {
                        indexed_fields: fields,
                    });
                    commands.push(UpsertWorkflowSearchAttributes { search_attributes }.into());
                }
                Some(Job::ResolveStreamRead(result)) => match next_read(result, Duration::ZERO) {
                    Some(next) if result.seq == 1 => {
                        after_first = Some(next);
                        commands.push(append(1, "a"));
                    }
                    Some(next) => commands.push(one(next)),
                    None => commands.push(timer(1)),
                },
                Some(Job::ResolveStreamAppend(_)) => commands.extend(after_first.take().map(one)),
                Some(Job::FireTimer(_)) => commands.push(complete()),
                _ => {}
            }
        }
        commands
    }
}

// A sequence that gives Workflow code no job leaves no completion to wait for the next part of
// Records, so the completion before it waits in its place. Here the append of the first page has
// no answer until task 4, so tasks 2 and 3 complete at their heartbeat, and task 3 has no job.
#[rstest::rstest]
#[case::upsert(NoJob::Upsert)]
#[case::empty(NoJob::EmptyUnchunked)]
#[tokio::test]
async fn a_replay_goes_on_after_a_task_with_no_job_before_the_next_part(#[case] no_job: NoJob) {
    let store = store(&["s", "a"]).await;
    fill(&store, "s", 2).await;
    store.hold(Op::Append);
    let mut opts = StartOpts::new(&store);
    let held = store.clone();
    opts.served = Some(Box::new(move |number, t| {
        if number == 1 && matches!(no_job, NoJob::Upsert) {
            t.add_we_signaled("go", vec![]);
        }
        if number == 3 {
            held.release();
        }
        Some(next_task(t))
    }));
    let run = start(history(Duration::from_secs(1)), opts);
    let mut workflow = reads_and_upserts();
    loop {
        let activation = run.worker.poll_workflow_activation().await.unwrap();
        let ended = ends(&activation);
        let commands = workflow(&activation);
        let completion = WorkflowActivationCompletion::from_cmds(activation.run_id, commands);
        run.worker
            .complete_workflow_activation(completion)
            .await
            .unwrap();
        if ended {
            break;
        }
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
    let sent = run.sent.lock().clone();
    // Each heartbeat leaves the append open, so its completion has an append marker with no
    // entry, unless the upsert keeps a replay from joining it to the next task.
    let no_job_commands = match no_job {
        NoJob::Upsert => vec![CommandType::UpsertWorkflowSearchAttributes],
        NoJob::EmptyUnchunked => vec![CommandType::RecordMarker],
    };
    assert_eq!(command_types(&sent[1]), no_job_commands);
    assert_eq!(command_types(&sent[2]), [CommandType::RecordMarker]);
    let mut t = run.history.lock().clone();
    set_chunking(&mut t, matches!(no_job, NoJob::Upsert));
    fire_last_timer(&mut t);
    let whole = next_task(&mut t);
    // The first marker is a part, and the next one comes after the two tasks with no page.
    let markers = recorded_markers(&whole.history.as_ref().unwrap().events);
    assert_eq!(markers.len(), 2, "{markers:?}");
    let poll = (whole, vec![]);
    let mut replays = vec![];
    for bound in [usize::MAX, markers[0].0 as usize] {
        let label = format!("{no_job:?} bound {bound}");
        replays.push(replay_with_bound(&store, &t, &poll, bound, label, reads_and_upserts()).await);
    }
    assert_eq!(replays[0], replays[1]);
}

#[derive(Clone, Copy, Debug)]
enum Nondeterminism {
    FoundByCore,
    ReportedByLang,
    /// Lang reports it in a Workflow with two timers and no Stream call.
    ReportedByLangWithNoStreamCall,
}

// The completion that fails the Workflow applies no next task while Records are unread, so it
// waits for none. With no Records to wait for, it applies the next task, and Core finds the error.
#[rstest::rstest]
#[case::found_by_core(Nondeterminism::FoundByCore)]
#[case::reported_by_lang(Nondeterminism::ReportedByLang)]
#[case::reported_by_lang_with_no_stream_call(Nondeterminism::ReportedByLangWithNoStreamCall)]
#[tokio::test]
async fn a_replay_fails_the_workflow_with_the_next_part_unread(
    #[case] nondeterminism: Nondeterminism,
) {
    let store = store(&["s"]).await;
    fill(&store, "s", 4).await;
    let (t, whole) = match nondeterminism {
        Nondeterminism::ReportedByLangWithNoStreamCall => {
            let mut t = history(Duration::from_secs(60));
            t.add_workflow_task_completed();
            let started = t.add_timer_started("1".to_string());
            t.add_timer_fired(started, "1".to_string());
            let whole = next_task(&mut t);
            (t, whole)
        }
        _ => {
            let (_, t, whole) = first_run_with_a_timer_at_each_page(&store, 1).await;
            (t, whole)
        }
    };
    let mut runs = vec![];
    // With parts of one marker, the first completion of the replay waits for the second part.
    for bound in [usize::MAX, 1] {
        let mut opts = StartOpts::new(&store);
        opts.responses = vec![ResponseType::Raw(whole.clone())];
        let label = format!("{nondeterminism:?} bound {bound}");
        opts.label = label.clone();
        opts.configure = Box::new(move |wc| {
            fails_workflow_for_nondeterminism(wc);
            wc.stream_runtimes.settings.max_task_record_bytes = bound;
        });
        let reads = store.reads().len();
        let replay = start(t.clone(), opts);
        let mut jobs = vec![];
        let drive = async {
            while let Ok(activation) = replay.worker.poll_workflow_activation().await {
                let run_id = activation.run_id.clone();
                if evicted(&replay.worker, &activation).await {
                    break;
                }
                jobs.push(jobs_of(&activation));
                let first = matches!(activation.jobs[0].variant, Some(Job::InitializeWorkflow(_)));
                let commands = read_with_a_timer_at_each_page(1)(&activation);
                let completion = match nondeterminism {
                    Nondeterminism::FoundByCore if first => {
                        WorkflowActivationCompletion::empty(run_id)
                    }
                    Nondeterminism::FoundByCore => {
                        WorkflowActivationCompletion::from_cmds(run_id, commands)
                    }
                    _ if first => {
                        let cause = Some(WorkflowTaskFailedCause::NonDeterministicError);
                        WorkflowActivationCompletion::fail(run_id, Default::default(), cause)
                    }
                    _ => WorkflowActivationCompletion::from_cmds(run_id, commands),
                };
                replay
                    .worker
                    .complete_workflow_activation(completion)
                    .await
                    .unwrap();
            }
        };
        tokio::time::timeout(Duration::from_secs(30), drive)
            .await
            .unwrap_or_else(|_| panic!("{label}: the replay hung"));
        replay.worker.drain_pollers_and_shutdown_ref().await;
        let sent: Vec<Vec<CommandType>> = replay.sent().iter().map(command_types).collect();
        runs.push((job_names(&jobs), sent, store.reads().len() - reads));
        let message = (replay.sent().iter().flat_map(|sent| &sent.commands))
            .find_map(|command| match command.attributes.as_ref()? {
                Attributes::FailWorkflowExecutionCommandAttributes(fail) => {
                    Some(fail.failure.as_ref()?.message.clone())
                }
                _ => None,
            })
            .unwrap_or_else(|| panic!("{label}: the Workflow didn't fail"));
        // After lang's failure, Core applies the next task unless Records are unread. It then
        // finds the read marker or the timer that has no command, and fails the Workflow with
        // that error in place of lang's, which has no message.
        let applied_next_task = !message.is_empty();
        let applies_next_task = match nondeterminism {
            Nondeterminism::FoundByCore => true,
            Nondeterminism::ReportedByLang => bound == usize::MAX,
            Nondeterminism::ReportedByLangWithNoStreamCall => true,
        };
        assert_eq!(applied_next_task, applies_next_task, "{label}: {message}");
    }
    let fails_the_workflow = vec![vec![CommandType::FailWorkflowExecution]];
    assert_eq!(runs[0].1, fails_the_workflow);
    assert_eq!(runs[1].0, runs[0].0);
    assert_eq!(runs[1].1, fails_the_workflow);
    // Core finds the error when it applies the next task, for which it read the second part.
    let reads = match nondeterminism {
        Nondeterminism::FoundByCore => [4, 2],
        Nondeterminism::ReportedByLang => [4, 1],
        Nondeterminism::ReportedByLangWithNoStreamCall => [0, 0],
    };
    assert_eq!([runs[0].2, runs[1].2], reads);
}

/// A task that is on its way to a run, with the places of its first reads among the requests of
/// the Store.
struct InFlight {
    reads: std::ops::Range<usize>,
    /// The activation of the test that is out when the task comes back, or `None` if it comes
    /// back once each task before it has its result.
    at: Option<usize>,
}

// The tasks of a run that isn't in the cache each read the first part of its Records before a run
// exists. The first one that comes back makes the run, and each other one meets that run, while
// it replays or later. Each of them still gets its result, with the jobs of the first run.
#[tokio::test]
async fn two_tasks_of_a_run_that_is_not_cached_each_get_their_result() {
    let (mut during_a_replay, mut ahead_during_a_replay, mut read_again) = (0, 0, 0);
    for seed in 0..200 {
        let _seed = SeedOnPanic(seed);
        let mut rng = StdRng::seed_from_u64(seed);
        let store = store(&["s"]).await;
        let page_records = rng.random_range(1..=3);
        fill(&store, "s", page_records * rng.random_range(2..=6)).await;
        let (live, mut t, _) = first_run_with_a_timer_at_each_page(&store, page_records).await;
        // Another Worker took the task of timer 1, and started timer 2. The server gives out a
        // query before a timer fires, when no Workflow Task is pending.
        let before_timer_1 = t.current_event_id() - 3;
        t.add_workflow_task_completed();
        let started = t.add_timer_started("2".to_string());
        let before_timer_2 = t.current_event_id();
        t.add_timer_fired(started, "2".to_string());
        let last = next_task(&mut t);
        let markers = recorded_markers(&last.history.as_ref().unwrap().events);
        // A part has the Records of one marker, or of some, or of all.
        let first_part = rng.random_range(1..=markers.len());
        let bound = (markers[..first_part].iter())
            .map(|(recorded, _)| recorded)
            .sum::<u64>() as usize;
        // The last Workflow Task and one or two queries, or two or three queries. One query can be
        // from before timer 1, so the run that it makes hasn't seen the task of that timer. Each
        // task comes with the number of activations of its replay.
        let has_workflow_task = rng.random_bool(0.6);
        let queries = rng.random_range(2..=3) - usize::from(has_workflow_task);
        let has_early_query = rng.random_bool(0.3);
        let mut tasks: Vec<(PollWorkflowTaskQueueResponse, usize)> = (0..queries)
            .map(|query| match has_early_query && query == 0 {
                true => (query_before(&last, before_timer_1), live.len() + 1),
                false => (query_before(&last, before_timer_2), live.len() + 2),
            })
            .collect();
        if has_workflow_task {
            tasks.push((last.clone(), live.len() + 2));
        }
        // The seed picks the order in which the first reads of the tasks end, and which
        // activation of the first task is out when each other task comes back.
        for last in (1..tasks.len()).rev() {
            tasks.swap(last, rng.random_range(0..=last));
        }
        let activations_of_first = tasks[0].1;
        let mut at: Vec<Option<usize>> = (1..tasks.len())
            .map(|_| (rng.random_bool(0.6)).then(|| rng.random_range(0..activations_of_first)))
            .collect();
        at.sort_by_key(|at| at.unwrap_or(usize::MAX));
        // A task that comes after a task that the run doesn't have evicts the run, which still
        // answers the query that it replays for.
        let seen_by_first = tasks[0].0.previous_started_event_id;
        let is_ahead = |(task, _): &(PollWorkflowTaskQueueResponse, usize)| {
            task.previous_started_event_id > seen_by_first
        };
        let replays_at = |at: &Option<usize>| at.is_some_and(|at| at + 1 < activations_of_first);
        ahead_during_a_replay += (tasks[1..].iter().zip(&at))
            .filter(|(task, at)| is_ahead(task) && replays_at(at))
            .count();
        during_a_replay += at.iter().flatten().count();

        store.gate_reads(true);
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut opts = StartOpts::new(&store);
        opts.tasks = Some(receiver);
        opts.served = None;
        // A Worker has a task slot for each run that its cache can hold, and three tasks come.
        opts.cache = 3;
        opts.queries = queries;
        opts.label = format!("seed {seed}");
        opts.configure = Box::new(move |wc| {
            wc.stream_runtimes.settings.max_task_record_bytes = bound;
            // Each first part reads its markers at the same time.
            wc.stream_runtimes.settings.replay_concurrency = 64;
            // A read that waits at the gate must not count as a try with no answer.
            wc.stream_runtimes.settings.try_deadline = Duration::from_secs(600);
        });
        let run = start(t.clone(), opts);
        // Core takes its tasks once lang polls for an activation.
        let mut first = Box::pin(run.worker.poll_workflow_activation());
        assert!(futures_util::poll!(&mut first).is_pending());
        let mut first = Some(first);
        let mut in_flight = VecDeque::new();
        let (count, before) = (tasks.len(), store.reads().len());
        let at = iter::once(None).chain(at);
        for ((task, _), at) in tasks.into_iter().zip(at) {
            let from = store.requests().len();
            let reads = from..from + first_part;
            sender.send(task).unwrap();
            wait_until(|| store.requests().len() == reads.end).await;
            in_flight.push_back(InFlight { reads, at });
        }
        store.gate_reads(false);

        // The jobs of each activation of each run, and the tasks that came back.
        let mut runs = vec![vec![]];
        let (mut back, mut activations) = (0, 0);
        let results = |run: &Run| run.answers.lock().len() + run.sent().len();
        loop {
            if results(&run) == back {
                let Some(next) = in_flight.pop_front() else {
                    break;
                };
                store.let_go(next.reads);
                back += 1;
            }
            let poll = async {
                match first.take() {
                    Some(first) => first.await,
                    None => run.worker.poll_workflow_activation().await,
                }
            };
            let activation = (tokio::time::timeout(Duration::from_secs(30), poll).await)
                .unwrap_or_else(|_| panic!("seed {seed}: a task got no activation"))
                .unwrap();
            if evicted(&run.worker, &activation).await {
                runs.push(vec![]);
                continue;
            }
            // A task that comes back now meets the run before this activation completes.
            let is_due = |next: &InFlight| next.at.is_some_and(|at| at <= activations);
            while in_flight.front().is_some_and(is_due) {
                let next = in_flight.pop_front().unwrap();
                let passed = store.reads_let_go() + next.reads.len();
                store.let_go(next.reads);
                wait_until(|| store.reads_let_go() == passed).await;
                back += 1;
            }
            activations += 1;
            runs.last_mut().unwrap().push(jobs_of(&activation));
            respond(&run.worker, &activation, |activation| {
                match &activation.jobs[0].variant {
                    Some(Job::FireTimer(fired)) if fired.seq == 1 => vec![timer(2)],
                    _ => read_with_a_timer_at_each_page(page_records)(activation),
                }
            })
            .await;
        }
        // Each query got an answer, and no task failed.
        let answers = run.answers.lock().clone();
        assert_eq!(answers.len(), queries, "seed {seed}");
        let failed = answers.iter().any(|(_, failure)| failure.is_some());
        assert!(!failed, "seed {seed}: {answers:?}");
        assert_eq!(run.failed.lock().clone(), [], "seed {seed}");
        // The Workflow Task completed once, and each run gave the jobs of the first run.
        let completes = vec![CommandType::CompleteWorkflowExecution];
        let sent: Vec<Vec<CommandType>> = run.sent().iter().map(command_types).collect();
        assert_eq!(
            sent,
            vec![completes; usize::from(has_workflow_task)],
            "seed {seed}"
        );
        let fired = |seq| vec![Job::FireTimer(FireTimer { seq })];
        let first_run = [live, vec![fired(1), fired(2)]].concat();
        let is_query = |jobs: &&Vec<Job>| matches!(jobs[..], [Job::QueryWorkflow(_)]);
        for jobs in &runs {
            let replayed: Vec<&Vec<Job>> = jobs.iter().filter(|jobs| !is_query(jobs)).collect();
            let of_first_run: Vec<&Vec<Job>> = first_run.iter().take(replayed.len()).collect();
            assert_eq!(replayed, of_first_run, "seed {seed}");
        }
        // A task that met a run dropped the Records that it read. So if that run left the cache
        // before it took the task, the task read its first part again to make the next run.
        let reads = store.reads();
        let first_parts = reads[before..]
            .iter()
            .filter(|read| read.from_offset.is_empty());
        read_again += first_parts.count() - count;
        // No thread of Core panicked, so the Worker still serves another run.
        let other = history(Duration::from_secs(60));
        sender.send(task_of(&other)).unwrap();
        loop {
            let activation = run.worker.poll_workflow_activation().await.unwrap();
            let run_id = activation.run_id.clone();
            let init = matches!(activation.jobs[0].variant, Some(Job::InitializeWorkflow(_)));
            let completion = WorkflowActivationCompletion::empty(run_id);
            let _ = run.worker.complete_workflow_activation(completion).await;
            if init {
                break;
            }
        }
        run.worker.drain_pollers_and_shutdown_ref().await;
    }
    // Some tasks met a run that still replayed, some of them evicted it, and some made the next
    // run.
    assert!(
        during_a_replay > 0 && ahead_during_a_replay > 0 && read_again > 0,
        "{during_a_replay} {ahead_during_a_replay} {read_again}"
    );
}

/// The read of a replay that gets no answer.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Unanswered {
    /// The first part, which a task reads before its run exists.
    TheFirstPart,
    /// A later part, which a completion of the run waits for.
    ALaterPart,
}

// A replay reads its Records outside the stage that every poll passes, which takes one poll or
// fetch at a time here. So a Store that gives no answer holds up no Workflow that doesn't read it.
#[rstest::rstest]
#[case(Unanswered::TheFirstPart)]
#[case(Unanswered::ALaterPart)]
#[tokio::test]
async fn a_poll_passes_while_a_replay_waits_for_a_store(#[case] unanswered: Unanswered) {
    let store = store(&["s"]).await;
    fill(&store, "s", 3).await;
    let (_, t, whole) = first_run_with_a_timer_at_each_page(&store, 1).await;
    let reads = store.reads().len();
    let (tasks, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut opts = StartOpts::new(&store);
    opts.tasks = Some(receiver);
    opts.served = None;
    opts.cache = 2;
    opts.configure = Box::new(|wc| {
        wc.stream_runtimes.settings.max_task_record_bytes = 1;
        wc.stream_runtimes.settings.try_deadline = Duration::from_secs(600);
    });
    let run = start(t, opts);
    let workflow = read_with_a_timer_at_each_page(1);
    tasks.send(whole).unwrap();
    let mut next = Box::pin(run.worker.poll_workflow_activation());
    let mut waiting = None;
    if unanswered == Unanswered::TheFirstPart {
        store.hold(Op::Read);
        assert!(futures_util::poll!(&mut next).is_pending());
    } else {
        // The completion of the first activation waits for the second part.
        let init = next.await.unwrap();
        store.hold(Op::Read);
        let commands = workflow(&init);
        let completion = WorkflowActivationCompletion::from_cmds(init.run_id, commands);
        let mut completion = Box::pin(run.worker.complete_workflow_activation(completion));
        assert!(futures_util::poll!(&mut completion).is_pending());
        waiting = Some(completion);
        next = Box::pin(run.worker.poll_workflow_activation());
    }
    wait_until(|| store.reads().len() > reads).await;
    // A Workflow with no Stream call still gets its task.
    let other = history(Duration::from_secs(60));
    tasks.send(task_of(&other)).unwrap();
    let activation = tokio::time::timeout(Duration::from_secs(30), next)
        .await
        .expect("The poll waited for the Store")
        .unwrap();
    assert_eq!(activation.run_id, other.get_orig_run_id());
    let completion = WorkflowActivationCompletion::from_cmds(activation.run_id, vec![complete()]);
    (run.worker.complete_workflow_activation(completion).await).unwrap();
    // Once the Store answers, the replay goes on to its end.
    store.release();
    if let Some(completion) = waiting {
        completion.await.unwrap();
    }
    while run.sent().len() < 2 {
        let activation = run.worker.poll_workflow_activation().await.unwrap();
        respond(&run.worker, &activation, workflow).await;
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
}

// The completion waits for the Records of its own task. The failed History fetch of another task
// of the run isn't a failed fetch of that completion, so its task goes on.
#[tokio::test]
async fn a_failed_fetch_of_another_task_fails_no_completion_that_waits_for_records() {
    let store = store(&["s"]).await;
    fill(&store, "s", 3).await;
    let (_, t, whole) = first_run_with_a_timer_at_each_page(&store, 1).await;
    let reads = store.reads().len();
    let (tasks, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut opts = StartOpts::new(&store);
    opts.tasks = Some(receiver);
    opts.served = None;
    opts.failed_fetches = 1;
    opts.fails = 1;
    opts.configure = Box::new(|wc| {
        wc.stream_runtimes.settings.max_task_record_bytes = 1;
        wc.stream_runtimes.settings.try_deadline = Duration::from_secs(600);
    });
    let mut run = start(t, opts);
    let workflow = read_with_a_timer_at_each_page(1);
    tasks.send(whole.clone()).unwrap();
    // The completion of the first activation waits for the second part.
    let init = run.worker.poll_workflow_activation().await.unwrap();
    store.hold(Op::Read);
    let commands = workflow(&init);
    let completion = WorkflowActivationCompletion::from_cmds(init.run_id, commands);
    let mut completion = Box::pin(run.worker.complete_workflow_activation(completion));
    assert!(futures_util::poll!(&mut completion).is_pending());
    wait_until(|| store.reads().len() > reads).await;
    // The server gives out a query of the run, as it does once the Workflow was terminated. The
    // query has the first page of its History, and the fetch of the next page fails.
    let mut query = query_before(&whole, i64::MAX);
    query.history.as_mut().unwrap().events.truncate(2);
    query.next_page_token = vec![1];
    tasks.send(query).unwrap();
    drop(tasks);
    run.all_taken.take().unwrap().await.unwrap();
    store.release();
    completion.await.unwrap();
    // The replay goes on to the end of the task, which completes. Then the run leaves the cache,
    // and Core fails the task of the query.
    run.drive(workflow).await;
    assert_eq!(run.sent().len(), 1);
    let failed = run.failed.lock().clone();
    let [(cause, message)] = &failed[..] else {
        panic!("{failed:?}");
    };
    assert_eq!(
        *cause,
        WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
    );
    assert!(message.starts_with("Fetching history failed"), "{message}");
    run.worker.drain_pollers_and_shutdown_ref().await;
}

// A replay reads its Records on the Worker's runtime, as a live call does. So a Store whose read
// keeps its thread holds up no completion of another Workflow.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_store_that_keeps_its_thread_in_a_replay_holds_up_no_other_workflow() {
    let store = store(&["s"]).await;
    fill(&store, "s", 1).await;
    let (_, t, whole) = first_run_with_a_timer_at_each_page(&store, 1).await;
    let reads = store.reads().len();
    let (tasks, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut opts = StartOpts::new(&store);
    opts.tasks = Some(receiver);
    opts.served = None;
    opts.cache = 2;
    let run = start(t, opts);
    // The other Workflow has an activation out when the replay starts to read.
    let other = history(Duration::from_secs(60));
    tasks.send(task_of(&other)).unwrap();
    let activation = run.worker.poll_workflow_activation().await.unwrap();
    store.block_reads();
    tasks.send(whole).unwrap();
    wait_until(|| store.reads().len() > reads).await;
    let completion = WorkflowActivationCompletion::from_cmds(activation.run_id, vec![complete()]);
    let completed = run.worker.complete_workflow_activation(completion);
    let completed = tokio::time::timeout(Duration::from_secs(30), completed).await;
    store.unblock();
    (completed.expect("The completion waited for the Store")).unwrap();
    while run.sent().len() < 2 {
        let activation = run.worker.poll_workflow_activation().await.unwrap();
        respond(&run.worker, &activation, read_with_a_timer_at_each_page(1)).await;
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
}

/// Returns Workflow code that reads two pages of one Record of Stream `s` in its first task,
/// starts timer 1 at the second page, and completes once timer 1 fires.
fn two_pages_then_a_timer(activation: &WorkflowActivation) -> Vec<Command> {
    let one = |read: ReadStream| -> Command {
        ReadStream {
            max_records: 1,
            ..read
        }
        .into()
    };
    if let Some(Job::InitializeWorkflow(_)) = &activation.jobs[0].variant {
        return vec![one(read(1, "s", &[], Duration::ZERO))];
    }
    if fires_a_last_timer(activation) {
        return vec![complete()];
    }
    (results(activation))
        .filter_map(|result| {
            let page = page_of(result)?;
            Some(match result.seq {
                1 => one(read(2, "s", &page.last_offset, Duration::ZERO)),
                _ => timer(1),
            })
        })
        .collect()
}

// The first task of a replay peeks each read marker of the completion of the task before, so a
// part of the pre-read must hold the Records of a whole task. Here the Record bound of a task is
// one Record, so the second page waits for the next task, and its marker for the next completion.
// The replay reads in parts of the same bound.
#[tokio::test]
async fn a_replay_reads_each_marker_that_the_first_look_ahead_peeks() {
    let store = store(&["s"]).await;
    fill(&store, "s", 3).await;
    let bound = (store.records("s").iter())
        .map(|record| field_len(prost::Message::encoded_len(record)))
        .max()
        .unwrap();
    let mut opts = StartOpts::new(&store);
    opts.configure = Box::new(move |wc| wc.stream_runtimes.settings.max_task_record_bytes = bound);
    let first = start(history(Duration::from_secs(60)), opts);
    let mut live = vec![];
    for _ in 0..3 {
        let activation = first.worker.poll_workflow_activation().await.unwrap();
        live.push(jobs_of(&activation));
        respond(&first.worker, &activation, two_pages_then_a_timer).await;
    }
    first.worker.drain_pollers_and_shutdown_ref().await;
    let mut t = first.history.lock().clone();
    fire_last_timer(&mut t);
    let whole = next_task(&mut t);
    let markers = recorded_markers(&whole.history.as_ref().unwrap().events);
    assert_eq!(markers.len(), 2, "{markers:?}");
    assert_ne!(markers[0].1, markers[1].1, "{markers:?}");
    let label = format!(
        "read markers {markers:?} (recorded bytes, completion), tasks and parts of {bound} bytes"
    );
    let replayed = replay_with_bound(
        &store,
        &t,
        &(whole, vec![]),
        bound,
        label.clone(),
        two_pages_then_a_timer,
    )
    .await;
    let first_run = [live, vec![vec![Job::FireTimer(FireTimer { seq: 1 })]]].concat();
    assert_eq!(replayed.jobs, job_names(&first_run), "{label}");
}

/// Drives `run` with `workflow` until the run leaves the cache or `limit` passes. Returns true if
/// the run left the cache, and the sequence of the run for a failure message: the jobs of each
/// activation, each completion, the events of History and the reads that the Store got.
async fn drive_for(
    run: &Run,
    limit: Duration,
    mut workflow: impl FnMut(&WorkflowActivation) -> Vec<Command>,
) -> (bool, String) {
    let seen = std::cell::RefCell::new(vec![]);
    let watched = |activation: &WorkflowActivation| {
        let jobs: Vec<String> = (activation.jobs.iter())
            .filter_map(|job| job.variant.as_ref().map(summary))
            .collect();
        seen.borrow_mut().push(jobs);
        workflow(activation)
    };
    let left = tokio::time::timeout(limit, run.drive(watched))
        .await
        .is_ok();
    let completions: Vec<String> = (run.sent().iter())
        .map(|sent| {
            let forced = sent.force_create_new_workflow_task;
            format!("{:?} forced {forced}", command_types(sent))
        })
        .collect();
    let events: Vec<EventType> = (run.history.lock().get_full_history_info().unwrap())
        .into_events()
        .iter()
        .map(HistoryEvent::event_type)
        .collect();
    let failed = run.failed.lock().clone();
    let sequence = format!(
        "activations {:?}, completions {completions:?}, failed tasks {failed:?}, History \
         {events:?}, {} reads of the Store",
        seen.borrow(),
        run.store.reads().len()
    );
    (left, sequence)
}

/// Returns the number of Records in the pages of read results of `activation`.
fn records_in(activation: &WorkflowActivation) -> Option<usize> {
    results(activation)
        .filter_map(page_of)
        .map(|page| page.records.len())
        .reduce(|sum, count| sum + count)
}

// A read in the same completion as a command doesn't hold the task. Here no later event comes for
// the Workflow, so the Records of the Stream must still reach the read.
#[tokio::test]
async fn a_read_with_an_upsert_in_the_first_task_gets_its_page() {
    let store = store(&["s"]).await;
    fill(&store, "s", 3).await;
    let run = start(history(Duration::from_secs(60)), StartOpts::new(&store));
    let workflow = |activation: &WorkflowActivation| -> Vec<Command> {
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => {
                let fields = HashMap::from([("k".to_string(), Payload::default())]);
                let search_attributes = Some(SearchAttributes {
                    indexed_fields: fields,
                });
                vec![
                    UpsertWorkflowSearchAttributes { search_attributes }.into(),
                    read(1, "s", &[], Duration::ZERO).into(),
                ]
            }
            _ if records_in(activation).is_some() => vec![complete()],
            _ => vec![],
        }
    };
    let (left, sequence) = drive_for(&run, Duration::from_secs(3), workflow).await;
    run.worker.drain_pollers_and_shutdown_ref().await;
    assert!(left, "The read got no page: {sequence}");
}

// Workflow code waits for a Signal with a timeout. At the Signal it cancels the timer and reads in
// the same activation, so the completion has a command and the read doesn't hold the task. No
// later event comes, so the Records of the Stream must still reach the read.
#[tokio::test]
async fn a_read_with_a_timer_cancel_at_a_signal_gets_its_page() {
    let store = store(&["s"]).await;
    fill(&store, "s", 3).await;
    let mut opts = StartOpts::new(&store);
    opts.eager = Some(Box::new(|completion, t| {
        if completion.force_create_new_workflow_task {
            return Some(next_task(t));
        }
        let starts_the_timer = command_types(completion).contains(&CommandType::StartTimer);
        starts_the_timer.then(|| {
            t.add_we_signaled("go", vec![]);
            next_task(t)
        })
    }));
    let run = start(history(Duration::from_secs(60)), opts);
    let workflow = |activation: &WorkflowActivation| -> Vec<Command> {
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
            Some(Job::SignalWorkflow(_)) => vec![
                workflow_commands::CancelTimer { seq: 1 }.into(),
                read(1, "s", &[], Duration::ZERO).into(),
            ],
            _ if records_in(activation).is_some() => vec![complete()],
            _ => vec![],
        }
    };
    let (left, sequence) = drive_for(&run, Duration::from_secs(3), workflow).await;
    run.worker.drain_pollers_and_shutdown_ref().await;
    assert!(left, "The read got no page: {sequence}");
}

// An Update handler reads a Stream. The acceptance is a message for the server, so the read
// doesn't hold the task. No later event comes, so the handler must still get its page, and the
// Update must complete.
#[tokio::test]
async fn a_read_of_an_update_handler_gets_its_page() {
    let store = store(&["s"]).await;
    fill(&store, "s", 3).await;
    let mut t = history(Duration::from_secs(60));
    t.add_workflow_task_completed();
    t.add_timer_started("1".to_string());
    let mut update = next_task(&mut t);
    update.add_update_request("u", 6);
    let mut opts = StartOpts::new(&store);
    opts.responses = vec![ResponseType::Raw(update)];
    let run = start(t, opts);
    let mut instance = None;
    let workflow = |activation: &WorkflowActivation| -> Vec<Command> {
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
            Some(Job::DoUpdate(update)) => {
                instance = Some(update.protocol_instance_id.clone());
                let accepted = update_response::Response::Accepted(());
                vec![
                    update_response(activation, accepted),
                    read(1, "s", &[], Duration::ZERO).into(),
                ]
            }
            _ if records_in(activation).is_some() => {
                let completed = UpdateResponse {
                    protocol_instance_id: instance.clone().unwrap_or_default(),
                    response: Some(update_response::Response::Completed(Payload::default())),
                };
                vec![completed.into(), complete()]
            }
            _ => vec![],
        }
    };
    let (left, sequence) = drive_for(&run, Duration::from_secs(3), workflow).await;
    run.worker.drain_pollers_and_shutdown_ref().await;
    assert!(left, "The Update handler got no page: {sequence}");
}

// Workflow code drains a Stream. Its last page comes before the read limit of the task, and lang
// answers after the limit with the next read, which is a check. With no wait the check's empty
// page ends the drain in the same task. With a wait an empty page would make Workflow code read
// again at once, so the check parks the read until the next task, and the completion asks for no
// new task.
#[rstest::rstest]
#[case::with_no_wait(Duration::ZERO)]
#[case::with_a_wait(Duration::from_secs(60))]
#[tokio::test]
async fn a_drain_that_crosses_the_read_limit_ends_with_no_wait(#[case] wait: Duration) {
    let store = store(&["s"]).await;
    fill(&store, "s", 2).await;
    let run = start(history(Duration::from_secs(60)), StartOpts::new(&store));
    let workflow = move |activation: &WorkflowActivation| -> Vec<Command> {
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![read(1, "s", &[], wait).into()],
            _ => (results(activation))
                .filter_map(|result| {
                    let page = page_of(result)?;
                    Some(match page.records.is_empty() {
                        true => complete(),
                        false => read(result.seq + 1, "s", &page.last_offset, wait).into(),
                    })
                })
                .collect(),
        }
    };
    let mut seen: Vec<Vec<String>> = vec![];
    let mut ended = false;
    for _ in 0..4 {
        let poll = run.worker.poll_workflow_activation();
        let Ok(Ok(activation)) = tokio::time::timeout(Duration::from_secs(3), poll).await else {
            break;
        };
        if evicted(&run.worker, &activation).await {
            ended = true;
            break;
        }
        let jobs: Vec<String> = (activation.jobs.iter())
            .filter_map(|job| job.variant.as_ref().map(summary))
            .collect();
        // The harness sets a read time of 250 ms, so lang answers the page after the limit.
        if records_in(&activation).is_some_and(|count| count > 0) {
            tokio::time::sleep(Duration::from_millis(600)).await;
        }
        seen.push(jobs);
        respond(&run.worker, &activation, workflow).await;
    }
    let sent = run.sent();
    let completions: Vec<String> = (sent.iter())
        .map(|sent| {
            let forced = sent.force_create_new_workflow_task;
            format!("{:?} forced {forced}", command_types(sent))
        })
        .collect();
    let waits: Vec<bool> = (store.reads().iter())
        .map(|read| read.wait.is_some())
        .collect();
    run.worker.drain_pollers_and_shutdown_ref().await;
    let first_page = seen
        .get(1)
        .is_some_and(|jobs| jobs.iter().any(|job| job.starts_with("read 1 2 ")));
    assert!(
        first_page,
        "The first page didn't come in the first task: {seen:?}"
    );
    let outcome = format!(
        "activations {seen:?}, completions {completions:?}, waits of the reads of the Store \
         {waits:?}"
    );
    // The read after the limit asks the Store not to wait.
    assert_eq!(waits, [!wait.is_zero(), false], "{outcome}");
    if wait.is_zero() {
        assert!(ended, "The drain got no last page: {outcome}");
    } else {
        assert!(!ended, "{outcome}");
        let [completion] = &sent[..] else {
            panic!("{outcome}");
        };
        assert!(!completion.force_create_new_workflow_task, "{outcome}");
        let entries: Vec<usize> = (read_markers(completion).iter())
            .map(|marker| marker.reads.len())
            .collect();
        assert_eq!(entries, [1], "{outcome}");
    }
}

/// Returns the read entries of the read markers of `completion`, and whether one of them has a
/// failure.
fn read_entries(completion: &WorkflowTaskCompletion) -> (usize, bool) {
    let entries: Vec<StreamReadResult> = (read_markers(completion).into_iter())
        .flat_map(|marker| marker.reads)
        .collect();
    let failed = (entries.iter())
        .any(|entry| matches!(entry.result, Some(stream_read_result::Result::Failure(_))));
    (entries.len(), failed)
}

// Workflow code drains a Stream one Record at a time. The runtime cut the first page while the
// Store had more, and then the Store gives no answer. A read outage fails the task as an append
// outage does, so Core must not make a new task with an empty marker at each read limit while the
// read tries again.
#[tokio::test]
async fn a_read_outage_after_a_cut_page_fails_the_task_and_forces_no_task() {
    let store = store(&["s"]).await;
    fill(&store, "s", 4).await;
    // The Store answers the first read only.
    store.hold(Op::Read);
    store.pass(Op::Read, 1);
    let mut opts = StartOpts::new(&store);
    opts.fails = 1;
    opts.configure =
        Box::new(|wc| wc.stream_runtimes.settings.try_deadline = Duration::from_secs(1));
    let run = start(history(Duration::from_secs(60)), opts);
    let one = |seq, from_offset: &[u8]| -> Command {
        ReadStream {
            max_records: 1,
            ..read(seq, "s", from_offset, Duration::ZERO)
        }
        .into()
    };
    let workflow = move |activation: &WorkflowActivation| -> Vec<Command> {
        if let Some(Job::InitializeWorkflow(_)) = &activation.jobs[0].variant {
            return vec![one(1, &[])];
        }
        (results(activation))
            .map(|result| match page_of(result) {
                Some(page) => one(result.seq + 1, &page.last_offset),
                None => complete(),
            })
            .collect()
    };
    // About eight read limits of 250 ms.
    let (_, sequence) = drive_for(&run, Duration::from_millis(2500), workflow).await;
    let sent = run.sent();
    let forced_empty = (sent.iter())
        .filter(|completion| {
            completion.force_create_new_workflow_task
                && command_types(completion) == [CommandType::RecordMarker]
                && read_entries(completion).0 == 0
        })
        .count();
    let recorded_failure = sent.iter().any(|completion| read_entries(completion).1);
    let failed = run.failed.lock().len();
    let outcome = format!(
        "{forced_empty} forced completions with an empty read marker, a read marker with a \
         failure: {recorded_failure}, {failed} failed tasks. {sequence}"
    );
    assert!(
        forced_empty == 0 && !recorded_failure && failed == 1,
        "{outcome}"
    );
    run.worker.drain_pollers_and_shutdown_ref().await;
}

// A read with a wait is open when the Store goes down. A read outage fails the task as an append
// outage does, so Workflow code must get no `StoreUnavailable`, and History must keep no failure
// of the read.
#[tokio::test]
async fn a_read_outage_during_a_wait_fails_the_task() {
    let store = store(&["s"]).await;
    let down = StreamError::store_unavailable("The Store is down");
    store.fail(Op::Read, down, usize::MAX);
    let mut opts = StartOpts::new(&store);
    opts.fails = 1;
    let run = start(history(Duration::from_secs(60)), opts);
    let workflow = |activation: &WorkflowActivation| -> Vec<Command> {
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => {
                vec![read(1, "s", &[], Duration::from_secs(60)).into()]
            }
            _ if results(activation).next().is_some() => vec![complete()],
            _ => vec![],
        }
    };
    let (_, sequence) = drive_for(&run, Duration::from_secs(2), workflow).await;
    let sent = run.sent();
    let recorded_failure = sent.iter().any(|completion| read_entries(completion).1);
    let failed = run.failed.lock().clone();
    let outage = failed
        .iter()
        .any(|(_, message)| message.starts_with("StreamStoreUnavailable:"));
    assert!(
        !recorded_failure && outage,
        "a read marker with a failure: {recorded_failure}, a task failed for the outage: \
         {outage}. {sequence}"
    );
    run.worker.drain_pollers_and_shutdown_ref().await;
}

// A Local Activity holds the task, and Workflow code reads an empty Stream in a loop with no wait.
// Each read gets a page with no Record at once, so a budget bounds the read results of the task,
// as the Record budget bounds its bytes. The read that the budget keeps back makes the completion
// with the timer ask for the next task, and starts when that task opens.
#[tokio::test]
async fn the_read_results_of_one_task_have_a_bound() {
    const BOUND: usize = 256;
    let store = store(&["s"]).await;
    let run = start(history(Duration::from_secs(60)), StartOpts::new(&store));
    let workflow = move |activation: &WorkflowActivation| -> Vec<Command> {
        let resolved = (activation.jobs.iter())
            .any(|job| matches!(job.variant, Some(Job::ResolveActivity(_))));
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => {
                vec![local_activity(1), read(1, "s", &[], Duration::ZERO).into()]
            }
            _ if resolved => vec![timer(1)],
            _ if results(activation).any(|result| result.seq > BOUND as u32) => vec![complete()],
            _ => (results(activation))
                .map(|result| read(result.seq + 1, "s", &[], Duration::ZERO).into())
                .collect(),
        }
    };
    let local_activity_ends = async {
        let task = run.worker.poll_activity_task().await.unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let completion = ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(ActivityExecutionResult::ok(Payload::default())),
        };
        run.worker.complete_activity_task(completion).await.unwrap();
    };
    let ((left, sequence), ()) = tokio::join!(
        drive_for(&run, Duration::from_secs(10), workflow),
        local_activity_ends
    );
    let sent = run.sent();
    run.worker.drain_pollers_and_shutdown_ref().await;
    assert!(left, "The Workflow didn't complete: {sequence}");
    let seqs = |completion: &WorkflowTaskCompletion| -> Vec<u32> {
        (read_markers(completion).iter())
            .flat_map(|marker| marker.reads.iter().map(|entry| entry.seq))
            .collect()
    };
    let first: Vec<u32> = (1..=BOUND as u32).collect();
    assert_eq!(seqs(&sent[0]), first, "{sequence}");
    assert!(command_types(&sent[0]).contains(&CommandType::StartTimer));
    assert!(sent[0].force_create_new_workflow_task, "{sequence}");
    assert_eq!(seqs(&sent[1]), [BOUND as u32 + 1], "{sequence}");
    assert_eq!(sent.len(), 2, "{sequence}");
}

// A Workflow reads an empty Stream in a loop with no wait, and has no other work. After the read
// results of its first task, only the read that the budget keeps back is open. It still gets the
// next task with no other event, and a replay gives the same jobs.
#[tokio::test]
async fn a_read_that_the_result_budget_holds_back_forces_the_next_task() {
    const BOUND: u32 = 256;
    let store = store(&["s"]).await;
    let run = start(history(Duration::from_secs(60)), StartOpts::new(&store));
    let workflow = |activation: &WorkflowActivation| -> Vec<Command> {
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![read(1, "s", &[], Duration::ZERO).into()],
            _ if results(activation).any(|result| result.seq > BOUND) => vec![complete()],
            _ => (results(activation))
                .map(|result| read(result.seq + 1, "s", &[], Duration::ZERO).into())
                .collect(),
        }
    };
    let live = tokio::time::timeout(Duration::from_secs(10), run.drive(workflow)).await;
    let sent = run.sent();
    let completions: Vec<String> = (sent.iter())
        .map(|sent| {
            let forced = sent.force_create_new_workflow_task;
            format!("{:?} forced {forced}", command_types(sent))
        })
        .collect();
    let live = live.unwrap_or_else(|_| panic!("The Workflow didn't complete: {completions:?}"));
    let seqs = |completion: &WorkflowTaskCompletion| -> Vec<u32> {
        (read_markers(completion).iter())
            .flat_map(|marker| marker.reads.iter().map(|entry| entry.seq))
            .collect()
    };
    let [first, second] = &sent[..] else {
        panic!("{completions:?}");
    };
    assert_eq!(seqs(first), (1..=BOUND).collect::<Vec<u32>>());
    assert!(first.force_create_new_workflow_task, "{completions:?}");
    assert_eq!(seqs(second), [BOUND + 1]);
    assert!(!second.force_create_new_workflow_task, "{completions:?}");
    run.worker.drain_pollers_and_shutdown_ref().await;
    assert_eq!(run.replay(workflow).await, live);
}

/// Returns Workflow code that reads Stream `s` with `read` in its first task. At a page with
/// Records it starts timer 1 and reads on with `read`, and it completes at the next page.
fn reads_on_with_a_timer(
    read: impl Fn(u32, &[u8]) -> ReadStream + Copy,
) -> impl Fn(&WorkflowActivation) -> Vec<Command> + Copy {
    move |activation| match &activation.jobs[0].variant {
        Some(Job::InitializeWorkflow(_)) => vec![read(1, &[]).into()],
        _ => (results(activation))
            .filter_map(|result| {
                let page = page_of(result)?;
                Some(match result.seq {
                    1 if !page.records.is_empty() => {
                        vec![timer(1), read(2, &page.last_offset).into()]
                    }
                    _ => vec![complete()],
                })
            })
            .flatten()
            .collect(),
    }
}

// The runtime cuts a page by its bytes while the Store has more. The read from its end then has
// Records, so the completion with the timer asks for a new task, whose first activation gives the
// next page.
#[tokio::test]
async fn a_page_cut_by_bytes_with_more_records_forces_the_next_task() {
    let store = store(&["s"]).await;
    fill(&store, "s", 3).await;
    let one = (store.records("s").iter())
        .map(|record| field_len(prost::Message::encoded_len(record)))
        .max()
        .unwrap();
    let run = start(history(Duration::from_secs(60)), StartOpts::new(&store));
    let workflow = reads_on_with_a_timer(move |seq, from_offset| ReadStream {
        max_bytes: one as i64,
        ..read(seq, "s", from_offset, Duration::ZERO)
    });
    let live = run.drive(workflow).await;
    let sent = run.sent();
    assert!(sent[0].force_create_new_workflow_task, "{sent:?}");
    let names: Vec<Vec<String>> = (live.iter())
        .map(|jobs| jobs.iter().map(summary).collect())
        .collect();
    assert!(names[1][0].starts_with("read 1 1 "), "{names:?}");
    assert!(names[2][0].starts_with("read 2 1 "), "{names:?}");
    assert_eq!(names.len(), 3, "{names:?}");
    run.worker.drain_pollers_and_shutdown_ref().await;
    assert_eq!(run.replay(workflow).await, live);
}

// A page at its `max_records` with no Record after it tells Core that the Stream has no more. So
// a read from its end with a wait gets a check with no Record, and the completion with the timer
// asks for no new task.
#[tokio::test]
async fn a_full_page_with_nothing_after_it_forces_no_task() {
    let store = store(&["s"]).await;
    fill(&store, "s", 2).await;
    let mut opts = StartOpts::new(&store);
    opts.served = None;
    let run = start(history(Duration::from_secs(60)), opts);
    let workflow = reads_on_with_a_timer(|seq, from_offset| ReadStream {
        max_records: 2,
        ..read(seq, "s", from_offset, Duration::from_secs(60))
    });
    let (_, sequence) = drive_for(&run, Duration::from_secs(2), workflow).await;
    let sent = run.sent();
    let [completion] = &sent[..] else {
        panic!("{sequence}");
    };
    assert!(!completion.force_create_new_workflow_task, "{sequence}");
    // The runtime asked for one Record more than the page, and the Store had none.
    let asked: Vec<(i32, bool)> = (store.reads().iter())
        .map(|read| (read.max_records, read.wait.is_some()))
        .collect();
    assert_eq!(asked, [(3, true), (3, false)], "{sequence}");
    run.worker.drain_pollers_and_shutdown_ref().await;
}

// A page over the Record budget of its task waits for the next task. The completion asks for that
// task, and its first activation gives the page.
#[tokio::test]
async fn a_held_page_forces_the_next_task_and_comes_first_in_it() {
    let store = store(&["a", "b"]).await;
    fill(&store, "a", 1).await;
    fill(&store, "b", 1).await;
    let one = (store.records("a").iter().chain(&store.records("b")))
        .map(|record| field_len(prost::Message::encoded_len(record)))
        .max()
        .unwrap();
    let mut opts = StartOpts::new(&store);
    opts.configure = Box::new(move |wc| wc.stream_runtimes.settings.max_task_record_bytes = one);
    let run = start(history(Duration::from_secs(60)), opts);
    let workflow = |activation: &WorkflowActivation| -> Vec<Command> {
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![
                read(1, "a", &[], Duration::ZERO).into(),
                read(2, "b", &[], Duration::ZERO).into(),
            ],
            _ if results(activation).any(|result| result.seq == 2) => vec![complete()],
            _ => vec![timer(1)],
        }
    };
    let live = run.drive(workflow).await;
    let names: Vec<Vec<String>> = (live.iter())
        .map(|jobs| jobs.iter().map(summary).collect())
        .collect();
    let sent = run.sent();
    assert!(sent[0].force_create_new_workflow_task, "{names:?}");
    assert!(
        command_types(&sent[0]).contains(&CommandType::StartTimer),
        "{names:?}"
    );
    assert!(names[1][0].starts_with("read 1 1 "), "{names:?}");
    assert_eq!(names[2].len(), 1, "{names:?}");
    assert!(names[2][0].starts_with("read 2 1 "), "{names:?}");
    run.worker.drain_pollers_and_shutdown_ref().await;
    assert_eq!(run.replay(workflow).await, live);
}

// A read with a wait on a Stream that gets no Record never makes a new task. At each completion
// with a timer its check gets no Record, so the read starts again in the task that the timer
// makes.
#[tokio::test]
async fn a_read_of_an_idle_stream_forces_no_task_over_several_tasks() {
    const TASKS: usize = 4;
    let store = store(&["s"]).await;
    let mut opts = StartOpts::new(&store);
    opts.eager = Some(Box::new(|completion, t| {
        let starts_a_timer = command_types(completion).contains(&CommandType::StartTimer);
        starts_a_timer.then(|| {
            fire_last_timer(t);
            next_task(t)
        })
    }));
    let run = start(history(Duration::from_secs(60)), opts);
    let workflow = |activation: &WorkflowActivation| -> Vec<Command> {
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => {
                vec![read(1, "s", &[], Duration::from_secs(60)).into(), timer(1)]
            }
            Some(Job::FireTimer(fired)) if (fired.seq as usize) < TASKS => {
                vec![timer(fired.seq + 1)]
            }
            _ => vec![complete()],
        }
    };
    let (left, sequence) = drive_for(&run, Duration::from_secs(5), workflow).await;
    assert!(left, "{sequence}");
    let sent = run.sent();
    assert_eq!(sent.len(), TASKS + 1, "{sequence}");
    let forced = sent
        .iter()
        .filter(|c| c.force_create_new_workflow_task)
        .count();
    assert_eq!(forced, 0, "{sequence}");
    let checks = (store.reads().iter())
        .filter(|read| read.wait.is_none())
        .count();
    assert_eq!(checks, TASKS, "{sequence}");
    assert!(
        sent.iter().all(|c| read_markers(c).is_empty()),
        "{sequence}"
    );
    run.worker.drain_pollers_and_shutdown_ref().await;
}

// A query answer asks for a new task whatever the reads say. A read with no result in the task
// then leaves one read marker with no entry, so a replay with chunking on doesn't join the task to
// the next one.
#[tokio::test]
async fn a_forced_completion_with_an_open_read_and_no_result_has_one_empty_marker() {
    let store = store(&["s"]).await;
    let t = history(Duration::from_secs(60));
    let mut task = hist_to_poll_resp(&t, "fake_wf_id", ResponseType::AllHistory).resp;
    let question = WorkflowQuery {
        query_type: "q".to_string(),
        ..Default::default()
    };
    task.queries.insert("q1".to_string(), question);
    let mut opts = StartOpts::new(&store);
    opts.responses = vec![ResponseType::Raw(task)];
    opts.served = None;
    let run = start(t, opts);
    let workflow = |activation: &WorkflowActivation| -> Vec<Command> {
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => {
                vec![read(1, "s", &[], Duration::from_secs(60)).into()]
            }
            Some(Job::QueryWorkflow(_)) => answer(activation).into_iter().collect(),
            _ => vec![],
        }
    };
    let (_, sequence) = drive_for(&run, Duration::from_secs(2), workflow).await;
    let sent = run.sent();
    let [completion] = &sent[..] else {
        panic!("{sequence}");
    };
    assert!(completion.force_create_new_workflow_task, "{sequence}");
    assert_eq!(completion.query_responses.len(), 1, "{sequence}");
    assert_eq!(
        command_types(completion),
        [CommandType::RecordMarker],
        "{sequence}"
    );
    assert_eq!(
        read_markers(completion),
        [StreamReadMarkerData::default()],
        "{sequence}"
    );
    run.worker.drain_pollers_and_shutdown_ref().await;
}

// A replay reads nothing in the read time of its task, so the read limit counts from the end of the
// replay. Here Workflow code takes longer than the read time of 250 ms to replay the first task,
// and the read of the live task still waits for the Store.
#[tokio::test]
async fn the_read_limit_counts_from_the_end_of_the_replay() {
    let store = store(&["s"]).await;
    let mut t = history(Duration::from_secs(60));
    t.add_workflow_task_completed();
    let started = t.add_timer_started("1".to_string());
    t.add_timer_fired(started, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    let mut opts = StartOpts::new(&store);
    opts.served = None;
    let run = start(t, opts);
    let workflow = |activation: &WorkflowActivation| -> Vec<Command> {
        match &activation.jobs[0].variant {
            Some(Job::InitializeWorkflow(_)) => vec![timer(1)],
            Some(Job::FireTimer(_)) => vec![read(1, "s", &[], Duration::from_secs(60)).into()],
            _ => vec![complete()],
        }
    };
    let replayed = run.worker.poll_workflow_activation().await.unwrap();
    assert!(replayed.is_replaying);
    tokio::time::sleep(Duration::from_millis(400)).await;
    respond(&run.worker, &replayed, workflow).await;
    let live = run.worker.poll_workflow_activation().await.unwrap();
    assert!(!live.is_replaying);
    respond(&run.worker, &live, workflow).await;
    let cut = run.worker.poll_workflow_activation().await.unwrap();
    let [Job::ResolveStreamRead(result)] = &jobs_of(&cut)[..] else {
        panic!("{cut:?}");
    };
    assert!(page_of(result).unwrap().wait_cut);
    respond(&run.worker, &cut, workflow).await;
    let waits: Vec<Option<Duration>> = (store.reads().iter())
        .map(|read| read.wait.map(|wait| wait.try_into().unwrap()))
        .collect();
    let [Some(wait)] = waits[..] else {
        panic!("{waits:?}");
    };
    assert!(
        wait > Duration::from_millis(150) && wait < Duration::from_millis(250),
        "{wait:?}"
    );
    run.worker.drain_pollers_and_shutdown_ref().await;
}

// The replays of a Worker read their Records from the Stores for at most `REPLAY_READ_RUNS` runs
// at a time, which bounds the Records that the Worker holds for them. The test sets other bounds.
#[rstest::rstest]
#[case::one_run_at_a_time(1)]
#[case::two_runs_at_a_time(2)]
#[tokio::test]
async fn the_replays_of_a_worker_read_for_a_bounded_number_of_runs(#[case] runs: usize) {
    let store = store(&["s"]).await;
    fill(&store, "s", 1).await;
    let (_, t, first) = first_run_with_a_timer_at_each_page(&store, 1).await;
    let (_, _, second) = first_run_with_a_timer_at_each_page(&store, 1).await;
    let reads = store.reads().len();
    let (tasks, receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut opts = StartOpts::new(&store);
    opts.tasks = Some(receiver);
    opts.served = None;
    opts.cache = 2;
    opts.configure = Box::new(move |wc| wc.stream_runtimes.settings.replay_read_runs = runs);
    let run = start(t, opts);
    store.hold(Op::Read);
    tasks.send(first).unwrap();
    tasks.send(second).unwrap();
    drop(tasks);
    // A poll drives the replays, so the test watches the Store while one waits.
    let watch = async {
        wait_until(|| store.reads().len() > reads).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let reading = store.reads().len() - reads;
        store.release();
        reading
    };
    let (reading, activation) = tokio::join!(watch, run.worker.poll_workflow_activation());
    assert_eq!(reading, runs);
    let mut activation = Some(activation.unwrap());
    let mut completed = HashSet::new();
    while completed.len() < 2 {
        let activation = match activation.take() {
            Some(activation) => activation,
            None => run.worker.poll_workflow_activation().await.unwrap(),
        };
        if fires_a_last_timer(&activation) {
            completed.insert(activation.run_id.clone());
        }
        respond(&run.worker, &activation, read_with_a_timer_at_each_page(1)).await;
    }
    run.worker.drain_pollers_and_shutdown_ref().await;
}
