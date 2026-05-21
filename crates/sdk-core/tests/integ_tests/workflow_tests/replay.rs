use crate::{
    common::{
        ActivationAssertionsInterceptor, build_fake_sdk, history_from_proto_binary,
        init_core_replay_preloaded, replay_sdk_worker, replay_sdk_worker_stream,
    },
    integ_tests::workflow_tests::patches::ChangesWf,
};
use assert_matches::assert_matches;
use parking_lot::Mutex;
use std::{collections::HashSet, sync::Arc, time::Duration};
use temporalio_common::protos::{
    coresdk::{
        AsJsonPayloadExt,
        workflow_activation::remove_from_cache::EvictionReason,
        workflow_commands::{ScheduleActivity, StartTimer},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::enums::v1::EventType,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    SyncWorkflowContext, Worker, WorkflowContext, WorkflowContextView, WorkflowResult,
    interceptors::WorkerInterceptor,
};
use temporalio_sdk_core::{
    PollError, prost_dur,
    replay::{
        DEFAULT_WORKFLOW_TYPE, HistoryFeeder, HistoryForReplay, TestHistoryBuilder,
        canned_histories,
    },
    test_help::{MockPollCfg, ResponseType, WorkerTestHelpers},
};
use tokio::join;

fn test_hist_to_replay(t: TestHistoryBuilder) -> HistoryForReplay {
    let hi = t.get_full_history_info().unwrap();
    HistoryForReplay::new(hi, "fake".to_string())
}

#[workflow]
struct TimersWf {
    num_timers: u32,
}

#[workflow_methods]
impl TimersWf {
    #[init]
    fn new(_ctx: &WorkflowContextView, num_timers: u32) -> Self {
        Self { num_timers }
    }

    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let num_timers = ctx.state(|wf| wf.num_timers);
        for _ in 1..=num_timers {
            ctx.timer(Duration::from_secs(1)).await;
        }
        Ok(())
    }
}

#[fixture(num_timers = 1)]
fn fire_happy_hist(num_timers: u32) -> Worker {
    let mut t = canned_histories::long_sequential_timers(num_timers as usize);
    t.set_wf_input(num_timers.as_json_payload().unwrap());
    let mut worker = build_fake_sdk(MockPollCfg::from_resps(t, [ResponseType::AllHistory]));
    worker.register_workflow::<TimersWf>();
    worker
}

#[rstest]
#[case::one_timer(fire_happy_hist(1), 1)]
#[case::five_timers(fire_happy_hist(5), 5)]
#[tokio::test]
async fn replay_flag_is_correct(#[case] mut worker: Worker, #[case] num_timers: usize) {
    // Verify replay flag is correct by constructing a workflow manager that already has a complete
    // history fed into it. It should always be replaying, because history is complete.

    let mut aai = ActivationAssertionsInterceptor::default();

    for _ in 1..=num_timers + 1 {
        aai.then(|a| assert!(a.is_replaying));
    }

    worker.set_worker_interceptor(aai);
    worker.run().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_flag_is_correct_partial_history() {
    let mut t = canned_histories::long_sequential_timers(2);
    t.set_wf_input(1u32.as_json_payload().unwrap());
    let mut worker = build_fake_sdk(MockPollCfg::from_resps(t, [1]));
    worker.register_workflow::<TimersWf>();

    let mut aai = ActivationAssertionsInterceptor::default();
    aai.then(|a| assert!(!a.is_replaying));

    worker.set_worker_interceptor(aai);
    worker.run().await.unwrap();
}

#[tokio::test]
async fn timer_workflow_replay() {
    let core = init_core_replay_preloaded(
        "timer_workflow_replay",
        [HistoryForReplay::new(
            history_from_proto_binary("timer_workflow_history.bin")
                .await
                .unwrap(),
            "fake".to_owned(),
        )],
    );
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            StartTimer {
                seq: 0,
                start_to_fire_timeout: Some(prost_dur!(from_secs(1))),
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    // Verify that an in-progress poll is interrupted by completion finishing processing history
    let act_poll_fut = async {
        assert_matches!(core.poll_activity_task().await, Err(PollError::ShutDown));
    };
    let poll_fut = async {
        let evict_task = core
            .poll_workflow_activation()
            .await
            .expect("Should be an eviction activation");
        assert!(evict_task.eviction_reason().is_some());
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(evict_task.run_id))
            .await
            .unwrap();
        assert_matches!(
            core.poll_workflow_activation().await,
            Err(PollError::ShutDown)
        );
    };
    let complete_fut = async {
        core.complete_execution(&task.run_id).await;
    };
    join!(act_poll_fut, poll_fut, complete_fut);

    // Subsequent polls should still return shutdown
    assert_matches!(
        core.poll_workflow_activation().await,
        Err(PollError::ShutDown)
    );

    core.shutdown().await;
}

#[tokio::test]
async fn workflow_nondeterministic_replay() {
    let core = init_core_replay_preloaded(
        "timer_workflow_replay",
        [HistoryForReplay::new(
            history_from_proto_binary("timer_workflow_history.bin")
                .await
                .unwrap(),
            "fake".to_owned(),
        )],
    );
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            ScheduleActivity {
                seq: 0,
                activity_id: "0".to_string(),
                activity_type: "fake_act".to_string(),
                ..Default::default()
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(task.eviction_reason(), Some(EvictionReason::Nondeterminism));
    // Complete eviction
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    // Call shutdown explicitly because we saw a nondeterminism eviction
    core.shutdown().await;
    assert_matches!(
        core.poll_workflow_activation().await,
        Err(PollError::ShutDown)
    );
}

#[tokio::test]
async fn replay_using_wf_function() {
    let num_timers = 10u32;
    let mut t = canned_histories::long_sequential_timers(num_timers as usize);
    t.set_wf_input(num_timers.as_json_payload().unwrap());
    let mut worker = replay_sdk_worker([test_hist_to_replay(t)]);
    worker.register_workflow::<TimersWf>();
    worker.run().await.unwrap();
}

#[tokio::test]
async fn replay_ending_wft_complete_with_commands_but_no_scheduled_started() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();

    for i in 1..=2 {
        let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add_timer_fired(timer_started_event_id, i.to_string());
        t.add_full_wf_task();
    }
    t.set_wf_input(3u32.as_json_payload().unwrap());
    let mut worker = replay_sdk_worker([test_hist_to_replay(t)]);
    worker.register_workflow::<TimersWf>();
    worker.run().await.unwrap();
}

async fn replay_abrupt_ending(mut t: TestHistoryBuilder) {
    t.set_wf_input(1u32.as_json_payload().unwrap());
    let mut worker = replay_sdk_worker([test_hist_to_replay(t)]);
    worker.register_workflow::<TimersWf>();
    worker.run().await.unwrap();
}
#[tokio::test]
async fn replay_ok_ending_with_terminated() {
    let mut t1 = canned_histories::single_timer("1");
    t1.add_workflow_execution_terminated();
    replay_abrupt_ending(t1).await;
}
#[tokio::test]
async fn replay_ok_ending_with_timed_out() {
    let mut t2 = canned_histories::single_timer("1");
    t2.add_workflow_execution_timed_out();
    replay_abrupt_ending(t2).await;
}

#[tokio::test]
async fn replay_shutdown_worker() {
    let mut t = canned_histories::single_timer("1");
    t.set_wf_input(1u32.as_json_payload().unwrap());
    let mut worker = replay_sdk_worker([test_hist_to_replay(t)]);
    worker.register_workflow::<TimersWf>();
    let shutdown_ctr_i = UniqueShutdownWorker::default();
    let shutdown_ctr = shutdown_ctr_i.runs.clone();
    worker.set_worker_interceptor(shutdown_ctr_i);
    worker.run().await.unwrap();
    assert_eq!(shutdown_ctr.lock().len(), 1);
}

#[workflow]
struct OneTimerWf {
    num_timers: u32,
}

#[workflow_methods]
impl OneTimerWf {
    #[init]
    fn new(_ctx: &WorkflowContextView, num_timers: u32) -> Self {
        Self { num_timers }
    }

    #[run(name = "onetimer")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let num_timers = ctx.state(|wf| wf.num_timers);
        for _ in 1..=num_timers {
            ctx.timer(Duration::from_secs(1)).await;
        }
        Ok(())
    }
}

#[workflow]
struct SeqTimerWf {
    num_timers: u32,
}

#[workflow_methods]
impl SeqTimerWf {
    #[init]
    fn new(_ctx: &WorkflowContextView, num_timers: u32) -> Self {
        Self { num_timers }
    }

    #[run(name = "seqtimer")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let num_timers = ctx.state(|wf| wf.num_timers);
        for _ in 1..=num_timers {
            ctx.timer(Duration::from_secs(1)).await;
        }
        Ok(())
    }
}

#[rstest::rstest]
#[tokio::test]
async fn multiple_histories_replay(#[values(false, true)] use_feeder: bool) {
    let num_timers = 10u32;
    let mut one_timer_hist = canned_histories::single_timer("1");
    one_timer_hist.set_wf_type("onetimer");
    one_timer_hist.set_wf_input(1u32.as_json_payload().unwrap());
    let mut seq_timer_hist = canned_histories::long_sequential_timers(num_timers as usize);
    seq_timer_hist.set_wf_type("seqtimer");
    seq_timer_hist.set_wf_input(num_timers.as_json_payload().unwrap());
    let (feeder, stream) = HistoryFeeder::new(1);
    let mut worker = if use_feeder {
        replay_sdk_worker_stream(stream)
    } else {
        replay_sdk_worker([
            test_hist_to_replay(one_timer_hist.clone()),
            test_hist_to_replay(seq_timer_hist.clone()),
        ])
    };
    let runs_ctr_i = UniqueRunsCounter::default();
    let runs_ctr = runs_ctr_i.runs.clone();
    worker.set_worker_interceptor(runs_ctr_i);
    worker.register_workflow::<OneTimerWf>();
    worker.register_workflow::<SeqTimerWf>();

    if use_feeder {
        let feed_fut = async move {
            feeder
                .feed(test_hist_to_replay(one_timer_hist))
                .await
                .unwrap();
            feeder
                .feed(test_hist_to_replay(seq_timer_hist))
                .await
                .unwrap();
        };
        let (_, runr) = join!(feed_fut, worker.run());
        runr.unwrap();
    } else {
        worker.run().await.unwrap();
    }
    assert_eq!(runs_ctr.lock().len(), 2);
}

#[tokio::test]
async fn multiple_histories_can_handle_dupe_run_ids() {
    let mut hist1 = canned_histories::single_timer("1");
    hist1.set_wf_type("onetimer");
    hist1.set_wf_input(1u32.as_json_payload().unwrap());
    let mut worker = replay_sdk_worker([
        test_hist_to_replay(hist1.clone()),
        test_hist_to_replay(hist1.clone()),
        test_hist_to_replay(hist1),
    ]);
    worker.register_workflow::<OneTimerWf>();
    worker.run().await.unwrap();
}

// Verifies SDK can decode patch markers before changing them to use json encoding.
#[tokio::test]
async fn replay_old_patch_format() {
    let mut worker = replay_sdk_worker([HistoryForReplay::new(
        history_from_proto_binary("old_change_marker_format.bin")
            .await
            .unwrap(),
        "fake".to_owned(),
    )]);
    worker.register_workflow::<ChangesWf>();
    worker.run().await.unwrap();
}

#[tokio::test]
async fn replay_ends_with_empty_wft() {
    let core = init_core_replay_preloaded(
        "SayHelloWorkflow",
        [HistoryForReplay::new(
            history_from_proto_binary("ends_empty_wft_complete.bin")
                .await
                .unwrap(),
            "fake".to_owned(),
        )],
    );
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            ScheduleActivity {
                seq: 1,
                activity_id: "1".to_string(),
                activity_type: "say_hello".to_string(),
                ..Default::default()
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.eviction_reason().is_some());
}

/// Confirms WFT chunking v2 propagates end-to-end through Core's replay path
/// and produces one activation per real WFT, each with its own observed
/// timestamp.
///
/// This is the integration-level analogue of the unit tests in
/// `crates/sdk-core/src/worker/workflow/history_update.rs` (which exercise the
/// chunking algorithm itself) — it confirms the chunking decisions also reach
/// lang correctly through the SDK replay machinery, with each WFT's
/// `WorkflowTaskStarted` timestamp surfacing on its own activation. If chunking
/// ever collapsed WFTs that should remain distinct, the activation count would
/// drop and/or two activations would share a timestamp.
#[tokio::test]
async fn wft_chunking_v2_replay_preserves_per_wft_timestamps() {
    let num_timers = 3u32;
    let mut t = canned_histories::long_sequential_timers(num_timers as usize);
    // Builder-level opt-in: stamp the first WFTCompleted with the `WftChunkingV2`
    // flag so the worker selects v2 chunking on replay. The canned history was
    // built without v2 in mind, so we set the flag retroactively via the
    // dedicated helper (no flags were previously set on the first WFTCompleted).
    use temporalio_sdk_core::test_help::CoreInternalFlags;
    t.set_flags_first_wft(&[CoreInternalFlags::WftChunkingV2], &[]);
    t.set_wf_input(num_timers.as_json_payload().unwrap());

    let mut worker = replay_sdk_worker([test_hist_to_replay(t)]);
    worker.register_workflow::<TimersWf>();

    let collected: Arc<Mutex<Vec<prost_types::Timestamp>>> = Arc::new(Mutex::new(vec![]));
    worker.set_worker_interceptor(WftChunkingV2TimestampCollector {
        timestamps: collected.clone(),
    });
    worker.run().await.unwrap();

    let times = collected.lock();
    assert_eq!(
        times.len(),
        num_timers as usize + 1,
        "expected one activation per real WFT under v2 chunking \
         (InitializeWorkflow + one per TimerFired), got {}: {:?}",
        times.len(),
        &*times,
    );
    let unique: HashSet<_> = times.iter().collect();
    assert_eq!(
        times.len(),
        unique.len(),
        "every activation should observe its own WFTStarted timestamp under v2 \
         chunking; observed repeated timestamps would indicate a chunking collapse: \
         {:?}",
        &*times,
    );
}

struct WftChunkingV2TimestampCollector {
    timestamps: Arc<Mutex<Vec<prost_types::Timestamp>>>,
}

#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for WftChunkingV2TimestampCollector {
    async fn on_workflow_activation(
        &self,
        act: &temporalio_common::protos::coresdk::workflow_activation::WorkflowActivation,
    ) -> Result<(), anyhow::Error> {
        // Skip eviction-only activations — they're not the "real" replay
        // activations whose timestamps we care about here.
        if act.eviction_reason().is_some() {
            return Ok(());
        }
        if let Some(ts) = act.timestamp.clone() {
            self.timestamps.lock().push(ts);
        }
        Ok(())
    }
}

/// A workflow that runs a local activity around a `workflow_time()` observation.
/// At workflow start it records the seconds since UNIX_EPOCH of the observed
/// time, runs the LA, and then emits a `StartTimer` command **only if** that
/// observed value matches a hard-coded "expected WFT1 timestamp".
///
/// We construct the replay history with `WorkflowTaskStarted` events placed at
/// known wall-clock times, and we expect the workflow's first observation to
/// equal WFT1's timestamp. Under WFT chunking v2 this is what the workflow
/// sees, because `WorkflowExecutionStarted` forces WFT1 to be its own LWFT.
/// Under v1 chunking, the first LWFT collapses WFT1 with the following
/// heartbeat-shaped WFTs, and the workflow's first observation lands on the
/// *last* `WorkflowTaskStarted` in the collapsed chain (a different timestamp),
/// so the conditional branch is skipped, no timer command is emitted, and
/// replay NDEs against the recorded `TimerStarted` event.
const WFT1_TIMESTAMP_SECS: u64 = 1_700_000_000;
const WFT3_TIMESTAMP_SECS: u64 = 1_700_001_000;

#[workflow]
#[derive(Default)]
struct WftChunkingV2LaHeartbeatWf;

#[workflow_methods]
impl WftChunkingV2LaHeartbeatWf {
    #[run(name = "wft_chunking_v2_la_heartbeat")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let initial = ctx
            .workflow_time()
            .expect("workflow time should be set");
        let initial_secs = initial
            .duration_since(std::time::UNIX_EPOCH)
            .expect("workflow time should be after UNIX_EPOCH")
            .as_secs();
        ctx.start_local_activity(
            crate::common::activity_functions::StdActivities::echo,
            "hi!".to_string(),
            temporalio_sdk::LocalActivityOptions::default(),
        )
        .await?;
        // Emit a real command iff the observed timestamp matches what v2 chunking
        // (which keeps WFT1 as its own LWFT) is supposed to surface.
        if initial_secs == WFT1_TIMESTAMP_SECS {
            ctx.timer(Duration::from_millis(1)).await;
        }
        Ok(())
    }
}

/// A workflow that records `workflow_time()` each time it receives a signal,
/// then emits a `StartTimer` command *only if the two observed times differ*.
///
/// The conditional command makes the test sensitive to chunking decisions in
/// a way the workflow's return value cannot be: the return value is recorded
/// in `WorkflowExecutionCompletedEventAttributes` in history and is therefore
/// fixed on replay regardless of what the workflow code computed; but the
/// command sequence emitted by the workflow code is replayed and matched
/// against history's command events on every replay. If chunking ever
/// collapsed the two signal-receiving WFTs into a single LWFT, both handler
/// invocations would observe the same timestamp, the workflow would not emit
/// the timer command, and the replay would NDE against the recorded
/// `TimerStarted` event.
#[workflow]
#[derive(Default)]
struct WftChunkingV2SignalTimeWf {
    received: u32,
    time1: Option<std::time::SystemTime>,
    time2: Option<std::time::SystemTime>,
}

#[workflow_methods]
impl WftChunkingV2SignalTimeWf {
    #[run(name = "wft_chunking_v2_signal_time")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.wait_condition(|s| s.received >= 2).await;
        let t1 = ctx.state(|s| s.time1.expect("time1 set in sig1 handler"));
        let t2 = ctx.state(|s| s.time2.expect("time2 set in sig2 handler"));
        if t1 != t2 {
            // Under correct chunking the two signals were delivered in distinct
            // LWFTs at distinct timestamps; emit a real command so the chunking
            // decision becomes observable in the command stream (and therefore
            // checkable against history on replay).
            ctx.timer(Duration::from_millis(1)).await;
        }
        Ok(())
    }

    #[signal(name = "sig1")]
    fn handle_sig1(&mut self, ctx: &mut SyncWorkflowContext<Self>, _input: ()) {
        self.time1 = ctx.workflow_time();
        self.received += 1;
    }

    #[signal(name = "sig2")]
    fn handle_sig2(&mut self, ctx: &mut SyncWorkflowContext<Self>, _input: ()) {
        self.time2 = ctx.workflow_time();
        self.received += 1;
    }
}

/// Confirms the "time sensitivity" fix of WFT chunking v2 from the workflow
/// author's point of view.
///
/// The history below records a workflow that:
///   1. Started.
///   2. Received `sig1` in WFT2 — workflow_time observed = WFT2's WFTStarted ts.
///   3. Received `sig2` in WFT3 — workflow_time observed = WFT3's WFTStarted ts.
///   4. Saw the two observed times differ, emitted a `StartTimer` from WFT3.
///   5. The timer fired in WFT4, then completed.
///
/// Under v2 chunking this same shape of command stream is produced on replay —
/// because each signal-receiving WFT stays in its own LWFT with its own
/// timestamp, the workflow's `t1 != t2` branch fires again and re-emits the
/// timer. Under a hypothetical chunking collapse, the workflow would instead
/// observe `t1 == t2`, skip the timer, and NDE against the recorded
/// `TimerStarted` event in history (caught by
/// [`FailOnNondeterminismInterceptor`] that `replay_sdk_worker` installs).
#[tokio::test]
async fn wft_chunking_v2_signal_observed_times_are_per_wft() {
    let mut t = TestHistoryBuilder::default();
    t.set_use_wft_chunking_v2();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task(); // WFT1: workflow main runs, awaits signals
    t.add_we_signaled("sig1", vec![]);
    t.add_full_wf_task(); // WFT2: handles sig1 (records time1)
    t.add_we_signaled("sig2", vec![]);
    t.add_full_wf_task(); // WFT3: handles sig2 (records time2), emits StartTimer
    let timer_id = t.add_timer_started("1".to_string());
    t.add_timer_fired(timer_id, "1".to_string());
    t.add_full_wf_task(); // WFT4: timer fires, workflow completes
    t.add_workflow_execution_completed();
    t.set_wf_type("wft_chunking_v2_signal_time");

    let mut worker = replay_sdk_worker([test_hist_to_replay(t)]);
    worker.register_workflow::<WftChunkingV2SignalTimeWf>();
    // `replay_sdk_worker` pre-installs `FailOnNondeterminismInterceptor`, which
    // is what catches any mismatch between the workflow's emitted commands and
    // history if chunking ever observed the wrong timestamps.
    worker.run().await.unwrap();
}

/// Strict v1-vs-v2 differentiator using a LA-heartbeat history.
///
/// History layout:
///   1. `WorkflowExecutionStarted`
///   2. WFT1 (Sched/Started/Completed) — workflow code observes time at this WFT's
///      `WorkflowTaskStarted` event under v2, then starts an LA and suspends.
///   3. WFT2 (Sched/Started/Completed) — heartbeat WFT (long-running LA still in
///      progress; the worker completed with no commands to dodge WFT timeout).
///   4. WFT3 (Sched/Started/Completed) — the LA completes during this WFT.
///   5. `MarkerRecorded`               — LA result marker (recorded as WFT3's command).
///   6. WFT4 (Sched/Started/Completed) — workflow code resumes, observes the LA
///      result, and emits a `StartTimer` command iff the workflow's first
///      `workflow_time()` observation matched WFT1's WFTStarted timestamp.
///   7. `TimerStarted` / `TimerFired`  — the timer (the chunking-sensitive command).
///   8. WFT5 (Sched/Started/Completed) — timer fired, workflow completes.
///   9. `WorkflowExecutionCompleted`
///
/// The WFTStarted events for WFT1 and WFT3 are stamped with deliberately
/// distinct wall-clock times.
///
/// Under v2 chunking, the first LWFT is `[WFExecutionStarted, WFTScheduled,
/// WFTStarted]` covering WFT1 alone. The workflow's first `workflow_time()`
/// observation surfaces WFT1's timestamp, the conditional branch fires, and
/// the timer command is emitted — matching the recorded `TimerStarted` event.
/// Under v1 chunking, the heartbeat heuristic collapses WFT1/WFT2/WFT3 into a
/// single LWFT whose activation timestamp is the last `WorkflowTaskStarted` in
/// the chain (WFT3's). The workflow's first observation lands on the wrong
/// timestamp, the conditional branch is skipped, no timer command is emitted,
/// and the worker's `FailOnNondeterminismInterceptor` triggers on the
/// unmatched `TimerStarted` event in history.
#[tokio::test]
async fn wft_chunking_v2_la_heartbeat_keeps_wft1_distinct() {
    let mut t = TestHistoryBuilder::default();
    t.set_use_wft_chunking_v2();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task(); // WFT1 (events 2-4) — workflow starts LA, awaits
    t.add_full_wf_task(); // WFT2 (events 5-7) — LA-heartbeat (empty WFT)
    t.add_full_wf_task(); // WFT3 (events 8-10) — LA completes
    t.add_local_activity_result_marker(
        1,
        "1",
        temporalio_common::protos::coresdk::AsJsonPayloadExt::as_json_payload(&"hi!".to_string())
            .unwrap(),
    ); // event 11 (LA result marker, recorded as WFT3's command)
    t.add_full_wf_task(); // WFT4 (events 12-14) — workflow resumes, emits StartTimer
    let timer_id = t.add_timer_started("1".to_string()); // event 15
    t.add_timer_fired(timer_id, "1".to_string()); // event 16
    t.add_full_wf_task(); // WFT5 (events 17-19) — timer fires, workflow completes
    t.add_workflow_execution_completed(); // event 20
    t.set_wf_type("wft_chunking_v2_la_heartbeat");

    // Stamp WFT1 and WFT3's `WorkflowTaskStarted` events with deliberately distinct
    // wall-clock times so the workflow's `workflow_time()` observation can detect
    // which one its activation timestamp came from. `set_current_time` in
    // workflow_machines is monotonic, so we also push `WorkflowExecutionStarted` and
    // the no-op WFT2 events strictly below WFT1's target time — otherwise the
    // default `SystemTime::now()` event times (set when the events were appended)
    // would clamp the workflow clock to the present and our modifications would be
    // ignored. The events past WFT3 (LA marker, WFT4, timer, WFT5) keep their
    // default wall-clock times; they are only reached after the workflow's first
    // observation, so they don't affect this test's signal.
    let pre = std::time::UNIX_EPOCH + Duration::from_secs(WFT1_TIMESTAMP_SECS - 1);
    let wft1_start = std::time::UNIX_EPOCH + Duration::from_secs(WFT1_TIMESTAMP_SECS);
    let wft3_start = std::time::UNIX_EPOCH + Duration::from_secs(WFT3_TIMESTAMP_SECS);
    t.modify_event(1, |e| e.event_time = Some(pre.into())); // WFExecutionStarted
    t.modify_event(2, |e| e.event_time = Some(pre.into())); // WFT1.WFTScheduled
    t.modify_event(3, |e| e.event_time = Some(wft1_start.into())); // WFT1.WFTStarted
    t.modify_event(9, |e| e.event_time = Some(wft3_start.into())); // WFT3.WFTStarted

    let mut worker = replay_sdk_worker([test_hist_to_replay(t)]);
    worker.register_workflow::<WftChunkingV2LaHeartbeatWf>();
    worker.register_activities(crate::common::activity_functions::StdActivities);
    // `replay_sdk_worker` already installs `FailOnNondeterminismInterceptor`,
    // which is what catches the missing `StartTimer` command if chunking is wrong.
    worker.run().await.unwrap();
}

#[derive(Default)]
struct UniqueRunsCounter {
    runs: Arc<Mutex<HashSet<String>>>,
}
#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for UniqueRunsCounter {
    async fn on_workflow_activation_completion(&self, completion: &WorkflowActivationCompletion) {
        self.runs.lock().insert(completion.run_id.clone());
    }
}

#[derive(Default)]
struct UniqueShutdownWorker {
    runs: Arc<Mutex<HashSet<String>>>,
}
#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for UniqueShutdownWorker {
    fn on_shutdown(&self, _sdk_worker: &Worker) {
        // Assumed one worker per task queue.
        self.runs
            .lock()
            .insert(_sdk_worker.task_queue().to_string());
    }
}
