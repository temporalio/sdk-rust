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
    Worker, WorkflowContext, WorkflowContextView, WorkflowResult, interceptors::WorkerInterceptor,
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
    worker.register_workflow::<TimersWf>().unwrap();
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
    worker.register_workflow::<TimersWf>().unwrap();

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
    worker.register_workflow::<TimersWf>().unwrap();
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
    worker.register_workflow::<TimersWf>().unwrap();
    worker.run().await.unwrap();
}

async fn replay_abrupt_ending(mut t: TestHistoryBuilder) {
    t.set_wf_input(1u32.as_json_payload().unwrap());
    let mut worker = replay_sdk_worker([test_hist_to_replay(t)]);
    worker.register_workflow::<TimersWf>().unwrap();
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
    worker.register_workflow::<TimersWf>().unwrap();
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
    worker.register_workflow::<OneTimerWf>().unwrap();
    worker.register_workflow::<SeqTimerWf>().unwrap();

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
    worker.register_workflow::<OneTimerWf>().unwrap();
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
    worker.register_workflow::<ChangesWf>().unwrap();
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
