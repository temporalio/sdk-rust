use crate::common::{ActivationAssertionsInterceptor, CoreWfStarter, WorkflowHandleExt};
use std::{
    collections::{HashSet, VecDeque, hash_map::RandomState},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_client::{WorkflowSignalOptions, WorkflowStartOptions};
use temporalio_common::{
    data_converters::RawValue,
    protos::{
        VERSION_SEARCH_ATTR_KEY,
        constants::PATCH_MARKER_NAME,
        coresdk::{
            AsJsonPayloadExt, FromJsonPayloadExt,
            common::decode_change_marker_details,
            workflow_activation::{NotifyHasPatch, WorkflowActivationJob, workflow_activation_job},
        },
        temporal::api::{
            command::v1::{
                RecordMarkerCommandAttributes, ScheduleActivityTaskCommandAttributes,
                UpsertWorkflowSearchAttributesCommandAttributes, command::Attributes,
            },
            common::v1::ActivityType,
            enums::v1::{CommandType, EventType},
            history::v1::{
                ActivityTaskCompletedEventAttributes, ActivityTaskScheduledEventAttributes,
                ActivityTaskStartedEventAttributes, TimerFiredEventAttributes,
                history_event::Attributes as EventAttributes,
            },
        },
    },
};

use temporalio_macros::{activity_definitions, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, PatchActivationCallback, SyncWorkflowContext, WorkflowContext, WorkflowResult,
    activities::ActivityError,
};
use temporalio_sdk_core::{
    replay::{DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder},
    test_help::{CoreInternalFlags, MockPollCfg, ResponseType},
};
use tokio::{join, sync::Notify};

const MY_PATCH_ID: &str = "integ_test_change_name";
const ROLLOUT_PATCH_ID: &str = "rollout-patch";

#[workflow]
#[derive(Default)]
pub(crate) struct ChangesWf;

#[workflow_methods]
impl ChangesWf {
    #[run(name = "writes_change_markers")]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        if ctx.patched(MY_PATCH_ID) {
            ctx.timer(Duration::from_millis(100)).await;
        } else {
            ctx.timer(Duration::from_millis(200)).await;
        }
        ctx.timer(Duration::from_millis(200)).await;
        if ctx.patched(MY_PATCH_ID) {
            ctx.timer(Duration::from_millis(100)).await;
        } else {
            ctx.timer(Duration::from_millis(200)).await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn writes_change_markers() {
    let wf_name = "writes_change_markers";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_workflow::<ChangesWf>().unwrap();
    let mut worker = starter.worker().await;

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

/// This one simulates a run as if the worker had the "old" code, then it fails at the end as
/// a cheapo way of being re-run, at which point it runs with change checks and the "new" code.
#[workflow]
pub(crate) struct NoChangeThenChangeWf {
    did_die: Arc<AtomicBool>,
}

#[workflow_methods(factory_only)]
impl NoChangeThenChangeWf {
    #[run(name = "can_add_change_markers")]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        if ctx.state(|wf| wf.did_die.load(Ordering::Acquire)) {
            assert!(!ctx.patched(MY_PATCH_ID));
        }
        ctx.timer(Duration::from_millis(200)).await;
        ctx.timer(Duration::from_millis(200)).await;
        if ctx.state(|wf| wf.did_die.load(Ordering::Acquire)) {
            assert!(!ctx.patched(MY_PATCH_ID));
        }
        ctx.timer(Duration::from_millis(200)).await;

        if !ctx.state(|wf| wf.did_die.load(Ordering::Acquire)) {
            ctx.state(|wf| wf.did_die.store(true, Ordering::Release));
            ctx.force_task_fail(anyhow::anyhow!("i'm ded"));
        }
        Ok(())
    }
}

#[tokio::test]
async fn can_add_change_markers() {
    let wf_name = "can_add_change_markers";
    let mut starter = CoreWfStarter::new(wf_name);
    let did_die = Arc::new(AtomicBool::new(false));
    starter
        .sdk_config
        .register_workflow_with_factory(move || NoChangeThenChangeWf {
            did_die: did_die.clone(),
        })
        .unwrap();
    let mut worker = starter.worker().await;

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[workflow]
pub(crate) struct ReplayWithChangeMarkerWf {
    did_die: Arc<AtomicBool>,
}

#[workflow_methods(factory_only)]
impl ReplayWithChangeMarkerWf {
    #[run(name = "replaying_with_patch_marker")]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        assert!(ctx.patched(MY_PATCH_ID));
        ctx.timer(Duration::from_millis(200)).await;
        if !ctx.state(|wf| wf.did_die.load(Ordering::Acquire)) {
            ctx.state(|wf| wf.did_die.store(true, Ordering::Release));
            ctx.force_task_fail(anyhow::anyhow!("i'm ded"));
        }
        Ok(())
    }
}

#[tokio::test]
async fn replaying_with_patch_marker() {
    let wf_name = "replaying_with_patch_marker";
    let mut starter = CoreWfStarter::new(wf_name);
    let did_die = Arc::new(AtomicBool::new(false));
    starter
        .sdk_config
        .register_workflow_with_factory(move || ReplayWithChangeMarkerWf {
            did_die: did_die.clone(),
        })
        .unwrap();
    let mut worker = starter.worker().await;

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct PatchActivationTwiceWf;

#[workflow_methods]
impl PatchActivationTwiceWf {
    #[run(name = "patch_activation_twice")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<(bool, bool)> {
        let first = ctx.patched(ROLLOUT_PATCH_ID);
        ctx.timer(Duration::from_millis(1)).await;
        let second = ctx.patched(ROLLOUT_PATCH_ID);
        Ok((first, second))
    }
}

#[tokio::test]
async fn patch_activation_callback_is_memoized_across_replay() {
    let wf_name = "patch_activation_callback_is_memoized_across_replay";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.max_cached_workflows = 0;
    let callback_calls = Arc::new(AtomicUsize::new(0));
    let callback_calls_clone = callback_calls.clone();
    starter.sdk_config.patch_activation_callback = Some(Arc::new(move |_| {
        callback_calls_clone.fetch_add(1, Ordering::Relaxed);
        false
    }));
    starter
        .sdk_config
        .register_workflow::<PatchActivationTwiceWf>()
        .unwrap();
    let mut worker = starter.worker().await;
    let workflow_id = starter.get_task_queue().to_string();
    let handle = worker
        .submit_workflow(
            PatchActivationTwiceWf::run,
            (),
            WorkflowStartOptions::new(workflow_id.clone(), workflow_id).build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();

    assert_eq!(
        handle.get_result(Default::default()).await.unwrap(),
        (false, false)
    );
    assert_eq!(callback_calls.load(Ordering::Relaxed), 1);
    let history = handle
        .fetch_history(Default::default())
        .into_events()
        .await
        .unwrap();
    assert!(!history.iter().any(|event| matches!(
        &event.attributes,
        Some(EventAttributes::MarkerRecordedEventAttributes(attrs))
            if attrs.marker_name == PATCH_MARKER_NAME
    )));
}

#[workflow]
struct PatchActivationRolloutWf {
    ready: Arc<Notify>,
    released: bool,
}

#[workflow_methods(factory_only)]
impl PatchActivationRolloutWf {
    #[run(name = "patch_activation_rollout")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let patched = ctx.patched(ROLLOUT_PATCH_ID);
        ctx.timer(Duration::from_millis(1)).await;
        ctx.state(|wf| wf.ready.notify_one());
        ctx.wait_condition(|wf| wf.released).await?;
        Ok(if patched { "new" } else { "old" }.to_string())
    }

    #[signal]
    fn release(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.released = true;
    }
}

#[workflow]
#[derive(Default)]
struct PatchActivationOldRolloutWf {
    released: bool,
}

#[workflow_methods]
impl PatchActivationOldRolloutWf {
    #[run(name = "patch_activation_rollout")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        ctx.timer(Duration::from_millis(1)).await;
        ctx.wait_condition(|wf| wf.released).await?;
        Ok("old".to_string())
    }

    #[signal]
    fn release(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.released = true;
    }
}

#[tokio::test]
async fn declined_patch_can_roll_out_to_old_worker() {
    let wf_name = "declined_patch_can_roll_out_to_old_worker";
    let mut starter = CoreWfStarter::new(wf_name);
    let callback_calls = Arc::new(AtomicUsize::new(0));
    let callback_calls_clone = callback_calls.clone();
    let workflow_id = starter.get_task_queue().to_string();
    let expected_workflow_id = workflow_id.clone();
    let callback: PatchActivationCallback = Arc::new(move |input| {
        assert_eq!(input.workflow_info.workflow_id(), expected_workflow_id);
        assert_eq!(input.patch_id, ROLLOUT_PATCH_ID);
        callback_calls_clone.fetch_add(1, Ordering::Relaxed);
        false
    });
    starter.sdk_config.patch_activation_callback = Some(callback);
    let mut old_starter = starter.clone_no_worker();
    let ready = Arc::new(Notify::new());
    let ready_clone = ready.clone();
    starter
        .sdk_config
        .register_workflow_with_factory(move || PatchActivationRolloutWf {
            ready: ready_clone.clone(),
            released: false,
        })
        .unwrap();
    let mut worker = starter.worker().await;
    let handle = worker
        .submit_workflow(
            PatchActivationRolloutWf::run,
            (),
            WorkflowStartOptions::new(workflow_id.clone(), workflow_id.clone()).build(),
        )
        .await
        .unwrap();
    let core = worker.core_worker();
    let (run_result, ()) = join!(worker.inner_mut().run(), async {
        ready.notified().await;
        core.shutdown().await;
    });
    run_result.unwrap();
    assert_eq!(callback_calls.load(Ordering::Relaxed), 1);
    let history = handle
        .fetch_history(Default::default())
        .into_events()
        .await
        .unwrap();
    assert!(!history.iter().any(|event| matches!(
        &event.attributes,
        Some(EventAttributes::MarkerRecordedEventAttributes(attrs))
            if attrs.marker_name == PATCH_MARKER_NAME
    )));

    old_starter.sdk_config.patch_activation_callback = None;
    old_starter
        .sdk_config
        .register_workflow::<PatchActivationOldRolloutWf>()
        .unwrap();
    let mut old_worker = old_starter.worker().await;
    old_worker.expect_workflow_completion(workflow_id, handle.info().run_id.clone());
    let (signal_result, run_result) = join!(
        handle.signal(
            PatchActivationRolloutWf::release,
            (),
            WorkflowSignalOptions::default(),
        ),
        old_worker.run_until_done(),
    );
    signal_result.unwrap();
    run_result.unwrap();
    assert_eq!(handle.get_result(Default::default()).await.unwrap(), "old");
}

#[tokio::test]
async fn activated_patch_replays_without_consulting_declining_callback() {
    let wf_name = "activated_patch_replays_without_consulting_declining_callback";
    let mut starter = CoreWfStarter::new(wf_name);
    let activated_calls = Arc::new(AtomicUsize::new(0));
    let activated_calls_clone = activated_calls.clone();
    starter.sdk_config.patch_activation_callback = Some(Arc::new(move |_| {
        activated_calls_clone.fetch_add(1, Ordering::Relaxed);
        true
    }));
    let mut declining_starter = starter.clone_no_worker();
    let ready = Arc::new(Notify::new());
    let ready_clone = ready.clone();
    starter
        .sdk_config
        .register_workflow_with_factory(move || PatchActivationRolloutWf {
            ready: ready_clone.clone(),
            released: false,
        })
        .unwrap();
    let mut worker = starter.worker().await;
    let workflow_id = starter.get_task_queue().to_string();
    let handle = worker
        .submit_workflow(
            PatchActivationRolloutWf::run,
            (),
            WorkflowStartOptions::new(workflow_id.clone(), workflow_id.clone()).build(),
        )
        .await
        .unwrap();
    let core = worker.core_worker();
    let (run_result, ()) = join!(worker.inner_mut().run(), async {
        ready.notified().await;
        core.shutdown().await;
    });
    run_result.unwrap();
    assert_eq!(activated_calls.load(Ordering::Relaxed), 1);
    let history = handle
        .fetch_history(Default::default())
        .into_events()
        .await
        .unwrap();
    assert!(history.iter().any(|event| matches!(
        &event.attributes,
        Some(EventAttributes::MarkerRecordedEventAttributes(attrs))
            if attrs.marker_name == PATCH_MARKER_NAME
    )));

    let declining_calls = Arc::new(AtomicUsize::new(0));
    let declining_calls_clone = declining_calls.clone();
    declining_starter.sdk_config.patch_activation_callback = Some(Arc::new(move |_| {
        declining_calls_clone.fetch_add(1, Ordering::Relaxed);
        false
    }));
    declining_starter
        .sdk_config
        .register_workflow_with_factory(move || PatchActivationRolloutWf {
            ready: Arc::new(Notify::new()),
            released: false,
        })
        .unwrap();
    let mut declining_worker = declining_starter.worker().await;
    declining_worker.expect_workflow_completion(workflow_id, handle.info().run_id.clone());
    let (signal_result, run_result) = join!(
        handle.signal(
            PatchActivationRolloutWf::release,
            (),
            WorkflowSignalOptions::default(),
        ),
        declining_worker.run_until_done(),
    );
    signal_result.unwrap();
    run_result.unwrap();
    assert_eq!(handle.get_result(Default::default()).await.unwrap(), "new");
    assert_eq!(declining_calls.load(Ordering::Relaxed), 0);
}

/// Test that the internal patching mechanism works on the second workflow task when replaying.
/// Used as regression test for a bug that detected that we did not look ahead far enough to find
/// the next workflow task completion, which the flags are attached to.
#[workflow]
struct TimerPatchedTimerWf {
    fail_once: Arc<AtomicBool>,
}

#[workflow_methods(factory_only)]
impl TimerPatchedTimerWf {
    #[run(name = "timer_patched_timer")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.timer(Duration::from_millis(1)).await;
        if ctx.state(|wf| wf.fail_once.load(Ordering::Acquire)) {
            ctx.state(|wf| wf.fail_once.store(false, Ordering::Release));
            panic!("Enchi is hungry!");
        }
        assert!(ctx.patched(MY_PATCH_ID));
        ctx.timer(Duration::from_millis(1)).await;
        Ok(())
    }
}

#[tokio::test]
async fn patched_on_second_workflow_task_is_deterministic() {
    let wf_name = "timer_patched_timer";
    let mut starter = CoreWfStarter::new(wf_name);
    // Disable caching to force replay from beginning
    starter.sdk_config.max_cached_workflows = 0_usize;
    let fail_once = Arc::new(AtomicBool::new(true));
    starter
        .sdk_config
        .register_workflow_with_factory(move || TimerPatchedTimerWf {
            fail_once: fail_once.clone(),
        })
        .unwrap();
    let mut worker = starter.worker().await;

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[workflow]
struct RemoveDeprecatedPatchNearOtherPatchWf {
    did_die: Arc<AtomicBool>,
}

#[workflow_methods(factory_only)]
impl RemoveDeprecatedPatchNearOtherPatchWf {
    #[run(name = "can_add_change_markers")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.timer(Duration::from_millis(200)).await;
        if !ctx.state(|wf| wf.did_die.load(Ordering::Acquire)) {
            assert!(ctx.deprecate_patch("getting-deprecated"));
            assert!(ctx.patched("staying"));
        } else {
            assert!(ctx.patched("staying"));
        }
        ctx.timer(Duration::from_millis(200)).await;

        if !ctx.state(|wf| wf.did_die.load(Ordering::Acquire)) {
            ctx.state(|wf| wf.did_die.store(true, Ordering::Release));
            ctx.force_task_fail(anyhow::anyhow!("i'm ded"));
        }
        Ok(())
    }
}

#[tokio::test]
async fn can_remove_deprecated_patch_near_other_patch() {
    let wf_name = "can_add_change_markers";
    let mut starter = CoreWfStarter::new(wf_name);
    let did_die = Arc::new(AtomicBool::new(false));
    starter
        .sdk_config
        .register_workflow_with_factory(move || RemoveDeprecatedPatchNearOtherPatchWf {
            did_die: did_die.clone(),
        })
        .unwrap();
    let mut worker = starter.worker().await;

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[workflow]
struct DeprecatedPatchRemovalWf {
    did_die: Arc<AtomicBool>,
    notify: Arc<Notify>,
    signal_received: bool,
}

#[workflow_methods(factory_only)]
impl DeprecatedPatchRemovalWf {
    #[run(name = "deprecated_patch_removal")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        if !ctx.state(|wf| wf.did_die.load(Ordering::Acquire)) {
            assert!(ctx.deprecate_patch("getting-deprecated"));
        }
        ctx.state(|wf| wf.notify.notify_one());
        ctx.wait_condition(|s| s.signal_received).await?;

        ctx.timer(Duration::from_millis(1)).await;

        if !ctx.state(|wf| wf.did_die.load(Ordering::Acquire)) {
            ctx.state(|wf| wf.did_die.store(true, Ordering::Release));
            ctx.force_task_fail(anyhow::anyhow!("i'm ded"));
        }
        Ok(())
    }

    #[signal]
    fn handle_sig(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.signal_received = true;
    }
}

#[tokio::test]
async fn deprecated_patch_removal() {
    let wf_name = "deprecated_patch_removal";
    let mut starter = CoreWfStarter::new(wf_name);
    let wf_id = starter.get_task_queue().to_string();
    let did_die = Arc::new(AtomicBool::new(false));
    let send_sig = Arc::new(Notify::new());
    let send_sig_clone = send_sig.clone();
    starter
        .sdk_config
        .register_workflow_with_factory(move || DeprecatedPatchRemovalWf {
            did_die: did_die.clone(),
            notify: send_sig_clone.clone(),
            signal_received: false,
        })
        .unwrap();
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            DeprecatedPatchRemovalWf::run,
            (),
            WorkflowStartOptions::new(wf_id.clone(), wf_id).build(),
        )
        .await
        .unwrap();
    let sig_fut = async {
        send_sig.notified().await;
        handle
            .signal(
                DeprecatedPatchRemovalWf::handle_sig,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap()
    };
    let run_fut = async {
        worker.run_until_done().await.unwrap();
    };
    join!(sig_fut, run_fut);
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
enum MarkerType {
    Deprecated,
    NotDeprecated,
    NoMarker,
}

const ONE_SECOND: Duration = Duration::from_secs(1);

/// EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
/// EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// EVENT_TYPE_WORKFLOW_TASK_STARTED
/// EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// EVENT_TYPE_MARKER_RECORDED (depending on marker_type)
/// EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
/// EVENT_TYPE_ACTIVITY_TASK_STARTED
/// EVENT_TYPE_ACTIVITY_TASK_COMPLETED
/// EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
/// EVENT_TYPE_WORKFLOW_TASK_STARTED
/// EVENT_TYPE_WORKFLOW_TASK_COMPLETED
/// EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
fn patch_marker_single_activity(
    marker_type: MarkerType,
    version: usize,
    replay: bool,
) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.set_flags_first_wft(&[CoreInternalFlags::UpsertSearchAttributeOnPatch], &[]);
    match marker_type {
        MarkerType::Deprecated => {
            t.add_has_change_marker(MY_PATCH_ID, true);
            t.add_upsert_search_attrs_for_patch(&[MY_PATCH_ID.to_string()]);
        }
        MarkerType::NotDeprecated => {
            t.add_has_change_marker(MY_PATCH_ID, false);
            t.add_upsert_search_attrs_for_patch(&[MY_PATCH_ID.to_string()]);
        }
        MarkerType::NoMarker => {}
    };

    let activity_id = if replay {
        match (marker_type, version) {
            (_, 1) => "no_change",
            (MarkerType::NotDeprecated, 2) => "had_change",
            (MarkerType::Deprecated, 2) => "had_change",
            (MarkerType::NoMarker, 2) => "no_change",
            (_, 3) => "had_change",
            (_, 4) => "had_change",
            v => panic!("Nonsense marker / version combo {v:?}"),
        }
    } else {
        // If the workflow isn't replaying (we're creating history here for a workflow which
        // wasn't replaying at the time of scheduling the activity, and has done that, and now
        // we're feeding back the history it would have produced) then it always has the change,
        // except in v1.
        if version > 1 {
            "had_change"
        } else {
            "no_change"
        }
    };

    let scheduled_event_id = t.add(ActivityTaskScheduledEventAttributes {
        activity_id: activity_id.to_string(),
        ..Default::default()
    });
    let started_event_id = t.add(ActivityTaskStartedEventAttributes {
        scheduled_event_id,
        ..Default::default()
    });
    t.add(ActivityTaskCompletedEventAttributes {
        scheduled_event_id,
        started_event_id,
        ..Default::default()
    });
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    t
}

struct FakeAct;
#[activity_definitions]
impl FakeAct {
    #[activity(name = "")]
    fn nameless() -> Result<RawValue, ActivityError> {
        unimplemented!()
    }
}

async fn v1(ctx: &mut WorkflowContext<PatchWf>) {
    let _ = ctx
        .execute_activity(
            FakeAct::nameless,
            (),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                .activity_id("no_change".to_owned())
                .build(),
        )
        .await;
}

async fn v2(ctx: &mut WorkflowContext<PatchWf>) -> bool {
    if ctx.patched(MY_PATCH_ID) {
        let _ = ctx
            .execute_activity(
                FakeAct::nameless,
                (),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                    .activity_id("had_change".to_owned())
                    .build(),
            )
            .await;
        true
    } else {
        let _ = ctx
            .execute_activity(
                FakeAct::nameless,
                (),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                    .activity_id("no_change".to_owned())
                    .build(),
            )
            .await;
        false
    }
}

async fn v3(ctx: &mut WorkflowContext<PatchWf>) {
    ctx.deprecate_patch(MY_PATCH_ID);
    let _ = ctx
        .execute_activity(
            FakeAct::nameless,
            (),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                .activity_id("had_change".to_owned())
                .build(),
        )
        .await;
}

async fn v4(ctx: &mut WorkflowContext<PatchWf>) {
    let _ = ctx
        .execute_activity(
            FakeAct::nameless,
            (),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                .activity_id("had_change".to_owned())
                .build(),
        )
        .await;
}

fn patch_setup(replaying: bool, marker_type: MarkerType, workflow_version: usize) -> MockPollCfg {
    let t = patch_marker_single_activity(marker_type, workflow_version, replaying);
    if replaying {
        MockPollCfg::from_resps(t, [ResponseType::AllHistory])
    } else {
        MockPollCfg::from_hist_builder(t)
    }
}

#[workflow]
struct PatchWf {
    version: usize,
}

#[workflow_methods(factory_only)]
impl PatchWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        match ctx.state(|wf| wf.version) {
            1 => {
                v1(ctx).await;
            }
            2 => {
                v2(ctx).await;
            }
            3 => {
                v3(ctx).await;
            }
            4 => {
                v4(ctx).await;
            }
            _ => panic!("Invalid workflow version for test setup"),
        }
        Ok(())
    }
}

#[rstest]
#[case::v1_breaks_on_normal_marker(false, MarkerType::NotDeprecated, 1)]
#[case::v1_accepts_dep_marker(false, MarkerType::Deprecated, 1)]
#[case::v1_replay_breaks_on_normal_marker(true, MarkerType::NotDeprecated, 1)]
#[case::v1_replay_accepts_dep_marker(true, MarkerType::Deprecated, 1)]
#[case::v4_breaks_on_normal_marker(false, MarkerType::NotDeprecated, 4)]
#[case::v4_accepts_dep_marker(false, MarkerType::Deprecated, 4)]
#[case::v4_replay_breaks_on_normal_marker(true, MarkerType::NotDeprecated, 4)]
#[case::v4_replay_accepts_dep_marker(true, MarkerType::Deprecated, 4)]
#[tokio::test]
async fn v1_and_v4_changes(
    #[case] replaying: bool,
    #[case] marker_type: MarkerType,
    #[case] wf_version: usize,
) {
    let mut mock_cfg = patch_setup(replaying, marker_type, wf_version);

    if marker_type != MarkerType::Deprecated {
        // should explode b/c non-dep marker is present
        mock_cfg.num_expected_fails = 1;
    }

    let mut aai = ActivationAssertionsInterceptor::default();
    aai.skip_one().then(move |a| {
        if marker_type == MarkerType::Deprecated {
            // Activity is resolved
            assert_matches!(
                a.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(_))
                }]
            );
        }
    });

    if !replaying {
        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts.then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type,
                    CommandType::ScheduleActivityTask as i32
                );
            });
        });
    }

    let mut worker =
        crate::common::build_fake_sdk_intercepted_with_options(mock_cfg, aai, |options| {
            options
                .register_workflow_with_factory(move || PatchWf {
                    version: wf_version,
                })
                .unwrap();
        });
    worker.run().await.unwrap();
}

// Note that the not-replaying and no-marker cases don't make sense and hence are absent
#[rstest]
#[case::v2_marker_new_path(false, MarkerType::NotDeprecated, 2)]
#[case::v2_dep_marker_new_path(false, MarkerType::Deprecated, 2)]
#[case::v2_replay_no_marker_old_path(true, MarkerType::NoMarker, 2)]
#[case::v2_replay_marker_new_path(true, MarkerType::NotDeprecated, 2)]
#[case::v2_replay_dep_marker_new_path(true, MarkerType::Deprecated, 2)]
#[case::v3_marker_new_path(false, MarkerType::NotDeprecated, 3)]
#[case::v3_dep_marker_new_path(false, MarkerType::Deprecated, 3)]
#[case::v3_replay_no_marker_old_path(true, MarkerType::NoMarker, 3)]
#[case::v3_replay_marker_new_path(true, MarkerType::NotDeprecated, 3)]
#[case::v3_replay_dep_marker_new_path(true, MarkerType::Deprecated, 3)]
#[tokio::test]
async fn v2_and_v3_changes(
    #[case] replaying: bool,
    #[case] marker_type: MarkerType,
    #[case] wf_version: usize,
) {
    let mut mock_cfg = patch_setup(replaying, marker_type, wf_version);

    let mut aai = ActivationAssertionsInterceptor::default();
    aai.then(move |act| {
        // replaying cases should immediately get a resolve change activation when marker is
        // present
        if replaying && marker_type != MarkerType::NoMarker {
            assert_matches!(
                &act.jobs[1],
                 WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::NotifyHasPatch(
                        NotifyHasPatch {
                            patch_id,
                        }
                    ))
                } => patch_id == MY_PATCH_ID
            );
        } else {
            assert_eq!(act.jobs.len(), 1);
        }
    })
    .then(move |act| {
        assert_matches!(
            act.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_))
            }]
        );
    });

    if !replaying {
        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts.then(move |wft| {
                let mut commands = VecDeque::from(wft.commands.clone());
                let expected_num_cmds = if marker_type == MarkerType::NoMarker {
                    2
                } else {
                    3
                };
                assert_eq!(commands.len(), expected_num_cmds);
                let dep_flag_expected = wf_version != 2;
                assert_matches!(
                    commands.pop_front().unwrap().attributes.as_ref().unwrap(),
                    Attributes::RecordMarkerCommandAttributes(
                        RecordMarkerCommandAttributes { marker_name, details,.. })
                    if marker_name == PATCH_MARKER_NAME
                      && decode_change_marker_details(details).unwrap().1 == dep_flag_expected
                );
                if expected_num_cmds == 3 {
                    let mut as_payload = [MY_PATCH_ID].as_json_payload().unwrap();
                    as_payload
                        .metadata
                        .insert("type".to_string(), "KeywordList".as_bytes().to_vec());
                    assert_matches!(
                        commands.pop_front().unwrap().attributes.as_ref().unwrap(),
                        Attributes::UpsertWorkflowSearchAttributesCommandAttributes(
                            UpsertWorkflowSearchAttributesCommandAttributes
                            { search_attributes: Some(attrs) }
                        )
                        if attrs.indexed_fields.get(VERSION_SEARCH_ATTR_KEY).unwrap()
                          == &as_payload
                    );
                }
                // The only time the "old" timer should fire is in v2, replaying, without a marker.
                let expected_activity_id =
                    if replaying && marker_type == MarkerType::NoMarker && wf_version == 2 {
                        "no_change"
                    } else {
                        "had_change"
                    };
                assert_matches!(
                    commands.pop_front().unwrap().attributes.as_ref().unwrap(),
                    Attributes::ScheduleActivityTaskCommandAttributes(
                        ScheduleActivityTaskCommandAttributes { activity_id, .. }
                    )
                    if activity_id == expected_activity_id
                );
            });
        });
    }

    let mut worker =
        crate::common::build_fake_sdk_intercepted_with_options(mock_cfg, aai, |options| {
            options
                .register_workflow_with_factory(move || PatchWf {
                    version: wf_version,
                })
                .unwrap();
        });
    worker.run().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct SameChangeMultipleSpotsWf;

#[workflow_methods]
impl SameChangeMultipleSpotsWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        if ctx.patched(MY_PATCH_ID) {
            let _ = ctx
                .execute_activity(
                    FakeAct::nameless,
                    (),
                    ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
                )
                .await;
        } else {
            ctx.timer(ONE_SECOND).await;
        }
        ctx.timer(ONE_SECOND).await;
        if ctx.patched(MY_PATCH_ID) {
            let _ = ctx
                .execute_activity(
                    FakeAct::nameless,
                    (),
                    ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
                )
                .await;
        } else {
            ctx.timer(ONE_SECOND).await;
        }
        Ok(())
    }
}

#[rstest]
#[case::has_change_replay(true, true)]
#[case::no_change_replay(false, true)]
#[case::has_change_inc(true, false)]
// The false-false case doesn't make sense, as the incremental cases act as if working against
// a sticky queue, and it'd be impossible for a worker with the call to get an incremental
// history that then suddenly doesn't have the marker.
#[tokio::test]
async fn same_change_multiple_spots(#[case] have_marker_in_hist: bool, #[case] replay: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.set_flags_first_wft(&[CoreInternalFlags::UpsertSearchAttributeOnPatch], &[]);
    if have_marker_in_hist {
        t.add_has_change_marker(MY_PATCH_ID, false);
        t.add_upsert_search_attrs_for_patch(&[MY_PATCH_ID.to_string()]);
        let scheduled_event_id = t.add(ActivityTaskScheduledEventAttributes {
            activity_id: "1".to_owned(),
            activity_type: Some(ActivityType {
                name: "".to_string(),
            }),
            ..Default::default()
        });
        let started_event_id = t.add(ActivityTaskStartedEventAttributes {
            scheduled_event_id,
            ..Default::default()
        });
        t.add(ActivityTaskCompletedEventAttributes {
            scheduled_event_id,
            started_event_id,
            ..Default::default()
        });
        t.add_full_wf_task();
        let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add(TimerFiredEventAttributes {
            started_event_id: timer_started_event_id,
            timer_id: "1".to_owned(),
        });
    } else {
        let started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add(TimerFiredEventAttributes {
            started_event_id,
            timer_id: "1".to_owned(),
        });
        t.add_full_wf_task();
        let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add(TimerFiredEventAttributes {
            started_event_id: timer_started_event_id,
            timer_id: "2".to_owned(),
        });
    }
    t.add_full_wf_task();

    if have_marker_in_hist {
        let scheduled_event_id = t.add(ActivityTaskScheduledEventAttributes {
            activity_id: "2".to_string(),
            activity_type: Some(ActivityType {
                name: "".to_string(),
            }),
            ..Default::default()
        });
        let started_event_id = t.add(ActivityTaskStartedEventAttributes {
            scheduled_event_id,
            ..Default::default()
        });
        t.add(ActivityTaskCompletedEventAttributes {
            scheduled_event_id,
            started_event_id,
            ..Default::default()
        });
    } else {
        let started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add(TimerFiredEventAttributes {
            started_event_id,
            timer_id: "3".to_owned(),
        });
    }
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mock_cfg = if replay {
        MockPollCfg::from_resps(t, [ResponseType::AllHistory])
    } else {
        MockPollCfg::from_hist_builder(t)
    };

    let mut worker = crate::common::build_fake_sdk_with_options(mock_cfg, |options| {
        options
            .register_workflow::<SameChangeMultipleSpotsWf>()
            .unwrap();
    });
    worker.run().await.unwrap();
}

const SIZE_OVERFLOW_PATCH_AMOUNT: usize = 180;

#[workflow]
struct ManyPatchesWf {
    num_patches: usize,
}

#[workflow_methods(factory_only)]
impl ManyPatchesWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        for i in 1..=ctx.state(|wf| wf.num_patches) {
            let _dontcare = ctx.patched(&format!("patch-{i}"));
            ctx.timer(ONE_SECOND).await;
        }
        Ok(())
    }
}

#[rstest]
#[case::happy_path(50)]
// We start exceeding the 2k size limit at 180 patches with this format
#[case::size_overflow(SIZE_OVERFLOW_PATCH_AMOUNT)]
#[tokio::test]
async fn many_patches_combine_in_search_attrib_update(#[case] num_patches: usize) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.set_flags_first_wft(&[CoreInternalFlags::UpsertSearchAttributeOnPatch], &[]);
    for i in 1..=num_patches {
        let id = format!("patch-{i}");
        t.add_has_change_marker(&id, false);
        if i < SIZE_OVERFLOW_PATCH_AMOUNT {
            t.add_upsert_search_attrs_for_patch(&[id]);
        }
        let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add(TimerFiredEventAttributes {
            started_event_id: timer_started_event_id,
            timer_id: i.to_string(),
        });
        t.add_full_wf_task();
    }
    t.add_workflow_execution_completed();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        // Iterate through all activations/responses except the final one with complete workflow
        for i in 2..=num_patches + 1 {
            asserts.then(move |wft| {
                let cmds = &wft.commands;
                if i > SIZE_OVERFLOW_PATCH_AMOUNT {
                    assert_eq!(2, cmds.len());
                    assert_matches!(cmds[1].command_type(), CommandType::StartTimer);
                } else {
                    assert_eq!(3, cmds.len());
                    let attrs = assert_matches!(
                        cmds[1].attributes.as_ref().unwrap(),
                        Attributes::UpsertWorkflowSearchAttributesCommandAttributes(
                            UpsertWorkflowSearchAttributesCommandAttributes
                            { search_attributes: Some(attrs) }
                        ) => attrs
                    );
                    let expected_patches: HashSet<String, _> =
                        (1..i).map(|i| format!("patch-{i}")).collect();
                    let deserialized = HashSet::<String, RandomState>::from_json_payload(
                        attrs.indexed_fields.get(VERSION_SEARCH_ATTR_KEY).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(deserialized, expected_patches);
                }
            });
        }
    });

    let mut worker = crate::common::build_fake_sdk_with_options(mock_cfg, |options| {
        options
            .register_workflow_with_factory(move || ManyPatchesWf { num_patches })
            .unwrap();
    });
    worker.run().await.unwrap();
}

const MANY_PATCHES_IN_ONE_WFT_COUNT: usize = 200;

#[workflow]
#[derive(Default)]
struct ManyPatchesInOneWftWf;

#[workflow_methods]
impl ManyPatchesInOneWftWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        for i in 1..=MANY_PATCHES_IN_ONE_WFT_COUNT {
            let _ = ctx.patched(&format!("patch-{i}"));
        }
        ctx.timer(Duration::from_millis(1)).await;
        Ok(())
    }
}

// The main difference with many_patches_combine_in_search_attrib_update are that
// this one creates multiple patches in a single WFT, rather than spread them out
// over multiple WFTs. See https://github.com/temporalio/sdk-core/issues/1223.
#[tokio::test]
async fn patch_marker_size_overflow_replay_is_deterministic() {
    let wf_name = "patch_marker_size_overflow_replay_is_deterministic";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<ManyPatchesInOneWftWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ManyPatchesInOneWftWf::run,
            (),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    // Confirm that the original execution did in fact hit the size limit: the last upsert SA
    // event in history must contain fewer than the total number of patches issued by the workflow.
    let history = handle
        .fetch_history(Default::default())
        .into_events()
        .await
        .unwrap();
    let last_upsert_patches = history
        .iter()
        .rev()
        .find_map(|e| match &e.attributes {
            Some(EventAttributes::UpsertWorkflowSearchAttributesEventAttributes(a)) => a
                .search_attributes
                .as_ref()
                .and_then(|sa| sa.indexed_fields.get(VERSION_SEARCH_ATTR_KEY))
                .map(|p| HashSet::<String, RandomState>::from_json_payload(p).unwrap()),
            _ => None,
        })
        .expect("history should contain at least one UpsertWorkflowSearchAttributes event");
    assert!(
        last_upsert_patches.len() < MANY_PATCHES_IN_ONE_WFT_COUNT,
        "expected the last upsert SA event to be missing patches due to size overflow, \
         but it contained all {MANY_PATCHES_IN_ONE_WFT_COUNT} of them",
    );

    // Replay the workflow from the fetched history. This must succeed: the SDK must produce the
    // same sequence of upsert SA commands during replay as it did during the original execution.
    handle.fetch_history_and_replay(&mut worker).await.unwrap();
}
