use crate::{
    common::{
        CoreWfStarter, activity_functions::StdActivities, fake_grpc_server::fake_server,
        get_integ_runtime_options, get_integ_server_options, get_integ_telem_options, mock_sdk_cfg,
    },
    shared_tests::{self, is_oversize_grpc_event},
};
use assert_matches::assert_matches;
use futures_util::FutureExt;
use std::{
    cell::Cell,
    sync::{
        Arc, Mutex,
        atomic::{
            AtomicBool, AtomicU8,
            Ordering::{self, Relaxed},
        },
    },
    time::Duration,
};
use temporalio_client::{Connection, WorkflowStartOptions};
use temporalio_common::{
    data_converters::{DataConverter, RawValue},
    protos::{
        coresdk::{
            ActivityTaskCompletion,
            activity_result::ActivityExecutionResult,
            workflow_completion::{
                Failure, WorkflowActivationCompletion, workflow_activation_completion::Status,
            },
        },
        temporal::api::{
            command::v1::command::Attributes,
            common::v1::WorkerVersionStamp,
            enums::v1::{
                EventType,
                WorkflowTaskFailedCause::{self},
            },
            failure::v1::Failure as InnerFailure,
            history::v1::{
                ActivityTaskScheduledEventAttributes,
                history_event::{
                    self,
                    Attributes::{self as EventAttributes},
                },
            },
            workflowservice::v1::{
                GetWorkflowExecutionHistoryResponse, PollActivityTaskQueueResponse,
                RespondActivityTaskCompletedResponse,
            },
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, LocalActivityOptions, WorkerOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
    interceptors::WorkerInterceptor,
};
use temporalio_sdk_core::{
    ActivitySlotKind, CoreRuntime, LocalActivitySlotKind, PollError, PollerBehavior,
    ResourceBasedTuner, ResourceSlotOptions, SlotInfo, SlotInfoTrait, SlotMarkUsedContext,
    SlotReleaseContext, SlotReservationContext, SlotSupplier, SlotSupplierPermit, TunerBuilder,
    WorkerConfig, WorkerValidationError, WorkerVersioningStrategy, WorkflowSlotKind, init_worker,
    replay::{DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder, canned_histories},
    test_help::{
        FakeWfResponses, MockPollCfg, ResponseType, build_mock_pollers, drain_pollers_and_shutdown,
        hist_to_poll_resp, mock_worker, mock_worker_client,
    },
};
use tokio::sync::{Barrier, Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
async fn worker_validation_fails_on_nonexistent_namespace() {
    let mut opts = get_integ_server_options();
    let runtime =
        CoreRuntime::new_assume_tokio(get_integ_runtime_options(get_integ_telem_options()))
            .unwrap();
    opts.metrics_meter = runtime.telemetry().get_temporal_metric_meter();
    let connection = Connection::connect(opts).await.unwrap();

    let worker = init_worker(
        &runtime,
        WorkerConfig::builder()
            .namespace("i_dont_exist")
            .task_queue("Wheee!")
            .versioning_strategy(WorkerVersioningStrategy::None {
                build_id: "blah".to_owned(),
            })
            .task_types(WorkerTaskTypes::all())
            .build()
            .unwrap(),
        connection,
    )
    .unwrap();

    let res = worker.validate().await;
    assert_matches!(
        res,
        Err(WorkerValidationError::NamespaceDescribeError { .. })
    );
}

#[tokio::test]
async fn worker_handles_unknown_workflow_types_gracefully() {
    let wf_type = "worker_handles_unknown_workflow_types_gracefully";
    let mut starter = CoreWfStarter::new(wf_type);
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let wf_id = format!("wce-{}", Uuid::new_v4());
    let run_id = worker
        .submit_wf(
            "unregistered".to_string(),
            vec![],
            WorkflowStartOptions::new(task_queue, wf_id).build(),
        )
        .await
        .unwrap();

    struct GracefulAsserter {
        notify: Arc<Notify>,
        run_id: String,
        unregistered_failure_seen: Cell<bool>,
    }
    #[async_trait::async_trait(?Send)]
    impl WorkerInterceptor for GracefulAsserter {
        async fn on_workflow_activation_completion(
            &self,
            completion: &WorkflowActivationCompletion,
        ) {
            if matches!(
                completion,
                WorkflowActivationCompletion {
                    status: Some(Status::Failed(Failure {
                        failure: Some(InnerFailure { message, .. }),
                        ..
                    })),
                    run_id,
                } if message == "Workflow type unregistered not found" && *run_id == self.run_id
            ) {
                self.unregistered_failure_seen.set(true);
            }
            // If we've seen the failure, and the completion is a success for the same run, we're done
            if matches!(
                completion,
                WorkflowActivationCompletion {
                    status: Some(Status::Successful(..)),
                    run_id,
                } if self.unregistered_failure_seen.get() && *run_id == self.run_id
            ) {
                // Shutdown the worker
                self.notify.notify_one();
            }
        }
        fn on_shutdown(&self, _: &temporalio_sdk::Worker) {}
    }

    let inner = worker.inner_mut();
    let notify = Arc::new(Notify::new());
    inner.set_worker_interceptor(GracefulAsserter {
        notify: notify.clone(),
        run_id,
        unregistered_failure_seen: Cell::new(false),
    });
    tokio::join!(async { inner.run().await.unwrap() }, async move {
        notify.notified().await;
        let worker = starter.get_worker().await.clone();
        drain_pollers_and_shutdown(&worker).await;
    });
}

#[workflow]
#[derive(Default)]
struct ResourceBasedNonStickyWf;

#[workflow_methods]
impl ResourceBasedNonStickyWf {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn resource_based_few_pollers_guarantees_non_sticky_poll() {
    let wf_name = "resource_based_few_pollers_guarantees_non_sticky_poll";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    // 3 pollers so the minimum slots of 2 can both be handed out to a sticky poller
    starter.sdk_config.workflow_task_poller_behavior = PollerBehavior::SimpleMaximum(3_usize);
    // Set the limits to zero so it's essentially unwilling to hand out slots
    let mut tuner = ResourceBasedTuner::new(0.0, 0.0);
    tuner.with_workflow_slots_options(ResourceSlotOptions::new(2, 10, Duration::from_millis(0)));
    starter.sdk_config.tuner = Arc::new(tuner);
    let mut worker = starter.worker().await;

    // Workflow doesn't actually need to do anything. We just need to see that we don't get stuck
    // by assigning all slots to sticky pollers.
    worker
        .register_workflow::<ResourceBasedNonStickyWf>()
        .unwrap();
    let task_queue = starter.get_task_queue().to_owned();
    for i in 0..20 {
        worker
            .submit_workflow(
                ResourceBasedNonStickyWf::run,
                (),
                WorkflowStartOptions::new(task_queue.clone(), format!("{wf_name}_{i}")).build(),
            )
            .await
            .unwrap();
    }
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn oversize_grpc_message() {
    use crate::common::{NAMESPACE, prom_metrics};
    let wf_name = "oversize_grpc_message";
    // Enable Prometheus metrics for this test and capture the address
    let (telemopts, addr, _aborter) = prom_metrics(None);
    let runtime = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime(wf_name, runtime);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut core = starter.worker().await;

    let has_run = Arc::new(AtomicBool::new(false));
    let has_run_clone = has_run.clone();

    #[workflow]
    struct OversizeGrpcMessageWf {
        has_run: Arc<AtomicBool>,
    }

    #[workflow_methods(factory_only)]
    impl OversizeGrpcMessageWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<Vec<u8>> {
            if ctx.state(|wf| wf.has_run.load(Relaxed)) {
                Ok(vec![])
            } else {
                ctx.state(|wf| wf.has_run.store(true, Relaxed));
                let result: Vec<u8> = vec![0; 5000000];
                Ok(result)
            }
        }
    }

    core.register_workflow_with_factory(move || OversizeGrpcMessageWf {
        has_run: has_run_clone.clone(),
    })
    .unwrap();
    starter
        .start_with_worker(OversizeGrpcMessageWf::name(), &mut core)
        .await;
    core.run_until_done().await.unwrap();

    assert!(
        starter
            .get_history()
            .await
            .events
            .iter()
            .any(is_oversize_grpc_event)
    );

    // Verify the workflow task failure metric includes the GrpcMessageTooLarge reason
    let tq = starter.get_task_queue();
    crate::common::eventually(
        || async {
            let body =
                crate::integ_tests::metrics_tests::get_text(format!("http://{addr}/metrics")).await;
            if body.lines().any(|line| {
                line.starts_with("temporal_workflow_task_execution_failed{")
                    && line.contains("failure_reason=\"GrpcMessageTooLarge\"")
                    && line.contains(&format!("namespace=\"{NAMESPACE}\""))
                    && line.contains("service_name=\"temporal-core-sdk\"")
                    && line.contains(&format!("task_queue=\"{tq}\""))
                    && line.ends_with(" 1")
            }) {
                Ok(())
            } else {
                Err(())
            }
        },
        Duration::from_secs(2),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn grpc_message_too_large_test() {
    shared_tests::grpc_message_too_large().await
}

#[tokio::test]
async fn activity_tasks_from_completion_reserve_slots() {
    let wf_id = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let schedid = t.add(EventAttributes::ActivityTaskScheduledEventAttributes(
        ActivityTaskScheduledEventAttributes {
            activity_id: "1".to_string(),
            activity_type: Some("act1".into()),
            ..Default::default()
        },
    ));
    let startid = t.add_activity_task_started(schedid);
    t.add_activity_task_completed(schedid, startid, b"hi".into());
    t.add_full_wf_task();
    let schedid = t.add(EventAttributes::ActivityTaskScheduledEventAttributes(
        ActivityTaskScheduledEventAttributes {
            activity_id: "2".to_string(),
            activity_type: Some("act2".into()),
            ..Default::default()
        },
    ));
    let startid = t.add_activity_task_started(schedid);
    t.add_activity_task_completed(schedid, startid, b"hi".into());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock = mock_worker_client();
    // Set up two tasks to be returned via normal activity polling
    let act_tasks = vec![
        PollActivityTaskQueueResponse {
            task_token: vec![1],
            activity_id: "act1".to_string(),
            ..Default::default()
        }
        .into(),
        PollActivityTaskQueueResponse {
            task_token: vec![2],
            activity_id: "act2".to_string(),
            ..Default::default()
        }
        .into(),
    ];
    mock.expect_complete_activity_task()
        .times(2)
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));
    let barr: &'static Barrier = Box::leak(Box::new(Barrier::new(2)));
    let mut mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [
            ResponseType::ToTaskNum(1),
            // We don't want the second task to be delivered until *after* the activity tasks
            // have been completed, so that the second activity schedule will have slots available
            ResponseType::UntilResolved(
                async {
                    barr.wait().await;
                    barr.wait().await;
                }
                .boxed(),
                2,
            ),
            ResponseType::AllHistory,
        ],
        mock,
    );
    mh.completion_mock_fn = Some(Box::new(|wftc| {
        // Make sure when we see the completion with the schedule act command that it does
        // not have the eager execution flag set the first time, and does the second.
        if let Some(Attributes::ScheduleActivityTaskCommandAttributes(attrs)) = wftc
            .commands
            .first()
            .and_then(|cmd| cmd.attributes.as_ref())
        {
            if attrs.activity_id == "1" {
                assert!(!attrs.request_eager_execution);
            } else {
                assert!(attrs.request_eager_execution);
            }
        }
        Ok(Default::default())
    }));
    mh.activity_responses = Some(act_tasks);
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|cfg| {
        cfg.max_cached_workflows = 2;
        cfg.max_outstanding_activities = Some(2);
    });
    let core = Arc::new(mock_worker(mock));
    let mut worker = crate::common::TestWorker::new(temporalio_sdk::Worker::new_from_core(
        core.clone(),
        DataConverter::default(),
    ));

    // First poll for activities twice, occupying both slots
    let at1 = core.poll_activity_task().await.unwrap();
    let at2 = core.poll_activity_task().await.unwrap();
    let workflow_complete_token = CancellationToken::new();
    let workflow_complete_token_clone = workflow_complete_token.clone();

    struct FakeAct;
    #[activities]
    impl FakeAct {
        #[activity(name = "act1")]
        fn act1(_: ActivityContext) -> Result<RawValue, ActivityError> {
            unreachable!("doesn't actually run")
        }

        #[activity(name = "act2")]
        fn act2(_: ActivityContext) -> Result<RawValue, ActivityError> {
            unreachable!("doesn't actually run")
        }
    }

    #[workflow]
    struct ActivityTasksCompletionWf {
        complete_token: CancellationToken,
    }

    #[workflow_methods(factory_only)]
    impl ActivityTasksCompletionWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            ctx.start_activity(
                FakeAct::act1,
                (),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;
            ctx.start_activity(
                FakeAct::act2,
                (),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await?;
            ctx.state(|wf| wf.complete_token.cancel());
            Ok(())
        }
    }

    let wf_token = workflow_complete_token.clone();
    worker
        .register_workflow_with_factory(move || ActivityTasksCompletionWf {
            complete_token: wf_token.clone(),
        })
        .unwrap();

    let act_completer = async {
        barr.wait().await;
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: at1.task_token,
            result: Some(ActivityExecutionResult::ok("hi".into())),
        })
        .await
        .unwrap();
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: at2.task_token,
            result: Some(ActivityExecutionResult::ok("hi".into())),
        })
        .await
        .unwrap();
        barr.wait().await;
        // Wait for workflow to complete in order for all eager activities to be requested before
        // shutting down. After shutdown, no eager activities slots can be allocated.
        workflow_complete_token_clone.cancelled().await;
        core.initiate_shutdown();
        // Even though this test requests eager activity tasks, none are returned in poll responses.
        let err = core.poll_activity_task().await.unwrap_err();
        assert_matches!(err, PollError::ShutDown);
    };
    // This wf poll should *not* set the flag that it wants tasks back since both slots are
    // occupied
    let run_fut = async { worker.run_until_done().await.unwrap() };
    tokio::join!(run_fut, act_completer);
}

#[tokio::test]
async fn max_wft_respected() {
    let total_wfs = 100;
    let wf_ids: Vec<_> = (0..total_wfs).map(|i| format!("fake-wf-{i}")).collect();
    let hists = wf_ids.iter().map(|wf_id| {
        let hist = canned_histories::single_timer("1");
        FakeWfResponses {
            wf_id: wf_id.to_string(),
            hist,
            response_batches: vec![1.into(), 2.into()],
        }
    });
    let mh = MockPollCfg::new(hists.into_iter().collect(), true, 0);
    let mut worker = mock_sdk_cfg(mh, |cfg| {
        cfg.max_cached_workflows = total_wfs as usize;
        cfg.max_outstanding_workflow_tasks = Some(1);
    });
    static ACTIVE_COUNT: Semaphore = Semaphore::const_new(1);

    #[workflow]
    #[derive(Default)]
    struct MaxWftWf;

    #[workflow_methods]
    impl MaxWftWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            drop(
                ACTIVE_COUNT
                    .try_acquire()
                    .expect("No multiple concurrent workflow tasks!"),
            );
            ctx.timer(Duration::from_secs(1)).await;
            Ok(())
        }
    }

    worker.register_workflow::<MaxWftWf>().unwrap();
    worker.run_until_done().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn history_length_with_fail_and_timeout(
    #[values(true, false)] use_cache: bool,
    #[values(1, 2, 3)] history_responses_case: u8,
) {
    if !use_cache && history_responses_case == 3 {
        eprintln!(
            "Skipping history_length_with_fail_and_timeout::use_cache_2_false::history_responses_case_3_3 due to flaky hang"
        );
        return;
    }
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(WorkflowTaskFailedCause::Unspecified, "ahh".into());
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_timed_out();
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_client = mock_worker_client();
    let history_responses = match history_responses_case {
        1 => vec![ResponseType::AllHistory],
        2 => vec![
            ResponseType::ToTaskNum(1),
            ResponseType::ToTaskNum(2),
            ResponseType::AllHistory,
        ],
        3 => {
            let mut needs_fetch = hist_to_poll_resp(&t, wfid, ResponseType::ToTaskNum(2)).resp;
            needs_fetch.next_page_token = vec![1];
            // Truncate the history a bit in order to force incomplete WFT
            needs_fetch.history.as_mut().unwrap().events.truncate(6);
            let needs_fetch_resp = ResponseType::Raw(needs_fetch);
            let mut empty_fetch_resp: GetWorkflowExecutionHistoryResponse =
                t.get_history_info(1).unwrap().into();
            empty_fetch_resp.history.as_mut().unwrap().events = vec![];
            mock_client
                .expect_get_workflow_execution_history()
                .returning(move |_, _, _| Ok(empty_fetch_resp.clone()))
                .times(1);
            vec![
                ResponseType::ToTaskNum(1),
                needs_fetch_resp,
                ResponseType::ToTaskNum(2),
                ResponseType::AllHistory,
            ]
        }
        _ => unreachable!(),
    };

    let mut mh = MockPollCfg::from_resp_batches(wfid, t, history_responses, mock_client);
    if history_responses_case == 3 {
        // Expect the failed pagination fetch
        mh.num_expected_fails = 1;
    }
    let mut worker = mock_sdk_cfg(mh, |wc| {
        if use_cache {
            wc.max_cached_workflows = 1;
        }
    });
    #[workflow]
    #[derive(Default)]
    struct HistoryLengthWf;

    #[workflow_methods]
    impl HistoryLengthWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            assert_eq!(ctx.history_length(), 3);
            ctx.timer(Duration::from_secs(1)).await;
            assert_eq!(ctx.history_length(), 14);
            ctx.timer(Duration::from_secs(1)).await;
            assert_eq!(ctx.history_length(), 19);
            Ok(())
        }
    }

    worker.register_workflow::<HistoryLengthWf>().unwrap();
    worker.run_until_done().await.unwrap();
}

#[allow(deprecated)]
#[tokio::test]
async fn sets_build_id_from_wft_complete() {
    let wfid = "fake_wf_id";

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.modify_event(t.current_event_id(), |he| {
        if let history_event::Attributes::WorkflowTaskCompletedEventAttributes(a) =
            he.attributes.as_mut().unwrap()
        {
            a.worker_version = Some(WorkerVersionStamp {
                build_id: "enchi-cat".to_string(),
                ..Default::default()
            });
        }
    });
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_workflow_task_scheduled_and_started();

    let mock = mock_worker_client();
    let mut worker = mock_sdk_cfg(
        MockPollCfg::from_resp_batches(wfid, t, [ResponseType::AllHistory], mock),
        |cfg| {
            cfg.versioning_strategy = WorkerVersioningStrategy::None {
                build_id: "fierce-predator".to_string(),
            };
            cfg.max_cached_workflows = 1;
        },
    );

    #[workflow]
    #[derive(Default)]
    struct BuildIdWf;

    #[workflow_methods]
    impl BuildIdWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            // First task, it should be empty, since replaying and nothing in first WFT completed
            assert_eq!(ctx.current_deployment_version(), None);
            ctx.timer(Duration::from_secs(1)).await;
            assert_eq!(
                ctx.current_deployment_version().unwrap().build_id,
                "enchi-cat"
            );
            ctx.timer(Duration::from_secs(1)).await;
            // Not replaying at this point, so we should see the worker's build id
            assert_eq!(
                ctx.current_deployment_version().unwrap().build_id,
                "fierce-predator"
            );
            ctx.timer(Duration::from_secs(1)).await;
            assert_eq!(
                ctx.current_deployment_version().unwrap().build_id,
                "fierce-predator"
            );
            Ok(())
        }
    }

    worker.register_workflow::<BuildIdWf>().unwrap();
    worker.run_until_done().await.unwrap();
}

#[derive(Debug, Clone)]
enum SlotEvent {
    ReserveSlot {
        slot_type: &'static str,
    },
    TryReserveSlot {
        slot_type: &'static str,
    },
    MarkSlotUsed {
        slot_type: &'static str,
        is_sticky: bool,
        workflow_type: Option<String>,
        activity_type: Option<String>,
    },
    ReleaseSlot {
        slot_type: &'static str,
    },
}

struct TrackingSlotSupplier<SK> {
    events: Arc<Mutex<Vec<SlotEvent>>>,
    slot_type: &'static str,
    _phantom: std::marker::PhantomData<SK>,
}

impl<SK> TrackingSlotSupplier<SK> {
    fn new(slot_type: &'static str) -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            slot_type,
            _phantom: std::marker::PhantomData,
        }
    }

    fn get_events(&self) -> Vec<SlotEvent> {
        self.events.lock().unwrap().clone()
    }

    fn add_event(&self, event: SlotEvent) {
        self.events.lock().unwrap().push(event);
    }

    fn extract_slot_info(info: &dyn SlotInfoTrait) -> (bool, Option<String>, Option<String>) {
        match info.downcast() {
            SlotInfo::Workflow(w) => (w.is_sticky, Some(w.workflow_type.clone()), None),
            SlotInfo::Activity(a) => (false, None, Some(a.activity_type.clone())),
            SlotInfo::LocalActivity(a) => (false, None, Some(a.activity_type.clone())),
            SlotInfo::Nexus(_) => (false, None, None),
        }
    }
}

#[async_trait::async_trait]
impl<SK> SlotSupplier for TrackingSlotSupplier<SK>
where
    SK: temporalio_sdk_core::SlotKind + Send + Sync,
    SK::Info: SlotInfoTrait,
{
    type SlotKind = SK;

    async fn reserve_slot(&self, _ctx: &dyn SlotReservationContext) -> SlotSupplierPermit {
        self.add_event(SlotEvent::ReserveSlot {
            slot_type: self.slot_type,
        });
        SlotSupplierPermit::with_user_data(())
    }

    fn try_reserve_slot(&self, _ctx: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
        self.add_event(SlotEvent::TryReserveSlot {
            slot_type: self.slot_type,
        });
        Some(SlotSupplierPermit::with_user_data(()))
    }

    fn mark_slot_used(&self, ctx: &dyn SlotMarkUsedContext<SlotKind = Self::SlotKind>) {
        let (is_sticky, workflow_type, activity_type) = Self::extract_slot_info(ctx.info());
        self.add_event(SlotEvent::MarkSlotUsed {
            slot_type: self.slot_type,
            is_sticky,
            workflow_type,
            activity_type,
        });
    }

    fn release_slot(&self, _ctx: &dyn SlotReleaseContext<SlotKind = Self::SlotKind>) {
        self.add_event(SlotEvent::ReleaseSlot {
            slot_type: self.slot_type,
        });
    }
}

#[tokio::test]
async fn test_custom_slot_supplier_simple() {
    let wf_supplier = Arc::new(TrackingSlotSupplier::<WorkflowSlotKind>::new("workflow"));
    let activity_supplier = Arc::new(TrackingSlotSupplier::<ActivitySlotKind>::new("activity"));
    let local_activity_supplier = Arc::new(TrackingSlotSupplier::<LocalActivitySlotKind>::new(
        "local_activity",
    ));

    let mut starter = CoreWfStarter::new("test_custom_slot_supplier_simple");
    starter.sdk_config.register_activities(StdActivities);

    let mut tb = TunerBuilder::default();
    tb.workflow_slot_supplier(wf_supplier.clone());
    tb.activity_slot_supplier(activity_supplier.clone());
    tb.local_activity_slot_supplier(local_activity_supplier.clone());
    starter.sdk_config.tuner = Arc::new(tb.build());

    let mut worker = starter.worker().await;

    #[workflow]
    #[derive(Default)]
    struct SlotSupplierWorkflow;

    #[workflow_methods]
    impl SlotSupplierWorkflow {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let _result = ctx
                .start_activity(
                    StdActivities::no_op,
                    (),
                    ActivityOptions::start_to_close_timeout(Duration::from_secs(10)),
                )
                .await;
            let _result = ctx
                .start_local_activity(
                    StdActivities::no_op,
                    (),
                    LocalActivityOptions {
                        start_to_close_timeout: Some(Duration::from_secs(10)),
                        ..Default::default()
                    },
                )
                .await;
            Ok(())
        }
    }

    worker.register_workflow::<SlotSupplierWorkflow>().unwrap();

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            SlotSupplierWorkflow::run,
            (),
            WorkflowStartOptions::new(task_queue, "test-wf".to_owned()).build(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();

    // Collect all events
    let wf_events = wf_supplier.get_events();
    let activity_events = activity_supplier.get_events();
    let local_activity_events = local_activity_supplier.get_events();

    // Verify workflow slot events - should have reserve, mark used, and release events
    assert!(wf_events.iter().any(
        |e| matches!(e, SlotEvent::ReserveSlot { slot_type, .. } if *slot_type == "workflow")
    ));
    assert!(wf_events.iter().any(
        |e| matches!(e, SlotEvent::MarkSlotUsed { slot_type, .. } if *slot_type == "workflow")
    ));
    assert!(
        wf_events
            .iter()
            .any(|e| matches!(e, SlotEvent::ReleaseSlot { slot_type } if *slot_type == "workflow"))
    );

    // Verify activity slot events - should have reserve, try_reserve (for eager execution), mark
    // used, and release
    assert!(activity_events.iter().any(
        |e| matches!(e, SlotEvent::ReserveSlot { slot_type, .. } if *slot_type == "activity")
    ));
    assert!(
        activity_events.iter().any(
            |e| matches!(e, SlotEvent::TryReserveSlot { slot_type } if *slot_type == "activity")
        )
    );
    assert!(activity_events.iter().any(
        |e| matches!(e, SlotEvent::MarkSlotUsed { slot_type, .. } if *slot_type == "activity")
    ));
    assert!(
        activity_events
            .iter()
            .any(|e| matches!(e, SlotEvent::ReleaseSlot { slot_type } if *slot_type == "activity"))
    );

    // Verify local activity slot events
    assert!(local_activity_events.iter().any(
        |e| matches!(e, SlotEvent::ReserveSlot { slot_type, .. } if *slot_type == "local_activity")
    ));
    assert!(local_activity_events.iter().any(
        |e| matches!(e, SlotEvent::MarkSlotUsed { slot_type, .. } if *slot_type == "local_activity")
    ));
    assert!(local_activity_events.iter().any(
        |e| matches!(e, SlotEvent::ReleaseSlot { slot_type } if *slot_type == "local_activity")
    ));

    assert!(
        wf_events
            .iter()
            .any(|e| matches!(e, SlotEvent::MarkSlotUsed {
                                    slot_type: "workflow",
                                    workflow_type: Some(wf_type),
                                    ..
                                } if wf_type == "SlotSupplierWorkflow"))
    );
    assert!(
        activity_events
            .iter()
            .any(|e| matches!(e, SlotEvent::MarkSlotUsed {
                                    slot_type: "activity",
                                    activity_type: Some(act_type),
                                    ..
                                } if act_type.contains("no_op")))
    );
    assert!(
        local_activity_events
            .iter()
            .any(|e| matches!(e, SlotEvent::MarkSlotUsed {
                                    slot_type: "local_activity",
                                    activity_type: Some(act_type),
                                    ..
                                } if act_type.contains("no_op")))
    );
    assert!(wf_events.iter().any(|e| matches!(
        e,
        SlotEvent::MarkSlotUsed {
            slot_type: "workflow",
            is_sticky: false,
            ..
        }
    )));

    // Verify that the number of reserve/try_reserve events matches the number of release events
    let total_reserves = wf_events
        .iter()
        .filter(|e| {
            matches!(
                e,
                SlotEvent::ReserveSlot { .. } | SlotEvent::TryReserveSlot { .. }
            )
        })
        .count()
        + activity_events
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    SlotEvent::ReserveSlot { .. } | SlotEvent::TryReserveSlot { .. }
                )
            })
            .count()
        + local_activity_events
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    SlotEvent::ReserveSlot { .. } | SlotEvent::TryReserveSlot { .. }
                )
            })
            .count();

    let total_releases = wf_events
        .iter()
        .filter(|e| matches!(e, SlotEvent::ReleaseSlot { .. }))
        .count()
        + activity_events
            .iter()
            .filter(|e| matches!(e, SlotEvent::ReleaseSlot { .. }))
            .count()
        + local_activity_events
            .iter()
            .filter(|e| matches!(e, SlotEvent::ReleaseSlot { .. }))
            .count();

    assert_eq!(
        total_reserves, total_releases,
        "Number of reserves should equal number of releases"
    );
}

#[tokio::test]
async fn shutdown_worker_not_retried() {
    let shutdown_call_count = Arc::new(AtomicU8::new(0));
    let scc = shutdown_call_count.clone();
    let fs = fake_server(move |req| {
        if req.uri().to_string().contains("ShutdownWorker") {
            scc.fetch_add(1, Ordering::Relaxed);
        }
        let s = tonic::Status::new(tonic::Code::Unknown, "bla").into_http();
        async { s }.boxed()
    })
    .await;

    let mut opts = get_integ_server_options();
    opts.target = format!("http://localhost:{}", fs.addr.port())
        .parse::<url::Url>()
        .unwrap();
    opts.set_skip_get_system_info(true);
    let connection = Connection::connect(opts).await.unwrap();
    let client_opts = temporalio_client::ClientOptions::new("ns").build();
    let client = temporalio_client::Client::new(connection, client_opts).unwrap();

    let wf_type = "shutdown_worker_not_retried";
    let mut starter = CoreWfStarter::new_with_overrides(wf_type, None, Some(client));
    let worker = starter.get_worker().await;
    drain_pollers_and_shutdown(&worker).await;
    assert_eq!(shutdown_call_count.load(Ordering::Relaxed), 1);
}

#[test]
fn test_default_build_id() {
    let o = WorkerOptions::new("task_queue").build();
    assert!(!o.deployment_options.version.build_id.is_empty());
    assert_ne!(o.deployment_options.version.build_id, "undetermined");
}

#[tokio::test]
async fn shutdown_during_active_timer_activity_workflows() {
    shared_tests::shutdown_during_active_timer_activity_workflows().await
}
