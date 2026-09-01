use crate::common::{CoreWfStarter, SEARCH_ATTR_TXT};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use temporalio_client::WorkflowStartOptions;
use temporalio_common::{
    protos::temporal::api::{
        command::v1::command::Attributes,
        enums::v1::{
            CommandType, ContinueAsNewVersioningBehavior as ProtoContinueAsNewVersioningBehavior,
        },
        history::v1::history_event,
    },
    search_attributes::{SearchAttributeKey, SearchAttributes},
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ContinueAsNewOptions, WorkflowContext, WorkflowResult, WorkflowTermination};
use temporalio_sdk_core::{
    TunerHolder,
    replay::{DEFAULT_WORKFLOW_TYPE, canned_histories},
    test_help::MockPollCfg,
};
use temporalio_workflow::runtime::types::ContinueAsNewRequest;

const SA_TXT: SearchAttributeKey<String> = SearchAttributeKey::text(SEARCH_ATTR_TXT);

#[workflow]
#[derive(Default)]
struct ContinueAsNewWf;

#[workflow_methods]
impl ContinueAsNewWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, run_ct: u8) -> WorkflowResult<()> {
        ctx.timer(Duration::from_millis(500)).await;
        if run_ct < 5 {
            ctx.continue_as_new(run_ct + 1, ContinueAsNewOptions::default())?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn continue_as_new_happy_path() {
    let wf_name = "continue_as_new_happy_path";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<ContinueAsNewWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            ContinueAsNewWf::run,
            1u8,
            WorkflowStartOptions::new(task_queue, wf_name.to_string()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
struct ContinueAsNewRandomWf {
    saw_independent_stream: Arc<AtomicBool>,
}

#[workflow_methods(factory_only)]
impl ContinueAsNewRandomWf {
    #[run]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        (run, previous_value): (u8, Option<u64>),
    ) -> WorkflowResult<()> {
        let value = ctx.random_stream("continue-as-new-test").random::<u64>();
        if run == 1 {
            ctx.continue_as_new((2, Some(value)), ContinueAsNewOptions::default())?;
        } else {
            ctx.state(|wf| {
                assert_ne!(
                    value,
                    previous_value.expect("first run should pass its stream value"),
                    "continue-as-new should independently seed named streams"
                );
                wf.saw_independent_stream.store(true, Ordering::Relaxed);
            });
        }
        Ok(())
    }
}

#[tokio::test]
async fn continue_as_new_reseeds_named_random_streams() {
    let wf_name = "continue_as_new_reseeds_named_random_streams";
    let mut starter = CoreWfStarter::new(wf_name);
    let saw_independent_stream = Arc::new(AtomicBool::new(false));
    let saw_independent_stream_for_wf = saw_independent_stream.clone();
    starter
        .sdk_config
        .register_workflow_with_factory(move || ContinueAsNewRandomWf {
            saw_independent_stream: saw_independent_stream_for_wf.clone(),
        })
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            ContinueAsNewRandomWf::run,
            (1, None),
            WorkflowStartOptions::new(task_queue, wf_name).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    assert!(saw_independent_stream.load(Ordering::Relaxed));
}

#[tokio::test]
async fn continue_as_new_multiple_concurrent() {
    let wf_name = "continue_as_new_multiple_concurrent";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.max_cached_workflows = 5_usize;
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(5, 1, 1, 1));
    starter
        .sdk_config
        .register_workflow::<ContinueAsNewWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let wf_names = (1..=20).map(|i| format!("{wf_name}-{i}"));
    for name in wf_names.clone() {
        worker
            .submit_workflow(
                ContinueAsNewWf::run,
                1u8,
                WorkflowStartOptions::new(task_queue.clone(), name).build(),
            )
            .await
            .unwrap();
    }
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct WfWithTimer;

#[workflow_methods]
impl WfWithTimer {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.timer(Duration::from_millis(500)).await;
        Err(WorkflowTermination::continue_as_new(ContinueAsNewRequest {
            arguments: vec![[1].into()],
            initial_versioning_behavior: ProtoContinueAsNewVersioningBehavior::AutoUpgrade.into(),
            ..Default::default()
        }))
    }
}

#[tokio::test]
async fn wf_completing_with_continue_as_new() {
    let t = canned_histories::timer_then_continue_as_new("1");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_matches!(wft.commands[0].command_type(), CommandType::StartTimer);
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_matches!(
                    wft.commands[0].command_type(),
                    CommandType::ContinueAsNewWorkflowExecution
                );
                assert_matches!(
                    wft.commands[0].attributes.as_ref().unwrap(),
                    Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(can_attrs)
                        if can_attrs.initial_versioning_behavior == ProtoContinueAsNewVersioningBehavior::AutoUpgrade as i32
                );
            });
    });

    let mut worker = crate::common::build_fake_sdk_with_options(mock_cfg, |options| {
        options.register_workflow::<WfWithTimer>().unwrap();
    });
    worker.run().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct ContinueAsNewSuggestedWf;

#[workflow_methods]
impl ContinueAsNewSuggestedWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        // First WFT: flag should be false
        assert!(!ctx.continue_as_new_suggested());
        assert!(!ctx.target_worker_deployment_version_changed());
        ctx.timer(Duration::from_millis(500)).await;
        // Second WFT: flag should be true (set on WFT started event 8)
        assert!(ctx.continue_as_new_suggested());
        assert!(ctx.target_worker_deployment_version_changed());
        ctx.continue_as_new((), ContinueAsNewOptions::default())?;
        Ok(())
    }
}

#[tokio::test]
async fn continue_as_new_suggested_flag_exposed() {
    let mut t = canned_histories::timer_then_continue_as_new("1");
    // Modify the second WFT started event (event 8) to suggest continue-as-new
    t.modify_event(8, |he| {
        if let Some(history_event::Attributes::WorkflowTaskStartedEventAttributes(ref mut attrs)) =
            he.attributes
        {
            attrs.suggest_continue_as_new = true;
            attrs.target_worker_deployment_version_changed = true;
        }
    });

    let mock_cfg = MockPollCfg::from_hist_builder(t);
    let mut worker = crate::common::build_fake_sdk_with_options(mock_cfg, |options| {
        options
            .register_workflow::<ContinueAsNewSuggestedWf>()
            .unwrap();
    });
    worker.run().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct ClearSearchAttrsOnContinueAsNewWf;

#[workflow_methods]
impl ClearSearchAttrsOnContinueAsNewWf {
    #[run(name = "clear_search_attrs_on_continue_as_new")]
    async fn run(ctx: &mut WorkflowContext<Self>, first_run: bool) -> WorkflowResult<()> {
        if first_run {
            let mut opts = ContinueAsNewOptions::default();
            opts.search_attributes = Some(SearchAttributes::default());
            ctx.continue_as_new(false, opts)?;
        }

        assert!(ctx.search_attributes().is_empty());
        Ok(())
    }
}

#[tokio::test]
async fn clear_search_attributes_on_continue_as_new() {
    let wf_name = "clear_search_attrs_on_continue_as_new";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<ClearSearchAttrsOnContinueAsNewWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            ClearSearchAttrsOnContinueAsNewWf::run,
            true,
            WorkflowStartOptions::new(task_queue, wf_name.to_string())
                .search_attributes(SearchAttributes::new([SA_TXT.value_set("hello".into())]))
                .build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}
