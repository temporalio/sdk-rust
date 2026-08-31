use crate::common::CoreWfStarter;
use std::time::Duration;
use temporalio_client::{Priority, WorkflowGetResultOptions};
use temporalio_common::{
    UntypedWorkflow, data_converters::RawValue,
    protos::temporal::api::history::v1::history_event::Attributes,
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ChildWorkflowOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};

pub(crate) async fn priority_values_sent_to_server() {
    let mut starter = if let Some(wfs) =
        CoreWfStarter::new_cloud_or_local("priority_values_sent_to_server", ">=1.29.0-139.2").await
    {
        wfs
    } else {
        return;
    };
    starter.workflow_options.priority = Priority::builder()
        .priority_key(1)
        .fairness_key("fair-wf")
        .fairness_weight(4.2)
        .build();
    let child_type = "child-wf";

    struct PriorityActivities;
    #[activities]
    impl PriorityActivities {
        #[activity]
        async fn echo(ctx: ActivityContext, echo_me: String) -> Result<String, ActivityError> {
            assert_eq!(
                ctx.info().priority,
                Priority::builder()
                    .priority_key(5)
                    .fairness_key("fair-act")
                    .fairness_weight(1.1)
                    .build()
            );
            Ok(echo_me)
        }
    }

    #[workflow]
    #[derive(Default)]
    struct ParentWf {
        child_type: String,
    }

    #[workflow_methods]
    impl ParentWf {
        #[run]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let child_type = ctx.state(|wf| wf.child_type.clone());
            let started = ctx
                .start_child_workflow(
                    UntypedWorkflow::new(&child_type),
                    RawValue::new(vec![]),
                    ChildWorkflowOptions::builder()
                        .workflow_id(format!("{}-child", ctx.task_queue()))
                        .priority(
                            Priority::builder()
                                .priority_key(4)
                                .fairness_key("fair-child")
                                .fairness_weight(1.23)
                                .build(),
                        )
                        .build(),
                )
                .await?;
            let activity = ctx.execute_activity(
                PriorityActivities::echo,
                "hello".to_string(),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                    .priority(
                        Priority::builder()
                            .priority_key(5)
                            .fairness_key("fair-act")
                            .fairness_weight(1.1)
                            .build(),
                    )
                    .do_not_eagerly_execute(true)
                    .build(),
            );
            let _ = started.result().await;
            let _ = activity.await;
            Ok(())
        }
    }

    #[workflow]
    #[derive(Default)]
    struct ChildWf;

    #[workflow_methods]
    impl ChildWf {
        #[run(name = "child-wf")]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            assert_eq!(
                ctx.info().priority(),
                Priority::builder()
                    .priority_key(4)
                    .fairness_key("fair-child")
                    .fairness_weight(1.23)
                    .build()
            );
            Ok(())
        }
    }

    starter
        .sdk_config
        .register_activities(PriorityActivities)
        .register_workflow_with_factory::<ParentWf, _>(move || ParentWf {
            child_type: child_type.to_owned(),
        })
        .unwrap()
        .register_workflow::<ChildWf>()
        .unwrap();
    let mut worker = starter.worker().await;

    worker
        .submit_workflow(ParentWf::run, (), starter.workflow_options.clone())
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let client = starter.get_core_client().await;
    let handle = client.get_workflow_handle::<UntypedWorkflow>(starter.get_task_queue());
    handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .unwrap();
    let events = handle
        .fetch_history(Default::default())
        .into_events()
        .await
        .unwrap();
    let workflow_init_event = events
        .iter()
        .find_map(|e| {
            if let Attributes::WorkflowExecutionStartedEventAttributes(e) =
                e.attributes.as_ref().unwrap()
            {
                Some(e)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(
        workflow_init_event.priority.as_ref().unwrap().priority_key,
        1
    );
    let child_init_event = events
        .iter()
        .find_map(|e| {
            if let Attributes::StartChildWorkflowExecutionInitiatedEventAttributes(e) =
                e.attributes.as_ref().unwrap()
            {
                Some(e)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(child_init_event.priority.as_ref().unwrap().priority_key, 4);
    let activity_sched_event = events
        .iter()
        .find_map(|e| {
            if let Attributes::ActivityTaskScheduledEventAttributes(e) =
                e.attributes.as_ref().unwrap()
            {
                Some(e)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(
        activity_sched_event.priority.as_ref().unwrap().priority_key,
        5
    );
}
