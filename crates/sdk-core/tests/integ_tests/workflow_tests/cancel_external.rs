use crate::common::CoreWfStarter;
use temporalio_client::WorkflowStartOptions;
use temporalio_common::protos::{
    coresdk::common::NamespacedWorkflowExecution,
    temporal::api::enums::v1::{CommandType, EventType},
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ApplicationFailure, CancelExternalWorkflowError, WorkflowContext, WorkflowResult,
};
use temporalio_sdk_core::{
    replay::{DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder},
    test_help::MockPollCfg,
};

#[workflow]
#[derive(Default)]
struct CancelSender;

#[workflow_methods]
impl CancelSender {
    #[run]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        (run_id, workflow_id, expect_not_found): (String, String, bool),
    ) -> WorkflowResult<()> {
        let handle = ctx.external_workflow(workflow_id, Some(run_id));
        let result = handle.cancel(Some("cancel-reason".into())).await;
        if expect_not_found {
            assert_matches!(result, Err(CancelExternalWorkflowError::NotFound(_)));
        } else {
            result.unwrap();
        }
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct CancelReceiver;

#[workflow_methods]
impl CancelReceiver {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let r = ctx.cancelled().await;
        Ok(r.unwrap_or_default())
    }
}

#[tokio::test]
async fn sends_cancel_to_other_wf() {
    let mut starter = CoreWfStarter::new("sends_cancel_to_other_wf");
    starter
        .sdk_config
        .register_workflow::<CancelSender>()
        .unwrap();
    starter
        .sdk_config
        .register_workflow::<CancelReceiver>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let receiver_wfid = "sends-cancel-receiver";
    let receiver_handle = worker
        .submit_workflow(
            CancelReceiver::run,
            (),
            WorkflowStartOptions::new(task_queue.clone(), receiver_wfid).build(),
        )
        .await
        .unwrap();
    let receiver_run_id = receiver_handle.run_id().unwrap();
    let sender_handle = worker
        .submit_workflow(
            CancelSender::run,
            (receiver_run_id.to_owned(), receiver_wfid.to_owned(), false),
            WorkflowStartOptions::new(task_queue, "sends-cancel-sender").build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    sender_handle
        .get_result(Default::default())
        .await
        .expect("sender workflow should complete successfully");
    let receiver_result = receiver_handle
        .get_result(Default::default())
        .await
        .expect("receiver workflow should complete successfully");
    assert!(
        receiver_result.contains("Cancel requested by workflow"),
        "expected cancellation message, got: {receiver_result}"
    );
    assert!(
        receiver_result.contains("cancel-reason"),
        "expected cancel reason in message, got: {receiver_result}"
    );
}

#[tokio::test]
async fn cancel_missing_external_wf_returns_not_found() {
    let wf_name = "cancel_missing_external_wf_returns_not_found";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<CancelSender>()
        .unwrap();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            CancelSender::run,
            (uuid::Uuid::new_v4().to_string(), wf_name.to_owned(), true),
            WorkflowStartOptions::new(task_queue, wf_name).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    handle
        .get_result(Default::default())
        .await
        .expect("workflow should observe a not-found cancellation error");
}

#[workflow]
#[derive(Default)]
struct CancelSenderCanned;

#[workflow_methods]
impl CancelSenderCanned {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let handle = ctx.external_workflow("fake_wid", Some("fake_rid".into()));
        let res = handle.cancel(None).await;
        match res {
            Err(CancelExternalWorkflowError::Failed(_)) => {
                Err(ApplicationFailure::new("Cancel fail!").into())
            }
            Err(error) => panic!("unexpected external cancellation error: {error}"),
            Ok(()) => Ok(()),
        }
    }
}

#[rstest::rstest]
#[case::succeeds(false)]
#[case::fails(true)]
#[tokio::test]
async fn sends_cancel_canned(#[case] fails: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let id = t.add_cancel_external_wf(NamespacedWorkflowExecution {
        namespace: "some_namespace".to_string(),
        workflow_id: "fake_wid".to_string(),
        run_id: "fake_rid".to_string(),
    });
    if fails {
        t.add_cancel_external_wf_failed(id);
    } else {
        t.add_cancel_external_wf_completed(id);
    }
    t.add_full_wf_task();
    if fails {
        t.add_workflow_execution_failed();
    } else {
        t.add_workflow_execution_completed();
    }

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(|wft| {
                assert_matches!(
                    wft.commands[0].command_type(),
                    CommandType::RequestCancelExternalWorkflowExecution
                );
            })
            .then(move |wft| {
                if fails {
                    assert_eq!(
                        wft.commands[0].command_type(),
                        CommandType::FailWorkflowExecution
                    );
                } else {
                    assert_eq!(
                        wft.commands[0].command_type(),
                        CommandType::CompleteWorkflowExecution
                    );
                }
            });
    });
    let mut worker = crate::common::build_fake_sdk_with_options(mock_cfg, |options| {
        options.register_workflow::<CancelSenderCanned>().unwrap();
    });
    worker.run().await.unwrap();
}
