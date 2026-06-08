use crate::common::{CoreWfStarter, eventually, rand_6_chars};
use futures::TryStreamExt;
use std::{collections::HashSet, time::Duration};
use temporalio_client::{
    UntypedWorkflow, WorkflowCountOptions, WorkflowListOptions, WorkflowStartOptions,
    WorkflowTerminateOptions, errors::WorkflowStartError,
};
use temporalio_common::{data_converters::RawValue, worker::WorkerTaskTypes};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowResult};

#[workflow]
#[derive(Default)]
struct EmptyWorkflow;

#[workflow_methods]
impl EmptyWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }
}

#[rstest::rstest]
#[case::no_limit(None)]
#[case::with_limit(Some(2))]
#[tokio::test]
async fn list_workflows(#[case] limit: Option<usize>) {
    let test_name = "list_workflows_returns_started_workflows";
    let mut starter = CoreWfStarter::new(test_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let client = starter.get_client().await;
    let mut worker = starter.worker().await;
    worker.register_workflow::<EmptyWorkflow>().unwrap();

    let suffix = rand_6_chars();
    let num_workflows = 5;
    let task_queue = starter.get_task_queue().to_owned();
    let mut started_workflow_ids = Vec::new();
    let mut expected_run_ids = Vec::new();

    for i in 0..num_workflows {
        let wf_id = format!("{test_name}_{suffix}_{i}");
        started_workflow_ids.push(wf_id.clone());
        let handle = worker
            .submit_workflow(
                EmptyWorkflow::run,
                (),
                WorkflowStartOptions::new(task_queue.clone(), wf_id).build(),
            )
            .await
            .unwrap();
        expected_run_ids.push(handle.info().run_id.clone());
    }

    worker.run_until_done().await.unwrap();

    let results = eventually(
        || {
            let client = client.clone();
            let expected_ids: HashSet<_> = started_workflow_ids.iter().cloned().collect();
            let task_queue = task_queue.clone();
            let expected_count = limit.unwrap_or(num_workflows);
            async move {
                let query = format!("TaskQueue = '{task_queue}'");
                let opts = match limit {
                    Some(l) => WorkflowListOptions::builder().limit(l).build(),
                    None => WorkflowListOptions::default(),
                };
                let stream = client.list_workflows(query, opts);
                let results: Vec<_> = stream.try_collect().await.expect("No errors");

                if results.len() != expected_count {
                    return Err(format!(
                        "Expected {} workflows, got {}",
                        expected_count,
                        results.len()
                    ));
                }

                let found_ids: HashSet<_> = results.iter().map(|w| w.id().to_owned()).collect();
                if !found_ids.is_subset(&expected_ids) {
                    return Err(format!(
                        "Found unexpected workflow IDs. Expected subset of: {:?}, Found: {:?}",
                        expected_ids, found_ids
                    ));
                }

                Ok(results)
            }
        },
        Duration::from_secs(10),
    )
    .await
    .unwrap();

    // Verify accessor fields are populated
    for wf in &results {
        assert!(
            started_workflow_ids.contains(&wf.id().to_owned()),
            "Workflow ID {} not in started list",
            wf.id()
        );
        assert!(!wf.run_id().is_empty(), "run_id should be populated");
        assert_eq!(wf.task_queue(), task_queue, "task_queue mismatch");
        assert!(wf.start_time().is_some(), "start_time should be populated");
        assert!(
            !wf.workflow_type().is_empty(),
            "workflow_type should be populated"
        );
    }

    // Verify count_workflows works too
    let workflow_count = eventually(
        || {
            let client = client.clone();
            let task_queue = task_queue.clone();
            async move {
                let query = format!("TaskQueue = '{task_queue}'");
                let count = client
                    .count_workflows(&query, WorkflowCountOptions::default())
                    .await
                    .unwrap();
                if count.count() != num_workflows {
                    return Err(format!(
                        "Expected {} workflows, got {}",
                        num_workflows,
                        count.count()
                    ));
                }
                Ok(count.count())
            }
        },
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(workflow_count == num_workflows);
}

#[tokio::test]
async fn already_started_error_contains_run_id() {
    let test_name = "already_started_error_contains_run_id";
    let mut starter = CoreWfStarter::new(test_name);
    let client = starter.get_client().await;
    let task_queue = starter.get_task_queue().to_owned();
    let wf_id = format!("{test_name}_{}", rand_6_chars());

    let handle = client
        .start_workflow(
            UntypedWorkflow::new(test_name),
            RawValue::empty(),
            WorkflowStartOptions::new(task_queue.clone(), wf_id.clone()).build(),
        )
        .await
        .unwrap();
    let first_run_id = handle.run_id().unwrap().to_string();

    let err = client
        .start_workflow(
            UntypedWorkflow::new(test_name),
            RawValue::empty(),
            WorkflowStartOptions::new(task_queue, wf_id).build(),
        )
        .await
        .err()
        .expect("duplicate wfid should error");

    match err {
        WorkflowStartError::AlreadyStarted { run_id, .. } => {
            assert_eq!(run_id.as_deref(), Some(first_run_id.as_str()));
        }
        other => panic!("Expected AlreadyStarted, got: {other}"),
    }

    handle
        .terminate(WorkflowTerminateOptions::default())
        .await
        .unwrap();
}
