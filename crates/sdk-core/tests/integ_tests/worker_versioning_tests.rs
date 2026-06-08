use crate::common::{CoreWfStarter, activity_functions::StdActivities, eventually};
use std::time::Duration;
use temporalio_client::{NamespacedClient, WorkflowStartOptions, grpc::WorkflowService};
use temporalio_common::{
    protos::{
        coresdk::{
            workflow_commands::CompleteWorkflowExecution, workflow_completion,
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            enums::v1::VersioningBehavior,
            history::v1::history_event::Attributes,
            workflowservice::v1::{
                DescribeWorkerDeploymentRequest, SetWorkerDeploymentCurrentVersionRequest,
            },
        },
    },
    worker::{WorkerDeploymentOptions, WorkerDeploymentVersion, WorkerTaskTypes},
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowResult};
use temporalio_sdk_core::test_help::WorkerTestHelpers;
use tokio::join;
use tonic::IntoRequest;

#[rstest::rstest]
#[tokio::test]
async fn sets_deployment_info_on_task_responses(#[values(true, false)] use_default: bool) {
    let wf_type = "sets_deployment_info_on_task_responses";
    let mut starter = CoreWfStarter::new(wf_type);
    let deploy_name = format!("deployment-{}", starter.get_task_queue());
    let version = WorkerDeploymentVersion {
        deployment_name: deploy_name.clone(),
        build_id: "1.0".to_string(),
    };
    starter.sdk_config.deployment_options = WorkerDeploymentOptions {
        version: version.clone(),
        use_worker_versioning: true,
        default_versioning_behavior: VersioningBehavior::AutoUpgrade.into(),
    };
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let core = starter.get_worker().await;
    let client = starter.get_client().await;

    // A bit annoying. We have to start up polling here so that the deployment will exist before
    // we can describe it and then set the current version.
    let worker_task = async {
        let res = core.poll_workflow_activation().await.unwrap();
        assert_eq!(
            version,
            res.deployment_version_for_current_task.unwrap().into(),
        );

        let mut success_complete = workflow_completion::Success::from_variants(vec![
            CompleteWorkflowExecution { result: None }.into(),
        ]);
        if !use_default {
            success_complete.versioning_behavior = VersioningBehavior::Pinned.into();
        }
        core.complete_workflow_activation(WorkflowActivationCompletion {
            run_id: res.run_id.clone(),
            status: Some(success_complete.into()),
        })
        .await
        .unwrap();
    };

    let ops_task = async {
        let desc_resp = eventually(
            async || {
                client
                    .connection()
                    .clone()
                    .describe_worker_deployment(
                        DescribeWorkerDeploymentRequest {
                            namespace: client.namespace(),
                            deployment_name: deploy_name.clone(),
                        }
                        .into_request(),
                    )
                    .await
            },
            Duration::from_secs(5),
        )
        .await
        .unwrap()
        .into_inner();

        #[allow(deprecated)]
        client
            .connection()
            .clone()
            .set_worker_deployment_current_version(
                SetWorkerDeploymentCurrentVersionRequest {
                    namespace: client.namespace(),
                    deployment_name: deploy_name.clone(),
                    version: format!("{deploy_name}.1.0"),
                    conflict_token: desc_resp.conflict_token,
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap();

        starter.start_wf().await;
    };

    join!(worker_task, ops_task);
    core.handle_eviction().await;
    core.shutdown().await;

    // Fetch history & verify task complete is properly stamped
    let history = starter.get_history().await;
    let wft_complete = history
        .events
        .into_iter()
        .find_map(|e| {
            if let Attributes::WorkflowTaskCompletedEventAttributes(a) = e.attributes.unwrap() {
                Some(a)
            } else {
                None
            }
        })
        .unwrap();
    if use_default {
        assert_eq!(
            wft_complete.versioning_behavior,
            VersioningBehavior::AutoUpgrade as i32
        );
    } else {
        assert_eq!(
            wft_complete.versioning_behavior,
            VersioningBehavior::Pinned as i32
        );
    }
    assert_eq!(wft_complete.worker_deployment_name, deploy_name);
    let dv = wft_complete.deployment_version.unwrap();
    assert_eq!(dv.deployment_name, deploy_name);
    assert_eq!(dv.build_id, "1.0");
}

#[tokio::test]
async fn activity_has_deployment_stamp() {
    let wf_name = "activity_has_deployment_stamp";
    let mut starter = CoreWfStarter::new(wf_name);
    let deploy_name = format!("deployment-{}", starter.get_task_queue());
    starter.sdk_config.deployment_options = WorkerDeploymentOptions {
        version: WorkerDeploymentVersion {
            deployment_name: deploy_name.clone(),
            build_id: "1.0".to_string(),
        },
        use_worker_versioning: true,
        default_versioning_behavior: VersioningBehavior::AutoUpgrade.into(),
    };
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;

    worker
        .register_workflow::<ActivityHasDeploymentStampWf>()
        .unwrap();
    let submitter = worker.get_submitter_handle();
    let shutdown_handle = worker.inner_mut().shutdown_handle();

    let client_task = async {
        let desc_resp = eventually(
            async || {
                client
                    .connection()
                    .clone()
                    .describe_worker_deployment(
                        DescribeWorkerDeploymentRequest {
                            namespace: client.namespace(),
                            deployment_name: deploy_name.clone(),
                        }
                        .into_request(),
                    )
                    .await
            },
            Duration::from_secs(50),
        )
        .await
        .unwrap()
        .into_inner();

        #[allow(deprecated)]
        client
            .connection()
            .clone()
            .set_worker_deployment_current_version(
                SetWorkerDeploymentCurrentVersionRequest {
                    namespace: client.namespace(),
                    deployment_name: deploy_name.clone(),
                    version: format!("{deploy_name}.1.0"),
                    conflict_token: desc_resp.conflict_token,
                    ..Default::default()
                }
                .into_request(),
            )
            .await
            .unwrap();

        let task_queue = starter.get_task_queue().to_owned();
        let workflow_id = starter.get_wf_id();
        submitter
            .submit_wf(
                wf_name.to_owned(),
                vec![],
                WorkflowStartOptions::new(task_queue, workflow_id).build(),
            )
            .await
            .unwrap();
        starter.wait_for_default_wf_finish().await.unwrap();
        shutdown_handle();
    };
    join!(
        async {
            worker.inner_mut().run().await.unwrap();
        },
        client_task
    );
    let hist = starter.get_history().await;
    let _activity_completed = hist
        .events
        .into_iter()
        .find_map(|e| {
            if let Attributes::ActivityTaskCompletedEventAttributes(a) = e.attributes.unwrap() {
                Some(a)
            } else {
                None
            }
        })
        .unwrap();
    // TODO: Can't actually verify this at the moment as the deployment options are not transferred
    //   to the event.
}

#[tokio::test]
async fn versioning_off_with_custom_build_id() {
    let wf_type = "versioning_off_with_custom_build_id";
    let mut starter = CoreWfStarter::new(wf_type);
    let build_id = "my-custom-build-id-1.0";
    starter.sdk_config.deployment_options = WorkerDeploymentOptions {
        version: WorkerDeploymentVersion {
            deployment_name: format!("deployment-{}", starter.get_task_queue()),
            build_id: build_id.to_string(),
        },
        use_worker_versioning: false,
        default_versioning_behavior: None,
    };
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let core = starter.get_worker().await;
    starter.start_wf().await;

    let res = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion {
        run_id: res.run_id.clone(),
        status: Some(
            workflow_completion::Success::from_variants(vec![
                CompleteWorkflowExecution { result: None }.into(),
            ])
            .into(),
        ),
    })
    .await
    .unwrap();

    core.handle_eviction().await;
    core.shutdown().await;

    let history = starter.get_history().await;
    // The SDK sends deployment_options on WFT completion. For unversioned workers, the server
    // records the deployment name in worker_deployment_name but does not populate
    // deployment_version.
    let wft_complete = history
        .events
        .into_iter()
        .find_map(|e| {
            if let Attributes::WorkflowTaskCompletedEventAttributes(a) = e.attributes.unwrap() {
                Some(a)
            } else {
                None
            }
        })
        .unwrap();
    assert!(
        !wft_complete.worker_deployment_name.is_empty(),
        "Expected deployment name to appear in workflow history"
    );
}

#[workflow]
#[derive(Default)]
struct ActivityHasDeploymentStampWf;

#[workflow_methods]
impl ActivityHasDeploymentStampWf {
    #[run(name = "activity_has_deployment_stamp")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let _ = ctx
            .start_activity(
                StdActivities::echo,
                "hi!".to_string(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(5)),
            )
            .await;
        Ok(())
    }
}
