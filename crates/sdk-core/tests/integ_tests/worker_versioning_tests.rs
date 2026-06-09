use crate::common::{CoreWfStarter, activity_functions::StdActivities, eventually};
use std::time::Duration;
use temporalio_client::{
    Client, NamespacedClient, WorkflowSignalOptions, WorkflowStartOptions, grpc::WorkflowService,
};
use temporalio_common::{
    protos::{
        coresdk::{
            workflow_commands::CompleteWorkflowExecution, workflow_completion,
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::WorkflowExecution,
            enums::v1::{RoutingConfigUpdateState, VersioningBehavior},
            history::v1::history_event::Attributes,
            workflowservice::v1::{
                DescribeWorkerDeploymentRequest, DescribeWorkflowExecutionRequest,
                SetWorkerDeploymentCurrentVersionRequest, SetWorkerDeploymentRampingVersionRequest,
            },
        },
    },
    worker::{WorkerDeploymentOptions, WorkerDeploymentVersion, WorkerTaskTypes},
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ContinueAsNewOptions, ContinueAsNewVersioningBehavior, SyncWorkflowContext,
    WorkflowContext, WorkflowResult,
};
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
struct ContinueAsNewAutoUpgradeV1 {
    should_continue_as_new: bool,
}

#[workflow_methods]
impl ContinueAsNewAutoUpgradeV1 {
    #[run(name = "continue_as_new_auto_upgrade_uses_current_deployment_version")]
    async fn run(ctx: &mut WorkflowContext<Self>, attempt: u8) -> WorkflowResult<String> {
        if attempt > 0 {
            return Ok("v1.0".to_string());
        }
        ctx.wait_condition(|state| state.should_continue_as_new)
            .await;
        assert!(ctx.target_worker_deployment_version_changed());
        let mut options = ContinueAsNewOptions::default();
        options.initial_versioning_behavior = Some(ContinueAsNewVersioningBehavior::AutoUpgrade);
        ctx.continue_as_new(&(attempt + 1), options)?;
        Ok("v1.0".to_string())
    }

    #[signal]
    fn continue_as_new(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _: ()) {
        self.should_continue_as_new = true;
    }
}

#[workflow]
#[derive(Default)]
struct ContinueAsNewAutoUpgradeV2;

#[workflow_methods]
impl ContinueAsNewAutoUpgradeV2 {
    #[run(name = "continue_as_new_auto_upgrade_uses_current_deployment_version")]
    async fn run(_ctx: &mut WorkflowContext<Self>, _attempt: u8) -> WorkflowResult<String> {
        Ok("v2.0".to_string())
    }
}

#[tokio::test]
async fn continue_as_new_auto_upgrade_uses_current_deployment_version() {
    let wf_type = "continue_as_new_auto_upgrade_uses_current_deployment_version";
    let mut starter = CoreWfStarter::new(wf_type);
    let deploy_name = format!("deployment-{}", starter.get_task_queue());
    let v1 = WorkerDeploymentVersion {
        deployment_name: deploy_name.clone(),
        build_id: "1.0".to_string(),
    };
    let v2 = WorkerDeploymentVersion {
        deployment_name: deploy_name.clone(),
        build_id: "2.0".to_string(),
    };
    starter.sdk_config.deployment_options = versioned_worker_options(v1.clone());
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker1 = starter.worker().await;
    worker1
        .register_workflow::<ContinueAsNewAutoUpgradeV1>()
        .unwrap();

    let mut starter2 = starter.clone_no_worker();
    starter2.sdk_config.deployment_options = versioned_worker_options(v2.clone());
    starter2.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker2 = starter2.worker().await;
    worker2
        .register_workflow::<ContinueAsNewAutoUpgradeV2>()
        .unwrap();

    let client = starter.get_client().await;
    let task_queue = starter.get_task_queue().to_owned();
    let workflow_id = starter.get_wf_id();
    let shutdown1 = worker1.inner_mut().shutdown_handle();
    let shutdown2 = worker2.inner_mut().shutdown_handle();

    let client_task = async {
        wait_for_worker_deployment_version(&client, &deploy_name, &v1).await;
        wait_for_worker_deployment_version(&client, &deploy_name, &v2).await;
        set_current_deployment_version(&client, &deploy_name, &v1).await;
        wait_for_worker_deployment_routing(&client, &deploy_name, Some(&v1), None, None).await;

        let handle = client
            .start_workflow(
                ContinueAsNewAutoUpgradeV1::run,
                0_u8,
                WorkflowStartOptions::new(task_queue, workflow_id).build(),
            )
            .await
            .unwrap();
        wait_for_workflow_deployment_version(
            &client,
            &handle.info().workflow_id,
            handle.run_id().unwrap_or_default(),
            &v1,
        )
        .await;

        set_current_deployment_version(&client, &deploy_name, &v2).await;
        wait_for_worker_deployment_routing(&client, &deploy_name, Some(&v2), None, None).await;
        handle
            .signal(
                ContinueAsNewAutoUpgradeV1::continue_as_new,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();

        let result = handle.get_result(Default::default()).await.unwrap();
        assert_eq!(result, "v2.0");
        shutdown1();
        shutdown2();
    };

    tokio::time::timeout(Duration::from_secs(60), async {
        join!(
            async {
                worker1.inner_mut().run().await.unwrap();
            },
            async {
                worker2.inner_mut().run().await.unwrap();
            },
            client_task
        );
    })
    .await
    .unwrap();
}

#[workflow]
#[derive(Default)]
struct ContinueAsNewUseRampingVersionV1 {
    should_continue_as_new: bool,
}

#[workflow_methods]
impl ContinueAsNewUseRampingVersionV1 {
    #[run(name = "continue_as_new_use_ramping_version_uses_ramping_deployment_version")]
    async fn run(ctx: &mut WorkflowContext<Self>, attempt: u8) -> WorkflowResult<String> {
        if attempt > 0 {
            return Ok("v1.0".to_string());
        }
        ctx.wait_condition(|state| state.should_continue_as_new)
            .await;
        let mut options = ContinueAsNewOptions::default();
        options.initial_versioning_behavior =
            Some(ContinueAsNewVersioningBehavior::UseRampingVersion);
        ctx.continue_as_new(&(attempt + 1), options)?;
        Ok("v1.0".to_string())
    }

    #[signal]
    fn continue_as_new(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _: ()) {
        self.should_continue_as_new = true;
    }
}

#[workflow]
#[derive(Default)]
struct ContinueAsNewUseRampingVersionV2;

#[workflow_methods]
impl ContinueAsNewUseRampingVersionV2 {
    #[run(name = "continue_as_new_use_ramping_version_uses_ramping_deployment_version")]
    async fn run(_ctx: &mut WorkflowContext<Self>, _attempt: u8) -> WorkflowResult<String> {
        Ok("v2.0".to_string())
    }
}

#[tokio::test]
async fn continue_as_new_use_ramping_version_uses_ramping_deployment_version() {
    let wf_type = "continue_as_new_use_ramping_version_uses_ramping_deployment_version";
    let mut starter = CoreWfStarter::new(wf_type);
    let deploy_name = format!("deployment-{}", starter.get_task_queue());
    let v1 = WorkerDeploymentVersion {
        deployment_name: deploy_name.clone(),
        build_id: "1.0".to_string(),
    };
    let v2 = WorkerDeploymentVersion {
        deployment_name: deploy_name.clone(),
        build_id: "2.0".to_string(),
    };
    starter.sdk_config.deployment_options = versioned_worker_options(v1.clone());
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker1 = starter.worker().await;
    worker1
        .register_workflow::<ContinueAsNewUseRampingVersionV1>()
        .unwrap();

    let mut starter2 = starter.clone_no_worker();
    starter2.sdk_config.deployment_options = versioned_worker_options(v2.clone());
    starter2.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker2 = starter2.worker().await;
    worker2
        .register_workflow::<ContinueAsNewUseRampingVersionV2>()
        .unwrap();

    let client = starter.get_client().await;
    let task_queue = starter.get_task_queue().to_owned();
    let workflow_id = starter.get_wf_id();
    let shutdown1 = worker1.inner_mut().shutdown_handle();
    let shutdown2 = worker2.inner_mut().shutdown_handle();

    let client_task = async {
        wait_for_worker_deployment_version(&client, &deploy_name, &v1).await;
        wait_for_worker_deployment_version(&client, &deploy_name, &v2).await;
        set_current_deployment_version(&client, &deploy_name, &v1).await;
        wait_for_worker_deployment_routing(&client, &deploy_name, Some(&v1), None, None).await;

        let handle = client
            .start_workflow(
                ContinueAsNewUseRampingVersionV1::run,
                0_u8,
                WorkflowStartOptions::new(task_queue, workflow_id).build(),
            )
            .await
            .unwrap();
        wait_for_workflow_deployment_version(
            &client,
            &handle.info().workflow_id,
            handle.run_id().unwrap_or_default(),
            &v1,
        )
        .await;

        set_ramping_deployment_version(&client, &deploy_name, &v2, 0.0).await;
        wait_for_worker_deployment_routing(&client, &deploy_name, Some(&v1), Some(&v2), Some(0.0))
            .await;
        handle
            .signal(
                ContinueAsNewUseRampingVersionV1::continue_as_new,
                (),
                WorkflowSignalOptions::default(),
            )
            .await
            .unwrap();

        let result = handle.get_result(Default::default()).await.unwrap();
        assert_eq!(result, "v2.0");
        shutdown1();
        shutdown2();
    };

    tokio::time::timeout(Duration::from_secs(60), async {
        join!(
            async {
                worker1.inner_mut().run().await.unwrap();
            },
            async {
                worker2.inner_mut().run().await.unwrap();
            },
            client_task
        );
    })
    .await
    .unwrap();
}

fn versioned_worker_options(version: WorkerDeploymentVersion) -> WorkerDeploymentOptions {
    WorkerDeploymentOptions {
        version,
        use_worker_versioning: true,
        default_versioning_behavior: VersioningBehavior::Pinned.into(),
    }
}

async fn try_describe_worker_deployment(
    client: &Client,
    deployment_name: &str,
) -> Result<
    temporalio_common::protos::temporal::api::workflowservice::v1::DescribeWorkerDeploymentResponse,
    tonic::Status,
> {
    client
        .connection()
        .clone()
        .describe_worker_deployment(
            DescribeWorkerDeploymentRequest {
                namespace: client.namespace(),
                deployment_name: deployment_name.to_string(),
            }
            .into_request(),
        )
        .await
        .map(|resp| resp.into_inner())
}

async fn wait_for_worker_deployment_version(
    client: &Client,
    deployment_name: &str,
    expected: &WorkerDeploymentVersion,
) {
    eventually(
        async || {
            let resp = try_describe_worker_deployment(client, deployment_name)
                .await
                .map_err(|err| format!("{err:?}"))?;
            let info = resp
                .worker_deployment_info
                .ok_or_else(|| "missing worker deployment info".to_string())?;
            if info
                .version_summaries
                .iter()
                .filter_map(|summary| summary.deployment_version.clone())
                .map(WorkerDeploymentVersion::from)
                .any(|version| version == *expected)
            {
                Ok(())
            } else {
                Err(format!("deployment version {expected:?} not visible yet"))
            }
        },
        Duration::from_secs(50),
    )
    .await
    .unwrap();
}

async fn wait_for_worker_deployment_routing(
    client: &Client,
    deployment_name: &str,
    expected_current: Option<&WorkerDeploymentVersion>,
    expected_ramping: Option<&WorkerDeploymentVersion>,
    expected_ramping_percentage: Option<f32>,
) {
    eventually(
        async || {
            let resp = try_describe_worker_deployment(client, deployment_name)
                .await
                .map_err(|err| format!("{err:?}"))?;
            let info = resp
                .worker_deployment_info
                .ok_or_else(|| "missing worker deployment info".to_string())?;
            let routing = info
                .routing_config
                .ok_or_else(|| "missing routing config".to_string())?;
            if RoutingConfigUpdateState::try_from(info.routing_config_update_state)
                .unwrap_or(RoutingConfigUpdateState::Unspecified)
                == RoutingConfigUpdateState::InProgress
            {
                return Err("routing config update still in progress".to_string());
            }
            let current = routing
                .current_deployment_version
                .map(WorkerDeploymentVersion::from);
            if current.as_ref() != expected_current {
                return Err(format!(
                    "current deployment version mismatch: {:?}",
                    current
                ));
            }
            let ramping = routing
                .ramping_deployment_version
                .map(WorkerDeploymentVersion::from);
            if ramping.as_ref() != expected_ramping {
                return Err(format!("ramping deployment version mismatch: {ramping:?}"));
            }
            if let Some(expected_ramping_percentage) = expected_ramping_percentage {
                let actual = routing.ramping_version_percentage;
                if (actual - expected_ramping_percentage).abs() > f32::EPSILON {
                    return Err(format!(
                        "ramping percentage mismatch: expected {expected_ramping_percentage}, got {actual}"
                    ));
                }
            }
            Ok(())
        },
        Duration::from_secs(50),
    )
    .await
    .unwrap();
}

async fn set_current_deployment_version(
    client: &Client,
    deployment_name: &str,
    version: &WorkerDeploymentVersion,
) {
    let desc = try_describe_worker_deployment(client, deployment_name)
        .await
        .unwrap();
    client
        .connection()
        .clone()
        .set_worker_deployment_current_version(
            SetWorkerDeploymentCurrentVersionRequest {
                namespace: client.namespace(),
                deployment_name: deployment_name.to_string(),
                build_id: version.build_id.clone(),
                conflict_token: desc.conflict_token,
                identity: client.identity(),
                ..Default::default()
            }
            .into_request(),
        )
        .await
        .unwrap();
}

async fn set_ramping_deployment_version(
    client: &Client,
    deployment_name: &str,
    version: &WorkerDeploymentVersion,
    percentage: f32,
) {
    let desc = try_describe_worker_deployment(client, deployment_name)
        .await
        .unwrap();
    client
        .connection()
        .clone()
        .set_worker_deployment_ramping_version(
            SetWorkerDeploymentRampingVersionRequest {
                namespace: client.namespace(),
                deployment_name: deployment_name.to_string(),
                build_id: version.build_id.clone(),
                percentage,
                conflict_token: desc.conflict_token,
                identity: client.identity(),
                ..Default::default()
            }
            .into_request(),
        )
        .await
        .unwrap();
}

async fn wait_for_workflow_deployment_version(
    client: &Client,
    workflow_id: &str,
    run_id: &str,
    expected: &WorkerDeploymentVersion,
) {
    eventually(
        async || {
            let resp = client
                .connection()
                .clone()
                .describe_workflow_execution(
                    DescribeWorkflowExecutionRequest {
                        namespace: client.namespace(),
                        execution: Some(WorkflowExecution {
                            workflow_id: workflow_id.to_string(),
                            run_id: run_id.to_string(),
                        }),
                    }
                    .into_request(),
                )
                .await
                .map_err(|err| format!("{err:?}"))?
                .into_inner();
            let info = resp
                .workflow_execution_info
                .ok_or_else(|| "missing workflow execution info".to_string())?;
            let versioning_info = info
                .versioning_info
                .ok_or_else(|| "missing workflow versioning info".to_string())?;
            let deployment_version = versioning_info
                .deployment_version
                .map(WorkerDeploymentVersion::from)
                .ok_or_else(|| "missing workflow deployment version".to_string())?;
            if deployment_version == *expected {
                Ok(())
            } else {
                Err(format!(
                    "workflow deployment version mismatch: {deployment_version:?}"
                ))
            }
        },
        Duration::from_secs(50),
    )
    .await
    .unwrap();
}
