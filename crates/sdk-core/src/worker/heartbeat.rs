use crate::{
    WorkerClient, WorkerConfig,
    worker::{PollError, PollerBehavior, TaskPollers, WorkerTelemetry, WorkerVersioningStrategy},
};
use parking_lot::RwLock;
use prost::Message;
use std::{collections::HashMap, sync::Arc, time::Duration};
use temporalio_client::worker::{SharedNamespaceWorkerTrait, WorkerCallbacks};
use temporalio_common::{
    protos::{
        TaskToken,
        coresdk::nexus::{NexusTask, NexusTaskCompletion, nexus_task, nexus_task_completion},
        temporal::api::{
            common::v1::Payload,
            nexus,
            nexusservices::workerservice::v1::{ExecuteCommandsRequest, ExecuteCommandsResponse},
            worker::v1::{
                WorkerCommandResult, WorkerHeartbeat, worker_command::Type as WorkerCommandType,
                worker_command_result::Type as WorkerCommandResultType,
            },
        },
    },
    worker::WorkerTaskTypes,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Callback used to collect heartbeat data from each worker at the time of heartbeat
pub(crate) type HeartbeatFn = Arc<dyn Fn() -> WorkerHeartbeat + Send + Sync>;

/// SharedNamespaceWorker is responsible for polling nexus-delivered worker commands and sending
/// worker heartbeats to the server. This invokes callbacks on all workers in the same process that
/// share the same namespace.
pub(crate) struct SharedNamespaceWorker {
    callbacks_map: Arc<RwLock<HashMap<Uuid, WorkerCallbacks>>>,
    namespace: String,
    cancel: CancellationToken,
}

impl SharedNamespaceWorker {
    pub(crate) fn new(
        client: Arc<dyn WorkerClient>,
        namespace: String,
        heartbeat_interval: Duration,
        telemetry: Option<WorkerTelemetry>,
    ) -> Result<Self, anyhow::Error> {
        let config = WorkerConfig::builder()
            .namespace(namespace.clone())
            .task_queue(format!(
                "temporal-sys/worker-commands/{namespace}/{}",
                client.worker_grouping_key(),
            ))
            .task_types(WorkerTaskTypes::nexus_only())
            .max_outstanding_nexus_tasks(5_usize)
            .versioning_strategy(WorkerVersioningStrategy::None {
                build_id: "1.0".to_owned(),
            })
            .nexus_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .build()
            .expect("internal shared namespace worker options are valid");
        let worker = crate::worker::Worker::new_with_pollers(
            config,
            None,
            client.clone(),
            TaskPollers::Real,
            telemetry,
            None,
            true,
        )?;

        let reset_notify = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let client_clone = client;
        let namespace_clone = namespace.clone();

        let callbacks_map = Arc::new(RwLock::new(HashMap::<Uuid, WorkerCallbacks>::new()));
        let callbacks_map_clone = callbacks_map.clone();

        tokio::spawn(async move {
            match client_clone.describe_namespace().await {
                Ok(namespace_resp) => {
                    if namespace_resp
                        .namespace_info
                        .and_then(|info| info.capabilities)
                        .map(|caps| caps.worker_heartbeats)
                        != Some(true)
                    {
                        debug!(
                            "Worker heartbeating configured for runtime, but server version does not support it."
                        );
                        worker.shutdown().await;
                        return;
                    }
                }
                Err(e) => {
                    warn!(error=?e, "Network error while describing namespace for heartbeat capabilities");
                    worker.shutdown().await;
                    return;
                }
            }
            let mut ticker = tokio::time::interval(heartbeat_interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let mut hb_to_send = Vec::new();
                        let hb_callbacks: Vec<_> = {
                            callbacks_map_clone.read().values()
                                .map(|cb| cb.heartbeat.clone())
                                .collect()
                        };
                        for heartbeat_callback in hb_callbacks {
                            let mut heartbeat = heartbeat_callback();
                            // All of these heartbeat details rely on a client. To avoid circular
                            // dependencies, this must be populated from within SharedNamespaceWorker
                            // to get info from the current client
                            client_clone.set_heartbeat_client_fields(&mut heartbeat);
                            hb_to_send.push(heartbeat);
                        }
                        if let Err(e) = client_clone.record_worker_heartbeat(namespace_clone.clone(), hb_to_send).await {
                            if matches!(e.code(), tonic::Code::Unimplemented) {
                                worker.shutdown().await;
                                return;
                            }
                            warn!(error=?e, "Network error while sending worker heartbeat");
                        }
                    }
                    nexus_result = worker.poll_nexus_task() => {
                        match nexus_result {
                            Ok(task) => {
                                handle_worker_command_task(
                                    &worker,
                                    &callbacks_map_clone,
                                    task,
                                ).await;
                            }
                            Err(PollError::ShutDown) => {
                                break;
                            }
                            Err(e) => {
                                warn!(error=?e, "Error polling nexus task for worker commands");
                            }
                        }
                    }
                    _ = reset_notify.notified() => {
                        ticker.reset();
                    }
                    _ = cancel_clone.cancelled() => {
                        worker.shutdown().await;
                        return;
                    }
                }
            }
        });

        Ok(Self {
            callbacks_map,
            namespace,
            cancel,
        })
    }
}

impl SharedNamespaceWorkerTrait for SharedNamespaceWorker {
    fn namespace(&self) -> String {
        self.namespace.clone()
    }

    fn register_callback(&self, worker_instance_key: Uuid, callbacks: WorkerCallbacks) {
        self.callbacks_map
            .write()
            .insert(worker_instance_key, callbacks);
    }

    fn unregister_callback(&self, worker_instance_key: Uuid) -> (Option<WorkerCallbacks>, bool) {
        let mut callbacks_map = self.callbacks_map.write();
        let callbacks = callbacks_map.remove(&worker_instance_key);
        if callbacks_map.is_empty() {
            self.cancel.cancel();
        }
        (callbacks, callbacks_map.is_empty())
    }

    fn num_workers(&self) -> usize {
        self.callbacks_map.read().len()
    }
}

async fn handle_worker_command_task(
    worker: &crate::worker::Worker,
    callbacks_map: &Arc<RwLock<HashMap<Uuid, WorkerCallbacks>>>,
    task: NexusTask,
) {
    let Some(nexus_task::Variant::Task(poll_resp)) = task.variant else {
        return;
    };

    let task_token = poll_resp.task_token.clone();

    let Some(request) = poll_resp.request.as_ref() else {
        warn!("Worker command nexus task missing request");
        return;
    };

    let Some(nexus::v1::request::Variant::StartOperation(start_op)) = &request.variant else {
        warn!("Worker command nexus task has unexpected request variant");
        return;
    };

    let payload_data = start_op
        .payload
        .as_ref()
        .map(|p| p.data.as_slice())
        .unwrap_or_default();

    let exec_req = match ExecuteCommandsRequest::decode(payload_data) {
        Ok(req) => req,
        Err(e) => {
            warn!(error=?e, "Failed to decode ExecuteCommandsRequest");
            return;
        }
    };

    let mut results = Vec::with_capacity(exec_req.commands.len());
    for command in &exec_req.commands {
        let result_type = match &command.r#type {
            Some(WorkerCommandType::CancelActivity(cancel_cmd)) => {
                let tt = TaskToken(cancel_cmd.task_token.clone());
                let cancel_callbacks: Vec<_> = callbacks_map
                    .read()
                    .values()
                    .filter_map(|cb| cb.cancel_activity.clone())
                    .collect();
                for cb in cancel_callbacks {
                    if cb(tt.clone()) {
                        break;
                    }
                }
                Some(WorkerCommandResultType::CancelActivity(
                    temporalio_common::protos::temporal::api::worker::v1::CancelActivityResult {},
                ))
            }
            None => {
                warn!("Worker command has no type set");
                None
            }
        };
        results.push(WorkerCommandResult {
            r#type: result_type,
        });
    }

    let response = ExecuteCommandsResponse { results };
    let response_bytes = response.encode_to_vec();

    let completion = NexusTaskCompletion {
        task_token,
        status: Some(nexus_task_completion::Status::Completed(
            nexus::v1::Response {
                variant: Some(nexus::v1::response::Variant::StartOperation(
                    nexus::v1::StartOperationResponse {
                        variant: Some(nexus::v1::start_operation_response::Variant::SyncSuccess(
                            nexus::v1::start_operation_response::Sync {
                                payload: Some(Payload {
                                    data: response_bytes,
                                    ..Default::default()
                                }),
                                links: vec![],
                            },
                        )),
                    },
                )),
            },
        )),
    };

    if let Err(e) = worker.complete_nexus_task(completion).await {
        warn!(error=?e, "Failed to complete worker command nexus task");
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        test_help::{WorkerExt, test_worker_cfg},
        worker,
        worker::{
            PollerBehavior,
            client::{
                MockWorkerClient,
                mocks::{DEFAULT_TEST_CAPABILITIES, mock_worker_client},
            },
        },
    };
    use prost::Message;
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use temporalio_client::worker::ClientWorkerSet;
    use temporalio_common::protos::temporal::api::{
        common::v1::Payload,
        namespace::v1::{NamespaceInfo, namespace_info::Capabilities},
        nexus::v1::{Request, StartOperationRequest, request},
        nexusservices::workerservice::v1::ExecuteCommandsRequest,
        worker::v1::{CancelActivityCommand, WorkerCommand, worker_command},
        workflowservice::v1::{
            DescribeNamespaceResponse, PollNexusTaskQueueResponse, RecordWorkerHeartbeatResponse,
            RespondNexusTaskCompletedResponse,
        },
    };
    use uuid::Uuid;

    #[tokio::test]
    async fn worker_heartbeat_basic() {
        let mut mock = mock_worker_client();
        let heartbeat_count = Arc::new(AtomicUsize::new(0));
        let heartbeat_count_clone = heartbeat_count.clone();
        mock.expect_poll_workflow_task()
            .returning(move |_namespace, _task_queue| Ok(Default::default()));
        mock.expect_poll_nexus_task()
            .returning(move |_poll_options, _send_heartbeat| Ok(Default::default()));
        mock.expect_record_worker_heartbeat().times(3).returning(
            move |_namespace, worker_heartbeat| {
                assert_eq!(1, worker_heartbeat.len());
                let heartbeat = worker_heartbeat[0].clone();
                let host_info = heartbeat.host_info.clone().unwrap();
                assert_eq!("test-identity", heartbeat.worker_identity);
                assert!(!heartbeat.worker_instance_key.is_empty());
                assert_eq!(
                    host_info.host_name,
                    gethostname::gethostname().to_string_lossy().to_string()
                );
                assert_eq!(host_info.process_id, std::process::id().to_string());
                assert_eq!(heartbeat.sdk_name, "test-core");
                assert_eq!(heartbeat.sdk_version, "0.0.0");
                assert!(heartbeat.heartbeat_time.is_some());
                assert!(heartbeat.start_time.is_some());

                heartbeat_count_clone.fetch_add(1, Ordering::Relaxed);

                Ok(RecordWorkerHeartbeatResponse {})
            },
        );
        mock.expect_describe_namespace().returning(move || {
            Ok(DescribeNamespaceResponse {
                namespace_info: Some(NamespaceInfo {
                    capabilities: Some(Capabilities {
                        worker_heartbeats: true,
                        ..Capabilities::default()
                    }),
                    ..NamespaceInfo::default()
                }),
                ..DescribeNamespaceResponse::default()
            })
        });

        let config = test_worker_cfg()
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .max_outstanding_activities(1_usize)
            .build()
            .unwrap();

        let client = Arc::new(mock);
        let worker = worker::Worker::new(
            config,
            None,
            client.clone(),
            None,
            Some(Duration::from_millis(100)),
        )
        .unwrap();

        tokio::time::sleep(Duration::from_millis(250)).await;
        worker.drain_activity_poller_and_shutdown().await;

        assert_eq!(3, heartbeat_count.load(Ordering::Relaxed));
    }

    fn make_execute_commands_nexus_response(
        task_token: Vec<u8>,
        commands: Vec<WorkerCommand>,
    ) -> PollNexusTaskQueueResponse {
        let exec_req = ExecuteCommandsRequest { commands };
        PollNexusTaskQueueResponse {
            task_token,
            request: Some(Request {
                header: Default::default(),
                scheduled_time: None,
                endpoint: String::new(),
                variant: Some(request::Variant::StartOperation(StartOperationRequest {
                    service: "temporal.api.nexusservices.workerservice.v1.WorkerService"
                        .to_string(),
                    operation: "ExecuteCommands".to_string(),
                    request_id: "test-req-id".to_string(),
                    callback: String::new(),
                    payload: Some(Payload {
                        data: exec_req.encode_to_vec(),
                        ..Default::default()
                    }),
                    callback_header: Default::default(),
                    links: vec![],
                })),
                capabilities: None,
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn worker_command_cancel_activity() {
        let mut mock = MockWorkerClient::new();
        let isolated_registry = Arc::new(ClientWorkerSet::new());
        mock.expect_workers().return_const(isolated_registry);
        mock.expect_capabilities()
            .returning(|| Some(*DEFAULT_TEST_CAPABILITIES));
        mock.expect_is_mock().returning(|| true);
        mock.expect_shutdown_worker()
            .returning(|_, _, _, _| {
                use temporalio_common::protos::temporal::api::workflowservice::v1::ShutdownWorkerResponse;
                Ok(ShutdownWorkerResponse {})
            });
        mock.expect_sdk_name_and_version()
            .returning(|| ("test-core".to_string(), "0.0.0".to_string()));
        mock.expect_identity()
            .returning(|| "test-identity".to_string());
        mock.expect_worker_grouping_key().returning(Uuid::new_v4);
        mock.expect_worker_instance_key().returning(Uuid::new_v4);
        mock.expect_set_heartbeat_client_fields()
            .returning(|_hb| {});

        let activity_task_token = vec![1, 2, 3, 4];

        let completed_response = Arc::new(Mutex::new(
            None::<temporalio_common::protos::temporal::api::nexus::v1::Response>,
        ));
        let completed_response_clone = completed_response.clone();

        let poll_returned_command = Arc::new(AtomicBool::new(false));
        let poll_returned_command_clone = poll_returned_command.clone();
        let at_clone = activity_task_token.clone();
        mock.expect_poll_nexus_task()
            .returning(move |poll_options, _send_heartbeat| {
                if poll_options
                    .task_queue
                    .starts_with("temporal-sys/worker-commands/")
                    && !poll_returned_command_clone.swap(true, Ordering::SeqCst)
                {
                    Ok(make_execute_commands_nexus_response(
                        vec![99],
                        vec![WorkerCommand {
                            r#type: Some(worker_command::Type::CancelActivity(
                                CancelActivityCommand {
                                    task_token: at_clone.clone(),
                                },
                            )),
                        }],
                    ))
                } else {
                    Ok(Default::default())
                }
            });
        mock.expect_complete_nexus_task()
            .returning(move |_task_token, response| {
                *completed_response_clone.lock().unwrap() = Some(response);
                Ok(RespondNexusTaskCompletedResponse {})
            });

        mock.expect_poll_workflow_task()
            .returning(move |_namespace, _task_queue| Ok(Default::default()));
        mock.expect_record_worker_heartbeat()
            .returning(move |_namespace, _worker_heartbeat| Ok(RecordWorkerHeartbeatResponse {}));
        mock.expect_describe_namespace().returning(move || {
            Ok(DescribeNamespaceResponse {
                namespace_info: Some(NamespaceInfo {
                    capabilities: Some(Capabilities {
                        worker_heartbeats: true,
                        ..Capabilities::default()
                    }),
                    ..NamespaceInfo::default()
                }),
                ..DescribeNamespaceResponse::default()
            })
        });

        let config = test_worker_cfg()
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .max_outstanding_activities(1_usize)
            .build()
            .unwrap();

        let client = Arc::new(mock);
        let worker = worker::Worker::new(
            config,
            None,
            client.clone(),
            None,
            Some(Duration::from_millis(100)),
        )
        .unwrap();

        // Give time for the SharedNamespaceWorker to poll and process the nexus task
        tokio::time::sleep(Duration::from_millis(500)).await;
        worker.drain_activity_poller_and_shutdown().await;

        // Verify the nexus task was completed with a valid ExecuteCommandsResponse
        let response = completed_response
            .lock()
            .unwrap()
            .take()
            .expect("Nexus task should have been completed");

        use temporalio_common::protos::temporal::api::{
            nexus::v1::{response, start_operation_response},
            nexusservices::workerservice::v1::ExecuteCommandsResponse,
            worker::v1::worker_command_result,
        };
        let start_op = match response.variant {
            Some(response::Variant::StartOperation(s)) => s,
            other => panic!("Expected StartOperation response, got {:?}", other),
        };
        let sync_resp = match start_op.variant {
            Some(start_operation_response::Variant::SyncSuccess(s)) => s,
            other => panic!("Expected SyncSuccess, got {:?}", other),
        };
        let payload_data = sync_resp.payload.expect("Should have payload").data;
        let exec_resp = ExecuteCommandsResponse::decode(payload_data.as_slice())
            .expect("Should decode ExecuteCommandsResponse");
        assert_eq!(exec_resp.results.len(), 1, "Should have one result");
        assert!(
            matches!(
                exec_resp.results[0].r#type,
                Some(worker_command_result::Type::CancelActivity(_))
            ),
            "Result should be CancelActivity"
        );
    }
}
