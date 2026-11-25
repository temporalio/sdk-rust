use super::nexus_workers::{
    ChargeInput, ChargeOutput, ChargeWorkflow, Outcome, PaymentHandlers, PaymentService,
};
use crate::{
    common::{eventually, get_integ_client, integ_namespace, rand_6_chars},
    integ_tests::mk_nexus_endpoint_for_client,
};
use futures_util::TryStreamExt;
use std::time::Duration;
use temporalio_client::{
    NexusOperationListOptions, NexusOperationStartOptions, nexus::NexusClientError,
};
use temporalio_common::{
    data_converters::RawValue,
    error::IncomingError,
    protos::temporal::api::{
        common::v1::{Link, link, link::workflow_event},
        enums::v1::{EventType, NexusOperationExecutionStatus, NexusOperationIdReusePolicy},
        history::v1::history_event::Attributes,
    },
};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};

#[temporalio_macros::cloud_test_exclusion(
    crate::CloudTestExclusionReason::RequiresCloudProvisioning,
    "Creates a Nexus endpoint through the Operator Service, unavailable to isolated Cloud namespace credentials."
)]
#[rstest::rstest]
#[tokio::test]
async fn nexus_standalone(
    #[values(Outcome::Sync, Outcome::Async, Outcome::Cancel)] outcome: Outcome,
) {
    let runtime = Runtime::from_current_tokio(Default::default()).unwrap();
    let namespace = integ_namespace();
    let client = get_integ_client(namespace.clone(), None).await;
    let task_queue = format!("nexus-standalone-{}", rand_6_chars());
    let options = WorkerOptions::new(task_queue.clone())
        .register_workflow::<ChargeWorkflow>()
        .unwrap()
        .register_nexus_service(PaymentHandlers {
            task_queue: task_queue.clone(),
        })
        .unwrap()
        .build();
    let endpoint = mk_nexus_endpoint_for_client(&client, &task_queue).await;
    let worker = Worker::new(&runtime, client.clone(), options).unwrap();
    // The ID is escaped on the wire; the workflow link must contain the original ID.
    let operation_id = format!("charge/{task_queue}");
    worker
        .run_until(async {
            let payments = client.nexus_client(PaymentService, endpoint);
            let asynchronous = !matches!(outcome, Outcome::Sync);
            let input = ChargeInput {
                amount: 123,
                wait_for_cancel: matches!(outcome, Outcome::Cancel),
            };
            let options = NexusOperationStartOptions::new(operation_id.clone())
                .schedule_to_close_timeout(Duration::from_secs(30))
                .build();
            let handle = if asynchronous {
                payments.start(PaymentService::charge, input, options).await
            } else {
                payments.start(PaymentService::quote, input, options).await
            }
            .unwrap();
            assert_eq!(handle.operation_id(), operation_id);
            assert_eq!(
                handle.operation_token().await.unwrap().is_some(),
                asynchronous
            );
            if matches!(outcome, Outcome::Cancel) {
                handle.cancel().await.unwrap();
                let NexusClientError::Operation { source, .. } = handle.result().await.unwrap_err()
                else {
                    panic!("expected operation failure");
                };
                assert!(source.as_cancelled().is_some());
                return;
            }
            let receipt: ChargeOutput = handle.result().await.unwrap();
            assert_eq!(
                receipt.receipt,
                if asynchronous {
                    "charged-123"
                } else {
                    "quote-123"
                },
            );
            if asynchronous {
                let description = handle.describe().await.unwrap().info.unwrap();
                let [
                    Link {
                        variant: Some(link::Variant::WorkflowEvent(workflow_link)),
                    },
                ] = description.links.as_slice()
                else {
                    panic!("expected one workflow link, got {:?}", description.links);
                };
                assert_eq!(workflow_link.namespace, namespace);
                assert_eq!(
                    workflow_link.workflow_id,
                    format!("charge-{}", description.request_id)
                );
                let history = client
                    .get_workflow_handle::<ChargeWorkflow>(&workflow_link.workflow_id)
                    .fetch_history(Default::default())
                    .into_events()
                    .await
                    .unwrap();
                let started = &history[0];
                assert_eq!(
                    workflow_link.reference,
                    Some(workflow_event::Reference::EventRef(
                        workflow_event::EventReference {
                            event_id: started.event_id,
                            event_type: EventType::WorkflowExecutionStarted as i32,
                        }
                    ))
                );
                let Some(Attributes::WorkflowExecutionStartedEventAttributes(attributes)) =
                    &started.attributes
                else {
                    panic!("expected the workflow start event");
                };
                assert_eq!(workflow_link.run_id, attributes.original_execution_run_id);
                let [callback] = attributes.completion_callbacks.as_slice() else {
                    panic!("expected one completion callback");
                };
                assert_eq!(
                    callback.links,
                    [Link {
                        variant: Some(link::Variant::NexusOperation(link::NexusOperation {
                            namespace,
                            operation_id: operation_id.clone(),
                            run_id: handle.run_id().unwrap().to_owned(),
                        })),
                    }]
                );
            } else {
                let error = payments
                    .start(
                        PaymentService::quote,
                        ChargeInput {
                            amount: 456,
                            wait_for_cancel: false,
                        },
                        NexusOperationStartOptions::new(operation_id.clone())
                            .id_reuse_policy(NexusOperationIdReusePolicy::RejectDuplicate)
                            .build(),
                    )
                    .await
                    .unwrap_err();
                let NexusClientError::AlreadyStarted {
                    operation_id: conflicting_id,
                    run_id,
                    ..
                } = error
                else {
                    panic!("expected a duplicate-operation error, got {error:?}");
                };
                assert_eq!(conflicting_id, operation_id);
                assert_eq!(run_id.as_deref(), handle.run_id());
            }
        })
        .await
        .unwrap();
}

#[temporalio_macros::cloud_test_exclusion(
    crate::CloudTestExclusionReason::RequiresCloudProvisioning,
    "Creates a Nexus endpoint through the Operator Service, unavailable to isolated Cloud namespace credentials."
)]
#[tokio::test]
async fn nexus_standalone_management_and_visibility() {
    let client = get_integ_client(integ_namespace(), None).await;
    let task_queue = format!("nexus-standalone-management-{}", rand_6_chars());
    let endpoint = mk_nexus_endpoint_for_client(&client, &task_queue).await;
    let payments = client.nexus_client(PaymentService, endpoint);
    let operation_id = format!("charge-{task_queue}");
    let start = || {
        payments.start(
            PaymentService::quote,
            ChargeInput {
                amount: 123,
                wait_for_cancel: false,
            },
            NexusOperationStartOptions::new(operation_id.clone())
                .schedule_to_close_timeout(Duration::from_secs(30))
                .build(),
        )
    };
    let first = start().await.unwrap();
    let first_run = first.run_id().unwrap().to_owned();
    let latest = client
        .get_nexus_operation_handle::<RawValue>(&operation_id, None)
        .unwrap();
    latest
        .terminate_with_options("no longer needed", Default::default())
        .await
        .unwrap();
    let second = start().await.unwrap();
    assert_ne!(second.run_id(), first.run_id());
    assert_eq!(
        latest.describe().await.unwrap().run_id,
        second.run_id().unwrap()
    );
    second.terminate().await.unwrap();

    let exact = client
        .get_nexus_operation_handle::<ChargeOutput>(&operation_id, Some(first_run.clone()))
        .unwrap();
    let NexusClientError::Operation { run_id, source, .. } = exact.result().await.unwrap_err()
    else {
        panic!("expected operation failure");
    };
    assert_eq!(run_id, first_run);
    let failure = source.operation_failure().unwrap();
    assert!(matches!(
        failure.cause(),
        Some(IncomingError::Terminated(_))
    ));
    assert_eq!(failure.failure().message, "no longer needed");

    let query = format!("OperationId = '{operation_id}' AND ExecutionStatus = 'Terminated'");
    eventually(
        || async {
            let count = client
                .count_nexus_operations(&query, Default::default())
                .await?;
            let listed: Vec<_> = client
                .list_nexus_operations(
                    &query,
                    NexusOperationListOptions::builder().page_size(1).build(),
                )
                .try_collect()
                .await?;
            anyhow::ensure!(
                count.count == 2 && listed.len() == 2,
                "count: {}, list: {listed:?}",
                count.count
            );
            anyhow::ensure!(
                listed
                    .iter()
                    .all(|op| op.status() == NexusOperationExecutionStatus::Terminated)
            );
            anyhow::ensure!(listed.iter().any(|op| op.run_id == first_run));
            anyhow::ensure!(
                listed
                    .iter()
                    .any(|op| Some(op.run_id.as_str()) == second.run_id())
            );
            Ok::<_, anyhow::Error>(())
        },
        Duration::from_secs(15),
    )
    .await
    .unwrap();
}
