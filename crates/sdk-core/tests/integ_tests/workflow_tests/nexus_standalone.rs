use super::nexus_workers::{
    ChargeInput, ChargeOutput, ChargeWorkflow, PaymentHandlers, PaymentService,
};
use crate::{
    common::{get_integ_client, integ_namespace, rand_6_chars},
    integ_tests::mk_nexus_endpoint_for_client,
};
use std::time::Duration;
use temporalio_client::{NexusOperationStartOptions, nexus::NexusClientError};
use temporalio_common::protos::temporal::api::{
    common::v1::{Link, link, link::workflow_event},
    enums::v1::{EventType, NexusOperationIdReusePolicy},
    history::v1::history_event::Attributes,
};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};

#[temporalio_macros::cloud_test_exclusion(
    crate::CloudTestExclusionReason::RequiresCloudProvisioning,
    "Creates a Nexus endpoint through the Operator Service, unavailable to isolated Cloud namespace credentials."
)]
#[rstest::rstest]
#[tokio::test]
async fn nexus_standalone(#[values(false, true)] asynchronous: bool) {
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
            let input = ChargeInput {
                amount: 123,
                wait_for_cancel: false,
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
                            run_id: handle.run_id().to_owned(),
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
                assert_eq!(run_id.as_deref(), Some(handle.run_id()));
            }
        })
        .await
        .unwrap();
}
