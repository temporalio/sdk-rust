use super::nexus_workers::{
    ChargeInput, ChargeOutput, ChargeWorkflow, PaymentHandlers, PaymentService,
};
use crate::{common::CoreWfStarter, integ_tests::mk_nexus_endpoint};
use temporalio_client::NexusOperationStartOptions;
use temporalio_common::worker::WorkerTaskTypes;

#[temporalio_macros::cloud_test_exclusion(
    crate::CloudTestExclusionReason::RequiresCloudProvisioning,
    "Creates a Nexus endpoint through the Operator Service, unavailable to isolated Cloud namespace credentials."
)]
#[rstest::rstest]
#[tokio::test]
async fn nexus_standalone(#[values(false, true)] asynchronous: bool) {
    let mut starter = CoreWfStarter::new("nexus_standalone");
    starter.set_core_task_types(WorkerTaskTypes {
        enable_workflows: true,
        enable_local_activities: false,
        enable_remote_activities: false,
        enable_nexus: true,
    });
    starter
        .sdk_config
        .register_workflow::<ChargeWorkflow>()
        .unwrap();
    starter
        .sdk_config
        .register_nexus_service(PaymentHandlers {
            task_queue: starter.get_task_queue().to_owned(),
        })
        .unwrap();
    let endpoint = mk_nexus_endpoint(&mut starter).await;
    let client = starter.get_core_client().await;
    let mut worker = starter.worker().await;
    let shutdown = worker.inner_mut().shutdown_handle();
    let operation_id = format!("charge-{}", starter.get_task_queue());
    let call = async {
        let payments = client.nexus_client(PaymentService, endpoint);
        let input = ChargeInput {
            amount: 123,
            wait_for_cancel: false,
        };
        let options = NexusOperationStartOptions::new(operation_id.clone()).build();
        let handle = if asynchronous {
            payments.start(PaymentService::charge, input, options).await
        } else {
            payments.start(PaymentService::quote, input, options).await
        }
        .unwrap();
        assert_eq!(handle.operation_id(), operation_id);
        let receipt: ChargeOutput = handle.result().await.unwrap();
        assert_eq!(
            receipt.receipt,
            if asynchronous {
                "charged-123"
            } else {
                "quote-123"
            },
        );
        shutdown();
    };
    tokio::join!(worker.inner_mut().run(), call).0.unwrap();
}
