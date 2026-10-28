//! This module implements traits defined in the client to dispatch a
//! WFT to a worker bypassing the server.
//! This enables latency optimizations such as Eager Workflow Start.

use crate::{
    abstractions::{ClosableMeteredPermitDealer, TrackedOwnedMeteredSemPermit},
    protosext::ValidPollWFTQResponse,
    worker::{WorkflowSlotKind, workflow::wft_poller::validate_wft},
};
use std::sync::Arc;
use temporalio_client::worker::Slot as SlotTrait;
use temporalio_common::{
    protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse,
    worker::WorkerDeploymentOptions,
};
use tokio::sync::mpsc::UnboundedSender;

type WFTStreamSender = UnboundedSender<(
    ValidPollWFTQResponse,
    TrackedOwnedMeteredSemPermit<WorkflowSlotKind>,
)>;

struct Slot {
    permit: TrackedOwnedMeteredSemPermit<WorkflowSlotKind>,
    external_wft_tx: WFTStreamSender,
}

impl Slot {
    fn new(
        permit: TrackedOwnedMeteredSemPermit<WorkflowSlotKind>,
        external_wft_tx: WFTStreamSender,
    ) -> Self {
        Self {
            permit,
            external_wft_tx,
        }
    }
}

impl SlotTrait for Slot {
    fn schedule_wft(
        self: Box<Self>,
        task: PollWorkflowTaskQueueResponse,
    ) -> Result<(), anyhow::Error> {
        let wft = validate_wft(task)?;
        self.external_wft_tx.send((wft, self.permit))?;
        Ok(())
    }
}

#[derive(derive_more::Debug)]
#[debug("SlotProvider {{ namespace:{namespace}, task_queue: {task_queue} }}")]
pub(super) struct SlotProvider {
    namespace: String,
    task_queue: String,
    wft_semaphore: Arc<ClosableMeteredPermitDealer<WorkflowSlotKind>>,
    external_wft_tx: WFTStreamSender,
    deployment_options: Option<WorkerDeploymentOptions>,
}

impl SlotProvider {
    pub(super) fn new(
        namespace: String,
        task_queue: String,
        wft_semaphore: Arc<ClosableMeteredPermitDealer<WorkflowSlotKind>>,
        external_wft_tx: WFTStreamSender,
        deployment_options: Option<WorkerDeploymentOptions>,
    ) -> Self {
        Self {
            namespace,
            task_queue,
            wft_semaphore,
            external_wft_tx,
            deployment_options,
        }
    }
    pub(super) fn namespace(&self) -> &str {
        &self.namespace
    }
    pub(super) fn task_queue(&self) -> &str {
        &self.task_queue
    }
    pub(super) fn try_reserve_wft_slot(&self) -> Option<Box<dyn SlotTrait + Send>> {
        match self.wft_semaphore.try_acquire_owned().ok() {
            Some(permit) => Some(Box::new(Slot::new(permit, self.external_wft_tx.clone()))),
            None => None,
        }
    }
    pub(super) fn deployment_options(&self) -> Option<WorkerDeploymentOptions> {
        self.deployment_options.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    use crate::{SlotKind, abstractions::tests::fixed_size_permit_dealer};
    use temporalio_common::protos::temporal::api::{
        common::v1::{WorkflowExecution, WorkflowType},
        history::v1::History,
        taskqueue::v1::TaskQueue,
    };
    use tokio::sync::mpsc::unbounded_channel;

    // make validate_wft() happy
    fn new_validatable_response() -> PollWorkflowTaskQueueResponse {
        PollWorkflowTaskQueueResponse {
            task_token: vec![1, 3, 3, 7],
            workflow_execution_task_queue: Some(TaskQueue::default()),
            workflow_execution: Some(WorkflowExecution::default()),
            workflow_type: Some(WorkflowType::default()),
            history: Some(History::default()),
            ..Default::default()
        }
    }

    fn new_dealer<SK: SlotKind>() -> Arc<ClosableMeteredPermitDealer<SK>> {
        ClosableMeteredPermitDealer::new_arc(Arc::new(fixed_size_permit_dealer(2)))
    }

    #[tokio::test]
    async fn slot_propagates_through_channel() {
        let wft_semaphore = new_dealer();
        let (external_wft_tx, mut external_wft_rx) = unbounded_channel();

        let provider = SlotProvider::new(
            "my_namespace".to_string(),
            "my_queue".to_string(),
            wft_semaphore,
            external_wft_tx,
            None,
        );

        let slot = provider
            .try_reserve_wft_slot()
            .expect("failed to reserve slot");
        let p = slot.schedule_wft(new_validatable_response());
        assert!(p.is_ok());
        assert!(external_wft_rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn channel_closes_when_provider_drops() {
        let (external_wft_tx, mut external_wft_rx) = unbounded_channel();
        {
            let external_wft_tx = external_wft_tx;
            let wft_semaphore = new_dealer();
            let provider = SlotProvider::new(
                "my_namespace".to_string(),
                "my_queue".to_string(),
                wft_semaphore,
                external_wft_tx,
                None,
            );
            assert!(provider.try_reserve_wft_slot().is_some());
        }
        assert!(external_wft_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn unused_slots_reclaimed() {
        let wft_semaphore = new_dealer();
        {
            let wft_semaphore = wft_semaphore.clone();
            let (external_wft_tx, _) = unbounded_channel();
            let provider = SlotProvider::new(
                "my_namespace".to_string(),
                "my_queue".to_string(),
                wft_semaphore.clone(),
                external_wft_tx,
                None,
            );
            let slot = provider.try_reserve_wft_slot();
            assert!(slot.is_some());
            assert_eq!(wft_semaphore.available_permits(), Some(1));
            // drop slot without using it
        }
        assert_eq!(wft_semaphore.available_permits(), Some(2));
    }

    #[tokio::test]
    async fn dealer_closes_after_pulling_task() {
        let (external_wft_tx, mut external_wft_rx) = unbounded_channel();
        let wft_semaphore = new_dealer();
        let provider = SlotProvider::new(
            "my_namespace".to_string(),
            "my_queue".to_string(),
            wft_semaphore.clone(),
            external_wft_tx,
            None,
        );
        let slot = provider.try_reserve_wft_slot().unwrap();

        wft_semaphore.close();
        assert!(provider.try_reserve_wft_slot().is_none());
        assert!(wft_semaphore.close_complete().now_or_never().is_none());
        slot.schedule_wft(new_validatable_response()).unwrap();
        assert!(wft_semaphore.close_complete().now_or_never().is_none());
        external_wft_rx.try_recv().unwrap();
        assert!(wft_semaphore.close_complete().now_or_never().is_some());
        assert!(external_wft_rx.try_recv().is_err());
    }
}
