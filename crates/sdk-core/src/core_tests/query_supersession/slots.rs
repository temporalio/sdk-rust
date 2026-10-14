use crate::worker::{
    SlotMarkUsedContext, SlotReleaseContext, SlotReservationContext, SlotSupplier,
    SlotSupplierPermit, WorkflowSlotKind,
};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;

#[derive(Default)]
pub(super) struct ObservedSlots {
    issued: AtomicUsize,
    pub(super) releases: Mutex<Vec<usize>>,
    pub(super) replaced: Notify,
    pub(super) released: Notify,
}

impl ObservedSlots {
    pub(super) fn assert_released_once(&self, expected: usize) {
        let releases = self.releases.lock().unwrap();
        for id in 0..expected {
            assert_eq!(
                releases.iter().filter(|&&released| released == id).count(),
                1
            );
        }
    }
}

#[async_trait::async_trait]
impl SlotSupplier for ObservedSlots {
    type SlotKind = WorkflowSlotKind;

    async fn reserve_slot(&self, _: &dyn SlotReservationContext) -> SlotSupplierPermit {
        SlotSupplierPermit::with_user_data(self.issued.fetch_add(1, Ordering::SeqCst))
    }

    fn try_reserve_slot(&self, _: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
        Some(SlotSupplierPermit::with_user_data(
            self.issued.fetch_add(1, Ordering::SeqCst),
        ))
    }

    fn mark_slot_used(&self, _: &dyn SlotMarkUsedContext<SlotKind = Self::SlotKind>) {}

    fn release_slot(&self, ctx: &dyn SlotReleaseContext<SlotKind = Self::SlotKind>) {
        let id = *ctx.permit().user_data::<usize>().unwrap();
        self.releases.lock().unwrap().push(id);
        self.released.notify_one();
        if id == 1 {
            // B is released only when C actually supersedes it in BufferedTasks.
            self.replaced.notify_one();
        }
    }
}
