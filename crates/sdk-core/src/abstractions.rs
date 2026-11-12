//! This module contains very generic helpers that can be used codebase-wide

pub(crate) mod take_cell;

use crate::{MetricsContext, worker::*};
use parking_lot::Mutex;
use std::{
    fmt::{Debug, Formatter},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};
use temporalio_common::{telemetry::metrics::TemporalMeter, worker::WorkerDeploymentVersion};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// Wraps a [SlotSupplier] and turns successful slot reservations into permit structs, as well
/// as handling associated metrics tracking.
#[derive(Clone)]
pub(crate) struct MeteredPermitDealer<SK: SlotKind> {
    supplier: Arc<dyn SlotSupplier<SlotKind = SK> + Send + Sync>,
    slot_supplier_kind: SlotSupplierKind,
    /// The number of permit owners who have acquired a permit, but are not yet meaningfully using
    /// that permit. This is useful for giving a more semantically accurate count of used task
    /// slots, since we typically wait for a permit first before polling, but that slot isn't used
    /// in the sense the user expects until we actually also get the corresponding task.
    unused_claimants: Arc<AtomicUsize>,
    /// The number of permits that have been handed out
    extant_permits: (watch::Sender<usize>, watch::Receiver<usize>),
    /// The maximum number of extant permits which are allowed. Once the number of extant permits
    /// is at this number, no more permits will be requested from the supplier until one is freed.
    /// This avoids requesting slots when we are at the workflow cache size limit. If and when
    /// we add user-defined cache sizing, that logic will need to live with the supplier and
    /// there will need to be some associated refactoring.
    max_permits: Option<usize>,
    metrics_ctx: MetricsContext,
    slot_metric_version: Arc<AtomicU64>,
    meter: Option<TemporalMeter>,
    /// Only applies to permit dealers for workflow tasks. True if this permit dealer is associated
    /// with a sticky queue poller.
    is_sticky_poller: bool,
    context_data: Arc<PermitDealerContextData>,
}

#[derive(Clone, Debug)]
#[cfg_attr(test, derive(Default))]
pub(crate) struct PermitDealerContextData {
    pub(crate) task_queue: String,
    pub(crate) worker_identity: String,
    pub(crate) worker_deployment_version: Option<WorkerDeploymentVersion>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SlotSupplierKind {
    Fixed,
    ResourceBased,
    Custom(String),
}

impl SlotSupplierKind {
    fn from_label(label: &str) -> Self {
        if label == "Fixed" {
            SlotSupplierKind::Fixed
        } else if label == "ResourceBased" {
            SlotSupplierKind::ResourceBased
        } else {
            SlotSupplierKind::Custom(label.to_string())
        }
    }
}

impl std::fmt::Display for SlotSupplierKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SlotSupplierKind::Fixed => f.write_str("Fixed"),
            SlotSupplierKind::ResourceBased => f.write_str("ResourceBased"),
            SlotSupplierKind::Custom(name) => f.write_str(name.as_str()),
        }
    }
}

impl<SK> MeteredPermitDealer<SK>
where
    SK: SlotKind,
{
    pub(crate) fn new(
        supplier: Arc<dyn SlotSupplier<SlotKind = SK> + Send + Sync>,
        metrics_ctx: MetricsContext,
        max_permits: Option<usize>,
        context_data: Arc<PermitDealerContextData>,
        meter: Option<TemporalMeter>,
    ) -> Self {
        let supplier_kind_label = supplier.slot_supplier_kind();
        let slot_supplier_kind = SlotSupplierKind::from_label(supplier_kind_label.as_ref());
        Self {
            supplier,
            slot_supplier_kind,
            unused_claimants: Arc::new(AtomicUsize::new(0)),
            extant_permits: watch::channel(0),
            metrics_ctx,
            slot_metric_version: Default::default(),
            meter,
            max_permits,
            is_sticky_poller: false,
            context_data,
        }
    }

    pub(crate) fn available_permits(&self) -> Option<usize> {
        self.supplier.available_slots()
    }

    /// Hard cap on extant permits (the workflow cache size), if any.
    pub(crate) fn max_permits(&self) -> Option<usize> {
        self.max_permits
    }

    pub(crate) fn slot_supplier_kind(&self) -> &SlotSupplierKind {
        &self.slot_supplier_kind
    }

    #[cfg(test)]
    pub(crate) fn unused_permits(&self) -> Option<usize> {
        self.available_permits()
            .map(|ap| ap + self.unused_claimants.load(Ordering::Acquire))
    }

    pub(crate) async fn acquire_owned(&self) -> OwnedMeteredSemPermit<SK> {
        if let Some(max) = self.max_permits {
            self.extant_permits
                .1
                .clone()
                .wait_for(|&ep| ep < max)
                .await
                .expect("Extant permit channel is never closed");
        }
        let res = self.supplier.reserve_slot(self).await;
        self.build_owned(res)
    }

    pub(crate) fn try_acquire_owned(&self) -> Result<OwnedMeteredSemPermit<SK>, ()> {
        if let Some(max) = self.max_permits
            && *self.extant_permits.1.borrow() >= max
        {
            return Err(());
        }
        if let Some(res) = self.supplier.try_reserve_slot(self) {
            Ok(self.build_owned(res))
        } else {
            Err(())
        }
    }

    pub(crate) fn get_extant_count_rcv(&self) -> watch::Receiver<usize> {
        self.extant_permits.1.clone()
    }

    fn build_owned(&self, res: SlotSupplierPermit) -> OwnedMeteredSemPermit<SK> {
        self.unused_claimants.fetch_add(1, Ordering::Release);
        self.extant_permits.0.send_modify(|ep| *ep += 1);
        // Eww
        let uc_c = self.unused_claimants.clone();
        let ep_rx_c = self.extant_permits.1.clone();
        let ep_tx_c = self.extant_permits.0.clone();
        let supp = self.supplier.clone();
        let supp_c = self.supplier.clone();
        let supp_c_c = self.supplier.clone();
        let mets = self.metrics_ctx.clone();
        let slot_metric_version = self.slot_metric_version.clone();
        let metric_rec = move || {
            // Suppliers and meters may call back into the worker. If a slot changed while
            // they were running, an older sample could have overwritten a newer one.
            let mut version = slot_metric_version
                .fetch_add(1, Ordering::AcqRel)
                .wrapping_add(1);
            loop {
                let unused = uc_c.load(Ordering::Acquire);
                if let Some(avail) = supp.available_slots() {
                    mets.available_task_slots(avail + unused);
                }
                let used = ep_rx_c.borrow().saturating_sub(unused) as u64;
                mets.task_slots_used(used);
                let latest = slot_metric_version.load(Ordering::Acquire);
                if latest == version {
                    break;
                }
                version = latest;
            }
        };
        let mrc = metric_rec.clone();
        mrc();

        OwnedMeteredSemPermit {
            unused_claimants: Some(self.unused_claimants.clone()),
            release_ctx: ReleaseCtx {
                permit: res,
                stored_info: None,
                meter: self.meter.clone(),
            },
            use_fn: Box::new(move |info| {
                supp_c.mark_slot_used(info);
                metric_rec()
            }),
            release_fn: Box::new(move |info| {
                supp_c_c.release_slot(info);
                ep_tx_c.send_modify(|ep| *ep -= 1);
                // The fixed-size supplier returns capacity when its permit is dropped.
                drop(std::mem::take(&mut info.permit));
                mrc()
            }),
        }
    }
}

impl MeteredPermitDealer<WorkflowSlotKind> {
    pub(crate) fn into_sticky(mut self) -> Self {
        self.is_sticky_poller = true;
        self
    }
}

impl<SK: SlotKind> SlotReservationContext for MeteredPermitDealer<SK> {
    fn task_queue(&self) -> &str {
        &self.context_data.task_queue
    }

    fn worker_identity(&self) -> &str {
        &self.context_data.worker_identity
    }

    fn worker_deployment_version(&self) -> &Option<WorkerDeploymentVersion> {
        &self.context_data.worker_deployment_version
    }

    fn num_issued_slots(&self) -> usize {
        *self.extant_permits.1.borrow()
    }

    fn is_sticky(&self) -> bool {
        self.is_sticky_poller
    }

    fn get_metrics_meter(&self) -> Option<TemporalMeter> {
        self.meter.clone()
    }
}

struct UseCtx<'a, SK: SlotKind> {
    stored_info: &'a SK::Info,
    permit: &'a SlotSupplierPermit,
    meter: Option<TemporalMeter>,
}

impl<SK: SlotKind> SlotMarkUsedContext for UseCtx<'_, SK> {
    type SlotKind = SK;

    fn permit(&self) -> &SlotSupplierPermit {
        self.permit
    }

    fn info(&self) -> &<Self::SlotKind as SlotKind>::Info {
        self.stored_info
    }

    fn get_metrics_meter(&self) -> Option<TemporalMeter> {
        self.meter.clone()
    }
}

struct ReleaseCtx<SK: SlotKind> {
    permit: SlotSupplierPermit,
    stored_info: Option<SK::Info>,
    meter: Option<TemporalMeter>,
}

impl<SK: SlotKind> SlotReleaseContext for ReleaseCtx<SK> {
    type SlotKind = SK;

    fn permit(&self) -> &SlotSupplierPermit {
        &self.permit
    }

    fn info(&self) -> Option<&<Self::SlotKind as SlotKind>::Info> {
        self.stored_info.as_ref()
    }

    fn get_metrics_meter(&self) -> Option<TemporalMeter> {
        self.meter.clone()
    }
}

/// A version of [MeteredPermitDealer] that can be closed and supports waiting for close to complete.
/// Once closed, no permits will be handed out.
/// Close completes when all permits have been returned.
pub(crate) struct ClosableMeteredPermitDealer<SK: SlotKind> {
    inner: Arc<MeteredPermitDealer<SK>>,
    state: Mutex<ClosableMeteredPermitDealerState>,
    close_complete_token: CancellationToken,
}

struct ClosableMeteredPermitDealerState {
    outstanding_permits: usize,
    close_requested: bool,
}

impl<SK> ClosableMeteredPermitDealer<SK>
where
    SK: SlotKind,
{
    pub(crate) fn new_arc(sem: Arc<MeteredPermitDealer<SK>>) -> Arc<Self> {
        Arc::new(Self {
            inner: sem,
            state: Mutex::new(ClosableMeteredPermitDealerState {
                outstanding_permits: 0,
                close_requested: false,
            }),
            close_complete_token: CancellationToken::new(),
        })
    }

    #[cfg(test)]
    pub(crate) fn unused_permits(&self) -> Option<usize> {
        self.inner.unused_permits()
    }

    /// Request to close the semaphore and prevent new permits from being acquired.
    pub(crate) fn close(&self) {
        let mut state = self.state.lock();
        state.close_requested = true;
        if state.outstanding_permits == 0 {
            drop(state);
            self.close_complete_token.cancel();
        }
    }

    /// Returns after close has been requested and all outstanding permits have been returned.
    pub(crate) async fn close_complete(&self) {
        self.close_complete_token.cancelled().await;
    }

    /// Acquire a permit if one is available and close was not requested.
    pub(crate) fn try_acquire_owned(
        self: &Arc<Self>,
    ) -> Result<TrackedOwnedMeteredSemPermit<SK>, ()> {
        let mut state = self.state.lock();
        if state.close_requested {
            return Err(());
        }
        state.outstanding_permits += 1;
        drop(state);

        let res = self.inner.try_acquire_owned();
        if res.is_err() {
            self.release_outstanding_permit();
        }
        res.map(|permit| TrackedOwnedMeteredSemPermit {
            inner: Some(permit),
            on_drop: self.on_permit_dropped(),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn available_permits(&self) -> Option<usize> {
        self.inner.available_permits()
    }

    fn on_permit_dropped(self: &Arc<Self>) -> Box<dyn Fn() + Send + Sync> {
        let sem = self.clone();
        Box::new(move || sem.release_outstanding_permit())
    }

    fn release_outstanding_permit(&self) {
        let mut state = self.state.lock();
        state.outstanding_permits -= 1;
        if state.close_requested && state.outstanding_permits == 0 {
            drop(state);
            self.close_complete_token.cancel();
        }
    }
}

/// Tracks an OwnedMeteredSemPermit and calls on_drop when dropped.
#[derive(derive_more::Debug)]
#[debug("Tracked({inner:?})")]
#[clippy::has_significant_drop]
pub(crate) struct TrackedOwnedMeteredSemPermit<SK: SlotKind> {
    inner: Option<OwnedMeteredSemPermit<SK>>,
    on_drop: Box<dyn Fn() + Send + Sync>,
}
impl<SK: SlotKind> From<TrackedOwnedMeteredSemPermit<SK>> for OwnedMeteredSemPermit<SK> {
    fn from(mut value: TrackedOwnedMeteredSemPermit<SK>) -> Self {
        value
            .inner
            .take()
            .expect("Inner permit should be available")
    }
}
impl<SK: SlotKind> Drop for TrackedOwnedMeteredSemPermit<SK> {
    fn drop(&mut self) {
        (self.on_drop)();
    }
}

/// Wraps an [SlotSupplierPermit] to update metrics & when it's dropped
#[clippy::has_significant_drop]
pub(crate) struct OwnedMeteredSemPermit<SK: SlotKind> {
    /// See [MeteredPermitDealer::unused_claimants]. If present when dropping, used to decrement the
    /// count.
    unused_claimants: Option<Arc<AtomicUsize>>,
    /// The actual [SlotSupplierPermit] is stored in here
    release_ctx: ReleaseCtx<SK>,
    #[allow(clippy::type_complexity)] // not really tho, bud
    use_fn: Box<dyn Fn(&UseCtx<SK>) + Send + Sync>,
    #[allow(clippy::type_complexity)] // not really tho, bud
    release_fn: Box<dyn Fn(&mut ReleaseCtx<SK>) + Send + Sync>,
}
impl<SK: SlotKind> Drop for OwnedMeteredSemPermit<SK> {
    fn drop(&mut self) {
        if let Some(uc) = self.unused_claimants.take() {
            uc.fetch_sub(1, Ordering::Release);
        }
        (self.release_fn)(&mut self.release_ctx);
    }
}
impl<SK: SlotKind> Debug for OwnedMeteredSemPermit<SK> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("OwnedMeteredSemPermit()")
    }
}
impl<SK: SlotKind> OwnedMeteredSemPermit<SK> {
    /// Should be called once this permit is actually being "used" for the work it was meant to
    /// permit.
    pub(crate) fn into_used(mut self, info: SK::Info) -> UsedMeteredSemPermit<SK> {
        if let Some(uc) = self.unused_claimants.take() {
            uc.fetch_sub(1, Ordering::Release);
        }
        let ctx = UseCtx {
            stored_info: &info,
            permit: &self.release_ctx.permit,
            meter: self.release_ctx.meter.clone(),
        };
        (self.use_fn)(&ctx);
        self.release_ctx.stored_info = Some(info);
        UsedMeteredSemPermit(self)
    }
}

#[derive(Debug)]
pub(crate) struct UsedMeteredSemPermit<SK: SlotKind>(#[allow(dead_code)] OwnedMeteredSemPermit<SK>);

macro_rules! dbg_panic {
    ($($arg:tt)*) => {{
        let message = format!($($arg)*);
        error!("{}", message);

        debug_assert!(false, "{}", message);
    }};
}
pub(crate) use dbg_panic;

pub(crate) struct ActiveCounter<F: Fn(usize)>(watch::Sender<usize>, Option<Arc<F>>);
impl<F> ActiveCounter<F>
where
    F: Fn(usize),
{
    pub(crate) fn new(a: watch::Sender<usize>, change_fn: Option<Arc<F>>) -> Self {
        a.send_modify(|v| {
            *v += 1;
            if let Some(cfn) = change_fn.as_ref() {
                cfn(*v);
            }
        });
        Self(a, change_fn)
    }
}
impl<F> Drop for ActiveCounter<F>
where
    F: Fn(usize),
{
    fn drop(&mut self) {
        self.0.send_modify(|v| {
            *v -= 1;
            if let Some(cfn) = self.1.as_ref() {
                cfn(*v)
            };
        });
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{
        advance_fut,
        telemetry::metrics::{WorkerHeartbeatMetrics, activity_worker_type},
        worker::{WorkflowSlotKind, tuner::FixedSizeSlotSupplier},
    };
    use futures_util::FutureExt;
    use std::sync::mpsc::{self, Sender};
    use temporalio_common::protos::coresdk::ActivitySlotInfo;

    pub(crate) fn fixed_size_permit_dealer<SK: SlotKind>(size: usize) -> MeteredPermitDealer<SK> {
        MeteredPermitDealer::new(
            Arc::new(FixedSizeSlotSupplier::new(size)),
            MetricsContext::no_op(),
            None,
            Arc::new(Default::default()),
            None,
        )
    }

    fn activity_slots(gauges: &WorkerHeartbeatMetrics) -> (u64, u64) {
        let used = gauges
            .worker_task_slots_used
            .activity_worker
            .load(Ordering::Relaxed);
        let available = gauges
            .worker_task_slots_available
            .activity_worker
            .load(Ordering::Relaxed);
        (used, available)
    }

    struct ReentrantSlotSupplier {
        inner: FixedSizeSlotSupplier<ActivitySlotKind>,
        on_capacity: parking_lot::Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }

    #[async_trait::async_trait]
    impl SlotSupplier for ReentrantSlotSupplier {
        type SlotKind = ActivitySlotKind;

        async fn reserve_slot(&self, ctx: &dyn SlotReservationContext) -> SlotSupplierPermit {
            self.inner.reserve_slot(ctx).await
        }

        fn try_reserve_slot(&self, ctx: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
            self.inner.try_reserve_slot(ctx)
        }

        fn mark_slot_used(&self, ctx: &dyn SlotMarkUsedContext<SlotKind = Self::SlotKind>) {
            self.inner.mark_slot_used(ctx);
        }

        fn release_slot(&self, ctx: &dyn SlotReleaseContext<SlotKind = Self::SlotKind>) {
            self.inner.release_slot(ctx);
        }

        fn available_slots(&self) -> Option<usize> {
            let on_capacity = self.on_capacity.lock().take();
            if let Some(on_capacity) = on_capacity {
                on_capacity();
            }
            self.inner.available_slots()
        }
    }

    #[test]
    fn poll_reservation_cannot_overwrite_a_newer_activity_slot_sample() {
        let supplier = Arc::new(ReentrantSlotSupplier {
            inner: FixedSizeSlotSupplier::new(2),
            on_capacity: Default::default(),
        });
        let metrics = MetricsContext::no_op().with_new_attrs([activity_worker_type()]);
        let gauges = metrics.in_memory_meter().unwrap();
        let dealer =
            MeteredPermitDealer::new(supplier.clone(), metrics, None, Default::default(), None);
        let activity = dealer.try_acquire_owned().unwrap();
        let (send_activity, activity_started) = mpsc::channel();
        *supplier.on_capacity.lock() = Some(Box::new(move || {
            send_activity
                .send(activity.into_used(ActivitySlotInfo::default()))
                .unwrap();
        }));

        // The poller has read the unused count when the activity claims its slot.
        let _idle_poll = dealer.try_acquire_owned().unwrap();
        let _in_flight = activity_started.try_recv().unwrap();
        assert_eq!(activity_slots(&gauges), (1, 1));
    }

    struct ReserveBeforeReturn {
        _permit: SlotSupplierPermit,
        dealer: Arc<MeteredPermitDealer<ActivitySlotKind>>,
        send_poll: Sender<OwnedMeteredSemPermit<ActivitySlotKind>>,
    }

    impl Drop for ReserveBeforeReturn {
        fn drop(&mut self) {
            self.send_poll
                .send(self.dealer.try_acquire_owned().unwrap())
                .unwrap();
        }
    }

    #[test]
    fn poll_reservation_during_release_sees_the_returned_slot() {
        let metrics = MetricsContext::no_op().with_new_attrs([activity_worker_type()]);
        let gauges = metrics.in_memory_meter().unwrap();
        let dealer = Arc::new(MeteredPermitDealer::new(
            Arc::new(FixedSizeSlotSupplier::new(2)),
            metrics,
            None,
            Default::default(),
            None,
        ));
        let mut activity = dealer
            .try_acquire_owned()
            .unwrap()
            .into_used(ActivitySlotInfo::default());
        let (send_poll, poll_reserved) = mpsc::channel();
        let permit = &mut activity.0.release_ctx.permit;
        *permit = SlotSupplierPermit::with_user_data(ReserveBeforeReturn {
            _permit: std::mem::take(permit),
            dealer: dealer.clone(),
            send_poll,
        });
        drop(activity);
        let _idle_poll = poll_reserved.try_recv().unwrap();

        assert_eq!(activity_slots(&gauges), (0, 2));
    }

    #[test]
    fn closable_semaphore_permit_drop_returns_permit() {
        let inner = fixed_size_permit_dealer::<WorkflowSlotKind>(2);
        let sem = ClosableMeteredPermitDealer::new_arc(Arc::new(inner));
        let perm = sem.try_acquire_owned().unwrap();
        let permits = sem.state.lock().outstanding_permits;
        assert_eq!(permits, 1);
        drop(perm);
        let permits = sem.state.lock().outstanding_permits;
        assert_eq!(permits, 0);
    }

    #[tokio::test]
    async fn closable_semaphore_permit_drop_after_close_resolves_close_complete() {
        let inner = fixed_size_permit_dealer::<WorkflowSlotKind>(2);
        let sem = ClosableMeteredPermitDealer::new_arc(Arc::new(inner));
        let perm = sem.try_acquire_owned().unwrap();
        sem.close();
        drop(perm);
        sem.close_complete().await;
    }

    #[tokio::test]
    async fn closable_semaphore_close_complete_ready_if_unused() {
        let inner = fixed_size_permit_dealer::<WorkflowSlotKind>(2);
        let sem = ClosableMeteredPermitDealer::new_arc(Arc::new(inner));
        sem.close();
        sem.close_complete().await;
    }

    #[test]
    fn closable_semaphore_does_not_hand_out_permits_after_closed() {
        let inner = fixed_size_permit_dealer::<WorkflowSlotKind>(2);
        let sem = ClosableMeteredPermitDealer::new_arc(Arc::new(inner));
        sem.close();
        sem.try_acquire_owned().unwrap_err();
    }

    #[tokio::test]
    async fn respects_max_extant_permits() {
        let mut sem = fixed_size_permit_dealer::<WorkflowSlotKind>(2);
        sem.max_permits = Some(1);
        let perm = sem.try_acquire_owned().unwrap();
        sem.try_acquire_owned().unwrap_err();
        let acquire_fut = sem.acquire_owned();
        // Will be pending
        advance_fut!(acquire_fut);
        drop(perm);
        // Now it'll proceed
        acquire_fut.await;
    }

    #[test]
    fn captures_slot_supplier_kind() {
        let dealer = fixed_size_permit_dealer::<WorkflowSlotKind>(1);
        assert_eq!(*dealer.slot_supplier_kind(), SlotSupplierKind::Fixed);
    }
}
