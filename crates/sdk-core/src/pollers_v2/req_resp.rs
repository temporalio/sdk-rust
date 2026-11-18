use super::*;

use std::{
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicIsize, Ordering},
    },
};

use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

use crate::abstractions::dbg_panic;

/// A pair of unbounded channels. Messages sent through `req_sender` are counted, and `resp_poller`
/// will be drained whenever the same number of messages has been sent through `resp_sender`. There
/// must never be more responses than requests.
///
/// This struct can be destructured and each part passed to a different subsystem.
pub(crate) struct ReqRespChannel<Req, Resp, P>
where
    P: Poller<Item = Resp>,
{
    pub req_sender: ReqSender<Req>,
    pub req_receiver: ReqReceiver<Req>,
    pub resp_sender: RespSender<Resp>,
    pub resp_poller: P,
}

/// Makes a new instance of [`ReqRespChannel`].
pub(crate) fn channel<Req, Resp>() -> ReqRespChannel<Req, Resp, impl Poller<Item = Resp>> {
    let pending = Arc::new(AtomicIsize::new(0));
    let (req_sender, req_receiver) = mpsc::unbounded_channel();
    let (resp_sender, resp_receiver) = mpsc::unbounded_channel();
    let resp_poller = from_unbounded_receiver_drainable(resp_receiver)
        .inspect({
            let pending = pending.clone();
            move |_| {
                let current_pending = pending.fetch_sub(1, Ordering::AcqRel);
                if current_pending < 0 {
                    // First, fix the counter. If this doesn't succeed, it means another thread
                    // fixed or will fix it in parallel - therefore we don't need to check if it
                    // succeeded.
                    let _ = pending.compare_exchange(
                        current_pending,
                        0,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    );
                    // Then, panic/print error.
                    dbg_panic!(
                        "internal error: ReqRespChannel received more responses than requests"
                    );
                }
            }
        })
        .pending_when({
            let pending = pending.clone();
            move || pending.load(Ordering::Acquire) > 0
        });

    ReqRespChannel {
        req_sender: ReqSender {
            sender: req_sender,
            pending: pending.clone(),
        },
        req_receiver,
        resp_sender,
        resp_poller,
    }
}

pub(crate) struct ReqSender<Req> {
    sender: UnboundedSender<Req>,
    pending: Arc<AtomicIsize>,
}

pub(crate) type ReqReceiver<Req> = UnboundedReceiver<Req>;

pub(crate) type RespSender<Resp> = UnboundedSender<Resp>;
