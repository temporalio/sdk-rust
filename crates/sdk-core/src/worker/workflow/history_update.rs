use crate::{
    protosext::ValidPollWFTQResponse,
    worker::{
        client::WorkerClient,
        workflow::{CacheMissFetchReq, PermittedWFT, PreparedWFT},
    },
};
use futures_util::{FutureExt, Stream, TryFutureExt, future::BoxFuture};
use itertools::Itertools;
use std::{
    collections::VecDeque,
    fmt::Debug,
    future::Future,
    mem,
    mem::transmute,
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
};
use temporalio_common::protos::temporal::api::{
    enums::v1::EventType,
    history::v1::{
        History, HistoryEvent, WorkflowTaskCompletedEventAttributes, history_event::Attributes,
    },
};
use tracing::Instrument;

static EMPTY_FETCH_ERR: LazyLock<tonic::Status> =
    LazyLock::new(|| tonic::Status::unknown("Fetched empty history page"));
static EMPTY_TASK_ERR: LazyLock<tonic::Status> = LazyLock::new(|| {
    tonic::Status::unknown("Received an empty workflow task with no queries or history")
});

/// Buffered history, including any partial sequence at the end. History events are consumed by
/// [HistoryUpdate::take_next_wft_sequence] once the run's chunker has sufficient evidence.
pub(crate) struct HistoryUpdate {
    events: Vec<HistoryEvent>,
    /// The event ID of the last started WFT, as according to the WFT which this update was
    /// extracted from. Hence, while processing multiple logical WFTs during replay which were part
    /// of one large history fetched from server, multiple updates may have the same value here.
    pub(crate) previous_wft_started_id: i64,
    /// The `started_event_id` field from the WFT which this update is tied to. Multiple updates
    /// may have the same value if they're associated with the same WFT.
    pub(crate) wft_started_id: i64,
    /// True if this update contains the final WFT in history, and no more attempts to extract
    /// additional updates should be made.
    has_last_wft: bool,
    pub(super) chunking_v2: bool,
    pub(super) update_positions: Vec<i64>,
}

impl Debug for HistoryUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_real() {
            write!(
                f,
                "HistoryUpdate(previous_started_event_id: {}, started_id: {}, \
                 length: {}, first_event_id: {:?})",
                self.previous_wft_started_id,
                self.wft_started_id,
                self.events.len(),
                self.events.first().map(|e| e.event_id)
            )
        } else {
            write!(f, "DummyHistoryUpdate")
        }
    }
}

impl HistoryUpdate {
    pub(crate) fn get_events(&self) -> &[HistoryEvent] {
        &self.events
    }
}

#[derive(Debug)]
pub(crate) enum NextWFT {
    ReplayOver,
    WFT(Vec<HistoryEvent>, bool),
    NeedFetch,
}

#[derive(derive_more::Debug)]
#[debug("HistoryPaginator(run_id: {run_id})")]
pub(crate) struct HistoryPaginator {
    pub(crate) wf_id: String,
    pub(crate) run_id: String,
    pub(crate) previous_wft_started_id: i64,
    pub(crate) wft_started_event_id: i64,
    id_of_last_event_in_last_extracted_update: Option<i64>,

    client: Arc<dyn WorkerClient>,
    event_queue: VecDeque<HistoryEvent>,
    next_page_token: NextPageToken,
    /// These are events that should be returned once pagination has finished. This only happens
    /// during cache misses, where we got a partial task but need to fetch history from the start.
    final_events: Vec<HistoryEvent>,
}

#[derive(Clone, Debug)]
pub(crate) enum NextPageToken {
    /// There is no page token, we need to fetch history from the beginning
    FetchFromStart,
    /// There is a page token
    Next(Vec<u8>),
    /// There is no page token, we are done fetching history
    Done,
}

// If we're converting from a page token from the server, if it's empty, then we're done.
impl From<Vec<u8>> for NextPageToken {
    fn from(page_token: Vec<u8>) -> Self {
        if page_token.is_empty() {
            NextPageToken::Done
        } else {
            NextPageToken::Next(page_token)
        }
    }
}

impl HistoryPaginator {
    /// Use a new poll response to create a new [WFTPaginator], returning it and the
    /// [PreparedWFT] extracted from it that can be fed into workflow state.
    pub(super) async fn from_poll(
        wft: ValidPollWFTQResponse,
        client: Arc<dyn WorkerClient>,
    ) -> Result<(Self, PreparedWFT), tonic::Status> {
        let empty_hist = wft.history.events.is_empty();
        let npt = if empty_hist {
            NextPageToken::FetchFromStart
        } else {
            wft.next_page_token.into()
        };
        let mut paginator = HistoryPaginator::new(
            wft.history,
            wft.previous_started_event_id,
            wft.started_event_id,
            wft.workflow_execution.workflow_id.clone(),
            wft.workflow_execution.run_id.clone(),
            npt,
            client,
        );
        if empty_hist && wft.legacy_query.is_none() && wft.query_requests.is_empty() {
            return Err(EMPTY_TASK_ERR.clone());
        }
        let update = if empty_hist {
            HistoryUpdate::from_events(
                [],
                wft.previous_started_event_id,
                wft.started_event_id,
                true,
            )
            .0
        } else {
            paginator.extract_next_update().await?
        };
        let prepared = PreparedWFT {
            task_token: wft.task_token,
            attempt: wft.attempt,
            execution: wft.workflow_execution,
            workflow_type: wft.workflow_type,
            legacy_query: wft.legacy_query,
            query_requests: wft.query_requests,
            update,
            messages: wft.messages,
        };
        Ok((paginator, prepared))
    }

    pub(super) async fn from_fetchreq(
        mut req: Box<CacheMissFetchReq>,
        client: Arc<dyn WorkerClient>,
    ) -> Result<PermittedWFT, tonic::Status> {
        let mut paginator = Self {
            wf_id: req.original_wft.work.execution.workflow_id.clone(),
            run_id: req.original_wft.work.execution.run_id.clone(),
            previous_wft_started_id: req.original_wft.work.update.previous_wft_started_id,
            wft_started_event_id: req.original_wft.work.update.wft_started_id,
            id_of_last_event_in_last_extracted_update: None,
            client,
            event_queue: Default::default(),
            next_page_token: NextPageToken::FetchFromStart,
            final_events: req.original_wft.work.update.events,
        };
        let first_update = paginator.extract_next_update().await?;
        req.original_wft.work.update = first_update;
        req.original_wft.paginator = paginator;
        Ok(req.original_wft)
    }

    fn new(
        initial_history: History,
        previous_wft_started_id: i64,
        wft_started_event_id: i64,
        wf_id: String,
        run_id: String,
        next_page_token: impl Into<NextPageToken>,
        client: Arc<dyn WorkerClient>,
    ) -> Self {
        let next_page_token = next_page_token.into();
        let (event_queue, final_events) =
            if matches!(next_page_token, NextPageToken::FetchFromStart) {
                (VecDeque::new(), initial_history.events)
            } else {
                (initial_history.events.into(), vec![])
            };
        Self {
            client,
            event_queue,
            wf_id,
            run_id,
            next_page_token,
            final_events,
            previous_wft_started_id,
            wft_started_event_id,
            id_of_last_event_in_last_extracted_update: None,
        }
    }

    /// Fetch enough history for either chunker to produce the first activation and its lookahead.
    /// Return all fetched events so the run can select its boundaries without losing page tails.
    pub(crate) async fn extract_next_update(&mut self) -> Result<HistoryUpdate, tonic::Status> {
        loop {
            let no_next_page = !self.get_next_page().await?;
            let current_events = mem::take(&mut self.event_queue);
            let seen_enough_events = current_events
                .back()
                .map(|e| e.event_id)
                .unwrap_or_default()
                >= self.wft_started_event_id;

            // This handles a special case where the server might send us a page token along with
            // a real page which ends at the current end of history. The page token then points to
            // en empty page. We need to detect this, and consider it the end of history.
            //
            // This case unfortunately cannot be handled earlier, because we might fetch a page
            // from the server which contains two complete WFTs, and thus we are happy to return
            // an update at that time. But, if the page has a next page token, we *cannot* conclude
            // we are done with replay until we fetch that page. So, we have to wait until the next
            // extraction to determine (after fetching the next page and finding it to be empty)
            // that we are done. Fetching the page eagerly is another option, but would be wasteful
            // the overwhelming majority of the time.
            let already_sent_update_with_enough_events = self
                .id_of_last_event_in_last_extracted_update
                .unwrap_or_default()
                >= self.wft_started_event_id;
            if current_events.is_empty() && no_next_page && already_sent_update_with_enough_events {
                // We must return an empty update which also says is contains the final WFT so we
                // know we're done with replay.
                return Ok(HistoryUpdate::from_events(
                    [],
                    self.previous_wft_started_id,
                    self.wft_started_event_id,
                    true,
                )
                .0);
            }

            if current_events.is_empty() || (no_next_page && !seen_enough_events) {
                // If next page fetching happened, and we still ended up with no or insufficient
                // events, something is wrong. We're expecting there to be more events to be able to
                // extract this update, but server isn't giving us any. We have no choice except to
                // give up and evict.
                error!(
                    current_events=?current_events,
                    no_next_page,
                    seen_enough_events,
                    "We expected to be able to fetch more events but server says there are none"
                );
                return Err(EMPTY_FETCH_ERR.clone());
            }
            // We only *really* have the last WFT if the events go all the way up to at least the
            // WFT started event id. Otherwise we somehow still have partial history.
            let no_more = matches!(self.next_page_token, NextPageToken::Done) && seen_enough_events;
            let events: Vec<_> = current_events.into_iter().collect();
            let mut counts = [0, 0];
            for (mode, count) in counts.iter_mut().enumerate() {
                let mut after = self.previous_wft_started_id;
                let mut offset = 0;
                // Legacy lookahead needs the next sequence. The new scanner already requires the
                // current completion's entire command batch before yielding a boundary.
                let needed = if mode == 0 { 2 } else { 1 };
                while offset < events.len() && *count < needed {
                    let boundary = if mode == 0 {
                        match find_end_index_of_next_wft_seq(&events[offset..], after, no_more) {
                            NextWFTSeqEndIndex::Complete(index) => Some(index),
                            NextWFTSeqEndIndex::Incomplete(_) => None,
                        }
                    } else {
                        super::chunking::boundary(&events[offset..], after, no_more, &[])
                    };
                    let Some(index) = boundary else {
                        break;
                    };
                    *count += 1;
                    after = events[offset + index].event_id;
                    offset += index + 1;
                }
            }
            if !no_more && (counts[0] < 2 || counts[1] < 1) {
                self.event_queue.extend(events);
                continue;
            }
            let update = HistoryUpdate {
                events,
                previous_wft_started_id: self.previous_wft_started_id,
                wft_started_id: self.wft_started_event_id,
                has_last_wft: no_more,
                chunking_v2: false,
                update_positions: vec![],
            };
            self.id_of_last_event_in_last_extracted_update =
                update.events.last().map(|e| e.event_id);
            #[cfg(debug_assertions)]
            update.assert_contiguous();
            return Ok(update);
        }
    }

    /// Fetches the next page and adds it to the internal queue.
    /// Returns true if we still have a next page token after fetching.
    async fn get_next_page(&mut self) -> Result<bool, tonic::Status> {
        let history = loop {
            let npt = match mem::replace(&mut self.next_page_token, NextPageToken::Done) {
                // If the last page token we got was empty, we're done.
                NextPageToken::Done => break None,
                NextPageToken::FetchFromStart => vec![],
                NextPageToken::Next(v) => v,
            };
            debug!(run_id=%self.run_id, "Fetching new history page");
            let fetch_res = self
                .client
                .get_workflow_execution_history(self.wf_id.clone(), Some(self.run_id.clone()), npt)
                .instrument(span!(tracing::Level::TRACE, "fetch_history_in_paginator"))
                .await?;

            self.next_page_token = fetch_res.next_page_token.into();

            let history_is_empty = fetch_res
                .history
                .as_ref()
                .map(|h| h.events.is_empty())
                .unwrap_or(true);
            if history_is_empty && matches!(&self.next_page_token, NextPageToken::Next(_)) {
                // If the fetch returned an empty history, but there *was* a next page token,
                // immediately try to get that.
                continue;
            }
            // Async doesn't love recursion so we do this instead.
            break fetch_res.history;
        };

        let queue_back_id = self
            .event_queue
            .back()
            .map(|e| e.event_id)
            .unwrap_or_default()
            .max(
                self.id_of_last_event_in_last_extracted_update
                    .unwrap_or_default(),
            );
        self.event_queue.extend(
            history
                .map(|h| h.events)
                .unwrap_or_default()
                .into_iter()
                .skip_while(|e| e.event_id <= queue_back_id),
        );
        if matches!(&self.next_page_token, NextPageToken::Done) {
            // If finished, we need to extend the queue with the final events, skipping any
            // which are already present.
            let last_event_id = self
                .event_queue
                .back()
                .map(|e| e.event_id)
                .unwrap_or(queue_back_id);
            let final_events = mem::take(&mut self.final_events);
            if last_event_id > 0 {
                self.event_queue.extend(
                    final_events
                        .into_iter()
                        .skip_while(|e2| e2.event_id <= last_event_id),
                );
            }
        };
        Ok(!matches!(&self.next_page_token, NextPageToken::Done))
    }
}

#[pin_project::pin_project]
struct StreamingHistoryPaginator {
    inner: HistoryPaginator,
    #[pin]
    open_history_request: Option<BoxFuture<'static, Result<(), tonic::Status>>>,
}

impl StreamingHistoryPaginator {
    // Kept since can be used for history downloading
    #[cfg(test)]
    fn new(inner: HistoryPaginator) -> Self {
        Self {
            inner,
            open_history_request: None,
        }
    }
}

impl Stream for StreamingHistoryPaginator {
    type Item = Result<HistoryEvent, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if let Some(e) = this.inner.event_queue.pop_front() {
            return Poll::Ready(Some(Ok(e)));
        }
        if this.open_history_request.is_none() {
            // SAFETY: This is safe because the inner paginator cannot be dropped before the future,
            //   and the future won't be moved from out of this struct.
            this.open_history_request.set(Some(unsafe {
                transmute::<
                    BoxFuture<'_, Result<(), tonic::Status>>,
                    BoxFuture<'static, Result<(), tonic::Status>>,
                >(this.inner.get_next_page().map_ok(|_| ()).boxed())
            }));
        }
        let history_req = this.open_history_request.as_mut().as_pin_mut().unwrap();

        match Future::poll(history_req, cx) {
            Poll::Ready(resp) => {
                this.open_history_request.set(None);
                match resp {
                    Err(neterr) => Poll::Ready(Some(Err(neterr))),
                    Ok(_) => Poll::Ready(this.inner.event_queue.pop_front().map(Ok)),
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl HistoryUpdate {
    /// Sometimes it's useful to take an update out of something without needing to use an option
    /// field. Use this to replace the field with an empty update.
    pub(crate) fn dummy() -> Self {
        Self {
            events: vec![],
            previous_wft_started_id: -1,
            wft_started_id: -1,
            has_last_wft: false,
            chunking_v2: false,
            update_positions: vec![],
        }
    }

    pub(crate) fn is_real(&self) -> bool {
        self.previous_wft_started_id >= 0
    }

    pub(crate) fn first_event_id(&self) -> Option<i64> {
        self.events.first().map(|e| e.event_id)
    }

    #[cfg(debug_assertions)]
    fn assert_contiguous(&self) -> bool {
        use crate::abstractions::dbg_panic;

        for win in self.events.as_slice().windows(2) {
            if let &[e1, e2] = &win
                && e2.event_id != e1.event_id + 1
            {
                dbg_panic!("HistoryUpdate isn't contiguous! {:?} -> {:?}", e1, e2);
            }
        }
        true
    }

    /// Create an instance of an update directly from events. If the passed in event iterator has a
    /// partial WFT sequence at the end, all events after the last complete WFT sequence (ending
    /// with WFT started) are returned back to the caller, since the history update only works in
    /// terms of complete WFT sequences.
    pub(crate) fn from_events<I: IntoIterator<Item = HistoryEvent>>(
        events: I,
        previous_wft_started_id: i64,
        wft_started_id: i64,
        has_last_wft: bool,
    ) -> (Self, Vec<HistoryEvent>)
    where
        <I as IntoIterator>::IntoIter: Send + 'static,
    {
        let mut all_events: Vec<_> = events.into_iter().collect();
        let mut last_end = find_end_index_of_next_wft_seq(
            all_events.as_slice(),
            previous_wft_started_id,
            has_last_wft,
        );
        if matches!(last_end, NextWFTSeqEndIndex::Incomplete(_)) {
            return if has_last_wft {
                (
                    Self {
                        events: all_events,
                        previous_wft_started_id,
                        wft_started_id,
                        has_last_wft,
                        chunking_v2: false,
                        update_positions: vec![],
                    },
                    vec![],
                )
            } else {
                (
                    Self {
                        events: vec![],
                        previous_wft_started_id,
                        wft_started_id,
                        has_last_wft,
                        chunking_v2: false,
                        update_positions: vec![],
                    },
                    all_events,
                )
            };
        }
        while let NextWFTSeqEndIndex::Complete(next_end_ix) = last_end {
            let next_end_eid = all_events[next_end_ix].event_id;
            // To save skipping all events at the front of this slice, only pass the relevant
            // portion, but that means the returned index must be adjusted, hence the addition.
            let next_end = find_end_index_of_next_wft_seq(
                &all_events[next_end_ix..],
                next_end_eid,
                has_last_wft,
            )
            .add(next_end_ix);
            if matches!(next_end, NextWFTSeqEndIndex::Incomplete(_)) {
                break;
            }
            last_end = next_end;
        }
        // If we have the last WFT, there's no point in there being "remaining" events, because
        // they must be considered part of the last sequence
        let remaining_events = if all_events.is_empty() || has_last_wft {
            vec![]
        } else {
            all_events.split_off(last_end.index() + 1)
        };

        (
            Self {
                events: all_events,
                previous_wft_started_id,
                wft_started_id,
                has_last_wft,
                chunking_v2: false,
                update_positions: vec![],
            },
            remaining_events,
        )
    }

    /// Create an instance of an update directly from events. The passed in events *must* consist
    /// of one or more complete WFT sequences. IE: The event iterator must not end in the middle
    /// of a WFT sequence.
    #[cfg(test)]
    fn new_from_events<I: IntoIterator<Item = HistoryEvent>>(
        events: I,
        previous_wft_started_id: i64,
        wft_started_id: i64,
    ) -> Self
    where
        <I as IntoIterator>::IntoIter: Send + 'static,
    {
        Self {
            events: events.into_iter().collect(),
            previous_wft_started_id,
            wft_started_id,
            has_last_wft: true,
            chunking_v2: false,
            update_positions: vec![],
        }
    }

    /// Given a workflow task started id, return all events starting at that number (exclusive) to
    /// the next WFT started event (inclusive).
    ///
    /// Events are *consumed* by this process, to keep things efficient in workflow machines.
    ///
    /// If we are out of WFT sequences that can be yielded by this update, it will return an empty
    /// vec, indicating more pages will need to be fetched.
    pub(crate) fn take_next_wft_sequence(&mut self, from_wft_started_id: i64) -> NextWFT {
        // First, drop any events from the queue which are earlier than the passed-in id.
        if let Some(ix_first_relevant) = self.starting_index_after_skipping(from_wft_started_id) {
            self.events.drain(0..ix_first_relevant);
        }
        let next_wft_ix = self.next_boundary(&self.events, from_wft_started_id);
        match next_wft_ix {
            NextWFTSeqEndIndex::Incomplete(siz) => {
                if self.has_last_wft {
                    if siz == 0 {
                        NextWFT::ReplayOver
                    } else {
                        self.build_next_wft(siz)
                    }
                } else {
                    NextWFT::NeedFetch
                }
            }
            NextWFTSeqEndIndex::Complete(next_wft_ix) => self.build_next_wft(next_wft_ix),
        }
    }

    fn build_next_wft(&mut self, drain_this_much: usize) -> NextWFT {
        NextWFT::WFT(
            self.events.drain(0..=drain_this_much).collect(),
            self.events.is_empty() && self.has_last_wft,
        )
    }

    /// Lets the caller peek ahead at the next WFT sequence that will be returned by
    /// [take_next_wft_sequence]. Will always return the first available WFT sequence if that has
    /// not been called first. May also return an empty iterator or incomplete sequence if we are at
    /// the end of history.
    pub(crate) fn peek_next_wft_sequence(&self, from_wft_started_id: i64) -> &[HistoryEvent] {
        let ix_first_relevant = self
            .starting_index_after_skipping(from_wft_started_id)
            .unwrap_or_default();
        let relevant_events = &self.events[ix_first_relevant..];
        if relevant_events.is_empty() {
            return relevant_events;
        }
        let ix_end = self
            .next_boundary(relevant_events, from_wft_started_id)
            .index();
        &relevant_events[0..=ix_end]
    }

    /// Returns true if this update has the next needed WFT sequence, false if events will need to
    /// be fetched in order to create a complete update with the entire next WFT sequence.
    pub(crate) fn can_take_next_wft_sequence(&self, from_wft_started_id: i64) -> bool {
        let next_wft_ix = self.next_boundary(&self.events, from_wft_started_id);
        if let NextWFTSeqEndIndex::Incomplete(_) = next_wft_ix
            && !self.has_last_wft
        {
            return false;
        }
        true
    }

    fn next_boundary(&self, events: &[HistoryEvent], after: i64) -> NextWFTSeqEndIndex {
        if self.chunking_v2 {
            super::chunking::boundary(events, after, self.has_last_wft, &self.update_positions)
                .map_or(
                    NextWFTSeqEndIndex::Incomplete(events.len().saturating_sub(1)),
                    NextWFTSeqEndIndex::Complete,
                )
        } else {
            find_end_index_of_next_wft_seq(events, after, self.has_last_wft)
        }
    }

    pub(super) fn append(&mut self, mut next: Self) {
        self.events.append(&mut next.events);
        self.has_last_wft = next.has_last_wft;
        self.previous_wft_started_id = next.previous_wft_started_id;
        self.wft_started_id = next.wft_started_id;
    }

    pub(super) fn peek_commands(&self) -> &[HistoryEvent] {
        if self.events.first().map(HistoryEvent::event_type)
            != Some(EventType::WorkflowTaskCompleted)
        {
            return &[];
        }
        let count = self.events[1..]
            .iter()
            .take_while(|event| super::chunking::command_event(event))
            .count();
        &self.events[1..1 + count]
    }

    /// Returns the next WFT completed event attributes, if any, starting at (inclusive) the
    /// `from_id`
    pub(crate) fn peek_next_wft_completed(
        &self,
        from_id: i64,
    ) -> Option<&WorkflowTaskCompletedEventAttributes> {
        self.events
            .iter()
            .skip_while(|e| e.event_id < from_id)
            .find_map(|e| match &e.attributes {
                Some(Attributes::WorkflowTaskCompletedEventAttributes(a)) => Some(a),
                _ => None,
            })
    }

    fn starting_index_after_skipping(&self, from_wft_started_id: i64) -> Option<usize> {
        self.events
            .iter()
            .find_position(|e| e.event_id > from_wft_started_id)
            .map(|(ix, _)| ix)
    }
}

#[derive(Debug, Copy, Clone)]
enum NextWFTSeqEndIndex {
    /// The next WFT sequence is completely contained within the passed-in iterator
    Complete(usize),
    /// The next WFT sequence is not found within the passed-in iterator, and the contained
    /// value is the last index of the iterator.
    Incomplete(usize),
}
impl NextWFTSeqEndIndex {
    fn index(self) -> usize {
        match self {
            NextWFTSeqEndIndex::Complete(ix) | NextWFTSeqEndIndex::Incomplete(ix) => ix,
        }
    }
    fn add(self, val: usize) -> Self {
        match self {
            NextWFTSeqEndIndex::Complete(ix) => NextWFTSeqEndIndex::Complete(ix + val),
            NextWFTSeqEndIndex::Incomplete(ix) => NextWFTSeqEndIndex::Incomplete(ix + val),
        }
    }
}

/// Discovers the index of the last event in next WFT sequence within the passed-in slice
/// For more on workflow task chunking, see arch_docs/workflow_task_chunking.md
fn find_end_index_of_next_wft_seq(
    events: &[HistoryEvent],
    from_event_id: i64,
    has_last_wft: bool,
) -> NextWFTSeqEndIndex {
    if events.is_empty() {
        return NextWFTSeqEndIndex::Incomplete(0);
    }
    let mut last_index = 0;
    let mut saw_command_or_started = false;
    let mut saw_command = false;
    let mut wft_started_event_id_to_index = vec![];
    for (ix, e) in events.iter().enumerate() {
        last_index = ix;

        // It's possible to have gotten a new history update without eviction (ex: unhandled
        // command on completion), where we may need to skip events we already handled.
        if e.event_id <= from_event_id {
            continue;
        }

        if e.is_command_event() {
            saw_command = true;
            saw_command_or_started = true;
        }
        if e.event_type() == EventType::WorkflowExecutionStarted {
            saw_command_or_started = true;
        }
        if e.is_final_wf_execution_event() {
            return NextWFTSeqEndIndex::Complete(last_index);
        }

        if e.event_type() == EventType::WorkflowTaskStarted {
            wft_started_event_id_to_index.push((e.event_id, ix));
            if let Some(next_event) = events.get(ix + 1) {
                let next_event_type = next_event.event_type();
                // If the next event is WFT timeout or fail, or abrupt WF execution end, that
                // doesn't conclude a WFT sequence.
                if matches!(
                    next_event_type,
                    EventType::WorkflowTaskFailed
                        | EventType::WorkflowTaskTimedOut
                        | EventType::WorkflowExecutionTimedOut
                        | EventType::WorkflowExecutionTerminated
                        | EventType::WorkflowExecutionCanceled
                ) {
                    // Since we're skipping this WFT, we don't want to include it in the vec used
                    // for update accepted sequencing lookups.
                    wft_started_event_id_to_index.pop();
                    continue;
                } else if next_event_type == EventType::WorkflowTaskCompleted {
                    if let Some(next_next_event) = events.get(ix + 2) {
                        if !saw_command
                            && next_next_event.event_type() == EventType::WorkflowTaskScheduled
                        {
                            // If we've never seen an interesting event and the next two events are
                            // a completion followed immediately again by scheduled, then this is a
                            // WFT heartbeat and also doesn't conclude the sequence.
                            continue;
                        } else {
                            // If we see an update accepted command after WFT completed, we want to
                            // conclude the WFT sequence where that update should have been
                            // processed. We don't need to check for any other command types,
                            // because the only thing that can run before an update validator is a
                            // signal handler - but if a signal handler ran then there would have
                            // been a previous signal event, and we would've already concluded the
                            // previous WFT sequence.
                            if let Some(
                                Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(
                                    ref attr,
                                ),
                            ) = next_next_event.attributes
                            {
                                // Find index of closest unskipped WFT started before sequencing id.
                                // The fact that the WFT wasn't skipped is important. If it was, we
                                // need to avoid stopping at that point even though that's where the
                                // update was sequenced. If we did, we'll fail to actually include
                                // the update accepted event and therefore fail to generate the
                                // request to run the update handler on replay.
                                if let Some(ret_ix) = wft_started_event_id_to_index
                                    .iter()
                                    .rev()
                                    .find_map(|(eid, ix)| {
                                        if *eid < attr.accepted_request_sequencing_event_id {
                                            return Some(*ix);
                                        }
                                        None
                                    })
                                {
                                    return NextWFTSeqEndIndex::Complete(ret_ix);
                                }
                            }
                            return NextWFTSeqEndIndex::Complete(ix);
                        }
                    } else if !has_last_wft && !saw_command_or_started {
                        // Don't have enough events to look ahead of the WorkflowTaskCompleted. Need
                        // to fetch more.
                        continue;
                    }
                }
            } else if !has_last_wft && !saw_command_or_started {
                // Don't have enough events to look ahead of the WorkflowTaskStarted. Need to fetch
                // more.
                continue;
            }
            if saw_command_or_started {
                return NextWFTSeqEndIndex::Complete(ix);
            }
        }
    }

    NextWFTSeqEndIndex::Incomplete(last_index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        replay::{HistoryInfo, TestHistoryBuilder, canned_histories},
        test_help::{ResponseType, hist_to_poll_resp},
        worker::client::mocks::mock_worker_client,
    };
    use futures_util::TryStreamExt;
    use temporalio_common::protos::temporal::api::{
        common::v1::WorkflowExecution, enums::v1::WorkflowTaskFailedCause,
        workflowservice::v1::GetWorkflowExecutionHistoryResponse,
    };

    impl From<HistoryInfo> for HistoryUpdate {
        fn from(v: HistoryInfo) -> Self {
            Self::new_from_events(
                v.events().to_vec(),
                v.previous_started_event_id(),
                v.workflow_task_started_event_id(),
            )
        }
    }

    trait TestHBExt {
        fn as_history_update(&self) -> HistoryUpdate;
    }

    impl TestHBExt for TestHistoryBuilder {
        fn as_history_update(&self) -> HistoryUpdate {
            self.get_full_history_info().unwrap().into()
        }
    }

    impl NextWFT {
        fn unwrap_events(self) -> Vec<HistoryEvent> {
            match self {
                NextWFT::WFT(e, _) => e,
                o => panic!("Must be complete WFT: {o:?}"),
            }
        }
    }

    fn next_check_peek(update: &mut HistoryUpdate, from_id: i64) -> Vec<HistoryEvent> {
        let seq_peeked = update.peek_next_wft_sequence(from_id).to_vec();
        let seq = update.take_next_wft_sequence(from_id).unwrap_events();
        assert_eq!(seq, seq_peeked);
        seq
    }

    #[test]
    fn consumes_standard_wft_sequence() {
        let timer_hist = canned_histories::single_timer("t");
        let mut update = timer_hist.as_history_update();
        let seq_1 = next_check_peek(&mut update, 0);
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2_peeked = update.peek_next_wft_sequence(0).to_vec();
        let seq_2 = next_check_peek(&mut update, 3);
        assert_eq!(seq_2, seq_2_peeked);
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
    }

    #[test]
    fn skips_wft_failed() {
        let failed_hist = canned_histories::workflow_fails_with_reset_after_timer("t", "runid");
        let mut update = failed_hist.as_history_update();
        let seq_1 = next_check_peek(&mut update, 0);
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2 = next_check_peek(&mut update, 3);
        assert_eq!(seq_2.len(), 8);
        assert_eq!(seq_2.last().unwrap().event_id, 11);
    }

    #[test]
    fn skips_wft_timeout() {
        let failed_hist = canned_histories::wft_timeout_repro();
        let mut update = failed_hist.as_history_update();
        let seq_1 = next_check_peek(&mut update, 0);
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2 = next_check_peek(&mut update, 3);
        assert_eq!(seq_2.len(), 11);
        assert_eq!(seq_2.last().unwrap().event_id, 14);
    }

    #[test]
    fn skips_events_before_desired_wft() {
        let timer_hist = canned_histories::single_timer("t");
        let mut update = timer_hist.as_history_update();
        // We haven't processed the first 3 events, but we should still only get the second sequence
        let seq_2 = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
    }

    #[test]
    fn history_ends_abruptly() {
        let mut timer_hist = canned_histories::single_timer("t");
        timer_hist.add_workflow_execution_terminated();
        let mut update = timer_hist.as_history_update();
        let seq_2 = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq_2.len(), 6);
        assert_eq!(seq_2.last().unwrap().event_id, 9);
    }

    #[test]
    fn heartbeats_skipped() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task(); // wft started 6
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task(); // wft started 10
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_full_wf_task(); // wft started 19
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task(); // wft started 23
        t.add_we_signaled("whee", vec![]);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let mut update = t.as_history_update();
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.len(), 6);
        let seq = next_check_peek(&mut update, 6);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 10);
        assert_eq!(seq.len(), 9);
        let seq = next_check_peek(&mut update, 19);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 23);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 27);
        assert_eq!(seq.len(), 2);
    }

    #[test]
    fn heartbeat_marker_end() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_local_activity_result_marker(1, "1", "done".into());
        t.add_workflow_execution_completed();

        let mut update = t.as_history_update();
        let seq = next_check_peek(&mut update, 3);
        // completed, sched, started
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 6);
        assert_eq!(seq.len(), 3);
    }

    fn paginator_setup(history: TestHistoryBuilder, chunk_size: usize) -> HistoryPaginator {
        let events = history.get_full_history_info().unwrap().into_events();
        let pages = (0..events.len())
            .step_by(chunk_size)
            .map(|start| start..(start + chunk_size).min(events.len()))
            .collect::<Vec<_>>();
        paginator_from_pages(&events, &pages)
    }

    fn paginator_from_pages(
        events: &[HistoryEvent],
        pages: &[std::ops::Range<usize>],
    ) -> HistoryPaginator {
        let mut client = mock_worker_client();
        for (index, page) in pages.iter().enumerate().skip(1) {
            let events = events[page.clone()].to_vec();
            let next_page_token = if index + 1 == pages.len() {
                vec![]
            } else {
                vec![index as u8 + 1]
            };
            client
                .expect_get_workflow_execution_history()
                .times(0..=1)
                .return_once(move |_, _, token| {
                    assert_eq!(token, vec![index as u8]);
                    Ok(GetWorkflowExecutionHistoryResponse {
                        history: Some(History { events }),
                        next_page_token,
                        ..Default::default()
                    })
                });
        }
        HistoryPaginator::new(
            History {
                events: events[pages[0].clone()].to_vec(),
            },
            0,
            events
                .iter()
                .rev()
                .find(|event| event.event_type() == EventType::WorkflowTaskStarted)
                .unwrap()
                .event_id,
            "workflow".into(),
            "run".into(),
            if pages.len() == 1 { vec![] } else { vec![1] },
            Arc::new(client),
        )
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn paginator_extracts_updates(#[values(10, 11, 12, 13, 14)] chunk_size: usize) {
        let wft_count = 100;
        let mut paginator = paginator_setup(
            canned_histories::long_sequential_timers(wft_count),
            chunk_size,
        );
        let mut update = paginator.extract_next_update().await.unwrap();

        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.len(), 3);

        let mut last_event_id = 3;
        let mut last_started_id = 3;
        for i in 1..wft_count {
            let seq = {
                match update.take_next_wft_sequence(last_started_id) {
                    NextWFT::WFT(seq, _) => seq,
                    NextWFT::NeedFetch => {
                        update.append(paginator.extract_next_update().await.unwrap());
                        update
                            .take_next_wft_sequence(last_started_id)
                            .unwrap_events()
                    }
                    NextWFT::ReplayOver => {
                        assert_eq!(i, wft_count - 1);
                        break;
                    }
                }
            };
            for e in &seq {
                last_event_id += 1;
                assert_eq!(e.event_id, last_event_id);
            }
            assert_eq!(seq.len(), 5);
            last_started_id += 5;
        }
    }

    #[tokio::test]
    async fn paginator_streams() {
        let wft_count = 10;
        let paginator = StreamingHistoryPaginator::new(paginator_setup(
            canned_histories::long_sequential_timers(wft_count),
            10,
        ));
        let everything: Vec<_> = paginator.try_collect().await.unwrap();
        assert_eq!(everything.len(), (wft_count + 1) * 5);
        everything.iter().fold(1, |event_id, e| {
            assert_eq!(event_id, e.event_id);
            e.event_id + 1
        });
    }

    #[tokio::test]
    async fn overlapping_pages_preserve_the_unconsumed_tail_once() {
        let mut history = TestHistoryBuilder::default();
        history.add_by_type(EventType::WorkflowExecutionStarted);
        for index in 1..=3 {
            history.add_full_wf_task();
            history.add_timer_started(index.to_string());
        }
        history.add_full_wf_task();
        history.add_workflow_execution_completed();
        let events = history.get_full_history_info().unwrap().into_events();
        let mut paginator = paginator_from_pages(&events, &[0..4, 4..10, 8..events.len()]);
        let mut update = paginator.extract_next_update().await.unwrap();
        assert_eq!(update.get_events().last().unwrap().event_id, 10);
        update.chunking_v2 = true;
        let mut emitted = vec![];
        let mut after = 0;
        loop {
            match update.take_next_wft_sequence(after) {
                NextWFT::WFT(batch, _) => {
                    after = batch.last().unwrap().event_id;
                    emitted.extend(batch.into_iter().map(|event| event.event_id));
                }
                NextWFT::NeedFetch => update.append(paginator.extract_next_update().await.unwrap()),
                NextWFT::ReplayOver => break,
            }
        }
        assert_eq!(
            emitted,
            events
                .into_iter()
                .map(|event| event.event_id)
                .collect::<Vec<_>>()
        );
    }

    fn three_wfts_then_heartbeats() -> TestHistoryBuilder {
        let mut t = TestHistoryBuilder::default();
        // Start with two complete normal WFTs
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task(); // wft start - 3
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task(); // wft start - 7
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task(); // wft start - 11
        for _ in 1..50 {
            // Add a bunch of heartbeats with no commands, which count as one task
            t.add_full_wf_task();
        }
        t.add_workflow_execution_completed();
        t
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn needs_fetch_if_ending_in_middle_of_wft_seq(
        // These values test points truncation could've occurred in the middle of the heartbeat
        #[values(18, 19, 20, 21)] truncate_at: usize,
    ) {
        let t = three_wfts_then_heartbeats();
        let mut ends_in_middle_of_seq = t.as_history_update().events;
        ends_in_middle_of_seq.truncate(truncate_at);
        // The update should contain the first three complete WFTs, ending on the 11th event which
        // is WFT started. The remaining events should be returned. False flags means the creator
        // knows there are more events, so we should return need fetch
        let (mut update, remaining) = HistoryUpdate::from_events(
            ends_in_middle_of_seq,
            0,
            t.get_full_history_info()
                .unwrap()
                .workflow_task_started_event_id(),
            false,
        );
        assert_eq!(remaining[0].event_id, 12);
        assert_eq!(remaining.last().unwrap().event_id, truncate_at as i64);
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 7);
        let seq = update.take_next_wft_sequence(7).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 11);
        let next = update.take_next_wft_sequence(11);
        assert_matches!(next, NextWFT::NeedFetch);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn paginator_works_with_wft_over_multiple_pages(
        #[values(10, 11, 12, 13, 14)] chunk_size: usize,
    ) {
        let t = three_wfts_then_heartbeats();
        let mut paginator = paginator_setup(t, chunk_size);
        let mut update = paginator.extract_next_update().await.unwrap();
        let mut last_id = 0;
        loop {
            let seq = update.take_next_wft_sequence(last_id);
            match seq {
                NextWFT::WFT(seq, _) => {
                    last_id = seq.last().unwrap().event_id;
                }
                NextWFT::NeedFetch => {
                    update.append(paginator.extract_next_update().await.unwrap());
                }
                NextWFT::ReplayOver => break,
            }
        }
        assert_eq!(last_id, 160);
    }

    #[tokio::test]
    async fn task_just_before_heartbeat_chain_is_taken() {
        let t = three_wfts_then_heartbeats();
        let mut update = t.as_history_update();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 7);
        let seq = update.take_next_wft_sequence(7).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 11);
        let seq = update.take_next_wft_sequence(11).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 158);
        let seq = update.take_next_wft_sequence(158).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 160);
        assert_eq!(
            seq.last().unwrap().event_type(),
            EventType::WorkflowExecutionCompleted
        );
    }

    #[tokio::test]
    async fn handles_cache_misses() {
        let timer_hist = canned_histories::single_timer("t");
        let partial_task = timer_hist.get_one_wft(2).unwrap();
        let prev_started_wft_id = partial_task.previous_started_event_id();
        let wft_started_id = partial_task.workflow_task_started_event_id();
        let mut history_from_get: GetWorkflowExecutionHistoryResponse =
            timer_hist.get_history_info(2).unwrap().into();
        // Chop off the last event, which is WFT started, which server doesn't return in get
        // history
        history_from_get.history.as_mut().map(|h| h.events.pop());
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(history_from_get.clone()));

        let mut paginator = HistoryPaginator::new(
            partial_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            // A cache miss means we'll try to fetch from start
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        // We expect if we try to take the first task sequence that the first event is the first
        // event in the sequence.
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq[0].event_id, 1);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        // Verify anything extra (which should only ever be WFT started) was re-appended to the
        // end of the event iteration after fetching the old history.
        assert_eq!(seq.last().unwrap().event_id, 8);
    }

    #[test]
    fn la_marker_chunking() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_full_wf_task(); // started - 7
        t.add_local_activity_result_marker(1, "hi", Default::default());
        let act_s = t.add_activity_task_scheduled("1");
        let act_st = t.add_activity_task_started(act_s);
        t.add_activity_task_completed(act_s, act_st, Default::default());
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_timed_out();
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_timed_out();
        t.add_workflow_task_scheduled_and_started();

        let mut update = t.as_history_update();
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 7);
        assert_eq!(seq.len(), 13);
    }

    #[tokio::test]
    async fn handles_blank_fetch_response() {
        let timer_hist = canned_histories::single_timer("t");
        let partial_task = timer_hist.get_one_wft(2).unwrap();
        let prev_started_wft_id = partial_task.previous_started_event_id();
        let wft_started_id = partial_task.workflow_task_started_event_id();
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(Default::default()));

        let mut paginator = HistoryPaginator::new(
            partial_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            // A cache miss means we'll try to fetch from start
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
        );
        let err = paginator.extract_next_update().await.unwrap_err();
        assert_matches!(err.code(), tonic::Code::Unknown);
    }

    #[tokio::test]
    async fn handles_empty_page_with_next_token() {
        let timer_hist = canned_histories::single_timer("t");
        let partial_task = timer_hist.get_one_wft(2).unwrap();
        let prev_started_wft_id = partial_task.previous_started_event_id();
        let wft_started_id = partial_task.workflow_task_started_event_id();
        let full_resp: GetWorkflowExecutionHistoryResponse =
            timer_hist.get_full_history_info().unwrap().into();
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History { events: vec![] }),
                    raw_history: vec![],
                    next_page_token: vec![2],
                    archived: false,
                })
            })
            .times(1);
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(full_resp.clone()))
            .times(1);

        let mut paginator = HistoryPaginator::new(
            partial_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            // A cache miss means we'll try to fetch from start
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 8);
        assert_matches!(update.take_next_wft_sequence(8), NextWFT::ReplayOver);
    }

    // TODO: Test we dont re-feed pointless updates if fetching returns <= events we already
    //   processed

    #[tokio::test]
    async fn handles_fetching_page_with_complete_wft_and_page_token_to_empty_page() {
        let timer_hist = canned_histories::single_timer("t");
        let workflow_task = timer_hist.get_full_history_info().unwrap();
        let prev_started_wft_id = workflow_task.previous_started_event_id();
        let wft_started_id = workflow_task.workflow_task_started_event_id();

        let mut full_resp_with_npt: GetWorkflowExecutionHistoryResponse =
            timer_hist.get_full_history_info().unwrap().into();
        full_resp_with_npt.next_page_token = vec![1];

        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(full_resp_with_npt.clone()))
            .times(1);
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History { events: vec![] }),
                    raw_history: vec![],
                    next_page_token: vec![],
                    archived: false,
                })
            })
            .times(1);

        let mut paginator = HistoryPaginator::new(
            workflow_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 8);
        assert_matches!(update.take_next_wft_sequence(8), NextWFT::ReplayOver);
    }

    #[tokio::test]
    async fn extreme_pagination_doesnt_drop_wft_events_paginator() {
        // 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
        // 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
        // 3: EVENT_TYPE_WORKFLOW_TASK_STARTED // <- previous_started_event_id
        // 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED

        // 5: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 6: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
        // 7: EVENT_TYPE_WORKFLOW_TASK_STARTED
        // 8: EVENT_TYPE_WORKFLOW_TASK_FAILED

        // 9: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 10: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 11: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 12: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 13: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 14: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
        // 15: EVENT_TYPE_WORKFLOW_TASK_STARTED // <- started_event_id

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();

        t.add_we_signaled("hi", vec![]);
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_failed_with_failure(
            WorkflowTaskFailedCause::UnhandledCommand,
            Default::default(),
        );

        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_workflow_task_scheduled_and_started();

        let mut mock_client = mock_worker_client();

        let events: Vec<HistoryEvent> = t.get_full_history_info().unwrap().into_events();
        let first_event = events[0].clone();
        for (i, event) in events.into_iter().enumerate() {
            // Add an empty page
            mock_client
                .expect_get_workflow_execution_history()
                .returning(move |_, _, _| {
                    Ok(GetWorkflowExecutionHistoryResponse {
                        history: Some(History { events: vec![] }),
                        raw_history: vec![],
                        next_page_token: vec![(i * 10) as u8],
                        archived: false,
                    })
                })
                .times(1);

            // Add a page with only event i
            mock_client
                .expect_get_workflow_execution_history()
                .returning(move |_, _, _| {
                    Ok(GetWorkflowExecutionHistoryResponse {
                        history: Some(History {
                            events: vec![event.clone()],
                        }),
                        raw_history: vec![],
                        next_page_token: vec![(i * 10 + 1) as u8],
                        archived: false,
                    })
                })
                .times(1);
        }

        // Add an extra empty page at the end, with no NPT
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History { events: vec![] }),
                    raw_history: vec![],
                    next_page_token: vec![],
                    archived: false,
                })
            })
            .times(1);

        let mut paginator = HistoryPaginator::new(
            History {
                events: vec![first_event],
            },
            3,
            15,
            "wfid".to_string(),
            "runid".to_string(),
            vec![1],
            Arc::new(mock_client),
        );

        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.first().unwrap().event_id, 1);
        assert_eq!(seq.last().unwrap().event_id, 3);

        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.first().unwrap().event_id, 4);
        assert_eq!(seq.last().unwrap().event_id, 15);
    }

    #[tokio::test]
    async fn finding_end_index_with_started_as_last_event() {
        let wf_id = "fakeid";
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();

        t.add_we_signaled("hi", vec![]);
        t.add_workflow_task_scheduled_and_started();
        // We need to see more after this - it's not sufficient to end on a started event when
        // we know there might be more

        let workflow_task = t.get_history_info(1).unwrap();
        let prev_started_wft_id = workflow_task.previous_started_event_id();
        let wft_started_id = workflow_task.workflow_task_started_event_id();
        let mut wft_resp = workflow_task.as_poll_wft_response();
        wft_resp.workflow_execution = Some(WorkflowExecution {
            workflow_id: wf_id.to_string(),
            run_id: t.get_orig_run_id().to_string(),
        });
        wft_resp.next_page_token = vec![1];

        let mut resp_1: GetWorkflowExecutionHistoryResponse =
            t.get_full_history_info().unwrap().into();
        resp_1.next_page_token = vec![2];

        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(resp_1.clone()))
            .times(1);
        // Since there aren't sufficient events, we should try to see another fetch, and that'll
        // say there aren't any
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(Default::default()))
            .times(1);

        let mut paginator = HistoryPaginator::new(
            workflow_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        // We're done since the last fetch revealed nothing
        assert_eq!(seq.last().unwrap().event_id, 7);
    }

    #[tokio::test]
    async fn just_signal_is_complete_wft() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let workflow_task = t.get_full_history_info().unwrap();
        let prev_started_wft_id = workflow_task.previous_started_event_id();
        let wft_started_id = workflow_task.workflow_task_started_event_id();
        let mock_client = mock_worker_client();
        let mut paginator = HistoryPaginator::new(
            workflow_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::Done,
            Arc::new(mock_client),
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 7);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 11);
        assert_eq!(seq.len(), 2);
    }

    #[tokio::test]
    async fn heartbeats_then_signal() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        let mut need_fetch_resp =
            hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory).resp;
        need_fetch_resp.next_page_token = vec![1];
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_workflow_task_scheduled_and_started();

        let full_resp: GetWorkflowExecutionHistoryResponse =
            t.get_full_history_info().unwrap().into();

        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(full_resp.clone()))
            .times(1);

        let mut paginator = HistoryPaginator::new(
            need_fetch_resp.history.unwrap(),
            // Pretend we have already processed first WFT
            3,
            6,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::Next(vec![1]),
            Arc::new(mock_client),
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        // Starting past first wft
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 6);
        let seq = next_check_peek(&mut update, 9);
        assert_eq!(seq.len(), 4);
    }

    #[tokio::test]
    async fn cache_miss_with_only_one_wft_available_orders_properly() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task();
        t.add_by_type(EventType::TimerStarted);
        t.add_workflow_task_scheduled_and_started();

        let incremental_task =
            hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::OneTask(3)).resp;

        let mut mock_client = mock_worker_client();
        let mut one_task_resp: GetWorkflowExecutionHistoryResponse =
            t.get_history_info(1).unwrap().into();
        one_task_resp.next_page_token = vec![1];
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(one_task_resp.clone()))
            .times(1);
        let mut up_to_sched_start: GetWorkflowExecutionHistoryResponse =
            t.get_full_history_info().unwrap().into();
        up_to_sched_start
            .history
            .as_mut()
            .unwrap()
            .events
            .truncate(9);
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(up_to_sched_start.clone()))
            .times(1);

        let mut paginator = HistoryPaginator::new(
            incremental_task.history.unwrap(),
            6,
            9,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.last().unwrap().event_id, 7);
        let seq = next_check_peek(&mut update, 7);
        assert_eq!(seq.last().unwrap().event_id, 11);
    }

    #[tokio::test]
    async fn wft_fail_on_first_task_with_update() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_failed_with_failure(
            WorkflowTaskFailedCause::Unspecified,
            Default::default(),
        );
        t.add_full_wf_task();
        let accept_id = t.add_update_accepted("1", "upd");
        let timer_id = t.add_timer_started("1".to_string());
        t.add_update_completed(accept_id);
        t.add_timer_fired(timer_id, "1".to_string());
        t.add_full_wf_task();

        let mut update = t.as_history_update();
        let seq = next_check_peek(&mut update, 0);
        // In this case, we expect to see up to the task with update, since the task failure
        // should be skipped. This means that the peek of the _next_ task will include the update
        // and thus properly synthesize the update request with the first activation.
        assert_eq!(seq.len(), 6);
        let seq = next_check_peek(&mut update, 6);
        assert_eq!(seq.len(), 7);
    }

    #[test]
    fn update_accepted_after_empty_wft() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        let accept_id = t.add_update_accepted("1", "upd");
        let timer_id = t.add_timer_started("1".to_string());
        t.add_update_completed(accept_id);
        t.add_timer_fired(timer_id, "1".to_string());
        t.add_full_wf_task();

        let mut update = t.as_history_update();
        let seq = next_check_peek(&mut update, 0);
        // unlike the case with a wft failure, here the first task should not extend through to
        // the update, because here the first empty WFT happened with _just_ the workflow init,
        // not also with the update.
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 3);
    }
    mod chunking_tests {
        use super::*;
        use crate::{internal_flags::CoreInternalFlags, worker::workflow::chunking::RunVersion};
        use EventType::{
            WorkflowExecutionStarted as WES, WorkflowExecutionUpdateAccepted as ACCEPTED,
            WorkflowExecutionUpdateAdmitted as ADMITTED, WorkflowTaskCompleted as COMPLETED,
            WorkflowTaskScheduled as SCHEDULED, WorkflowTaskStarted as STARTED,
        };
        use temporalio_common::protos::temporal::api::{
            history::v1::WorkflowExecutionUpdateAcceptedEventAttributes,
            sdk::v1::WorkflowTaskCompletedMetadata,
        };

        fn history(types: &[EventType]) -> Vec<HistoryEvent> {
            types
                .iter()
                .enumerate()
                .map(|(index, kind)| HistoryEvent {
                    event_id: index as i64 + 1,
                    event_type: *kind as i32,
                    ..Default::default()
                })
                .collect()
        }

        fn set_wft_flags(events: &mut [HistoryEvent], event_id: i64, flags: Vec<u32>) {
            events[event_id as usize - 1].attributes =
                Some(Attributes::WorkflowTaskCompletedEventAttributes(
                    WorkflowTaskCompletedEventAttributes {
                        sdk_metadata: Some(WorkflowTaskCompletedMetadata {
                            core_used_flags: flags,
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ));
        }

        fn v2_update(events: &[HistoryEvent], finished: bool, updates: &[i64]) -> HistoryUpdate {
            let mut update = HistoryUpdate::new_from_events(events.to_vec(), 0, 0);
            update.has_last_wft = finished;
            update.chunking_v2 = true;
            update.update_positions = updates.to_vec();
            update
        }

        fn assert_boundary(
            events: &[HistoryEvent],
            after: i64,
            finished: bool,
            updates: &[i64],
            expected: Option<i64>,
        ) {
            let mut update = v2_update(events, finished, updates);
            assert_eq!(update.can_take_next_wft_sequence(after), expected.is_some());
            if let Some(expected) = expected {
                let batch = next_check_peek(&mut update, after);
                assert_eq!(batch.last().unwrap().event_id, expected);
            } else {
                assert_matches!(update.take_next_wft_sequence(after), NextWFT::NeedFetch);
            }
        }

        async fn assert_chunking(
            mut events: Vec<HistoryEvent>,
            after: i64,
            updates: &[i64],
            expected_prefix: &[i64],
            lookahead: Option<(i64, usize)>,
        ) {
            let completion = events
                .iter()
                .find(|event| event.event_type() == COMPLETED)
                .unwrap()
                .event_id;
            set_wft_flags(
                &mut events,
                completion,
                vec![CoreInternalFlags::WftChunkingV2 as u32],
            );
            let partitions = (1..=events.len())
                .map(|size| {
                    (0..events.len())
                        .step_by(size)
                        .map(|start| start..(start + size).min(events.len()))
                        .collect()
                })
                .chain((5..=events.len()).map(|cut| vec![0..4, 4..cut, cut..events.len()]));
            for pages in partitions {
                let mut paginator = paginator_from_pages(&events, &pages);
                let mut update = paginator.extract_next_update().await.unwrap();
                update.chunking_v2 = true;
                update.update_positions = updates.to_vec();
                let (mut from, mut boundaries, mut emitted) = (after, vec![], vec![]);
                loop {
                    let ready = update.can_take_next_wft_sequence(from);
                    let peeked = update.peek_next_wft_sequence(from).to_vec();
                    match update.take_next_wft_sequence(from) {
                        NextWFT::WFT(batch, _) => {
                            assert!(ready, "pages {pages:?}");
                            assert_eq!(batch, peeked, "pages {pages:?}");
                            from = batch.last().unwrap().event_id;
                            boundaries.push(from);
                            emitted.extend(batch.into_iter().map(|event| event.event_id));
                            if let Some((boundary, count)) = lookahead
                                && boundary == from
                            {
                                assert_eq!(update.peek_commands().len(), count, "pages {pages:?}");
                            }
                        }
                        NextWFT::NeedFetch => {
                            assert!(!ready, "pages {pages:?}");
                            update.append(paginator.extract_next_update().await.unwrap());
                        }
                        NextWFT::ReplayOver => break,
                    }
                }
                assert!(
                    boundaries.starts_with(expected_prefix),
                    "pages {pages:?}: expected {expected_prefix:?}, got {boundaries:?}"
                );
                if expected_prefix.last() == Some(&(events.len() as i64)) {
                    assert_eq!(boundaries, expected_prefix, "pages {pages:?}");
                }
                assert_eq!(
                    emitted,
                    (after + 1..=events.len() as i64).collect::<Vec<_>>(),
                    "pages {pages:?}"
                );
            }
        }

        #[test]
        fn run_choice_is_immutable_and_owned_by_the_run() {
            let mut events = history(&[WES, SCHEDULED, STARTED, COMPLETED]);
            set_wft_flags(
                &mut events,
                4,
                vec![CoreInternalFlags::WftChunkingV2 as u32],
            );
            let mut version = RunVersion::default();
            version.observe(&events).unwrap();
            assert_eq!(version.selected(), Some(true));
            set_wft_flags(&mut events, 4, vec![]);
            version.observe(&events).unwrap();
            assert_eq!(version.selected(), Some(true));
            let mut another = RunVersion::default();
            another.observe(&events).unwrap();
            assert_eq!(another.selected(), Some(false));
        }

        #[tokio::test]
        async fn v2_failure_inbound_and_update_matrix() {
            let heartbeat = [
                WES, SCHEDULED, STARTED, COMPLETED, SCHEDULED, STARTED, COMPLETED, SCHEDULED,
                STARTED,
            ];
            for failure in [
                EventType::WorkflowTaskFailed,
                EventType::WorkflowTaskTimedOut,
            ] {
                let mut types = heartbeat.to_vec();
                types.extend([failure, SCHEDULED, STARTED]);
                assert_chunking(history(&types[..9]), 0, &[8], &[3, 6, 9], None).await;
                assert_chunking(history(&types), 0, &[], &[3, 6, 12], None).await;
                assert_chunking(history(&types), 0, &[11], &[3, 6, 12], None).await;
                types.extend([COMPLETED, ACCEPTED, SCHEDULED, STARTED]);
                assert_chunking(history(&types), 0, &[], &[3, 6, 12, 16], None).await;
            }
            for inbound in [
                EventType::WorkflowExecutionSignaled,
                EventType::TimerFired,
                EventType::ActivityTaskCompleted,
            ] {
                assert_chunking(
                    history(&[
                        WES, SCHEDULED, STARTED, COMPLETED, SCHEDULED, STARTED, COMPLETED, inbound,
                        SCHEDULED, STARTED,
                    ]),
                    0,
                    &[],
                    &[3, 6, 10],
                    None,
                )
                .await;
            }
            let events = history(&[
                WES,
                SCHEDULED,
                STARTED,
                COMPLETED,
                SCHEDULED,
                STARTED,
                COMPLETED,
                SCHEDULED,
                STARTED,
                COMPLETED,
                EventType::MarkerRecorded,
                SCHEDULED,
                STARTED,
            ]);
            assert_chunking(events[..9].to_vec(), 0, &[8], &[3, 6, 9], None).await;
            assert_chunking(events, 0, &[], &[3, 9, 13], None).await;
            assert_chunking(
                history(&[
                    WES,
                    SCHEDULED,
                    STARTED,
                    COMPLETED,
                    EventType::TimerStarted,
                    SCHEDULED,
                    STARTED,
                    EventType::WorkflowExecutionTerminated,
                ]),
                0,
                &[],
                &[3, 7, 8],
                None,
            )
            .await;
        }

        #[tokio::test]
        async fn v2_update_command_batch_is_pagination_independent() {
            let mut events = history(&[
                WES,
                SCHEDULED,
                STARTED,
                COMPLETED,
                SCHEDULED,
                STARTED,
                COMPLETED,
                SCHEDULED,
                STARTED,
                COMPLETED,
                EventType::MarkerRecorded,
                EventType::Unspecified,
                ACCEPTED,
                ACCEPTED,
                EventType::MarkerRecorded,
                SCHEDULED,
                STARTED,
            ]);
            events[11].worker_may_ignore = true;
            let mut live = v2_update(&events[..9], true, &[8]);
            assert_eq!(next_check_peek(&mut live, 0).last().unwrap().event_id, 3);
            assert_eq!(next_check_peek(&mut live, 3).last().unwrap().event_id, 6);
            assert_chunking(events, 0, &[], &[3, 6, 9, 17], Some((9, 5))).await;
        }

        #[tokio::test]
        async fn failed_attempt_does_not_select_before_flagged_first_success() {
            let mut events = history(&[
                WES,
                SCHEDULED,
                STARTED,
                EventType::WorkflowTaskFailed,
                SCHEDULED,
                STARTED,
                COMPLETED,
            ]);
            set_wft_flags(
                &mut events,
                7,
                vec![CoreInternalFlags::WftChunkingV2 as u32],
            );
            for cut in 1..=events.len() {
                let pages = if cut == 1 {
                    vec![0..1, 1..4, 4..7]
                } else {
                    vec![0..cut, cut..events.len()]
                };
                let update = paginator_from_pages(&events, &pages)
                    .extract_next_update()
                    .await
                    .unwrap();
                let mut version = RunVersion::default();
                version.observe(update.get_events()).unwrap();
                assert_eq!(version.selected(), Some(true), "page cut {cut}");
                version.observe(&events[4..]).unwrap();
                assert_eq!(version.selected(), Some(true));
            }
            let mut events = history(&[
                WES, SCHEDULED, STARTED, COMPLETED, SCHEDULED, STARTED, COMPLETED,
            ]);
            set_wft_flags(
                &mut events,
                7,
                vec![CoreInternalFlags::WftChunkingV2 as u32],
            );
            for first_flags in [vec![], vec![1_000_000]] {
                set_wft_flags(&mut events, 4, first_flags.clone());
                let update =
                    paginator_from_pages(&events, std::slice::from_ref(&(0..events.len())))
                        .extract_next_update()
                        .await
                        .unwrap();
                let mut version = RunVersion::default();
                let result = version.observe(update.get_events());
                if first_flags.is_empty() {
                    result.unwrap();
                    assert_eq!(version.selected(), Some(false));
                } else {
                    let error = result.unwrap_err().to_string();
                    assert!(error.contains("1000000"));
                    assert!(error.contains("event 4"));
                }
            }
        }

        #[rstest::rstest]
        #[case::live(true)]
        #[case::admitted(false)]
        #[tokio::test]
        async fn update_evidence_preserves_successive_wft_boundaries(#[case] live: bool) {
            let mut types = vec![
                WES, SCHEDULED, STARTED, COMPLETED, SCHEDULED, STARTED, COMPLETED,
            ];
            let mut updates = vec![];
            let mut expected = vec![6];
            for count in [2, 1] {
                types.extend([SCHEDULED, EventType::WorkflowExecutionSignaled]);
                if live {
                    updates.extend(vec![types.len() as i64; count]);
                } else {
                    types.extend(vec![ADMITTED; count]);
                }
                types.push(STARTED);
                if count == 2 {
                    expected.push(types.len() as i64);
                    types.push(COMPLETED);
                }
            }
            assert_chunking(history(&types), 3, &updates, &expected, None).await;
        }

        #[test]
        fn sequencing_evidence_respects_page_cuts() {
            let events = history(&[
                WES,
                SCHEDULED,
                STARTED,
                COMPLETED,
                SCHEDULED,
                STARTED,
                COMPLETED,
                SCHEDULED,
                ADMITTED,
                EventType::WorkflowExecutionSignaled,
                STARTED,
            ]);
            for updates in [vec![], vec![11]] {
                assert_boundary(&events[..8], 3, false, &updates, None);
            }
            for cut in [6, 7, 8] {
                assert_boundary(&events[..cut], 3, false, &[], None);
            }
            for cut in [9, 10, 11] {
                assert_boundary(&events[..cut], 3, cut == 11, &[], Some(6));
            }
        }

        #[tokio::test]
        async fn admitted_then_accepted_paginates_across_boundary_splits() {
            let mut events = history(&[
                WES, SCHEDULED, STARTED, COMPLETED, SCHEDULED, STARTED, COMPLETED, SCHEDULED,
                ADMITTED, STARTED, COMPLETED, ACCEPTED,
            ]);
            events[11].attributes =
                Some(Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(
                    WorkflowExecutionUpdateAcceptedEventAttributes {
                        protocol_instance_id: "admitted-update".into(),
                        accepted_request_message_id: "admitted-update/request".into(),
                        accepted_request_sequencing_event_id: 9,
                        accepted_request: None,
                    },
                ));
            assert_chunking(events, 3, &[], &[6], None).await;
        }

        #[rstest::rstest]
        #[case::failed(EventType::WorkflowTaskFailed)]
        #[case::timed_out(EventType::WorkflowTaskTimedOut)]
        #[tokio::test]
        async fn admitted_update_boundary_skips_failed_wft_starts(#[case] failure: EventType) {
            assert_chunking(
                history(&[
                    WES, SCHEDULED, STARTED, COMPLETED, SCHEDULED, STARTED, COMPLETED, SCHEDULED,
                    STARTED, failure, SCHEDULED, ADMITTED, STARTED,
                ]),
                3,
                &[],
                &[6],
                None,
            )
            .await;
        }

        #[rstest::rstest]
        #[case::less_than_from(5)]
        #[case::equal_to_from(6)]
        #[tokio::test]
        async fn consumed_sequencing_evidence_does_not_create_a_new_boundary(
            #[case] position: i64,
        ) {
            let events = history(&[
                WES, SCHEDULED, STARTED, COMPLETED, SCHEDULED, STARTED, COMPLETED, SCHEDULED,
                STARTED, COMPLETED, SCHEDULED, STARTED, COMPLETED, SCHEDULED, STARTED,
            ]);
            assert_chunking(events.clone(), 6, &[], &[15], None).await;
            assert_chunking(events, 6, &[position], &[15], None).await;
        }

        #[tokio::test]
        async fn live_update_before_first_candidate_keeps_its_activation() {
            let events = history(&[
                WES, SCHEDULED, STARTED, COMPLETED, SCHEDULED, STARTED, COMPLETED, SCHEDULED,
                STARTED, COMPLETED, SCHEDULED, STARTED,
            ]);
            assert_chunking(events.clone(), 3, &[], &[12], None).await;
            assert_chunking(events, 3, &[4], &[6], None).await;
        }

        #[rstest::rstest]
        #[case::adjacent_live(true, &[], &[])]
        #[case::live_after_one_signal(true, &[], &[EventType::WorkflowExecutionSignaled])]
        #[case::live_after_two_signals(true, &[], &[EventType::WorkflowExecutionSignaled; 2])]
        #[case::live_after_cancel(true, &[], &[EventType::WorkflowExecutionCancelRequested])]
        #[case::live_after_command(true, &[EventType::TimerStarted], &[])]
        #[case::adjacent_admitted(false, &[], &[])]
        #[case::admitted_after_one_signal(false, &[], &[EventType::WorkflowExecutionSignaled])]
        #[case::admitted_after_two_signals(false, &[], &[EventType::WorkflowExecutionSignaled; 2])]
        #[tokio::test]
        async fn sequencing_evidence_selects_preceding_logical_wft(
            #[case] live: bool,
            #[case] commands: &[EventType],
            #[case] inbound: &[EventType],
        ) {
            let mut types = vec![
                WES, SCHEDULED, STARTED, COMPLETED, SCHEDULED, STARTED, COMPLETED,
            ];
            types.extend(commands);
            types.push(SCHEDULED);
            types.extend(inbound);
            let position = types.len() as i64;
            if !live {
                types.push(ADMITTED);
            }
            types.push(STARTED);
            let updates = if live { vec![position] } else { vec![] };
            assert_chunking(history(&types), 3, &updates, &[6], None).await;
        }
    }
}
