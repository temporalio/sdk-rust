//! Reads the Records of History's read entries again for a replay. The entry of a read has only a
//! count and a digest, so the replay reads that many Records after the entry's offset and checks
//! them against the digest.

use super::{
    StreamRuntimes,
    digest::DigestFormat,
    manager::{cut, is_outage, next_wait, retry_waits},
    marker::{is_read_marker, peeked_read_marker},
};
use crate::worker::{
    WorkerConfig,
    workflow::{
        NextPageReq, PermittedWFT,
        history_update::{HistoryPaginator, HistoryUpdate},
        wft_extraction::{FetchedPage, HistfetchRC, WFTExtractorOutput},
    },
};
use futures_util::{
    StreamExt,
    future::{self, Either},
    stream,
};
use prost::Message;
use std::{
    collections::{HashMap, VecDeque},
    pin::pin,
    sync::Arc,
    time::Duration,
};
use temporalio_common::protos::{
    coresdk::external_data::{StreamReadPage, StreamReadResult, stream_read_result},
    temporal::api::history::v1::HistoryEvent,
};
use temporalio_streams::{
    AnyReason, ReadReason,
    proto::{
        StreamRecord,
        sdk::{ReadRecordsRequest, StreamRef},
    },
    record::field_len,
};
use tokio::{sync::watch, time::Instant};
use tracing::debug;

/// Why a replay can't give Workflow code the Records of a read entry.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ReplayError {
    /// The Store has fewer Records after the entry's offset than the first run got, or other ones.
    Gone(String),
    /// The Store gave no answer, or this Worker can't read from it, so another attempt may still
    /// read the Records.
    Unavailable(String),
    /// This Worker has no Stream runtime for the Store of the entry, so it can't replay History.
    NoRuntime(String),
}

/// The Records that a replay read for the read entries of one marker, by the index of each entry.
/// An entry with no Records has no result.
pub(crate) type MarkerReads = Vec<Option<Result<Vec<StreamRecord>, ReplayError>>>;

/// The Records that a replay read for the markers of a History, by the event id of each marker.
#[derive(Debug, Default)]
pub(crate) struct ReplayReads {
    pub(crate) reads: HashMap<i64, MarkerReads>,
    pub(crate) unread: Option<i64>,
}

impl ReplayReads {
    /// Returns true if a run new to the cache must read the Records of the markers in the
    /// events of `update` before it replays them.
    pub(crate) fn needed_by(update: &HistoryUpdate) -> bool {
        update.stream_reads.is_none() && update.get_events().iter().any(is_read_marker)
    }

    /// Takes the Records that were read for the marker event `event_id` of `update`.
    pub(crate) fn take_from(update: &mut HistoryUpdate, event_id: i64) -> MarkerReads {
        (update.stream_reads.as_mut()).map_or_else(Vec::new, |reads| reads.take(event_id))
    }

    /// Adds the Records of the next markers to `update`, which a completion waited for.
    pub(crate) fn add_to(self, update: &mut HistoryUpdate) {
        if let Some(reads) = &mut update.stream_reads {
            reads.extend(self);
        }
    }

    /// Takes the results of the read entries of the marker event `event_id`.
    pub(crate) fn take(&mut self, event_id: i64) -> MarkerReads {
        self.reads.remove(&event_id).unwrap_or_default()
    }

    /// Returns the event id of the first marker whose Records the replay hasn't read yet.
    pub(crate) fn unread(&self) -> Option<i64> {
        self.unread
    }

    /// Adds the Records of the next part, and takes its first marker that is still unread.
    pub(crate) fn extend(&mut self, part: ReplayReads) {
        self.reads.extend(part.reads);
        self.unread = part.unread;
    }
}

/// A read of the Records that a replay needs for the markers of its History. The task
/// slots of the Worker bound these reads, since each one is for a task that holds a slot.
#[derive(Debug)]
pub(in crate::worker::workflow) enum StreamRecordsReq {
    /// The first part, for the task of a run new to the cache, before the run replays.
    NewRun(Box<PermittedWFT>, Arc<HistfetchRC>),
    /// The next part, for a completion of the run that waits for it.
    NextPart(Box<NextPageReq>, Arc<HistfetchRC>),
}

impl StreamRecordsReq {
    /// Reads the Records. Each read entry gets its own result, so the read never fails as a whole.
    pub(in crate::worker::workflow) async fn read(self) -> WFTExtractorOutput {
        match self {
            StreamRecordsReq::NewRun(mut pwft, rc) => {
                let PermittedWFT {
                    work, paginator, ..
                } = pwft.as_mut();
                ReplayReader::read_first_part(paginator, &mut work.update).await;
                WFTExtractorOutput::FetchResult(*pwft, rc)
            }
            StreamRecordsReq::NextPart(mut req, rc) => {
                let reads = ReplayReader::read_next_part(&mut req.paginator).await;
                WFTExtractorOutput::NextPage {
                    paginator: req.paginator,
                    update: FetchedPage::StreamRecords(reads),
                    span: req.span,
                    rc,
                }
            }
        }
    }
}

/// Returns true if the replay of `entry` needs its Records from the Store.
pub(crate) fn needs_records(entry: &StreamReadResult) -> bool {
    unread_page(entry).is_some()
}

/// Returns the page of `entry` if it has Records.
fn unread_page(entry: &StreamReadResult) -> Option<&StreamReadPage> {
    match &entry.result {
        Some(stream_read_result::Result::Page(page)) if page.record_count > 0 => Some(page),
        _ => None,
    }
}

/// A marker of History whose Records a replay hasn't read yet.
#[derive(Debug)]
struct UnreadMarker {
    event_id: i64,
    /// The number of read entries of the marker.
    len: usize,
    /// The read entries that need Records from the Store.
    entries: Vec<UnreadEntry>,
}

impl UnreadMarker {
    /// Returns the bytes of Records that the entries recorded. History sets them, so they can be
    /// any size.
    fn recorded_bytes(&self) -> usize {
        let bytes =
            (self.entries.iter()).fold(0, |sum: u64, entry| sum.saturating_add(entry.record_bytes));
        usize::try_from(bytes).unwrap_or(usize::MAX)
    }
}

/// What a replay needs of a read entry to read its Records again.
#[derive(Debug)]
struct UnreadEntry {
    /// The index of the entry among the read entries of its marker.
    index: usize,
    stream: StreamRef,
    after_offset: Vec<u8>,
    count: u64,
    digest: Vec<u8>,
    record_bytes: u64,
}

/// A reader of the Records of read entries, for the replay of one run. It keeps the markers of
/// History that it hasn't read yet, and reads them a part at a time.
#[derive(Debug)]
pub(crate) struct ReplayReader {
    runtimes: StreamRuntimes,
    unread: VecDeque<UnreadMarker>,
}

impl ReplayReader {
    pub(crate) fn new(runtimes: StreamRuntimes) -> Self {
        let unread = VecDeque::new();
        Self { runtimes, unread }
    }

    /// Queues each marker in `events` that has a read entry with Records, and returns the bytes of
    /// Records that they record.
    pub(crate) fn queue<'a>(&mut self, events: impl Iterator<Item = &'a HistoryEvent>) -> u64 {
        let mut recorded = 0_u64;
        for event in events {
            let Some(Ok(data)) = peeked_read_marker(event) else {
                continue;
            };
            let unread = |(index, entry): (usize, &StreamReadResult)| {
                let page = unread_page(entry)?;
                Some(UnreadEntry {
                    index,
                    stream: StreamRef {
                        id: entry.stream_id.clone(),
                        store: entry.store.clone(),
                    },
                    after_offset: entry.from_offset.clone(),
                    count: u64::from(page.record_count),
                    digest: page.digest.clone(),
                    record_bytes: page.record_bytes,
                })
            };
            let reads = &data.reads;
            let entries: Vec<UnreadEntry> = (reads.iter().enumerate()).filter_map(unread).collect();
            recorded = (entries.iter()).fold(recorded, |sum, e| sum.saturating_add(e.record_bytes));
            if !entries.is_empty() {
                self.unread.push_back(UnreadMarker {
                    event_id: event.event_id,
                    len: reads.len(),
                    entries,
                });
            }
        }
        recorded
    }

    /// Returns true while a queued marker has no Records yet.
    pub(crate) fn has_unread(&self) -> bool {
        !self.unread.is_empty()
    }

    /// Reads the Records of the next part of the queue. A part is the next markers while their
    /// recorded bytes stay within `MAX_REPLAY_PART_BYTES`, and at least one marker.
    pub(crate) async fn read_next(&mut self) -> ReplayReads {
        let max_bytes = self.runtimes.settings.max_replay_part_bytes();
        let mut reads = ReplayReads::default();
        // The read takes markers by their recorded bytes before any Store call, so that the
        // entries of all the markers that it takes can read at the same time.
        let (mut entries, mut taken_bytes) = (vec![], 0_usize);
        while let Some(marker) = self.unread.pop_front() {
            let marker_bytes = marker.recorded_bytes();
            if !reads.reads.is_empty() && taken_bytes.saturating_add(marker_bytes) > max_bytes {
                self.unread.push_front(marker);
                break;
            }
            taken_bytes = taken_bytes.saturating_add(marker_bytes);
            (reads.reads).insert(marker.event_id, vec![None; marker.len]);
            entries.extend((marker.entries.into_iter()).map(|entry| (marker.event_id, entry)));
        }
        reads.unread = self.unread.front().map(|marker| marker.event_id);
        // Each Store has its own deadline, so the outage of one Store ends no read of another.
        let deadlines: HashMap<String, Deadline> = (entries.iter())
            .map(|(_, entry)| (entry.stream.store.clone(), Deadline::default()))
            .collect();
        let (reader, deadlines) = (&*self, &deadlines);
        let mut read = stream::iter(entries)
            .map(|(event_id, entry)| async move {
                let deadline = &deadlines[&entry.stream.store];
                (
                    event_id,
                    entry.index,
                    reader.read_entry(&entry, deadline).await,
                )
            })
            .buffer_unordered(self.runtimes.settings.replay_concurrency());
        while let Some((event_id, index, result)) = read.next().await {
            let marker = (reads.reads.get_mut(&event_id)).expect("Each entry has its marker");
            marker[index] = Some(result);
        }
        reads
    }

    /// Makes `paginator` read the Records of the markers of its History, for a run new to
    /// the cache.
    pub(crate) fn add_to(paginator: &mut HistoryPaginator, config: &WorkerConfig) {
        let runtimes = config.stream_runtimes.clone();
        paginator.stream_reader = Some(ReplayReader::new(runtimes));
    }

    /// Reads the first part of the Records of the markers of `update`, which has the whole
    /// History, and keeps the markers after it for [`Self::read_next_part`].
    async fn read_first_part(paginator: &mut HistoryPaginator, update: &mut HistoryUpdate) {
        if let Some(reader) = &mut paginator.stream_reader {
            let recorded = reader.queue(update.get_events().iter());
            if recorded > 0 {
                debug!(
                    run_id = %paginator.run_id,
                    recorded_bytes = recorded,
                    "The replay reads the Records of the read markers of its History again"
                );
            }
            update.stream_reads = Some(reader.read_next().await);
        }
    }

    /// Reads the next part of the Records, for a completion that waits for them.
    async fn read_next_part(paginator: &mut HistoryPaginator) -> ReplayReads {
        let reader = paginator.stream_reader.as_mut();
        let reader = reader.expect("A paginator with unread Stream Records has their reader");
        reader.read_next().await
    }

    /// Returns true while a marker of the History of `paginator` has no Records yet.
    pub(crate) fn has_unread_in(paginator: &HistoryPaginator) -> bool {
        (paginator.stream_reader.as_ref()).is_some_and(ReplayReader::has_unread)
    }

    /// Drops the reader of `paginator` and the Records of `update`. A cached run needs neither,
    /// since it gave Workflow code each Record of its markers.
    pub(crate) fn forget(paginator: &mut HistoryPaginator, update: &mut HistoryUpdate) {
        paginator.stream_reader = None;
        update.stream_reads = None;
    }

    /// Reads the Records of one entry, and checks them against its count and its digest.
    async fn read_entry(
        &self,
        entry: &UnreadEntry,
        deadline: &Deadline,
    ) -> Result<Vec<StreamRecord>, ReplayError> {
        let (stream, count) = (&entry.stream, entry.count);
        let range = || {
            format!(
                "the {count} Records of Stream `{}` in Store `{}` after {}",
                stream.id,
                stream.store,
                offset_text(&entry.after_offset)
            )
        };
        let Some(runtime) = self.runtimes.get(&stream.store) else {
            return Err(ReplayError::NoRuntime(cut(format!(
                "This Worker has no Stream runtime for the Store `{}`, so it can't read {}",
                stream.store,
                range()
            ))));
        };
        let gone = |why: String| {
            ReplayError::Gone(cut(format!("The Store doesn't have {}: {why}", range())))
        };
        let format = DigestFormat::from_marker(&entry.digest).ok_or_else(|| {
            ReplayError::Unavailable(cut(format!(
                "This Worker doesn't know the digest format for {}",
                range()
            )))
        })?;
        let settings = self.runtimes.settings;
        let recorded = usize::try_from(entry.record_bytes).unwrap_or(usize::MAX);
        let mut records: Vec<StreamRecord> = vec![];
        let mut bytes = 0_usize;
        let mut cursor = entry.after_offset.clone();
        let mut waits = retry_waits(&settings);
        while (records.len() as u64) < count {
            let left = count - records.len() as u64;
            let request = ReadRecordsRequest {
                r#ref: Some(stream.clone()),
                from_offset: cursor.clone(),
                max_records: i32::try_from(left).unwrap_or(i32::MAX),
                // Past the recorded bytes the hint is 0, which asks for no cap, so a replay whose
                // Records grew reads the rest in pages of the Store's size.
                max_bytes: i64::try_from(recorded.saturating_sub(bytes)).unwrap_or(i64::MAX),
                ..Default::default()
            };
            let read = runtime.read_records(request);
            let read = tokio::time::timeout(settings.try_deadline, read);
            // Once the deadline passed, no try starts, and one in flight stops.
            let answer = match future::select(pin!(deadline.passed()), pin!(read)).await {
                Either::Left((first_error, _)) => {
                    let bound = settings.replay_retry_bound();
                    return Err(ReplayError::Unavailable(cut(format!(
                        "The Store is unavailable, so the replay can't read {} within {bound:?} \
                         of its first failed try: {first_error}",
                        range()
                    ))));
                }
                Either::Right((answer, _)) => answer,
            };
            let error = match answer {
                Ok(Ok(page)) => {
                    waits = retry_waits(&settings);
                    // An answer with no Records that moves the cursor passed entries that aren't
                    // Records, so it doesn't mean that Records are missing.
                    if page.records.is_empty() && page.last_offset == cursor {
                        return Err(gone(format!("it has only {} of them", records.len())));
                    }
                    let page_bytes = page
                        .records
                        .iter()
                        .map(|record| field_len(record.encoded_len()));
                    bytes = bytes.saturating_add(page_bytes.sum::<usize>());
                    // Old Workers could deliver one oversized Record. Its original marker is
                    // the only reason to allow more than the Workflow budget on replay.
                    let limit = match format {
                        DigestFormat::Legacy => recorded.max(super::MAX_TASK_RECORD_BYTES),
                        DigestFormat::V1 => super::MAX_TASK_RECORD_BYTES,
                    };
                    if bytes > limit {
                        return Err(gone(
                            "its Records exceed the Workflow read byte limit".to_string(),
                        ));
                    }
                    records.extend(page.records);
                    cursor = page.last_offset;
                    continue;
                }
                // These say what this Worker can't do, not what the Store lost, so another Worker
                // may still read the Records.
                Ok(Err(error))
                    if matches!(
                        error.reason,
                        ReadReason::Other(
                            AnyReason::FormatTooNew
                                | AnyReason::InvalidArgument
                                | AnyReason::Unsupported
                        )
                    ) =>
                {
                    return Err(ReplayError::Unavailable(cut(format!(
                        "This Worker can't read {}: {error}",
                        range()
                    ))));
                }
                Ok(Err(error)) if !is_outage(&error) => {
                    return Err(gone(error.to_string()));
                }
                Ok(Err(error)) => error.to_string(),
                Err(_) => format!("The Store gave no answer in {:?}", settings.try_deadline),
            };
            let left = deadline.start(settings.replay_retry_bound(), error);
            let wait = next_wait(&mut waits);
            tokio::time::sleep(wait.min(left)).await;
        }
        if format.hash(&records) != entry.digest {
            return Err(gone("it has other Records".to_string()));
        }
        Ok(records)
    }
}

/// The deadline of a replay's tries on one Store, as the time of the first try that failed, the
/// bound after it, and the error of that try. A bound past the end of the clock never passes.
#[derive(Default)]
struct Deadline(watch::Sender<Option<(Instant, Duration, String)>>);

impl Deadline {
    /// Starts the deadline `bound` from now, unless a try failed before, and returns the time left.
    fn start(&self, bound: Duration, error: String) -> Duration {
        self.0.send_if_modified(|start| {
            let unset = start.is_none();
            start.get_or_insert_with(|| (Instant::now(), bound, error));
            unset
        });
        let start = self.0.borrow();
        let (from, bound, _) = start.as_ref().expect("The deadline is set");
        bound.saturating_sub(from.elapsed())
    }

    /// Returns the error of the first failed try once the deadline passed.
    async fn passed(&self) -> String {
        let mut deadline = self.0.subscribe();
        let start = (deadline.wait_for(Option::is_some).await)
            .ok()
            .and_then(|start| start.clone());
        let (from, bound, error) = start.expect("The deadline holds its sender");
        // A sleep of zero waits for the next tick of the timer, so a deadline that passed must
        // return immediately, or a try that fails immediately would win each time.
        let left = bound.saturating_sub(from.elapsed());
        if !left.is_zero() {
            tokio::time::sleep(left).await;
        }
        error
    }
}

/// Returns an offset as hex, for a message.
pub(crate) fn offset_text(offset: &[u8]) -> String {
    if offset.is_empty() {
        return "the start".to_string();
    }
    let hex: String = offset.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("offset {hex}")
}

#[cfg(test)]
mod tests {
    use super::{
        super::{
            StreamSettings, marker_details,
            test_store::{Entry, Fault, Model, ModelStore, client},
        },
        *,
    };
    use parking_lot::Mutex;
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::sync::{Arc, atomic::Ordering};
    use temporalio_common::protos::{
        constants::STREAM_READ_MARKER_NAME,
        coresdk::external_data::StreamReadMarkerData,
        temporal::api::{
            common::v1::Payload,
            history::v1::{MarkerRecordedEventAttributes, history_event},
        },
    };
    use temporalio_streams::{
        ReadError, ReadReason, StreamError, StreamRuntime, proto::ReadRecordsRequest as StoreRead,
        record::StoreLimits,
    };

    /// How the Store of a replay differs from the Store of the first run.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Store {
        Same,
        /// The Stream lost its last entries.
        Fewer,
        /// A Record has another body of the same size.
        Other,
        /// Each Record has a larger body.
        Larger,
        /// The Stream is gone.
        Gone,
        /// Reads fail with `StoreUnavailable`, or get no answer, at random.
        Outage,
        /// The Store refuses reads for what this Worker can't do, such as a newer format or a call
        /// that the Store doesn't support.
        Refuses,
        /// The Worker has no runtime for the Store.
        NoRuntime,
    }

    /// Returns the read marker `event_id` with `data`.
    fn marker_event(event_id: i64, data: &StreamReadMarkerData) -> HistoryEvent {
        let marker = MarkerRecordedEventAttributes {
            marker_name: STREAM_READ_MARKER_NAME.to_string(),
            details: marker_details(data).unwrap(),
            ..Default::default()
        };
        HistoryEvent {
            event_id,
            attributes: Some(history_event::Attributes::MarkerRecordedEventAttributes(
                marker,
            )),
            ..Default::default()
        }
    }

    fn with_stream(mut entry: StreamReadResult, stream: &StreamRef) -> StreamReadResult {
        entry.stream_id.clone_from(&stream.id);
        entry.store.clone_from(&stream.store);
        entry
    }

    fn record(offset: &[u8], data: u8, size: usize) -> StreamRecord {
        StreamRecord {
            offset: offset.to_vec(),
            body: Some(Payload {
                data: vec![data; size],
                ..Default::default()
            }),
            producer: Some(client()),
        }
    }

    /// Returns the entry of a read that gave the page `given` after `from_offset`. It has the
    /// digest of the Records.
    fn page_entry(seq: u32, from_offset: &[u8], given: &[StreamRecord]) -> StreamReadResult {
        let mut page = StreamReadPage {
            record_count: given.len() as u32,
            ..Default::default()
        };
        if !given.is_empty() {
            page.digest = super::super::digest::digest(given);
            let size = |record: &StreamRecord| field_len(record.encoded_len());
            page.record_bytes = given.iter().map(size).sum::<usize>() as u64;
        }
        StreamReadResult {
            seq,
            from_offset: from_offset.to_vec(),
            result: Some(stream_read_result::Result::Page(page)),
            ..Default::default()
        }
    }

    fn page(entry: &StreamReadResult) -> &StreamReadPage {
        match &entry.result {
            Some(stream_read_result::Result::Page(page)) => page,
            other => panic!("{other:?}"),
        }
    }

    /// Returns a text of at most `bytes` bytes, and of fewer than 4 less, with characters of one
    /// to four bytes.
    fn text(rng: &mut StdRng, bytes: usize) -> String {
        let mut text = String::new();
        loop {
            let char = ['x', 'é', '€', '𝄞'][rng.random_range(0..4)];
            if text.len() + char.len_utf8() > bytes {
                return text;
            }
            text.push(char);
        }
    }

    /// Returns a Stream id or a Store name, which is sometimes as long as an id may be.
    fn name(rng: &mut StdRng, short: &str) -> String {
        if rng.random_bool(0.8) {
            return short.to_string();
        }
        let bytes = rng.random_range(4000..=4096);
        text(rng, bytes)
    }

    /// Checks that `message` fits the bound of a failure message, and returns true if it was cut.
    fn fits(message: &str, seed: u64) -> bool {
        assert!(
            message.len() <= 4096,
            "seed {seed}: {} bytes",
            message.len()
        );
        message.ends_with("...")
    }

    /// When each try of a Store came, and when it failed if it got `StoreUnavailable` or no answer.
    type Tries = Arc<Mutex<Vec<(Instant, Option<Instant>)>>>;

    /// A Store of a replay, with the Stream that the first run read from it.
    struct Replayed {
        kind: Store,
        stream: StreamRef,
        /// The Records that the first run read.
        records: Vec<StreamRecord>,
        model: Arc<ModelStore>,
        tries: Tries,
    }

    /// Makes a Stream, and the Store `store_name` of a random kind that has it now.
    fn replayed(
        rng: &mut StdRng,
        seed: u64,
        store_name: &str,
        settings: StreamSettings,
    ) -> Replayed {
        let kind = [
            Store::Same,
            Store::Fewer,
            Store::Other,
            Store::Larger,
            Store::Gone,
            Store::Outage,
            Store::Refuses,
            Store::NoRuntime,
        ][rng.random_range(0..8)];
        // A long Stream gives entries with more Records than one read can ask for.
        let len = if rng.random_bool(0.05) {
            rng.random_range(1000..1500u16)
        } else {
            rng.random_range(1..60u16)
        };
        let mut entries: Vec<Entry> = (0..len)
            .map(|index| {
                let offset = (index * 2 + 1).to_be_bytes().to_vec();
                let size = match rng.random_bool(0.9) {
                    true => rng.random_range(0..100),
                    false => rng.random_range(100..4000),
                };
                let record = rng.random_bool(0.7).then(|| record(&offset, 0, size));
                let at = Duration::ZERO;
                Entry { offset, at, record }
            })
            .collect();
        let records = entries.iter().filter_map(|e| e.record.clone()).collect();
        let stream = StreamRef {
            id: name(rng, "s"),
            store: name(rng, store_name),
        };
        let refusal = [
            AnyReason::FormatTooNew,
            AnyReason::InvalidArgument,
            AnyReason::Unsupported,
        ][rng.random_range(0..3)];
        let bytes = rng.random_range(0..6000);
        let message = text(rng, bytes);
        match kind {
            Store::Fewer => entries.truncate(rng.random_range(0..entries.len())),
            Store::Other => {
                let at = rng.random_range(0..entries.len());
                let size = (entries[at].record.as_ref())
                    .map_or(0, |record| record.body.as_ref().unwrap().data.len());
                entries[at].record = Some(record(&entries[at].offset, 1, size));
            }
            Store::Larger => {
                let more = rng.random_range(1..2000);
                for record in entries.iter_mut().filter_map(|entry| entry.record.as_mut()) {
                    record.body.as_mut().unwrap().data.extend(vec![0; more]);
                }
            }
            _ => {}
        }
        let tries = Tries::default();
        let kept = tries.clone();
        let fault = move |rng: &mut StdRng, _: &StoreRead| {
            let now = Instant::now();
            let fails = |error| Some(Fault::Fails(error));
            let (fault, failed) = match kind {
                Store::Gone => {
                    let gone = ReadError::operation(ReadReason::NotFound, &message);
                    (fails(gone), None)
                }
                Store::Outage if rng.random_bool(0.3) => {
                    (fails(StreamError::store_unavailable(&message)), Some(now))
                }
                Store::Outage if rng.random_bool(0.2) => {
                    (Some(Fault::Silent), Some(now + settings.try_deadline))
                }
                Store::Refuses => (fails(StreamError::new(refusal, &message)), None),
                _ => (None, None),
            };
            kept.lock().push((now, failed));
            fault
        };
        let model = Model::new(seed, entries, fault);
        Replayed {
            kind,
            stream,
            records,
            model: Arc::new(ModelStore::new(model)),
            tries,
        }
    }

    // Only a Store that gives no answer, or one that this Worker can't read or has no runtime for,
    // leaves the Records unknown. Fewer Records or other ones are gone. An entry gets the result
    // of its own Store, whatever the other Store does.
    #[tokio::test(start_paused = true)]
    async fn a_replay_read_gives_the_records_or_its_cause() {
        let (mut together, mut stopped, mut cut_off, mut timed_out) = (0, 0, 0, 0);
        let (mut markers_together, mut many_together, mut beside_an_outage) = (0, 0, 0);
        for seed in 0..300 {
            let mut rng = StdRng::seed_from_u64(seed);
            let settings = StreamSettings {
                retry_initial_interval: Duration::from_millis(rng.random_range(1..20)),
                retry_max_interval: Duration::from_millis(rng.random_range(20..50)),
                try_deadline: Duration::from_millis(rng.random_range(1..50)),
                replay_retry_bound: Duration::from_millis(rng.random_range(0..100)),
                replay_concurrency: rng.random_range(1..=4),
                ..Default::default()
            };
            let stores = ["mem", "disk"].map(|name| replayed(&mut rng, seed, name, settings));
            // The markers of the first run, each with read entries over the two Streams. An entry
            // of few Records lets a part take several markers.
            let (mut events, mut datas) = (vec![], vec![]);
            let mut expected = vec![];
            let markers = rng.random_range(1..=4);
            for event_id in 1..=markers {
                let mut marker = StreamReadMarkerData::default();
                for seq in 1..=rng.random_range(1..=4) {
                    let of = rng.random_range(0..2);
                    let records = &stores[of].records;
                    let from = rng.random_range(0..=records.len());
                    let count = match rng.random_bool(0.5) {
                        true => rng.random_range(0..=(records.len() - from).min(3)),
                        false => rng.random_range(0..=records.len() - from),
                    };
                    let after_offset = match from {
                        0 => vec![],
                        at => records[at - 1].offset.clone(),
                    };
                    let given = records[from..from + count].to_vec();
                    let entry = page_entry(seq, &after_offset, &given);
                    marker.reads.push(with_stream(entry, &stores[of].stream));
                    let read = (count > 0).then_some(given);
                    expected.push((event_id, seq as usize - 1, of, after_offset, read));
                }
                events.push(marker_event(event_id, &marker));
                datas.push(marker);
            }
            let served = (stores.iter())
                .filter(|store| store.kind != Store::NoRuntime)
                .map(|store| {
                    let runtime = StreamRuntime::new(store.model.clone(), StoreLimits::default());
                    (store.stream.store.clone(), Arc::new(runtime))
                });
            let mut runtimes = StreamRuntimes::new(served);
            runtimes.settings = settings;
            // The markers with an entry that needs Records, each with its recorded bytes.
            let needing: Vec<(i64, usize)> = (1..=markers)
                .zip(&datas)
                .filter(|(_, data)| data.reads.iter().any(needs_records))
                .map(|(event_id, data)| {
                    let bytes = data.reads.iter().map(|read| page(read).record_bytes);
                    (event_id, bytes.sum::<u64>() as usize)
                })
                .collect();
            let recorded: usize = needing.iter().map(|(_, bytes)| bytes).sum();
            runtimes.settings.max_task_record_bytes = rng.random_range(1..=2 * recorded + 1);
            let max_bytes = runtimes.settings.max_task_record_bytes;
            let mut reader = ReplayReader::new(runtimes);
            reader.queue(events.iter());
            // The reader gives each marker in one part, in the order of History.
            let mut results = HashMap::<i64, MarkerReads>::new();
            let mut next = 0;
            while reader.has_unread() {
                let seen = stores.each_ref().map(|store| store.tries.lock().len());
                let mut part = reader.read_next().await;
                // On each Store, the deadline stops each try, and no try starts after it.
                let end = Instant::now();
                let bound = settings.replay_retry_bound;
                let mut deadlines = vec![];
                for (store, seen) in stores.iter().zip(seen) {
                    let tries = store.tries.lock()[seen..].to_vec();
                    let Some(first) = tries.iter().filter_map(|(_, failed)| *failed).min() else {
                        continue;
                    };
                    let deadline = first + bound;
                    let early =
                        |at: &Instant| *at < deadline || (bound.is_zero() && *at == deadline);
                    assert!(tries.iter().all(|(at, _)| early(at)), "seed {seed}");
                    deadlines.push(deadline);
                }
                if let Some(last) = deadlines.iter().max() {
                    assert!(end <= *last, "seed {seed}: {:?} late", end - *last);
                }
                // A part takes at least one marker, and those that it takes fit the bound unless
                // it takes only one. The next one would pass the bound.
                let taken: Vec<&(i64, usize)> = (needing[next..].iter())
                    .take_while(|(event_id, _)| {
                        let marker = part.take(*event_id);
                        let read = !marker.is_empty();
                        results.insert(*event_id, marker);
                        read
                    })
                    .collect();
                let taken_bytes: usize = taken.iter().map(|(_, bytes)| bytes).sum();
                assert!(!taken.is_empty(), "seed {seed}");
                assert!(taken.len() == 1 || taken_bytes <= max_bytes, "seed {seed}");
                next += taken.len();
                let unread = needing.get(next);
                assert_eq!(
                    part.unread(),
                    unread.map(|(event_id, _)| *event_id),
                    "seed {seed}"
                );
                let passes = |(_, bytes): &(i64, usize)| taken_bytes + bytes > max_bytes;
                assert!(unread.is_none_or(passes), "seed {seed}");
                for event_id in 1..=markers {
                    assert!(part.take(event_id).is_empty(), "seed {seed}: {event_id}");
                }
                stopped += usize::from(unread.is_some());
                markers_together += usize::from(taken.len() > 1);
                // Markers of more than 10 small Records each fit the bound together.
                let many = |(event_id, _): &&(i64, usize)| {
                    let entries = datas[*event_id as usize - 1].reads.iter();
                    entries.map(|entry| page(entry).record_count).sum::<u32>() > 10
                };
                many_together += usize::from(taken.len() > 1 && taken.iter().all(many));
            }
            assert_eq!(next, needing.len(), "seed {seed}");
            // A marker holds no more than its recorded bytes.
            for (event_id, max) in &needing {
                let records = (results[event_id].iter().flatten()).filter_map(|r| r.as_ref().ok());
                let held: usize = records.flatten().map(|r| field_len(r.encoded_len())).sum();
                assert!(held <= *max, "seed {seed}: {held} of {max}");
            }
            for store in &stores {
                let read: Vec<&StreamReadPage> = (datas.iter())
                    .flat_map(|data| &data.reads)
                    .filter(|entry| {
                        entry.stream_id == store.stream.id && entry.store == store.stream.store
                    })
                    .filter_map(unread_page)
                    .collect();
                let largest = (store.model.records_after(&[]).iter())
                    .map(|record| field_len(record.encoded_len()))
                    .max()
                    .unwrap_or(0);
                // A try gets at most the Records that its entry has left and one more, which the
                // runtime asks for.
                let answered = store.model.answered.load(Ordering::Relaxed);
                let most = read.iter().map(|page| page.record_count).max().unwrap_or(0) as usize;
                let tries = store.tries.lock().len();
                let bound = tries * (most + 1) * largest;
                assert!(answered <= bound, "seed {seed}: {answered} of {bound}");
                let (_, peak) = store.model.model.lock().reading;
                assert!(peak <= settings.replay_concurrency, "seed {seed}: {peak}");
                together += usize::from(peak > 1);
            }
            for (event_id, index, of, after_offset, given) in expected {
                let store = &stores[of];
                let kind = store.kind;
                let result = (results.get_mut(&event_id))
                    .and_then(|marker| marker.get_mut(index))
                    .and_then(Option::take);
                // The Records that the Store of the replay has where the entry read.
                let unchanged = given.as_ref().is_some_and(|given| {
                    let now = store.model.records_after(&after_offset);
                    now.len() >= given.len() && now[..given.len()] == given[..]
                }) && kind != Store::Gone;
                match (given, result) {
                    // An entry with no Records reads no Store.
                    (None, None) => {}
                    (Some(given), Some(Ok(records))) => {
                        assert!(
                            unchanged && kind != Store::NoRuntime,
                            "seed {seed}: {kind:?}"
                        );
                        assert_eq!(records, given, "seed {seed}");
                        let other = stores[1 - of].kind;
                        beside_an_outage += usize::from(other == Store::Outage);
                    }
                    (Some(_), Some(Err(ReplayError::Gone(message)))) => {
                        assert!(!unchanged, "seed {seed}: {kind:?}");
                        cut_off += usize::from(fits(&message, seed));
                    }
                    (Some(_), Some(Err(ReplayError::Unavailable(message)))) => {
                        let unknown = matches!(kind, Store::Outage | Store::Refuses);
                        assert!(unknown, "seed {seed}: {kind:?}");
                        cut_off += usize::from(fits(&message, seed));
                        timed_out += usize::from(kind == Store::Outage);
                    }
                    (Some(_), Some(Err(ReplayError::NoRuntime(message)))) => {
                        assert_eq!(kind, Store::NoRuntime, "seed {seed}");
                        cut_off += usize::from(fits(&message, seed));
                    }
                    other => panic!("seed {seed}: {kind:?}: {other:?}"),
                }
            }
        }
        let reached = [
            together,
            stopped,
            cut_off,
            timed_out,
            markers_together,
            many_together,
            beside_an_outage,
        ];
        assert!(reached.iter().all(|count| *count > 0), "{reached:?}");
    }

    /// Replays a marker with one read entry, of one Record with `size` bytes of data that the
    /// entry recorded `less` bytes short of, from a Store whose reads get `fault`. Returns the
    /// Record and the entry's result.
    async fn replay_one_record(
        less: u64,
        size: usize,
        settings: StreamSettings,
        fault: impl FnMut(&mut StdRng, &StoreRead) -> Option<Fault> + Send + 'static,
    ) -> (StreamRecord, MarkerReads) {
        let given = record(&[1], 0, size);
        let result = replay_record(
            &given,
            settings,
            StoreLimits::default(),
            |page| {
                page.record_bytes -= less;
            },
            fault,
        )
        .await;
        (given, result)
    }

    async fn replay_record(
        given: &StreamRecord,
        settings: StreamSettings,
        limits: StoreLimits,
        edit: impl FnOnce(&mut StreamReadPage),
        fault: impl FnMut(&mut StdRng, &StoreRead) -> Option<Fault> + Send + 'static,
    ) -> MarkerReads {
        let stream = StreamRef {
            id: "s".to_string(),
            store: "mem".to_string(),
        };
        let mut entry = page_entry(1, &[], std::slice::from_ref(given));
        if let Some(stream_read_result::Result::Page(page)) = &mut entry.result {
            edit(page);
        }
        let data = StreamReadMarkerData {
            reads: vec![with_stream(entry, &stream)],
        };
        let event = marker_event(1, &data);
        let entry = Entry {
            offset: vec![1],
            at: Duration::ZERO,
            record: Some(given.clone()),
        };
        let model = ModelStore::new(Model::new(0, vec![entry], fault));
        let runtime = StreamRuntime::new(Arc::new(model), limits);
        let mut runtimes = StreamRuntimes::new([(stream.store, Arc::new(runtime))]);
        runtimes.settings = settings;
        let mut reader = ReplayReader::new(runtimes);
        reader.queue([event].iter());
        reader.read_next().await.take(1)
    }

    #[tokio::test]
    async fn an_oversized_legacy_record_replays_and_unknown_formats_fail_the_task() {
        let given = record(&[1], 0, super::super::MAX_TASK_RECORD_BYTES + 1);
        for format in [Some(DigestFormat::Legacy), Some(DigestFormat::V1), None] {
            let results = replay_record(
                &given,
                StreamSettings::default(),
                StoreLimits::UNBOUNDED,
                |page| match format {
                    Some(f) => page.digest = f.hash(std::slice::from_ref(&given)),
                    None => page.digest[0] = 2,
                },
                |_, _| None,
            )
            .await;
            match (format, results.as_slice()) {
                (Some(DigestFormat::Legacy), [Some(Ok(records))]) => {
                    assert_eq!(records, std::slice::from_ref(&given))
                }
                (Some(DigestFormat::V1), [Some(Err(ReplayError::Gone(_)))]) => {}
                (None, [Some(Err(ReplayError::Unavailable(message)))]) => {
                    assert!(message.contains("digest format"))
                }
                _ => panic!("unexpected replay result"),
            }
        }
    }

    // A Record over the Record size of the runtime leaves its entry unknown, since a runtime with
    // a larger Record size may read it.
    #[tokio::test]
    async fn only_the_count_and_the_digest_of_an_entry_tell_that_its_records_are_gone() {
        for size in [10, 2 << 20] {
            let settings = StreamSettings::default();
            let (given, result) = replay_one_record(1, size, settings, |_, _| None).await;
            match (size > 10, result.as_slice()) {
                (false, [Some(Ok(records))]) => assert_eq!(records, &[given]),
                (true, [Some(Err(ReplayError::Unavailable(_)))]) => {}
                (_, other) => panic!("{size}: {other:?}"),
            }
        }
    }

    // A replay with no bound in time on its tries, or on the wait between them, tries again after
    // an outage.
    #[tokio::test(start_paused = true)]
    async fn a_replay_with_no_bound_in_time_tries_again() {
        let settings = StreamSettings {
            retry_initial_interval: Duration::MAX,
            retry_max_interval: Duration::MAX,
            replay_retry_bound: Duration::MAX,
            ..Default::default()
        };
        let mut failed = false;
        let fault = move |_: &mut StdRng, _: &StoreRead| {
            let down = StreamError::store_unavailable("Down");
            (!std::mem::replace(&mut failed, true)).then_some(Fault::Fails(down))
        };
        let (given, result) = replay_one_record(0, 10, settings, fault).await;
        assert_eq!(result, vec![Some(Ok(vec![given]))]);
    }

    #[tokio::test]
    async fn replay_rejects_changed_optional_producer_timestamp() {
        let mut original = record(&[1], 0, 10);
        original.producer = Some(temporalio_streams::proto::Producer {
            kind: Some(temporalio_streams::proto::producer::Kind::Workflow(
                temporalio_streams::proto::WorkflowProducer {
                    namespace: "n".to_string(),
                    workflow_id: "w".to_string(),
                    run_id: "r".to_string(),
                    attempt: Some(temporalio_streams::proto::WorkflowTaskAttempt {
                        started_event_id: 3,
                        attempt: 1,
                        started_time: None,
                    }),
                },
            )),
        });
        let mut changed = original.clone();
        if let Some(temporalio_streams::proto::producer::Kind::Workflow(workflow)) =
            changed.producer.as_mut().unwrap().kind.as_mut()
        {
            workflow.attempt.as_mut().unwrap().started_time =
                Some(prost_types::Timestamp::default());
        }
        assert_ne!(original, changed);
        for record in [&original, &changed] {
            let request = temporalio_streams::proto::AppendRecordsRequest {
                stream_id: "s".to_string(),
                producer: record.producer.clone(),
                records: vec![temporalio_streams::proto::AppendRecord {
                    body: record.body.clone(),
                }],
            };
            StoreLimits::default().validate_append(&request).unwrap();
        }
        let stream = StreamRef {
            id: "s".to_string(),
            store: "mem".to_string(),
        };
        let entry = page_entry(1, &[], &[original]);
        let data = StreamReadMarkerData {
            reads: vec![with_stream(entry, &stream)],
        };
        let event = marker_event(1, &data);
        let entry = Entry {
            offset: vec![1],
            at: Duration::ZERO,
            record: Some(changed),
        };
        let model = ModelStore::new(Model::new(0, vec![entry], |_, _| None));
        let runtime = StreamRuntime::new(Arc::new(model), StoreLimits::default());
        let runtimes = StreamRuntimes::new([(stream.store, Arc::new(runtime))]);
        let mut reader = ReplayReader::new(runtimes);
        reader.queue([event].iter());
        let result = reader.read_next().await.take(1);
        assert!(
            matches!(result.as_slice(), [Some(Err(ReplayError::Gone(_)))]),
            "Replay accepted a changed Producer: {result:?}"
        );
    }
}
