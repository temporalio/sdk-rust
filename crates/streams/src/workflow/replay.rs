//! Reads the Records of History's read entries again for a replay. The entry of a read without
//! `records_in_history` has only a count and a digest, so the replay reads that many Records after
//! the entry's offset and checks them against the digest.

use super::{
    StreamRuntimes,
    digest::Digest,
    manager::{Backoff, cut},
    marker::{MAX_TASK_RECORD_BYTES, peeked_marker},
};
use crate::{
    MAX_RECORDS, Reason,
    proto::{ReadRecordsRequest, StreamRecord, StreamRef},
};
use futures_util::{
    StreamExt,
    future::{self, Either},
    stream,
};
use prost::Message;
use std::{collections::HashMap, pin::pin, time::Duration};
use temporalio_common::protos::{
    coresdk::external_data::StreamReadEntry, temporal::api::history::v1::HistoryEvent,
};
use tokio::{sync::watch, time::Instant};

/// Why a replay can't give Workflow code the Records of a read entry.
#[derive(Clone, Debug, PartialEq)]
pub enum ReplayError {
    /// The Store has fewer Records after the entry's offset than the first run got, or other ones.
    Gone(String),
    /// The Store gave no answer, or this Worker can't read from it, so another attempt may still
    /// read the Records.
    Unavailable(String),
}

/// The Records that a replay read for the read entries of one marker, by the index of each entry.
/// An entry whose marker holds its Records, or that has none, has no result.
pub type MarkerReads = Vec<Option<Result<Vec<StreamRecord>, ReplayError>>>;

/// The Records that a replay read for the markers of a History, by the event id of each marker.
#[derive(Debug, Default)]
pub struct ReplayReads {
    reads: HashMap<i64, MarkerReads>,
    unread: Option<i64>,
}

impl ReplayReads {
    /// Takes the results of the read entries of the marker event `event_id`.
    pub fn take(&mut self, event_id: i64) -> MarkerReads {
        self.reads.remove(&event_id).unwrap_or_default()
    }

    /// Returns the event id of the first marker that the read left, since the Records that it
    /// read before that marker reached the bound.
    pub fn unread(&self) -> Option<i64> {
        self.unread
    }
}

/// Returns true if the replay of `entry` needs its Records from the Store.
pub(crate) fn needs_records(entry: &StreamReadEntry) -> bool {
    entry.records.is_empty() && record_count(entry) > 0
}

fn record_count(entry: &StreamReadEntry) -> u64 {
    entry
        .page_records
        .iter()
        .map(|count| u64::from(*count))
        .sum()
}

/// Reads the Records of read entries again, for the replays of the Workflows of one namespace.
#[derive(Clone, Debug)]
pub struct ReplayReader {
    runtimes: StreamRuntimes,
    namespace: String,
}

impl ReplayReader {
    /// Creates a reader for the Workflows of `namespace`.
    pub fn new(runtimes: StreamRuntimes, namespace: String) -> Self {
        Self {
            runtimes,
            namespace,
        }
    }

    /// Reads the Records of each read entry that needs them, of the `core_stream` markers in
    /// `events` after the event `after`, in History order. It always reads the first marker with
    /// such an entry, and stops before a later one once the Records that it read are 16 MiB or
    /// more, which is all that one task can give Workflow code. Up to `replay_concurrency` entries
    /// of a marker read at the same time, and each gets its own result. The first try that gets
    /// `StorageUnavailable` or no answer starts a deadline for all of them, `replay_retry_bound`
    /// later. A marker that doesn't decode gets no result, and the replay reports it once it
    /// reaches the marker.
    pub async fn read<'a>(
        &self,
        events: impl Iterator<Item = &'a HistoryEvent> + Send,
        after: i64,
    ) -> ReplayReads {
        self.read_up_to(events, after, MAX_TASK_RECORD_BYTES).await
    }

    async fn read_up_to<'a>(
        &self,
        events: impl Iterator<Item = &'a HistoryEvent> + Send,
        after: i64,
        max_bytes: usize,
    ) -> ReplayReads {
        let mut reads = ReplayReads::default();
        let deadline = Deadline(watch::Sender::new(None));
        let mut held = 0;
        for event in events {
            if event.event_id <= after {
                continue;
            }
            let Some(Ok(data)) = peeked_marker(event) else {
                continue;
            };
            let entries: Vec<_> = (data.reads.iter().enumerate())
                .filter(|(_, entry)| needs_records(entry))
                .map(|(index, entry)| {
                    let stream = data.streams[entry.stream as usize].clone();
                    (index, stream, entry.clone())
                })
                .collect();
            if entries.is_empty() {
                continue;
            }
            if !reads.reads.is_empty() && held >= max_bytes {
                reads.unread = Some(event.event_id);
                break;
            }
            let mut results = vec![None; data.reads.len()];
            let deadline = &deadline;
            let mut read = stream::iter(entries)
                .map(|(index, stream, entry)| async move {
                    (index, self.read_entry(&stream, &entry, deadline).await)
                })
                .buffer_unordered(self.runtimes.settings.replay_concurrency);
            while let Some((index, result)) = read.next().await {
                if let Ok(records) = &result {
                    held += records.iter().map(Message::encoded_len).sum::<usize>();
                }
                results[index] = Some(result);
            }
            reads.reads.insert(event.event_id, results);
        }
        reads
    }

    /// Reads the Records of one entry, and checks them against its count and its digest.
    async fn read_entry(
        &self,
        stream: &StreamRef,
        entry: &StreamReadEntry,
        deadline: &Deadline,
    ) -> Result<Vec<StreamRecord>, ReplayError> {
        let count = record_count(entry);
        let range = || {
            format!(
                "the {count} Records of Stream `{}` in Store `{}` after {}",
                stream.id,
                stream.store,
                offset_text(&entry.after_offset)
            )
        };
        let Some(runtime) = self.runtimes.get(&stream.store) else {
            return Err(ReplayError::Unavailable(cut(format!(
                "This Worker has no Stream runtime for the Store `{}`, so it can't read {}",
                stream.store,
                range()
            ))));
        };
        let gone = |why: String| {
            ReplayError::Gone(cut(format!("The Store doesn't have {}: {why}", range())))
        };
        let settings = self.runtimes.settings;
        let mut records: Vec<StreamRecord> = vec![];
        let mut bytes = 0;
        let mut cursor = entry.after_offset.clone();
        let mut backoff = Backoff::new(&settings);
        let mut outage = None;
        while (records.len() as u64) < count {
            let left = count - records.len() as u64;
            let request = ReadRecordsRequest {
                namespace: self.namespace.clone(),
                r#ref: Some(stream.clone()),
                from_offset: cursor.clone(),
                max_records: left.min(MAX_RECORDS as u64) as i32,
                // A Store with other Records can have larger ones, and the replay holds no more
                // than a task gave.
                max_bytes: (MAX_TASK_RECORD_BYTES - bytes).max(1) as i64,
                ..Default::default()
            };
            let read = tokio::time::timeout(settings.try_deadline, runtime.read_records(request));
            // Once the deadline passed, no try starts, and one in flight stops.
            let answer = match future::select(pin!(deadline.passed()), pin!(read)).await {
                Either::Left(_) => {
                    let bound = settings.replay_retry_bound;
                    let why = outage.map_or_else(String::new, |error| format!(": {error}"));
                    return Err(ReplayError::Unavailable(cut(format!(
                        "The Store is unavailable, so the replay can't read {} within {bound:?} \
                         of its first failed try{why}",
                        range()
                    ))));
                }
                Either::Right((answer, _)) => answer,
            };
            let error = match answer {
                Ok(Ok(page)) => {
                    backoff = Backoff::new(&settings);
                    // An answer with no Records that moves the cursor passed entries that aren't
                    // Records, so it doesn't mean that Records are missing.
                    if page.records.is_empty() && (page.done || page.last_offset == cursor) {
                        return Err(gone(format!("it has only {} of them", records.len())));
                    }
                    bytes += page.records.iter().map(Message::encoded_len).sum::<usize>();
                    if bytes > MAX_TASK_RECORD_BYTES {
                        return Err(gone("it has other Records, which are larger".to_string()));
                    }
                    records.extend(page.records);
                    if page.done && (records.len() as u64) < count {
                        return Err(gone(format!("it has only {} of them", records.len())));
                    }
                    cursor = page.last_offset;
                    continue;
                }
                // These say what this Worker can't do, not what the Store lost, so another Worker
                // may still read the Records.
                Ok(Err(error))
                    if matches!(
                        error.reason,
                        Reason::FormatTooNew | Reason::Invalid | Reason::Unsupported
                    ) =>
                {
                    return Err(ReplayError::Unavailable(cut(format!(
                        "This Worker can't read {}: {error}",
                        range()
                    ))));
                }
                Ok(Err(error)) if error.reason != Reason::StorageUnavailable => {
                    return Err(gone(error.to_string()));
                }
                Ok(Err(error)) => error.to_string(),
                Err(_) => format!("The Store gave no answer in {:?}", settings.try_deadline),
            };
            let end = deadline.start(settings.replay_retry_bound);
            let wait = backoff.next_wait(rand::random());
            outage = Some(error);
            tokio::time::sleep_until((Instant::now() + wait).min(end)).await;
        }
        let mut digest = Digest::new();
        digest.add(&records);
        if digest.finish() != entry.digest {
            return Err(gone("it has other Records".to_string()));
        }
        Ok(records)
    }
}

/// The deadline of the tries of one replay read, which the first try that fails starts.
struct Deadline(watch::Sender<Option<Instant>>);

impl Deadline {
    /// Starts the deadline `bound` from now, unless a try failed before, and returns it.
    fn start(&self, bound: Duration) -> Instant {
        self.0.send_if_modified(|end| {
            let unset = end.is_none();
            end.get_or_insert_with(|| Instant::now() + bound);
            unset
        });
        self.0.borrow().expect("The deadline is set")
    }

    /// Returns once the deadline passed.
    async fn passed(&self) {
        let mut deadline = self.0.subscribe();
        let end = (deadline.wait_for(Option::is_some).await)
            .ok()
            .and_then(|end| *end);
        if let Some(end) = end {
            tokio::time::sleep_until(end).await;
        }
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
    use super::*;
    use crate::{
        StreamError, StreamRuntime,
        record::BatchLimits,
        test_store::{Entry, Fault, Model, ModelStore, client},
        workflow::{StreamSettings, marker::marker_details},
    };
    use parking_lot::Mutex;
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::sync::Arc;
    use temporalio_common::protos::{
        constants::STREAM_MARKER_NAME,
        coresdk::external_data::{StreamMarkerData, StreamReadEnded},
        temporal::api::{
            common::v1::Payload,
            history::v1::{MarkerRecordedEventAttributes, history_event},
        },
    };

    /// How the Store of a replay differs from the Store of the first run.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Store {
        Same,
        /// The Stream lost its last entries.
        Fewer,
        /// A Record has another body.
        Other,
        /// The Stream is gone.
        Gone,
        /// Reads fail with `StorageUnavailable`, or get no answer, at random.
        Outage,
        /// The Store refuses reads for what this Worker can't do: a newer format, a request that
        /// it finds malformed, or a call that it doesn't support.
        Refuses,
        /// The Worker has no runtime for the Store.
        NoRuntime,
    }

    fn record(offset: &[u8], data: u8) -> StreamRecord {
        StreamRecord {
            offset: offset.to_vec(),
            body: Some(Payload {
                data: vec![data],
                ..Default::default()
            }),
            producer: Some(client()),
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

    // A replay gives each read entry the Records that the first run read after its offset, for
    // any pages of the Store. Fewer Records or other ones are gone. Only a Store that gives no
    // answer, or one that this Worker can't read or has no runtime for, leaves the Records unknown.
    // The replay reads the markers after the event that it starts after, in History order, and
    // stops before a marker once the Records that it read reach the bound. No try runs past the
    // retry bound after the first try that fails, and each message fits the bound of a failure.
    #[tokio::test(start_paused = true)]
    async fn a_replay_read_gives_the_records_or_its_cause() {
        let (mut together, mut stopped, mut cut_off, mut timed_out) = (0, 0, 0, 0);
        for seed in 0..300 {
            let mut rng = StdRng::seed_from_u64(seed);
            // A long Stream gives entries with more Records than one read can ask for.
            let len = if rng.random_bool(0.05) {
                rng.random_range(1000..1500u16)
            } else {
                rng.random_range(1..60u16)
            };
            let entries: Vec<Entry> = (0..len)
                .map(|index| {
                    let offset = (index * 2 + 1).to_be_bytes().to_vec();
                    let record = rng.random_bool(0.7).then(|| record(&offset, 0));
                    let at = Duration::ZERO;
                    Entry { offset, at, record }
                })
                .collect();
            let records: Vec<StreamRecord> =
                entries.iter().filter_map(|e| e.record.clone()).collect();
            let stream = StreamRef {
                id: name(&mut rng, "s"),
                store: name(&mut rng, "mem"),
            };
            // The markers of the first run, each with read entries over the Stream.
            let mut events = vec![];
            let mut expected = vec![];
            let markers = rng.random_range(1..=4);
            for event_id in 1..=markers {
                let mut data = StreamMarkerData {
                    streams: vec![stream.clone()],
                    ..Default::default()
                };
                for seq in 1..=rng.random_range(1..=4) {
                    let from = rng.random_range(0..=records.len());
                    let count = rng.random_range(0..=records.len() - from);
                    let after_offset = match from {
                        0 => vec![],
                        at => records[at - 1].offset.clone(),
                    };
                    let given = records[from..from + count].to_vec();
                    let in_history = rng.random_bool(0.2);
                    let mut digest = Digest::new();
                    digest.add(&given);
                    let pages = vec![1; count];
                    // An entry with no page has no digest, and an end.
                    data.reads.push(StreamReadEntry {
                        seq,
                        after_offset: after_offset.clone(),
                        page_activations: (0..count as u64).collect(),
                        page_records: pages,
                        digest: match in_history || count == 0 {
                            true => vec![],
                            false => digest.finish(),
                        },
                        records: if in_history { given.clone() } else { vec![] },
                        end: (count == 0).then(StreamReadEnded::default),
                        ..Default::default()
                    });
                    let read = (!in_history && count > 0).then_some(given);
                    expected.push((event_id, seq as usize - 1, after_offset, read));
                }
                events.push(HistoryEvent {
                    event_id,
                    attributes: Some(history_event::Attributes::MarkerRecordedEventAttributes(
                        MarkerRecordedEventAttributes {
                            marker_name: STREAM_MARKER_NAME.to_string(),
                            details: marker_details(&data),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                });
            }
            let store = [
                Store::Same,
                Store::Fewer,
                Store::Other,
                Store::Gone,
                Store::Outage,
                Store::Refuses,
                Store::NoRuntime,
            ][rng.random_range(0..7)];
            let refusal = [Reason::FormatTooNew, Reason::Invalid, Reason::Unsupported]
                [rng.random_range(0..3)];
            let bytes = rng.random_range(0..6000);
            let message = text(&mut rng, bytes);
            let mut entries = entries;
            match store {
                Store::Fewer => entries.truncate(rng.random_range(0..entries.len())),
                Store::Other => {
                    let at = rng.random_range(0..entries.len());
                    entries[at].record = Some(record(&entries[at].offset, 1));
                }
                _ => {}
            }
            let settings = StreamSettings {
                retry_first_wait: Duration::from_millis(rng.random_range(1..20)),
                retry_max_wait: Duration::from_millis(rng.random_range(20..50)),
                try_deadline: Duration::from_millis(rng.random_range(1..50)),
                replay_retry_bound: Duration::from_millis(rng.random_range(0..100)),
                replay_concurrency: rng.random_range(1..=4),
                ..Default::default()
            };
            // When each try came, and when it failed if it got `StorageUnavailable` or no answer.
            let tries = Arc::new(Mutex::new(vec![]));
            let kept = tries.clone();
            let fault = move |rng: &mut StdRng, request: &ReadRecordsRequest| {
                assert_eq!(request.namespace, "ns", "seed {seed}");
                let now = Instant::now();
                let fails = |error| Some(Fault::Fails(error));
                let (fault, failed) = match store {
                    Store::Gone => (fails(StreamError::new(Reason::NotFound, &message)), None),
                    Store::Outage if rng.random_bool(0.3) => {
                        (fails(StreamError::storage_unavailable(&message)), Some(now))
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
            let model = Model {
                rng: StdRng::seed_from_u64(seed),
                entries,
                closed: rng.random_bool(0.5),
                fault: Box::new(fault),
                reading: (0, 0),
                moved_to: None,
                violations: vec![],
            };
            let model = Arc::new(ModelStore::new(model));
            let runtime = StreamRuntime::new(model.clone(), BatchLimits::default());
            let served =
                (store != Store::NoRuntime).then(|| (stream.store.clone(), Arc::new(runtime)));
            let mut runtimes = StreamRuntimes::new(served);
            runtimes.settings = settings;
            let after = rng.random_range(0..=markers);
            let max_bytes = rng.random_range(0..2000);
            let reader = ReplayReader::new(runtimes, "ns".to_string());
            let mut reads = reader.read_up_to(events.iter(), after, max_bytes).await;
            // The deadline stops each try, and no try starts after it.
            let end = Instant::now();
            let bound = settings.replay_retry_bound;
            let tries = tries.lock().clone();
            if let Some(first) = tries.iter().filter_map(|(_, failed)| *failed).min() {
                let deadline = first + bound;
                assert!(end <= deadline, "seed {seed}: {:?} late", end - deadline);
                let early = |at: &Instant| *at < deadline || (bound.is_zero() && *at == deadline);
                assert!(tries.iter().all(|(at, _)| early(at)), "seed {seed}");
            }
            // The read skips the markers up to `after`, takes the others in order, and stops
            // before one once the Records of the markers before it reach the bound.
            let mut results = HashMap::<i64, MarkerReads>::new();
            let (mut held, mut unread) = (0, None);
            for event_id in after + 1..=markers {
                if !(expected.iter()).any(|(id, _, _, read)| *id == event_id && read.is_some()) {
                    continue;
                }
                if !results.is_empty() && held >= max_bytes {
                    unread = Some(event_id);
                    break;
                }
                let marker = reads.take(event_id);
                let records = (marker.iter().flatten()).filter_map(|result| result.as_ref().ok());
                held += records.flatten().map(Message::encoded_len).sum::<usize>();
                results.insert(event_id, marker);
            }
            assert_eq!(reads.unread(), unread, "seed {seed}");
            for event_id in (1..=markers).filter(|id| !results.contains_key(id)) {
                assert!(reads.take(event_id).is_empty(), "seed {seed}: {event_id}");
            }
            stopped += usize::from(unread.is_some());
            for (event_id, index, after_offset, given) in expected {
                let result = (results.get_mut(&event_id))
                    .and_then(|marker| marker.get_mut(index))
                    .and_then(Option::take);
                // The Records that the Store of the replay has where the entry read.
                let unchanged = given.as_ref().is_some_and(|given| {
                    let now = model.records_after(&after_offset);
                    now.len() >= given.len() && now[..given.len()] == given[..]
                }) && store != Store::Gone;
                match (given, result) {
                    // An entry with its Records in History, or with none, reads no Store.
                    (None, None) => {}
                    (Some(_), None) => {
                        let read = event_id > after && unread.is_none_or(|at| event_id < at);
                        assert!(!read, "seed {seed}: {event_id}");
                    }
                    (Some(given), Some(Ok(records))) => {
                        assert!(
                            unchanged && store != Store::NoRuntime,
                            "seed {seed}: {store:?}"
                        );
                        assert_eq!(records, given, "seed {seed}");
                    }
                    (Some(_), Some(Err(ReplayError::Gone(message)))) => {
                        assert!(!unchanged, "seed {seed}: {store:?}");
                        cut_off += usize::from(fits(&message, seed));
                    }
                    (Some(_), Some(Err(ReplayError::Unavailable(message)))) => {
                        let unknown = matches!(store, Store::Outage | Store::Refuses);
                        assert!(unknown || store == Store::NoRuntime, "{seed}");
                        cut_off += usize::from(fits(&message, seed));
                        timed_out += usize::from(store == Store::Outage);
                    }
                    other => panic!("seed {seed}: {store:?}: {other:?}"),
                }
            }
            let (_, peak) = model.model.lock().reading;
            assert!(peak <= settings.replay_concurrency, "seed {seed}: {peak}");
            together += usize::from(peak > 1);
        }
        let reached = [together, stopped, cut_off, timed_out];
        assert!(reached.iter().all(|count| *count > 0), "{reached:?}");
    }
}
