//! Reads the Records of History's read entries again for a replay. The entry of a read without
//! `records_in_history` has only a count and a digest, so the replay reads that many Records after
//! the entry's offset and checks them against the digest.

use super::{
    StreamRuntimes,
    digest::Digest,
    manager::Backoff,
    marker::{MAX_TASK_RECORD_BYTES, peeked_marker},
};
use crate::{
    MAX_RECORDS, Reason,
    proto::{ReadRecordsRequest, StreamRecord, StreamRef},
};
use futures_util::{StreamExt, stream};
use prost::Message;
use std::collections::HashMap;
use temporalio_common::protos::{
    coresdk::external_data::StreamReadEntry, temporal::api::history::v1::HistoryEvent,
};
use tokio::time::Instant;

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
pub struct ReplayReads(HashMap<i64, MarkerReads>);

impl ReplayReads {
    /// Takes the results of the read entries of the marker event `event_id`.
    pub fn take(&mut self, event_id: i64) -> MarkerReads {
        self.0.remove(&event_id).unwrap_or_default()
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

/// Reads the Records of each read entry of the `core_stream` markers in `events` that needs them,
/// for a Workflow in `namespace`. Up to `replay_concurrency` entries read at the same time, and
/// each gets its own result, so that no entry's result depends on another's. A marker that doesn't
/// decode gets no result, and the replay reports it once it reaches the marker.
pub async fn read_marker_records(
    events: &[HistoryEvent],
    runtimes: &StreamRuntimes,
    namespace: &str,
) -> ReplayReads {
    let mut reads = HashMap::new();
    let mut entries = vec![];
    for event in events {
        let Some(Ok(data)) = peeked_marker(event) else {
            continue;
        };
        for (index, entry) in data.reads.iter().enumerate() {
            if needs_records(entry) {
                let stream = data.streams[entry.stream as usize].clone();
                entries.push((event.event_id, index, stream, entry.clone()));
                (reads.entry(event.event_id)).or_insert_with(|| vec![None; data.reads.len()]);
            }
        }
    }
    let results: Vec<_> = stream::iter(entries)
        .map(|(event_id, index, stream, entry)| async move {
            let result = read_entry(runtimes, namespace, &stream, &entry).await;
            (event_id, index, result)
        })
        .buffer_unordered(runtimes.settings.replay_concurrency)
        .collect()
        .await;
    for (event_id, index, result) in results {
        (reads.get_mut(&event_id)).expect("Each entry has its marker")[index] = Some(result);
    }
    ReplayReads(reads)
}

/// Reads the Records of one entry, and checks them against its count and its digest.
async fn read_entry(
    runtimes: &StreamRuntimes,
    namespace: &str,
    stream: &StreamRef,
    entry: &StreamReadEntry,
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
    let Some(runtime) = runtimes.get(&stream.store) else {
        return Err(ReplayError::Unavailable(format!(
            "This Worker has no Stream runtime for the Store `{}`, so it can't read {}",
            stream.store,
            range()
        )));
    };
    let gone =
        |why: String| ReplayError::Gone(format!("The Store doesn't have {}: {why}", range()));
    let settings = runtimes.settings;
    let mut records: Vec<StreamRecord> = vec![];
    let mut bytes = 0;
    let mut cursor = entry.after_offset.clone();
    let mut outage: Option<(Instant, Backoff)> = None;
    while (records.len() as u64) < count {
        let left = count - records.len() as u64;
        let request = ReadRecordsRequest {
            namespace: namespace.to_string(),
            r#ref: Some(stream.clone()),
            from_offset: cursor.clone(),
            max_records: left.min(MAX_RECORDS as u64) as i32,
            // A Store with other Records can have larger ones, and the replay holds no more than
            // a task gave.
            max_bytes: (MAX_TASK_RECORD_BYTES - bytes).max(1) as i64,
            ..Default::default()
        };
        let read = tokio::time::timeout(settings.try_deadline, runtime.read_records(request));
        let error = match read.await {
            Ok(Ok(page)) => {
                outage = None;
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
            // These say what this Worker can't do, not what the Store lost, so another Worker may
            // still read the Records.
            Ok(Err(error))
                if matches!(
                    error.reason,
                    Reason::FormatTooNew | Reason::Invalid | Reason::Unsupported
                ) =>
            {
                return Err(ReplayError::Unavailable(format!(
                    "This Worker can't read {}: {error}",
                    range()
                )));
            }
            Ok(Err(error)) if error.reason != Reason::StorageUnavailable => {
                return Err(gone(error.to_string()));
            }
            Ok(Err(error)) => error.to_string(),
            Err(_) => format!("The Store gave no answer in {:?}", settings.try_deadline),
        };
        let (since, backoff) =
            outage.get_or_insert_with(|| (Instant::now(), Backoff::new(&settings)));
        if since.elapsed() >= settings.replay_retry_bound {
            return Err(ReplayError::Unavailable(format!(
                "The Store is unavailable, so the replay can't read {}: {error}",
                range()
            )));
        }
        tokio::time::sleep(backoff.next_wait(rand::random())).await;
    }
    let mut digest = Digest::new();
    digest.add(&records);
    if digest.finish() != entry.digest {
        return Err(gone("it has other Records".to_string()));
    }
    Ok(records)
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
        StreamRuntime,
        record::BatchLimits,
        test_store::{Entry, Fault, Model, ModelStore, client},
        workflow::{StreamSettings, marker::marker_details},
    };
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use std::{sync::Arc, time::Duration};
    use temporalio_common::protos::{
        constants::STREAM_MARKER_NAME,
        coresdk::external_data::StreamMarkerData,
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

    // A replay gives each read entry the Records that the first run read after its offset, for
    // any pages of the Store. Fewer Records or other ones are gone. Only a Store that gives no
    // answer, or one that this Worker can't read or has no runtime for, leaves the Records unknown.
    #[tokio::test(start_paused = true)]
    async fn a_replay_read_gives_the_records_or_its_cause() {
        let mut together = 0;
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
            // The markers of the first run, each with read entries over the Stream.
            let mut events = vec![];
            let mut expected = vec![];
            for event_id in 1..=rng.random_range(1..=3) {
                let mut data = StreamMarkerData {
                    streams: vec![StreamRef {
                        id: "s".to_string(),
                        store: "mem".to_string(),
                    }],
                    ..Default::default()
                };
                for seq in 1..=rng.random_range(1..=3) {
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
                    data.reads.push(StreamReadEntry {
                        seq,
                        after_offset: after_offset.clone(),
                        page_activations: (0..count as u64).collect(),
                        page_records: pages,
                        digest: if in_history { vec![] } else { digest.finish() },
                        records: if in_history { given.clone() } else { vec![] },
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
            let mut entries = entries;
            match store {
                Store::Fewer => entries.truncate(rng.random_range(0..entries.len())),
                Store::Other => {
                    let at = rng.random_range(0..entries.len());
                    entries[at].record = Some(record(&entries[at].offset, 1));
                }
                _ => {}
            }
            let fault = move |rng: &mut StdRng, request: &ReadRecordsRequest| {
                assert_eq!(request.namespace, "ns", "seed {seed}");
                match store {
                    Store::Gone => Some(Fault::NotFound),
                    Store::Outage if rng.random_bool(0.3) => Some(Fault::Unavailable),
                    Store::Outage if rng.random_bool(0.05) => Some(Fault::Silent),
                    Store::Refuses => Some(Fault::Refused(refusal)),
                    _ => None,
                }
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
                (store != Store::NoRuntime).then(|| ("mem".to_string(), Arc::new(runtime)));
            let mut runtimes = StreamRuntimes::new(served);
            runtimes.settings = StreamSettings {
                retry_first_wait: Duration::from_millis(1),
                retry_max_wait: Duration::from_millis(5),
                try_deadline: Duration::from_millis(10),
                replay_retry_bound: Duration::from_millis(rng.random_range(0..100)),
                replay_concurrency: rng.random_range(1..=4),
                ..Default::default()
            };
            let mut reads = read_marker_records(&events, &runtimes, "ns").await;
            let mut results = HashMap::<i64, MarkerReads>::new();
            for (event_id, index, after_offset, given) in expected {
                let results = results
                    .entry(event_id)
                    .or_insert_with(|| reads.take(event_id));
                let result = results.get_mut(index).and_then(Option::take);
                // The Records that the Store of the replay has where the entry read.
                let unchanged = given.as_ref().is_some_and(|given| {
                    let now = model.records_after(&after_offset);
                    now.len() >= given.len() && now[..given.len()] == given[..]
                }) && store != Store::Gone;
                match (given, result) {
                    // An entry with its Records in History, or with none, reads no Store.
                    (None, None) => {}
                    (Some(given), Some(Ok(records))) => {
                        assert!(
                            unchanged && store != Store::NoRuntime,
                            "seed {seed}: {store:?}"
                        );
                        assert_eq!(records, given, "seed {seed}");
                    }
                    (Some(_), Some(Err(ReplayError::Gone(_)))) => {
                        assert!(!unchanged, "seed {seed}: {store:?}");
                    }
                    (Some(_), Some(Err(ReplayError::Unavailable(_)))) => {
                        let unknown = matches!(store, Store::Outage | Store::Refuses);
                        assert!(unknown || store == Store::NoRuntime, "{seed}");
                    }
                    other => panic!("seed {seed}: {store:?}: {other:?}"),
                }
            }
            let (_, peak) = model.model.lock().reading;
            assert!(
                peak <= runtimes.settings.replay_concurrency,
                "seed {seed}: {peak}"
            );
            together += usize::from(peak > 1);
        }
        assert!(together > 0);
    }
}
