//! The `core_stream` marker of a Workflow Task, which records the results and the close intents
//! of the task's Stream calls.

use super::CallError;
use crate::proto::StreamRef;
use prost::{
    Message,
    encoding::{encoded_len_varint, key_len, message::encoded_len},
};
use std::collections::HashMap;
use temporalio_common::protos::{
    constants::STREAM_MARKER_NAME,
    coresdk::{
        external_data::{
            StreamAppendOffsets, StreamAppendResult, StreamCloseIntent, StreamCloseResult,
            StreamMarkerData, stream_append_result,
        },
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, StreamAppended, resolve_stream_append,
        },
    },
    temporal::api::{
        common::v1::{Payload, Payloads},
        failure::v1::Failure,
        history::v1::{HistoryEvent, MarkerRecordedEventAttributes, history_event},
    },
};

/// The maximum size of the entries of one marker, in bytes. A task whose marker is full completes
/// with a new task forced, so a long task with many results still records small markers.
pub(crate) const MAX_MARKER_ENTRY_BYTES: usize = 256 * 1024;

/// The maximum activation index of a marker entry. No Workflow Task has that many activations, and
/// a replay that starts from it can still add one for each later activation.
const MAX_ACTIVATION: u64 = u32::MAX as u64;

const MARKER_DATA_KEY: &str = "data";

/// The field numbers of the entries in [`StreamMarkerData`].
const STREAMS: u32 = 1;
const CLOSE_INTENTS: u32 = 2;
const APPENDS: u32 = 3;
const CLOSES: u32 = 4;

/// A live result in the form that a marker records it. `len` is its encoded size before the
/// marker gives it an activation and a Stream index, so a size check doesn't have to encode it.
#[derive(Debug)]
pub(crate) struct Done<T> {
    entry: T,
    len: usize,
}

impl Done<StreamAppendResult> {
    /// Converts the result of append `seq` into its marker entry.
    pub(crate) fn append(seq: u32, status: resolve_stream_append::Status) -> Self {
        let result = match status {
            resolve_stream_append::Status::Appended(appended) => {
                stream_append_result::Result::Appended(StreamAppendOffsets {
                    first_offset: appended.first_offset,
                    last_offset: appended.last_offset,
                })
            }
            resolve_stream_append::Status::Failed(failure) => {
                stream_append_result::Result::Failure(failure)
            }
        };
        let entry = StreamAppendResult {
            seq,
            result: Some(result),
            ..Default::default()
        };
        let len = entry.encoded_len();
        Self { entry, len }
    }

    /// Returns the job that gives Workflow code this result.
    pub(crate) fn job(&self) -> ResolveStreamAppend {
        ResolveStreamAppend {
            seq: self.entry.seq,
            status: self.entry.result.clone().map(status_of),
        }
    }
}

impl Done<StreamCloseResult> {
    /// Converts the result of close `seq` into its marker entry.
    pub(crate) fn close(seq: u32, failure: Option<Failure>) -> Self {
        let entry = StreamCloseResult {
            seq,
            failure,
            ..Default::default()
        };
        let len = entry.encoded_len();
        Self { entry, len }
    }

    /// Returns the job that gives Workflow code this result.
    pub(crate) fn job(&self) -> ResolveStreamClose {
        ResolveStreamClose {
            seq: self.entry.seq,
            failed: self.entry.failure.clone(),
        }
    }

    /// Returns the failure of the close, if it failed.
    pub(crate) fn failure(&self) -> Option<&Failure> {
        self.entry.failure.as_ref()
    }
}

/// Returns the job status of an append result in a marker. A replay and a live run both build
/// their jobs with it, so both give Workflow code the same job for the same entry.
pub(crate) fn status_of(result: stream_append_result::Result) -> resolve_stream_append::Status {
    match result {
        stream_append_result::Result::Appended(offsets) => {
            resolve_stream_append::Status::Appended(StreamAppended {
                first_offset: offsets.first_offset,
                last_offset: offsets.last_offset,
            })
        }
        stream_append_result::Result::Failure(failure) => {
            resolve_stream_append::Status::Failed(failure)
        }
    }
}

/// The marker of the task in hand. Its results of each kind are in the order that Core took them,
/// and its intents are in the order of their seqs. Each caller stops at the first entry that
/// doesn't fit, so the marker never records an entry while an earlier one waits.
#[derive(Debug, Default)]
pub(crate) struct MarkerBuilder {
    data: StreamMarkerData,
    streams: HashMap<(String, String), u32>,
    /// The encoded size of `data`.
    bytes: usize,
}

impl MarkerBuilder {
    /// Records the result of an append on `stream`, given to Workflow code at `activation`.
    /// Returns false if the marker has no room for it.
    pub(crate) fn append_result(
        &mut self,
        stream: &StreamRef,
        done: &Done<StreamAppendResult>,
        activation: u64,
    ) -> bool {
        if !self.fits(self.append_len(stream, done, activation)) {
            return false;
        }
        let entry = StreamAppendResult {
            stream: self.index_of(stream),
            activation,
            ..done.entry.clone()
        };
        self.note_stream(stream);
        self.data.appends.push(entry);
        true
    }

    /// Returns true if the marker has room for the result of an append, without recording it.
    pub(crate) fn has_room_for_append(
        &self,
        stream: &StreamRef,
        done: &Done<StreamAppendResult>,
        activation: u64,
    ) -> bool {
        self.has_room(self.append_len(stream, done, activation))
    }

    /// Records the intent of close `seq` on `stream`. Returns false if the marker has no room.
    pub(crate) fn close_intent(&mut self, stream: &StreamRef, seq: u32) -> bool {
        let entry = StreamCloseIntent {
            seq,
            stream: self.index_of(stream),
        };
        if !self.fits(encoded_len(CLOSE_INTENTS, &entry) + self.table_len(stream)) {
            return false;
        }
        self.note_stream(stream);
        self.data.close_intents.push(entry);
        true
    }

    /// Records the result of a close, given at `activation`. Returns false if the marker has no
    /// room for it.
    pub(crate) fn close_result(&mut self, done: &Done<StreamCloseResult>, activation: u64) -> bool {
        if !self.fits(close_len(done, activation)) {
            return false;
        }
        let entry = StreamCloseResult {
            activation,
            ..done.entry.clone()
        };
        self.data.closes.push(entry);
        true
    }

    /// Returns true if the marker has room for the result of a close, without recording it.
    pub(crate) fn has_room_for_close(
        &self,
        done: &Done<StreamCloseResult>,
        activation: u64,
    ) -> bool {
        self.has_room(close_len(done, activation))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes == 0
    }

    /// Returns the marker, if it has an entry, and leaves an empty builder for the next task.
    pub(crate) fn take(&mut self) -> Option<StreamMarkerData> {
        let builder = std::mem::take(self);
        debug_assert_eq!(builder.bytes, builder.data.encoded_len());
        (!builder.is_empty()).then_some(builder.data)
    }

    /// An empty marker takes any entry, since each input of an entry has a bound.
    fn has_room(&self, len: usize) -> bool {
        self.is_empty() || self.bytes + len <= MAX_MARKER_ENTRY_BYTES
    }

    fn fits(&mut self, len: usize) -> bool {
        if !self.has_room(len) {
            return false;
        }
        self.bytes += len;
        true
    }

    fn append_len(
        &self,
        stream: &StreamRef,
        done: &Done<StreamAppendResult>,
        activation: u64,
    ) -> usize {
        let placed = StreamAppendResult {
            stream: self.index_of(stream),
            activation,
            ..Default::default()
        };
        field_len(APPENDS, done.len + placed.encoded_len()) + self.table_len(stream)
    }

    fn index_of(&self, stream: &StreamRef) -> u32 {
        match self.streams.get(&key(stream)) {
            Some(index) => *index,
            None => self.data.streams.len() as u32,
        }
    }

    fn table_len(&self, stream: &StreamRef) -> usize {
        if self.streams.contains_key(&key(stream)) {
            0
        } else {
            encoded_len(STREAMS, stream)
        }
    }

    fn note_stream(&mut self, stream: &StreamRef) {
        if let std::collections::hash_map::Entry::Vacant(vacant) = self.streams.entry(key(stream)) {
            vacant.insert(self.data.streams.len() as u32);
            self.data.streams.push(stream.clone());
        }
    }
}

fn close_len(done: &Done<StreamCloseResult>, activation: u64) -> usize {
    let placed = StreamCloseResult {
        activation,
        ..Default::default()
    };
    field_len(CLOSES, done.len + placed.encoded_len())
}

/// Returns the encoded size of a message field whose message has `len` bytes. An absent field and
/// a field with a default value encode to nothing, so the sizes of disjoint fields add up.
fn field_len(tag: u32, len: usize) -> usize {
    key_len(tag) + encoded_len_varint(len as u64) + len
}

fn key(stream: &StreamRef) -> (String, String) {
    (stream.store.clone(), stream.id.clone())
}

/// Returns the details of the marker command that records `data`.
pub fn marker_details(data: &StreamMarkerData) -> HashMap<String, Payloads> {
    let payload = Payload {
        metadata: HashMap::from([
            ("encoding".to_string(), b"binary/protobuf".to_vec()),
            (
                "messageType".to_string(),
                b"coresdk.external_data.StreamMarkerData".to_vec(),
            ),
        ]),
        data: data.encode_to_vec(),
        ..Default::default()
    };
    HashMap::from([(
        MARKER_DATA_KEY.to_string(),
        Payloads {
            payloads: vec![payload],
        },
    )])
}

/// Returns true if `event` is a `core_stream` marker.
pub fn is_stream_marker(event: &HistoryEvent) -> bool {
    marker_of(event).is_some()
}

/// Decodes a `core_stream` marker event, and returns `None` for any other event. A marker that no
/// Core could have written is an error.
pub fn peeked_marker(event: &HistoryEvent) -> Option<Result<StreamMarkerData, CallError>> {
    marker_of(event).map(decode)
}

fn marker_of(event: &HistoryEvent) -> Option<&MarkerRecordedEventAttributes> {
    match event.attributes.as_ref()? {
        history_event::Attributes::MarkerRecordedEventAttributes(marker)
            if marker.marker_name == STREAM_MARKER_NAME =>
        {
            Some(marker)
        }
        _ => None,
    }
}

fn decode(marker: &MarkerRecordedEventAttributes) -> Result<StreamMarkerData, CallError> {
    let bad = |why: String| CallError::Fatal(format!("The Stream marker {why}"));
    let payload = marker
        .details
        .get(MARKER_DATA_KEY)
        .and_then(|payloads| payloads.payloads.first())
        .ok_or_else(|| bad("has no data".to_string()))?;
    let data = StreamMarkerData::decode(payload.data.as_slice())
        .map_err(|error| bad(format!("doesn't decode: {error}")))?;
    let streams = data.streams.len();
    let indexes = (data.close_intents.iter().map(|intent| intent.stream))
        .chain(data.appends.iter().map(|append| append.stream))
        .chain(data.reads.iter().map(|read| read.stream));
    if let Some(index) = indexes.into_iter().find(|index| *index as usize >= streams) {
        return Err(bad(format!(
            "has an entry of Stream {index}, and its table has {streams}"
        )));
    }
    if let Some(append) = data.appends.iter().find(|append| append.result.is_none()) {
        return Err(bad(format!("has no result of append {}", append.seq)));
    }
    // A replay sets its activation index to the activation of an entry, and each later activation
    // of the task adds one to it.
    let activations = (data.appends.iter().map(|append| append.activation))
        .chain(data.closes.iter().map(|close| close.activation))
        .chain(
            data.reads
                .iter()
                .flat_map(|read| read.page_activations.iter().copied()),
        );
    if let Some(activation) = (activations.into_iter()).find(|at| *at > MAX_ACTIVATION) {
        return Err(bad(format!(
            "has an entry at activation {activation}, above the maximum of {MAX_ACTIVATION}"
        )));
    }
    for read in &data.reads {
        if read.page_records.len() != read.page_activations.len() {
            return Err(bad(format!(
                "has {} page counts and {} page activations of read {}",
                read.page_records.len(),
                read.page_activations.len(),
                read.seq
            )));
        }
        if read
            .end
            .as_ref()
            .is_some_and(|end| end.done && end.failure.is_some())
        {
            return Err(bad(format!(
                "has an end of read {} that is done and failed",
                read.seq
            )));
        }
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use temporalio_common::protos::coresdk::external_data::{StreamReadEnded, StreamReadEntry};

    fn event(details: HashMap<String, Payloads>) -> HistoryEvent {
        HistoryEvent {
            attributes: Some(history_event::Attributes::MarkerRecordedEventAttributes(
                MarkerRecordedEventAttributes {
                    marker_name: STREAM_MARKER_NAME.to_string(),
                    details,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    /// Returns a marker that Core could have written, with random entries of each kind.
    fn valid_marker(rng: &mut StdRng) -> StreamMarkerData {
        let streams: Vec<StreamRef> = (0..rng.random_range(1..=4))
            .map(|index| StreamRef {
                id: format!("s{index}"),
                store: "mem".to_string(),
            })
            .collect();
        let count = streams.len();
        let index = |rng: &mut StdRng| rng.random_range(0..count) as u32;
        let activation = |rng: &mut StdRng| match rng.random_bool(0.9) {
            true => rng.random_range(0..10),
            false => MAX_ACTIVATION,
        };
        let close_intents = (1..=rng.random_range(0..3))
            .map(|seq| StreamCloseIntent {
                seq,
                stream: index(rng),
            })
            .collect();
        let appends = (1..=rng.random_range(0..4))
            .map(|seq| StreamAppendResult {
                seq,
                stream: index(rng),
                activation: activation(rng),
                result: Some(match rng.random_bool(0.5) {
                    true => stream_append_result::Result::Appended(StreamAppendOffsets {
                        first_offset: vec![1],
                        last_offset: vec![2],
                    }),
                    false => stream_append_result::Result::Failure(Failure::default()),
                }),
            })
            .collect();
        let closes = (1..=rng.random_range(0..3))
            .map(|seq| StreamCloseResult {
                seq,
                activation: activation(rng),
                failure: rng.random_bool(0.5).then(Failure::default),
            })
            .collect();
        let reads = (1..=rng.random_range(0..3))
            .map(|seq| {
                let pages = rng.random_range(0..3);
                let done = rng.random_bool(0.5);
                StreamReadEntry {
                    seq,
                    stream: index(rng),
                    page_records: vec![1; pages],
                    page_activations: (0..pages).map(|_| activation(rng)).collect(),
                    end: rng.random_bool(0.5).then(|| StreamReadEnded {
                        done,
                        failure: (!done).then(Failure::default),
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            })
            .collect();
        StreamMarkerData {
            streams,
            close_intents,
            appends,
            closes,
            reads,
        }
    }

    /// Breaks one rule of `marker`. Returns the details of the marker event and the reason that
    /// the decode gives.
    fn corrupt(
        rng: &mut StdRng,
        mut marker: StreamMarkerData,
    ) -> (HashMap<String, Payloads>, String) {
        let streams = marker.streams.len() as u32;
        let why = match rng.random_range(0..7) {
            0 => return (HashMap::new(), "has no data".to_string()),
            1 => {
                let garbage = Payloads {
                    payloads: vec![Payload {
                        data: vec![0xff],
                        ..Default::default()
                    }],
                };
                let details = HashMap::from([(MARKER_DATA_KEY.to_string(), garbage)]);
                let why = "doesn't decode: failed to decode Protobuf message: invalid varint";
                return (details, why.to_string());
            }
            2 => {
                let index = streams + rng.random_range(0..3);
                match rng.random_range(0..3) {
                    0 if !marker.appends.is_empty() => marker.appends[0].stream = index,
                    1 if !marker.reads.is_empty() => marker.reads[0].stream = index,
                    _ => marker.close_intents.push(StreamCloseIntent {
                        seq: 99,
                        stream: index,
                    }),
                }
                format!("has an entry of Stream {index}, and its table has {streams}")
            }
            3 => {
                marker.appends.push(StreamAppendResult {
                    seq: 99,
                    ..Default::default()
                });
                "has no result of append 99".to_string()
            }
            4 => {
                let activation = rng.random_range(MAX_ACTIVATION + 1..=u64::MAX);
                match rng.random_range(0..3) {
                    0 => marker.appends.push(StreamAppendResult {
                        seq: 99,
                        activation,
                        result: Some(stream_append_result::Result::Failure(Failure::default())),
                        ..Default::default()
                    }),
                    1 => marker.closes.push(StreamCloseResult {
                        seq: 99,
                        activation,
                        failure: None,
                    }),
                    _ => marker.reads.push(StreamReadEntry {
                        seq: 99,
                        page_records: vec![1],
                        page_activations: vec![activation],
                        ..Default::default()
                    }),
                }
                format!("has an entry at activation {activation}, above the maximum of 4294967295")
            }
            5 => {
                marker.reads.push(StreamReadEntry {
                    seq: 99,
                    page_records: vec![1, 2],
                    page_activations: vec![0],
                    ..Default::default()
                });
                "has 2 page counts and 1 page activations of read 99".to_string()
            }
            _ => {
                marker.reads.push(StreamReadEntry {
                    seq: 99,
                    end: Some(StreamReadEnded {
                        done: true,
                        failure: Some(Failure::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
                "has an end of read 99 that is done and failed".to_string()
            }
        };
        (marker_details(&marker), why)
    }

    #[test]
    fn a_bad_marker_is_fatal() {
        for seed in 0..1000 {
            let mut rng = StdRng::seed_from_u64(seed);
            let valid = valid_marker(&mut rng);
            let decoded = peeked_marker(&event(marker_details(&valid)));
            assert_eq!(decoded, Some(Ok(valid.clone())), "seed {seed}");
            let (details, why) = corrupt(&mut rng, valid);
            let error = CallError::Fatal(format!("The Stream marker {why}"));
            assert_eq!(
                peeked_marker(&event(details)),
                Some(Err(error)),
                "seed {seed}"
            );
        }
    }
}
