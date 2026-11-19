//! The marker `core_stream` of a Workflow Task.

use super::CallError;
use crate::proto::StreamRef;
use prost::{Message, encoding::message::encoded_len};
use std::collections::HashMap;
use temporalio_common::protos::{
    constants::STREAM_MARKER_NAME,
    coresdk::{
        external_data::{
            StreamAppendOffsets, StreamAppendResult, StreamCloseIntent, StreamCloseResult,
            StreamMarkerData, stream_append_result,
        },
        workflow_activation::resolve_stream_append,
    },
    temporal::api::{
        common::v1::{Payload, Payloads},
        failure::v1::Failure,
        history::v1::{HistoryEvent, MarkerRecordedEventAttributes, history_event},
    },
};

/// The most bytes of the entries of one marker. A task that reaches it completes with a new one
/// forced, so a long task with many results still makes small markers.
pub(crate) const MAX_MARKER_ENTRY_BYTES: usize = 256 * 1024;

const MARKER_DATA_KEY: &str = "data";

/// The marker of the task in hand.
#[derive(Debug, Default)]
pub(crate) struct MarkerBuilder {
    data: StreamMarkerData,
    streams: HashMap<(String, String), u32>,
    /// The encoded size of `data`.
    bytes: usize,
    /// Set once an entry didn't fit. Every later entry of the task waits for the next one, so
    /// entries keep the order of their seqs.
    full: bool,
}

impl MarkerBuilder {
    pub(crate) fn append_result(
        &mut self,
        stream: &StreamRef,
        seq: u32,
        activation: u64,
        status: &resolve_stream_append::Status,
    ) -> bool {
        let (entry, len) = self.append_entry(stream, seq, activation, status);
        if !self.fits(len) {
            return false;
        }
        self.note_stream(stream);
        self.data.appends.push(entry);
        true
    }

    /// True if the marker has room for the result of an append, which it doesn't record.
    pub(crate) fn has_room_for_append(
        &self,
        stream: &StreamRef,
        seq: u32,
        activation: u64,
        status: &resolve_stream_append::Status,
    ) -> bool {
        self.has_room(self.append_entry(stream, seq, activation, status).1)
    }

    fn append_entry(
        &self,
        stream: &StreamRef,
        seq: u32,
        activation: u64,
        status: &resolve_stream_append::Status,
    ) -> (StreamAppendResult, usize) {
        let result = match status {
            resolve_stream_append::Status::Appended(appended) => {
                stream_append_result::Result::Appended(StreamAppendOffsets {
                    first_offset: appended.first_offset.clone(),
                    last_offset: appended.last_offset.clone(),
                })
            }
            resolve_stream_append::Status::Failed(failure) => {
                stream_append_result::Result::Failure(failure.clone())
            }
        };
        let entry = StreamAppendResult {
            seq,
            stream: self.index_of(stream),
            activation,
            result: Some(result),
        };
        let len = encoded_len(3, &entry) + self.table_len(stream);
        (entry, len)
    }

    pub(crate) fn close_intent(&mut self, stream: &StreamRef, seq: u32) -> bool {
        let entry = StreamCloseIntent {
            seq,
            stream: self.index_of(stream),
        };
        if !self.fits(encoded_len(2, &entry) + self.table_len(stream)) {
            return false;
        }
        self.note_stream(stream);
        self.data.close_intents.push(entry);
        true
    }

    pub(crate) fn close_result(
        &mut self,
        seq: u32,
        activation: u64,
        failure: Option<&Failure>,
    ) -> bool {
        let (entry, len) = close_entry(seq, activation, failure);
        if !self.fits(len) {
            return false;
        }
        self.data.closes.push(entry);
        true
    }

    /// True if the marker has room for the result of a close, which it doesn't record.
    pub(crate) fn has_room_for_close(
        &self,
        seq: u32,
        activation: u64,
        failure: Option<&Failure>,
    ) -> bool {
        self.has_room(close_entry(seq, activation, failure).1)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes == 0
    }

    pub(crate) fn is_full(&self) -> bool {
        self.full
    }

    /// The marker, if it has an entry, and an empty builder for the next task.
    pub(crate) fn take(&mut self) -> Option<StreamMarkerData> {
        let builder = std::mem::take(self);
        (!builder.is_empty()).then_some(builder.data)
    }

    /// An empty marker takes any entry, since each input of an entry has a bound.
    fn has_room(&self, len: usize) -> bool {
        !self.full && (self.is_empty() || self.bytes + len <= MAX_MARKER_ENTRY_BYTES)
    }

    fn fits(&mut self, len: usize) -> bool {
        if !self.has_room(len) {
            self.full = true;
            return false;
        }
        self.bytes += len;
        true
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
            encoded_len(1, stream)
        }
    }

    fn note_stream(&mut self, stream: &StreamRef) {
        if let std::collections::hash_map::Entry::Vacant(vacant) = self.streams.entry(key(stream)) {
            vacant.insert(self.data.streams.len() as u32);
            self.data.streams.push(stream.clone());
        }
    }
}

fn close_entry(seq: u32, activation: u64, failure: Option<&Failure>) -> (StreamCloseResult, usize) {
    let entry = StreamCloseResult {
        seq,
        activation,
        failure: failure.cloned(),
    };
    let len = encoded_len(4, &entry);
    (entry, len)
}

fn key(stream: &StreamRef) -> (String, String) {
    (stream.store.clone(), stream.id.clone())
}

/// The details of the marker command that records `data`.
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

/// True if `event` is a `core_stream` marker.
pub fn is_stream_marker(event: &HistoryEvent) -> bool {
    marker_of(event).is_some()
}

/// The data of a `core_stream` marker event, or `None` for any other event. A marker that no
/// Core writes is an error.
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

    fn stream(id: &str) -> StreamRef {
        StreamRef {
            id: id.to_string(),
            store: "mem".to_string(),
        }
    }

    #[test]
    fn a_bad_marker_is_fatal() {
        let read = |change: fn(&mut StreamReadEntry)| {
            let mut read = StreamReadEntry {
                page_records: vec![1],
                page_activations: vec![0],
                ..Default::default()
            };
            change(&mut read);
            StreamMarkerData {
                streams: vec![stream("s")],
                reads: vec![read],
                ..Default::default()
            }
        };
        let garbage = HashMap::from([(
            MARKER_DATA_KEY.to_string(),
            Payloads {
                payloads: vec![Payload {
                    data: vec![0xff],
                    ..Default::default()
                }],
            },
        )]);
        let cases = [
            (HashMap::new(), "has no data"),
            (
                garbage,
                "doesn't decode: failed to decode Protobuf message: invalid varint",
            ),
            (
                marker_details(&StreamMarkerData {
                    close_intents: vec![StreamCloseIntent { seq: 1, stream: 0 }],
                    ..Default::default()
                }),
                "has an entry of Stream 0, and its table has 0",
            ),
            (
                marker_details(&StreamMarkerData {
                    streams: vec![stream("s")],
                    appends: vec![StreamAppendResult {
                        seq: 2,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                "has no result of append 2",
            ),
            (
                marker_details(&read(|read| read.page_activations.clear())),
                "has 1 page counts and 0 page activations of read 0",
            ),
            (
                marker_details(&read(|read| {
                    read.end = Some(StreamReadEnded {
                        done: true,
                        failure: Some(Failure::default()),
                        ..Default::default()
                    })
                })),
                "has an end of read 0 that is done and failed",
            ),
        ];
        for (details, why) in cases {
            let error = CallError::Fatal(format!("The Stream marker {why}"));
            assert_eq!(peeked_marker(&event(details)), Some(Err(error)));
        }
    }
}
