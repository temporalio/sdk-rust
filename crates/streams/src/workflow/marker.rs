//! The `core_stream` marker of a Workflow Task, which records the results and the close intents
//! of the task's Stream calls.

use super::{
    CallError,
    digest::{DIGEST_LEN, Digest},
    reader::Page,
};
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
            StreamMarkerData, StreamReadEnded, StreamReadEntry, stream_append_result,
        },
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamClose, StreamAppended, StreamReadEnd,
            resolve_stream_append,
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

/// The maximum bytes of Records that one Workflow Task gives Workflow code from all of its reads.
/// It bounds what a replay holds for one marker.
pub(crate) const MAX_TASK_RECORD_BYTES: usize = 16 * 1024 * 1024;

/// The maximum bytes of Records that one marker holds for reads with `records_in_history`, so that
/// a History event stays small. One larger Record still fits as the marker's first.
pub(crate) const MAX_MARKER_RECORD_BYTES: usize = 1024 * 1024;

/// The maximum activation index of a marker entry. No Workflow Task has that many activations, and
/// a replay that starts from it can still add one for each later activation.
const MAX_ACTIVATION: u64 = u32::MAX as u64;

const MARKER_DATA_KEY: &str = "data";

/// The field numbers of the entries in [`StreamMarkerData`].
const STREAMS: u32 = 1;
const CLOSE_INTENTS: u32 = 2;
const APPENDS: u32 = 3;
const CLOSES: u32 = 4;
const READS: u32 = 5;

/// The field numbers in [`StreamReadEntry`] that grow with the pages of a read.
const PAGE_RECORDS: u32 = 4;
const PAGE_ACTIVATIONS: u32 = 5;
const DIGEST: u32 = 6;
const END: u32 = 8;

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
    /// The encoded size of `data`, apart from the Records of its reads.
    bytes: usize,
    /// The entry of each read in `data`, by seq.
    reads: HashMap<u32, ReadEntry>,
    /// The bytes of the Records that the task gave Workflow code, and of those that `data` holds.
    records: (usize, usize),
}

/// A read as its marker entry identifies it.
pub(crate) struct ReadOf<'a> {
    pub(crate) seq: u32,
    pub(crate) stream: &'a StreamRef,
    /// The offset of the last Record that Workflow code got before this page.
    pub(crate) after: &'a [u8],
    pub(crate) records_in_history: bool,
}

/// What the builder knows of one read's entry until the marker is taken.
#[derive(Debug)]
struct ReadEntry {
    /// The index of the entry in the marker's reads.
    index: usize,
    /// The encoded size of the entry's seq, Stream, offset and end.
    fixed: usize,
    /// The encoded sizes of the entry's page counts and of its page activations.
    pages: (usize, usize),
    /// The digest of the entry's Records, for a read without `records_in_history`.
    digest: Option<Digest>,
}

/// What one page adds to the entry of its read.
struct Growth {
    fixed: usize,
    pages: (usize, usize),
    /// The bytes that the marker grows by, apart from the Records.
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

    /// Records a page of a read, given to Workflow code at `activation`, with the end of the read
    /// if it has one. `records` is the size of the page's Records. Returns false if the marker or
    /// the task has no room for it.
    pub(crate) fn read_page(
        &mut self,
        read: &ReadOf,
        page: &Page,
        records: usize,
        activation: u64,
    ) -> bool {
        let Some(growth) = self.page_growth(read, page, records, activation) else {
            return false;
        };
        if !self.reads.contains_key(&read.seq) {
            let entry = StreamReadEntry {
                seq: read.seq,
                stream: self.index_of(read.stream),
                after_offset: read.after.to_vec(),
                ..Default::default()
            };
            self.note_stream(read.stream);
            let entry_of = ReadEntry {
                index: self.data.reads.len(),
                fixed: 0,
                pages: (0, 0),
                digest: (!read.records_in_history).then(Digest::new),
            };
            self.reads.insert(read.seq, entry_of);
            self.data.reads.push(entry);
        }
        let entry = (self.reads.get_mut(&read.seq)).expect("The entry was just added");
        let data = &mut self.data.reads[entry.index];
        (entry.fixed, entry.pages) = (growth.fixed, growth.pages);
        if !page.records.is_empty() {
            data.page_records.push(page.records.len() as u32);
            data.page_activations.push(activation);
            match &mut entry.digest {
                Some(digest) => digest.add(&page.records),
                None => data.records.extend(page.records.iter().cloned()),
            }
        }
        if let Some(end) = &page.end {
            data.end = Some(ended(end, activation));
        }
        self.bytes += growth.bytes;
        self.records.0 += records;
        if read.records_in_history {
            self.records.1 += records;
        }
        true
    }

    /// Returns true if the marker and the task have room for a page, without recording it.
    pub(crate) fn has_room_for_page(
        &self,
        read: &ReadOf,
        page: &Page,
        records: usize,
        activation: u64,
    ) -> bool {
        self.page_growth(read, page, records, activation).is_some()
    }

    /// Returns the bytes of Records that the task can still give Workflow code, and that the
    /// marker can still hold for reads with `records_in_history`.
    pub(crate) fn records_room(&self) -> (usize, usize) {
        let (task, marker) = self.records;
        (
            MAX_TASK_RECORD_BYTES.saturating_sub(task),
            MAX_MARKER_RECORD_BYTES.saturating_sub(marker),
        )
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes == 0
    }

    /// Returns the marker, if it has an entry, and leaves an empty builder for the next task.
    pub(crate) fn take(&mut self) -> Option<StreamMarkerData> {
        let mut builder = std::mem::take(self);
        for entry in std::mem::take(&mut builder.reads).into_values() {
            let data = &mut builder.data.reads[entry.index];
            if let Some(digest) = entry.digest
                && !data.page_records.is_empty()
            {
                data.digest = digest.finish();
            }
        }
        debug_assert_eq!(builder.bytes, {
            let mut bare = builder.data.clone();
            bare.reads.iter_mut().for_each(|read| read.records.clear());
            bare.encoded_len()
        });
        (!builder.is_empty()).then_some(builder.data)
    }

    /// Returns what a page adds to the marker, or `None` if the marker or the task has no room
    /// for it. An empty task or marker takes any page, since the reader asked for at most the room
    /// that the task had, and a larger page is one Record.
    fn page_growth(
        &self,
        read: &ReadOf,
        page: &Page,
        records: usize,
        activation: u64,
    ) -> Option<Growth> {
        let (task, marker) = self.records;
        if !page.records.is_empty()
            && ((task > 0 && task + records > MAX_TASK_RECORD_BYTES)
                || (read.records_in_history
                    && marker > 0
                    && marker + records > MAX_MARKER_RECORD_BYTES))
        {
            return None;
        }
        let entry = self.reads.get(&read.seq);
        let (mut fixed, mut pages) = match entry {
            Some(entry) => (entry.fixed, entry.pages),
            None => {
                let entry = StreamReadEntry {
                    seq: read.seq,
                    stream: self.index_of(read.stream),
                    after_offset: read.after.to_vec(),
                    ..Default::default()
                };
                (entry.encoded_len(), (0, 0))
            }
        };
        if !page.records.is_empty() {
            pages.0 += encoded_len_varint(page.records.len() as u64);
            pages.1 += encoded_len_varint(activation);
        }
        if let Some(end) = &page.end {
            fixed += field_len(END, ended(end, activation).encoded_len());
        }
        let digest = !read.records_in_history;
        let before = match entry {
            Some(entry) => field_len(READS, read_len(entry.fixed, entry.pages, digest)),
            None => 0,
        };
        let table = if entry.is_none() {
            self.table_len(read.stream)
        } else {
            0
        };
        let bytes = field_len(READS, read_len(fixed, pages, digest)) - before + table;
        self.has_room(bytes).then_some(Growth {
            fixed,
            pages,
            bytes,
        })
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

/// Returns the encoded size of a read entry apart from its Records. A page list with no element
/// encodes to nothing, and the digest is set once the entry has a page.
fn read_len(fixed: usize, pages: (usize, usize), digest: bool) -> usize {
    let packed = |tag, len| if len == 0 { 0 } else { field_len(tag, len) };
    let digest = if digest && pages.0 > 0 {
        field_len(DIGEST, DIGEST_LEN)
    } else {
        0
    };
    fixed + digest + packed(PAGE_RECORDS, pages.0) + packed(PAGE_ACTIVATIONS, pages.1)
}

fn ended(end: &StreamReadEnd, activation: u64) -> StreamReadEnded {
    StreamReadEnded {
        activation,
        done: end.done,
        failure: end.failed.clone(),
    }
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
        .chain(data.reads.iter().flat_map(|read| {
            let end = read.end.as_ref().map(|end| end.activation);
            read.page_activations.iter().copied().chain(end)
        }));
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
        // A read gives at most one page in an activation, and its end comes last.
        let activations = &read.page_activations;
        let last = activations.last().copied();
        if activations.windows(2).any(|pair| pair[0] >= pair[1])
            || (read.end.as_ref()).is_some_and(|end| last.is_some_and(|last| end.activation < last))
        {
            return Err(bad(format!(
                "has pages of read {} that aren't in the order of their activations",
                read.seq
            )));
        }
        let count: u64 = read
            .page_records
            .iter()
            .map(|count| u64::from(*count))
            .sum();
        if !read.records.is_empty() && count != read.records.len() as u64 {
            return Err(bad(format!(
                "counts {count} Records in the pages of read {}, and has {}",
                read.seq,
                read.records.len()
            )));
        }
        // Pages have their Records or a digest of them, and an entry with no page has an end.
        let (seq, digest) = (read.seq, read.digest.len());
        let shape = if read.page_records.contains(&0) {
            Some(format!("has a page with no Records in read {seq}"))
        } else if count == 0 && read.end.is_none() {
            Some(format!("has read {seq} with no page and no end"))
        } else if count == 0 && digest > 0 {
            Some(format!("has a digest of read {seq}, which has no page"))
        } else if count > 0 && read.records.is_empty() && digest != DIGEST_LEN {
            Some(format!(
                "has a digest of {digest} bytes for the pages of read {seq}, and no Records"
            ))
        } else if !read.records.is_empty() && digest > 0 {
            Some(format!(
                "has both the Records of read {seq} and their digest"
            ))
        } else {
            None
        };
        if let Some(why) = shape {
            return Err(bad(why));
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
                // A read gives at most one page in an activation, and its end comes last.
                let mut at = rng.random_range(0..4);
                let page_activations = (0..pages)
                    .map(|_| {
                        at += rng.random_range(1..3);
                        at
                    })
                    .collect();
                let end_at = match rng.random_bool(0.9) {
                    true => at + rng.random_range(0..2),
                    false => MAX_ACTIVATION,
                };
                let in_history = rng.random_bool(0.5);
                // Pages have their Records or a digest of them, and an entry with no page has an
                // end.
                StreamReadEntry {
                    seq,
                    stream: index(rng),
                    page_records: vec![1; pages],
                    page_activations,
                    digest: match in_history || pages == 0 {
                        true => vec![],
                        false => vec![7; DIGEST_LEN],
                    },
                    records: match in_history {
                        true => vec![Default::default(); pages],
                        false => vec![],
                    },
                    end: (pages == 0 || rng.random_bool(0.5)).then(|| StreamReadEnded {
                        activation: end_at,
                        done,
                        failure: (!done).then(Failure::default),
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
        let why = match rng.random_range(0..15) {
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
            6 => {
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
            at @ (7 | 8) => {
                // Two pages in one activation, or an end before the last page.
                let (pages, end) = match at {
                    7 => (vec![3, 3], None),
                    _ => (vec![2, 3], Some(2)),
                };
                marker.reads.push(StreamReadEntry {
                    seq: 99,
                    page_records: vec![1; 2],
                    page_activations: pages,
                    end: end.map(|activation| StreamReadEnded {
                        activation,
                        ..Default::default()
                    }),
                    ..Default::default()
                });
                "has pages of read 99 that aren't in the order of their activations".to_string()
            }
            9 => {
                marker.reads.push(StreamReadEntry {
                    seq: 99,
                    page_records: vec![2],
                    page_activations: vec![0],
                    records: vec![Default::default()],
                    ..Default::default()
                });
                "counts 2 Records in the pages of read 99, and has 1".to_string()
            }
            10 => {
                marker.reads.push(StreamReadEntry {
                    seq: 99,
                    page_records: vec![1, 0],
                    page_activations: vec![0, 1],
                    digest: vec![7; DIGEST_LEN],
                    ..Default::default()
                });
                "has a page with no Records in read 99".to_string()
            }
            11 => {
                marker.reads.push(StreamReadEntry {
                    seq: 99,
                    ..Default::default()
                });
                "has read 99 with no page and no end".to_string()
            }
            12 => {
                marker.reads.push(StreamReadEntry {
                    seq: 99,
                    digest: vec![7; DIGEST_LEN],
                    end: Some(StreamReadEnded::default()),
                    ..Default::default()
                });
                "has a digest of read 99, which has no page".to_string()
            }
            13 => {
                let digest = [0, 1, DIGEST_LEN - 1, DIGEST_LEN + 1, 32][rng.random_range(0..5)];
                marker.reads.push(StreamReadEntry {
                    seq: 99,
                    page_records: vec![1],
                    page_activations: vec![0],
                    digest: vec![7; digest],
                    ..Default::default()
                });
                format!("has a digest of {digest} bytes for the pages of read 99, and no Records")
            }
            _ => {
                marker.reads.push(StreamReadEntry {
                    seq: 99,
                    page_records: vec![1],
                    page_activations: vec![0],
                    records: vec![Default::default()],
                    digest: vec![7; DIGEST_LEN],
                    ..Default::default()
                });
                "has both the Records of read 99 and their digest".to_string()
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
