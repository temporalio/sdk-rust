//! The ledger of a Workflow Task, which has the results and the close intents of the task's Stream
//! calls. Core puts it on the completion of the task, as the `stream_calls` of its `sdk_metadata`,
//! and History keeps it in the `WorkflowTaskCompleted` event.

use super::{StreamSettings, digest::digest, manager::failure_of};
use crate::worker::workflow::WFMachinesError;
use prost::{Message, encoding::encoded_len_varint};
use temporalio_common::protos::{
    coresdk::workflow_activation::{
        ResolveStreamAppend, ResolveStreamClose, ResolveStreamRead, StreamAppended, StreamPage,
        resolve_stream_append, resolve_stream_read,
    },
    temporal::api::{
        history::v1::{HistoryEvent, history_event},
        sdk::v1::{
            WorkflowTaskStreamCalls,
            workflow_task_stream_calls::{
                AppendResult, CallFailure, CloseIntent, CloseResult, ReadPage, ReadResult,
                append_result, read_result,
            },
        },
    },
};
use temporalio_streams::{
    proto::{ReadRecordsResponse, StreamErrorReason, StreamRecord, StreamRef},
    record::field_len,
};

/// The entry of a result that waits for an activation, as a ledger records it, with its encoded
/// size.
#[derive(Debug)]
pub(crate) struct Waiting<T> {
    /// The entry, which gets its activation index when a ledger records it.
    pub(crate) entry: T,
    len: usize,
}

impl<T: Message> Waiting<T> {
    pub(crate) fn new(entry: T) -> Self {
        let len = entry.encoded_len();
        Self { entry, len }
    }

    /// Returns the index of `activation` as History keeps it, and the bytes that the entry adds to
    /// a ledger with that index. Returns `None` if an `int32` can't hold the index, so that the
    /// result waits for the next task.
    fn at(&self, activation: u64) -> Option<(i32, usize)> {
        let index = i32::try_from(activation).ok()?;
        // The entry was measured with index 0, which the encoding leaves out. Another index adds
        // its one-byte tag and its varint.
        let index_len = match index {
            0 => 0,
            _ => 1 + encoded_len_varint(activation),
        };
        Some((index, field_len(self.len + index_len)))
    }
}

/// The live result of a read, as its ledger entry and the Records of its page.
#[derive(Debug)]
pub(crate) struct ReadDone {
    entry: Waiting<ReadResult>,
    records: Vec<StreamRecord>,
    /// The size of `records`, as the page of a read measures it.
    bytes: usize,
}

impl ReadDone {
    /// Converts the result of read `seq` of `stream` into its ledger entry. The entry of a page
    /// gets the digest of its Records.
    pub(crate) fn new(
        stream: &StreamRef,
        seq: u32,
        from_offset: Vec<u8>,
        result: Result<ReadRecordsResponse, CallFailure>,
    ) -> Self {
        let page = match result {
            Ok(page) => page,
            Err(failure) => {
                let result = read_result::Result::Failure(failure);
                return Self::of(stream, seq, from_offset, result, vec![]);
            }
        };
        let bytes = record_bytes(&page.records);
        // The runtime cuts a page to the `max_records` of its read, which is an `i32`.
        let mut entry = ReadPage {
            record_count: page.records.len() as i32,
            done: page.done,
            last_offset: page.last_offset,
            ..Default::default()
        };
        if !page.records.is_empty() {
            entry.digest = digest(&page.records);
            entry.record_bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        }
        let result = read_result::Result::Page(entry);
        Self::of(stream, seq, from_offset, result, page.records)
    }

    /// Returns the result of read `seq` of `stream` that its task ended with no answer. That's the
    /// failure of its last try if that try failed. Otherwise it's a page with no Record that ends
    /// at `from_offset`, so the next read from there skips no Record.
    pub(crate) fn cut(
        stream: &StreamRef,
        seq: u32,
        from_offset: Vec<u8>,
        failure: Option<CallFailure>,
    ) -> Self {
        let page = ReadRecordsResponse {
            last_offset: from_offset.clone(),
            ..Default::default()
        };
        Self::new(stream, seq, from_offset, failure.map_or(Ok(page), Err))
    }

    fn of(
        stream: &StreamRef,
        seq: u32,
        from_offset: Vec<u8>,
        result: read_result::Result,
        records: Vec<StreamRecord>,
    ) -> Self {
        let entry = ReadResult {
            seq: seq as i32,
            r#ref: Some(stream.clone()),
            from_offset,
            result: Some(result),
            activation_index: 0,
        };
        Self {
            entry: Waiting::new(entry),
            bytes: record_bytes(&records),
            records,
        }
    }
}

/// Returns the size of `records` as the page of a read encodes them, which is the measure of the
/// `max_bytes` of a read.
fn record_bytes(records: &[StreamRecord]) -> usize {
    (records.iter())
        .map(|record| field_len(record.encoded_len()))
        .sum()
}

/// A live run and a replay both build the job from the ledger entry, so Workflow code gets the same
/// job in both.
pub(crate) fn append_job(seq: u32, entry: &AppendResult) -> ResolveStreamAppend {
    let status = entry.result.as_ref().map(|result| match result {
        append_result::Result::Appended(offsets) => {
            resolve_stream_append::Status::Appended(StreamAppended {
                first_offset: offsets.first_offset.clone(),
                last_offset: offsets.last_offset.clone(),
            })
        }
        append_result::Result::Failure(failure) => {
            resolve_stream_append::Status::Failed(failure_of(failure))
        }
    });
    ResolveStreamAppend { seq, status }
}

pub(crate) fn close_job(seq: u32, entry: &CloseResult) -> ResolveStreamClose {
    ResolveStreamClose {
        seq,
        failed: entry.failure.as_ref().map(failure_of),
    }
}

/// Returns the job that gives Workflow code the result of read `seq` in a ledger, with the Records
/// of its page.
pub(crate) fn read_job(
    seq: u32,
    entry: &ReadResult,
    records: Vec<StreamRecord>,
) -> ResolveStreamRead {
    let status = entry.result.as_ref().map(|result| match result {
        read_result::Result::Page(page) => resolve_stream_read::Status::Page(StreamPage {
            records,
            done: page.done,
            last_offset: page.last_offset.clone(),
        }),
        read_result::Result::Failure(failure) => {
            resolve_stream_read::Status::Failed(failure_of(failure))
        }
    });
    ResolveStreamRead { seq, status }
}

/// Returns the seq of an entry in History. Workflow code numbers its calls from 1, so an entry
/// with a negative seq is malformed.
pub(crate) fn seq_of(seq: i32) -> Result<u32, WFMachinesError> {
    u32::try_from(seq).map_err(|_| malformed(format!("has the negative seq {seq}")))
}

pub(crate) fn activation_of(index: i32) -> u64 {
    u64::try_from(index).expect("The check of a ledger in History refuses a negative index")
}

/// The ledger of the current task. Its results of each kind are in the order that Core took them,
/// and its intents are in `seq` order. Each caller stops at the first entry that doesn't fit, so
/// the ledger never records an entry while an earlier one waits.
#[derive(Debug)]
pub(crate) struct LedgerBuilder {
    data: WorkflowTaskStreamCalls,
    /// The encoded size of `data`.
    bytes: usize,
    /// The bytes of the Records that the task gave Workflow code.
    records: usize,
    settings: StreamSettings,
}

impl LedgerBuilder {
    pub(crate) fn new(settings: StreamSettings) -> Self {
        Self {
            bytes: 0,
            data: WorkflowTaskStreamCalls::default(),
            records: 0,
            settings,
        }
    }

    /// Records the result of an append, given to Workflow code at `activation`. Returns false if
    /// the ledger has no room for it.
    pub(crate) fn append_result(&mut self, done: &Waiting<AppendResult>, activation: u64) -> bool {
        let Some((index, len)) = done.at(activation) else {
            return false;
        };
        if !self.fits(len) {
            return false;
        }
        self.data.append_results.push(AppendResult {
            activation_index: index,
            ..done.entry.clone()
        });
        true
    }

    /// Records the intent of close `seq` on `stream`. Returns false if the ledger has no room.
    pub(crate) fn close_intent(&mut self, stream: &StreamRef, seq: u32) -> bool {
        let entry = CloseIntent {
            seq: seq as i32,
            r#ref: Some(stream.clone()),
        };
        if !self.fits(field_len(entry.encoded_len())) {
            return false;
        }
        self.data.close_intents.push(entry);
        true
    }

    /// Records the result of a close, given at `activation`. Returns false if the ledger has no
    /// room for it.
    pub(crate) fn close_result(&mut self, done: &Waiting<CloseResult>, activation: u64) -> bool {
        let Some((index, len)) = done.at(activation) else {
            return false;
        };
        if !self.fits(len) {
            return false;
        }
        self.data.close_results.push(CloseResult {
            activation_index: index,
            ..done.entry.clone()
        });
        true
    }

    /// Records the result of read `seq`, given at `activation`, and returns its job. The caller
    /// checked that the ledger and the task have room for it.
    pub(crate) fn read_result(
        &mut self,
        seq: u32,
        done: ReadDone,
        activation: u64,
    ) -> ResolveStreamRead {
        let (index, len) = (done.entry.at(activation)).expect("The caller checked the room");
        self.bytes += len;
        self.records += done.bytes;
        let job = read_job(seq, &done.entry.entry, done.records);
        self.data.read_results.push(ReadResult {
            activation_index: index,
            ..done.entry.entry
        });
        job
    }

    /// Returns true if the ledger and the task have room for the result of a read given at
    /// `activation`, without recording it. A task with no Records yet takes a page of any size,
    /// since a read asks for at most what such a task holds, and a larger page has a single Record.
    pub(crate) fn has_room_for_read(&self, done: &ReadDone, activation: u64) -> bool {
        let max = self.settings.max_task_record_bytes();
        let full = self.records > 0 && (self.records > max || done.bytes > max - self.records);
        (done.bytes == 0 || !full) && self.has_room_for(&done.entry, activation)
    }

    /// Returns true if the ledger has room for a result given at `activation`, without recording
    /// it.
    pub(crate) fn has_room_for<T: Message>(&self, done: &Waiting<T>, activation: u64) -> bool {
        (done.at(activation)).is_some_and(|(_, len)| self.has_room(len))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.data == WorkflowTaskStreamCalls::default()
    }

    /// Returns the ledger, if it has an entry, and leaves an empty builder for the next task.
    pub(crate) fn take(&mut self) -> Option<WorkflowTaskStreamCalls> {
        if self.is_empty() {
            return None;
        }
        let builder = std::mem::replace(self, Self::new(self.settings));
        debug_assert_eq!(builder.bytes, builder.data.encoded_len());
        Some(builder.data)
    }

    /// An empty ledger accepts any entry, because each input of an entry has a bound.
    fn has_room(&self, len: usize) -> bool {
        self.is_empty() || self.bytes + len <= self.settings.max_ledger_bytes()
    }

    fn fits(&mut self, len: usize) -> bool {
        if !self.has_room(len) {
            return false;
        }
        self.bytes += len;
        true
    }
}

/// Returns true if `event` is the completion of a Workflow Task with a ledger, which can have no
/// entry.
pub(crate) fn has_ledger(event: &HistoryEvent) -> bool {
    ledger_of(event).is_some()
}

/// Returns the ledger of a `WorkflowTaskCompleted` event, and `None` for an event with no ledger.
/// A ledger with an entry that Core can't give Workflow code is an error.
pub(crate) fn peeked_ledger(
    event: &HistoryEvent,
) -> Option<Result<WorkflowTaskStreamCalls, WFMachinesError>> {
    ledger_of(event).map(|ledger| check(ledger).map(|()| ledger.clone()))
}

fn ledger_of(event: &HistoryEvent) -> Option<&WorkflowTaskStreamCalls> {
    match event.attributes.as_ref()? {
        history_event::Attributes::WorkflowTaskCompletedEventAttributes(completed) => {
            completed.sdk_metadata.as_ref()?.stream_calls.as_deref()
        }
        _ => None,
    }
}

fn check(ledger: &WorkflowTaskStreamCalls) -> Result<(), WFMachinesError> {
    if let Some(append) = (ledger.append_results.iter()).find(|append| append.result.is_none()) {
        return Err(malformed(format!("has no result of append {}", append.seq)));
    }
    if let Some(read) = (ledger.read_results.iter()).find(|read| read.result.is_none()) {
        return Err(malformed(format!("has no result of read {}", read.seq)));
    }
    let pages = (ledger.read_results.iter()).filter_map(|read| match &read.result {
        Some(read_result::Result::Page(page)) => Some((read.seq, page)),
        _ => None,
    });
    for (seq, page) in pages {
        if page.record_count < 0 || page.record_bytes < 0 {
            return Err(malformed(format!(
                "has the negative count {} or size {} of the Records of read {seq}",
                page.record_count, page.record_bytes
            )));
        }
    }
    // Core builds the failure of a call from its reason, and a later version can have a reason
    // that this one doesn't know.
    let failed_appends = (ledger.append_results.iter()).filter_map(|append| match &append.result {
        Some(append_result::Result::Failure(failure)) => Some(failure),
        _ => None,
    });
    let failed_closes = (ledger.close_results.iter()).filter_map(|close| close.failure.as_ref());
    let failed_reads = (ledger.read_results.iter()).filter_map(|read| match &read.result {
        Some(read_result::Result::Failure(failure)) => Some(failure),
        _ => None,
    });
    let mut failures = failed_appends.chain(failed_closes).chain(failed_reads);
    if let Some(failure) =
        failures.find(|failure| StreamErrorReason::try_from(failure.reason).is_err())
    {
        return Err(malformed(format!(
            "has a failure with the reason {}, which this Worker doesn't know",
            failure.reason
        )));
    }
    let appends = ledger
        .append_results
        .iter()
        .map(|append| append.activation_index);
    let closes = ledger
        .close_results
        .iter()
        .map(|close| close.activation_index);
    let reads = ledger.read_results.iter().map(|read| read.activation_index);
    let mut indexes = appends.chain(closes).chain(reads);
    if let Some(index) = indexes.find(|index| *index < 0) {
        return Err(malformed(format!(
            "has the negative activation index {index}"
        )));
    }
    Ok(())
}

fn malformed(why: String) -> WFMachinesError {
    WFMachinesError::Fatal(format!("The Stream ledger {why}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::protos::temporal::api::{
        history::v1::WorkflowTaskCompletedEventAttributes, sdk::v1::WorkflowTaskCompletedMetadata,
    };

    fn event(ledger: WorkflowTaskStreamCalls) -> HistoryEvent {
        let sdk_metadata = WorkflowTaskCompletedMetadata {
            stream_calls: Some(Box::new(ledger)),
            ..Default::default()
        };
        HistoryEvent {
            attributes: Some(
                history_event::Attributes::WorkflowTaskCompletedEventAttributes(
                    WorkflowTaskCompletedEventAttributes {
                        sdk_metadata: Some(sdk_metadata),
                        ..Default::default()
                    },
                ),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn a_replay_refuses_a_malformed_ledger() {
        let appended = || Some(append_result::Result::Appended(Default::default()));
        // A later version of Core can have a reason that this one doesn't know.
        let later = || CallFailure {
            reason: 1000,
            message: String::new(),
        };
        let unknown = "has a failure with the reason 1000, which this Worker doesn't know";
        let append = |result, activation_index| WorkflowTaskStreamCalls {
            append_results: vec![AppendResult {
                seq: 9,
                result,
                activation_index,
                ..Default::default()
            }],
            ..Default::default()
        };
        let close = |failure, activation_index| WorkflowTaskStreamCalls {
            close_results: vec![CloseResult {
                seq: 9,
                failure,
                activation_index,
            }],
            ..Default::default()
        };
        let read = |result, activation_index| WorkflowTaskStreamCalls {
            read_results: vec![ReadResult {
                seq: 9,
                result,
                activation_index,
                ..Default::default()
            }],
            ..Default::default()
        };
        let page = |record_count, record_bytes| {
            Some(read_result::Result::Page(ReadPage {
                record_count,
                record_bytes,
                ..Default::default()
            }))
        };
        let cases = [
            (append(None, 0), "has no result of append 9"),
            (
                append(appended(), -1),
                "has the negative activation index -1",
            ),
            (close(None, -2), "has the negative activation index -2"),
            (
                append(Some(append_result::Result::Failure(later())), 0),
                unknown,
            ),
            (close(Some(later()), 0), unknown),
            (read(None, 0), "has no result of read 9"),
            (read(page(0, 0), -3), "has the negative activation index -3"),
            (
                read(page(-1, 0), 0),
                "has the negative count -1 or size 0 of the Records of read 9",
            ),
            (
                read(page(1, -1), 0),
                "has the negative count 1 or size -1 of the Records of read 9",
            ),
            (
                read(Some(read_result::Result::Failure(later())), 0),
                unknown,
            ),
        ];
        for (ledger, why) in cases {
            let Some(Err(WFMachinesError::Fatal(error))) = peeked_ledger(&event(ledger)) else {
                panic!("The check took a ledger that {why}");
            };
            assert_eq!(error, format!("The Stream ledger {why}"));
        }
        let valid = append(appended(), i32::MAX);
        assert_eq!(peeked_ledger(&event(valid.clone())), Some(Ok(valid)));
    }

    #[test]
    fn a_ledger_counts_the_bytes_of_an_activation_index() {
        let done = Waiting::new(CloseResult {
            seq: 1,
            ..Default::default()
        });
        for activation in [0, 1, 127, 128, i32::MAX as u64] {
            let mut builder = LedgerBuilder::new(StreamSettings::default());
            assert!(builder.close_result(&done, activation));
            // `take` asserts that the counted bytes are the encoded size.
            let ledger = builder.take().unwrap();
            assert_eq!(ledger.close_results[0].activation_index as u64, activation);
        }
        // History keeps the index as an `int32`, so a higher one has no room.
        let mut builder = LedgerBuilder::new(StreamSettings::default());
        assert!(!builder.close_result(&done, i32::MAX as u64 + 1));
    }
}
