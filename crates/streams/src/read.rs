//! The read of the runtime, a long poll that pages through the log.

use crate::{
    Reason, StreamError, StreamResult, StreamRuntime,
    offset::{offset_from_wire, offset_to_wire},
    proto::{ReadRecordsRequest, ReadRecordsResponse, StreamRecord, StreamRef},
    runtime::{duration, stream_id},
    store::{FORMAT_VERSION, StreamState},
};
use prost::Message;
use std::time::Duration;
use tokio::time::Instant;

/// The longest wait of one read for new Records.
pub const MAX_WAIT: Duration = Duration::from_secs(60);
/// The maximum number of Records in one read.
const MAX_RECORDS: usize = 1000;

impl StreamRuntime {
    /// A long poll that returns the Records it found, or nothing after `wait`. On a closed Stream
    /// with no Record left, it returns `done` at once.
    pub async fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> StreamResult<ReadRecordsResponse> {
        stream_id(request.r#ref.as_ref())?;
        let stream = request.r#ref.clone().unwrap_or_default();
        // The wire from_offset is exclusive, so a read starts after it. The empty value is a read
        // from the start.
        let from_offset = offset_from_wire(&request.from_offset)?;
        if from_offset < 0 {
            return Err(StreamError::invalid("The offset is below zero"));
        }
        let max_bytes = max_bytes(request.max_bytes)?;
        let max = match request.max_records {
            n if n <= 0 => MAX_RECORDS,
            n => (n as usize).min(MAX_RECORDS),
        };
        let started = Instant::now();
        let wait = duration(request.wait).unwrap_or_default().min(MAX_WAIT);
        let deadline = started + wait;

        // A creator hands out the ref only after its create, so a read never waits for one.
        let (mut state, _) = self.store.read(&stream, 0, 0).await?;
        if !state.exists {
            return Err(StreamError::new(
                Reason::NotFound,
                "Nobody created the Stream",
            ));
        }
        if state.version > FORMAT_VERSION {
            return Err(StreamError::new(
                Reason::FormatTooNew,
                format!("format {} is newer than {FORMAT_VERSION}", state.version),
            ));
        }
        // Before the start is one past the offset, since the largest offset has no next one.
        if from_offset > state.newest {
            return Err(StreamError::new(
                Reason::OffsetTooHigh,
                "The offset is past the newest Record. A failover may have lost the newest \
                 Records. Resume from the newest offset, or read again once newer Records pass it",
            ));
        }
        // An exclusive from_offset reads after that Record, so the inclusive start is one past it.
        let from = match from_offset {
            0 => 0,
            offset => offset + 1,
        };
        loop {
            check_expiry(&state, from)?;
            if let Some(page) = self.page(&stream, &state, from, (max, max_bytes)).await? {
                return Ok(page);
            }
            if deadline <= Instant::now() {
                // An empty read resumes from where it started, which is one before the inclusive
                // start.
                return Ok(ReadRecordsResponse {
                    last_offset: offset_to_wire(from - 1),
                    ..Default::default()
                });
            }
            let after = (from - 1).max(state.newest);
            self.metrics.read_waits(true);
            let woke = self.store.wait_after(&stream, after, deadline).await;
            self.metrics.read_waits(false);
            woke?;
            state = self.store.read(&stream, 0, 0).await?.0;
        }
    }

    /// The Records from `from`, or `None` when there is none yet and the Stream is open. A page
    /// of a closed Stream that gives its last Record is done, also with no Record. The pair
    /// `(max, max_bytes)` caps the Records and their bytes.
    async fn page(
        &self,
        stream: &StreamRef,
        state: &StreamState,
        from: i64,
        (max, max_bytes): (usize, usize),
    ) -> StreamResult<Option<ReadRecordsResponse>> {
        // A read from the start skips the Records that the log lost. A read from an offset fails on
        // them, since the Reader asked for those Records.
        let retained = state.retained_from();
        let mut start = if from > 0 { from } else { retained };
        let mut records = vec![];
        let mut full = false;
        let mut bytes = 0;
        // The bytes and the entries that the page read from the Store.
        let mut pulled = (0, 0);
        // Whether the Stream was closed, and its newest offset, at the newest Store read.
        let mut end = (state.closed, state.newest);
        while records.len() < max && !full {
            let room = (max_bytes != usize::MAX).then(|| max_bytes.saturating_sub(bytes));
            let chunk = chunk_len(max - records.len(), room, pulled);
            let (chunk_state, entries) = self.store.read(stream, start, chunk).await?;
            // Records that the log lost after the state read leave a gap before these entries.
            check_expiry(&chunk_state, if from > 0 { start } else { 0 })?;
            end = (chunk_state.closed, chunk_state.newest);
            if entries.is_empty() {
                // Only an expired log has no entry at an offset up to `newest`.
                if from > 0 && start == from && from <= chunk_state.newest {
                    return Err(expired());
                }
                break;
            }
            for entry in entries {
                if entry.offset < retained {
                    if from > 0 {
                        return Err(expired());
                    }
                    start = entry.offset + 1;
                    continue;
                }
                let Some(encoded) = entry.record else {
                    start = entry.offset + 1;
                    continue;
                };
                let record = decode_record(&encoded, entry.offset)?;
                // An entry over the caps is still returned alone, so that the read always makes
                // progress. The next read starts at the entry that didn't fit.
                let size = record.encoded_len();
                pulled = (pulled.0 + size, pulled.1 + 1);
                let over = bytes + size > max_bytes || records.len() + 1 > max;
                if !records.is_empty() && over {
                    full = true;
                    break;
                }
                bytes += size;
                records.push(record);
                start = entry.offset + 1;
                if records.len() >= max {
                    break;
                }
            }
        }
        // A closed Stream takes no more appends, so no Record is left past its newest offset.
        let done = end.0 && start > end.1;
        if records.is_empty() && !done {
            return Ok(None);
        }
        // The resume is after the last Record returned, which is one before the inclusive start.
        let page = ReadRecordsResponse {
            records,
            last_offset: offset_to_wire(start - 1),
            done,
        };
        Ok(Some(page))
    }
}

/// The number of entries of the next Store read, at most `left`. With a byte cap, the first read
/// takes one entry, and each later one takes what fits the `room` left at the average size so far.
fn chunk_len(left: usize, room: Option<usize>, (bytes, entries): (usize, usize)) -> usize {
    let Some(room) = room else {
        return left;
    };
    if entries == 0 {
        return 1;
    }
    let average = (bytes / entries).max(1);
    (room / average + 1).clamp(1, left.max(1))
}

fn decode_record(bytes: &[u8], offset: i64) -> StreamResult<StreamRecord> {
    let mut record = StreamRecord::decode(bytes).map_err(|error| {
        StreamError::storage_corrupt(format!("A stored Record is bad: {error}"))
    })?;
    record.offset = offset_to_wire(offset);
    Ok(record)
}

/// Fails a read whose Records the log lost or the expiry removed, so that the Reader always sees
/// the gap. A read from the start of the Stream has no gap.
///
/// A Tombstone fails every read. An open Stream that expired gets no more Records, so the Reader
/// would wait forever. A closed one lost its log.
fn check_expiry(state: &StreamState, from: i64) -> StreamResult<()> {
    if state.expired() || (from > 0 && state.trimmed >= from) {
        return Err(expired());
    }
    Ok(())
}

/// The byte cap of a page, from a request's `max_bytes`. Zero is no cap.
fn max_bytes(value: i64) -> StreamResult<usize> {
    match value {
        0 => Ok(usize::MAX),
        n if n < 0 => Err(StreamError::invalid("max_bytes must be 0 or more")),
        n => Ok(usize::try_from(n).unwrap_or(usize::MAX)),
    }
}

fn expired() -> StreamError {
    StreamError::new(Reason::Expired, "The Records at the offset expired")
}
