use super::{digest::digest, manager::failure_of};
use prost::Message;
use temporalio_common::protos::coresdk::{
    external_data::{StreamFailure, StreamReadPage, StreamReadResult, stream_read_result},
    workflow_activation::{ResolveStreamRead, StreamPage, resolve_stream_read},
};
use temporalio_streams::{
    ReadPage,
    proto::{StreamRecord, sdk::StreamRef},
    record::field_len,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum After {
    /// The Store returned Records after the page.
    More,
    /// The Store answered, and Core doesn't know if more Records follow.
    Unknown,
    /// Core made the result at the read limit, so it says nothing new.
    AsBefore,
}

#[derive(Debug)]
pub(crate) struct ReadDone {
    /// The marker entry, with an activation index of 0 until an activation takes it.
    pub(crate) entry: StreamReadResult,
    pub(crate) records: Vec<StreamRecord>,
    /// The size of `records`, as the page of a read measures it.
    pub(crate) bytes: usize,
    pub(crate) after: After,
}

impl ReadDone {
    pub(crate) fn new(
        stream: &StreamRef,
        seq: u32,
        from_offset: Vec<u8>,
        answer: Result<ReadPage, StreamFailure>,
    ) -> Self {
        match answer {
            Ok(ReadPage { response, more }) => {
                let after = if more { After::More } else { After::Unknown };
                Self::page(
                    stream,
                    seq,
                    from_offset,
                    response.records,
                    response.last_offset,
                    after,
                )
            }
            Err(failure) => {
                let result = stream_read_result::Result::Failure(failure);
                Self::of(stream, seq, from_offset, result, vec![], After::Unknown)
            }
        }
    }

    // Keep the cursor unchanged so the next read skips no Records.
    pub(crate) fn cut(stream: &StreamRef, seq: u32, from_offset: Vec<u8>) -> Self {
        let last_offset = from_offset.clone();
        let mut done = Self::page(
            stream,
            seq,
            from_offset,
            vec![],
            last_offset,
            After::AsBefore,
        );
        done.set_wait_cut();
        done
    }

    pub(crate) fn set_wait_cut(&mut self) {
        if let Some(stream_read_result::Result::Page(page)) = &mut self.entry.result {
            page.wait_cut = true;
        }
    }

    pub(crate) fn is_empty_page(&self) -> bool {
        matches!(&self.entry.result,
            Some(stream_read_result::Result::Page(page)) if page.record_count == 0)
    }

    pub(crate) fn failure(&self) -> Option<&StreamFailure> {
        match &self.entry.result {
            Some(stream_read_result::Result::Failure(failure)) => Some(failure),
            _ => None,
        }
    }

    fn page(
        stream: &StreamRef,
        seq: u32,
        from_offset: Vec<u8>,
        records: Vec<StreamRecord>,
        last_offset: Vec<u8>,
        after: After,
    ) -> Self {
        let mut page = StreamReadPage {
            // The runtime cuts a page to the `max_records` of its read, which is an `i32`.
            record_count: records.len() as u32,
            last_offset,
            ..Default::default()
        };
        if !records.is_empty() {
            page.digest = digest(&records);
            page.record_bytes = record_bytes(&records) as u64;
        }
        let result = stream_read_result::Result::Page(page);
        Self::of(stream, seq, from_offset, result, records, after)
    }

    fn of(
        stream: &StreamRef,
        seq: u32,
        from_offset: Vec<u8>,
        result: stream_read_result::Result,
        records: Vec<StreamRecord>,
        after: After,
    ) -> Self {
        let entry = StreamReadResult {
            seq,
            stream_id: stream.id.clone(),
            store: stream.store.clone(),
            activation_index: 0,
            from_offset,
            result: Some(result),
        };
        Self {
            entry,
            bytes: record_bytes(&records),
            records,
            after,
        }
    }
}

// Include each field's framing bytes to match the Store page budget.
pub(crate) fn record_bytes(records: &[StreamRecord]) -> usize {
    (records.iter())
        .map(|record| field_len(record.encoded_len()))
        .sum()
}

// Live execution and replay must give Workflow code the same result fields.
pub(crate) fn read_job(entry: &StreamReadResult, records: Vec<StreamRecord>) -> ResolveStreamRead {
    let status = entry.result.as_ref().map(|result| match result {
        stream_read_result::Result::Page(page) => resolve_stream_read::Status::Page(StreamPage {
            records,
            last_offset: page.last_offset.clone(),
            wait_cut: page.wait_cut,
        }),
        stream_read_result::Result::Failure(failure) => {
            resolve_stream_read::Status::Failed(failure_of(failure))
        }
    });
    ResolveStreamRead {
        seq: entry.seq,
        status,
    }
}
