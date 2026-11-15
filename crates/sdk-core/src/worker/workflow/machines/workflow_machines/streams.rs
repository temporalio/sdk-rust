//! The Stream calls of a run. A call has no command or event of its own, so it needs no state
//! machine.

use crate::worker::workflow::{
    OutgoingJob, WFMachinesError, fatal,
    streams::{
        DEFAULT_IDLE_TIMEOUT, ReadFlow, ReadSpec, StreamCallResult, StreamRequest,
        StreamResolution, next_call_id,
    },
};
use prost::Message;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use temporalio_common::protos::coresdk::{
    workflow_activation::{ResolveStreamRead, workflow_activation_job},
    workflow_commands::StartStreamRead,
};

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

/// What a result does to the activations of the run.
pub(super) enum StreamJob {
    /// Records of a read, and its end if it ended. They join the read's pending job, if it has one.
    Read(ResolveStreamRead),
}

struct Read {
    call_id: u64,
    flow: Arc<ReadFlow>,
    /// The offset of the last Record that Workflow code got, or `from_offset`.
    last_offset: Vec<u8>,
    /// False once the read ended or was closed.
    open: bool,
}

#[derive(Default)]
pub(super) struct StreamCalls {
    reads: HashMap<u32, Read>,
    requests: Vec<StreamRequest>,
}

impl StreamCalls {
    pub(super) fn start_read(
        &mut self,
        attrs: StartStreamRead,
        workflow_time: Option<SystemTime>,
    ) -> Result<()> {
        let stream = attrs
            .stream
            .ok_or_else(|| fatal!("Stream read {} has no Stream", attrs.seq))?;
        if self.reads.contains_key(&attrs.seq) {
            return Err(fatal!("Stream read {} was started twice", attrs.seq));
        }
        let call_id = next_call_id();
        let flow = Arc::new(ReadFlow::default());
        let duration = |value: Option<prost_types::Duration>| {
            value
                .and_then(|value| Duration::try_from(value).ok())
                .filter(|value| !value.is_zero())
        };
        // The bound counts from the Workflow time of the start, so no restart can stretch it.
        let deadline = duration(attrs.timeout)
            .map(|timeout| workflow_time.unwrap_or_else(SystemTime::now) + timeout);
        self.requests.push(StreamRequest::Read(ReadSpec {
            call_id,
            seq: attrs.seq,
            stream,
            from_offset: attrs.from_offset.clone(),
            deadline,
            idle_timeout: duration(attrs.idle_timeout).unwrap_or(DEFAULT_IDLE_TIMEOUT),
            max_records: attrs.max_records,
            max_bytes: attrs.max_bytes,
            took_a_record: false,
            flow: flow.clone(),
        }));
        self.reads.insert(
            attrs.seq,
            Read {
                call_id,
                last_offset: attrs.from_offset,
                flow,
                open: true,
            },
        );
        Ok(())
    }

    /// Stops a read. Returns true if it was still open, so that its pending Records are dropped.
    pub(super) fn close_read(&mut self, seq: u32) -> Result<bool> {
        let read = self
            .reads
            .get_mut(&seq)
            .ok_or_else(|| fatal!("Workflow code closed Stream read {seq}, which never started"))?;
        if !read.open {
            return Ok(false);
        }
        read.open = false;
        self.requests.push(StreamRequest::Cancel(read.call_id));
        Ok(true)
    }

    /// Applies a result. Returns the job it makes, or `None` when its call is gone, stopped or
    /// from an older dispatch.
    pub(super) fn resolve(&mut self, resolution: StreamResolution) -> Option<StreamJob> {
        let StreamResolution {
            call_id,
            seq,
            result,
        } = resolution;
        match result {
            StreamCallResult::Page { records, end } => {
                let read = self.reads.get_mut(&seq)?;
                if read.call_id != call_id || !read.open {
                    return None;
                }
                if end.is_some() {
                    read.open = false;
                }
                Some(StreamJob::Read(ResolveStreamRead { seq, records, end }))
            }
        }
    }

    /// Stops every read. A terminal command does it, as does a failed Workflow Task, whose
    /// results must not reach Workflow code.
    pub(super) fn stop_all(&mut self) {
        for read in self.reads.values_mut().filter(|read| read.open) {
            read.open = false;
            self.requests.push(StreamRequest::Cancel(read.call_id));
        }
    }

    pub(super) fn drain_requests(&mut self) -> Vec<StreamRequest> {
        std::mem::take(&mut self.requests)
    }

    pub(super) fn has_requests(&self) -> bool {
        !self.requests.is_empty()
    }

    pub(super) fn open_reads(&self) -> usize {
        self.reads.values().filter(|read| read.open).count()
    }

    /// Notes the Stream jobs of an activation, which Workflow code gets now.
    pub(super) fn note_delivered(&mut self, jobs: &[OutgoingJob]) {
        for job in jobs {
            if let workflow_activation_job::Variant::ResolveStreamRead(page) = &job.variant
                && let Some(read) = self.reads.get_mut(&page.seq)
                && let Some(last) = page.records.last()
            {
                read.last_offset.clone_from(&last.offset);
                let bytes = page.records.iter().map(Message::encoded_len).sum();
                read.flow.delivered(bytes);
            }
        }
    }
}
