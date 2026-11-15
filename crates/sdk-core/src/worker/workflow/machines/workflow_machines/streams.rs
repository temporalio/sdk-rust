//! The Stream calls of a run, and the marker that records what Workflow code got in one Workflow
//! Task. A call has no command or event of its own, so it needs no state machine.

use crate::worker::workflow::{
    OutgoingJob, WFMachinesError, fatal,
    streams::{
        AppendSpec, DEFAULT_IDLE_TIMEOUT, ReadFlow, ReadSpec, StreamCallResult, StreamRequest,
        StreamResolution, next_call_id,
    },
};
use prost::Message;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, SystemTime},
};
use temporalio_common::protos::{
    coresdk::{
        external_data::{StreamAppendEntry, StreamMarkerData, StreamReadEnded, StreamReadEntry},
        workflow_activation::{
            ResolveStreamAppend, ResolveStreamRead, StreamReadEnd, resolve_stream_append,
            workflow_activation_job,
        },
        workflow_commands::{AppendToStream, StartStreamRead},
    },
    temporal::api::stream::v1::{StreamRecord, StreamRef, producer::Kind},
};
use temporalio_streams::digest::records_digest;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

/// What a result does to the activations of the run.
pub(super) enum StreamJob {
    /// Records of a read, and its end if it ended. They join the read's pending job, if it has one.
    Read(ResolveStreamRead),
    Append(ResolveStreamAppend),
}

struct Read {
    call_id: u64,
    stream: StreamRef,
    from_offset: Vec<u8>,
    records_in_history: bool,
    flow: Arc<ReadFlow>,
    /// The offset of the last Record that Workflow code got, or `from_offset`.
    last_offset: Vec<u8>,
    /// False once the read ended or was closed.
    open: bool,
}

struct Append {
    call_id: u64,
    stream: StreamRef,
    /// Taken when the append is dispatched.
    spec: Option<AppendSpec>,
    resolved: bool,
}

/// What one read gave Workflow code in the current Workflow Task.
struct ReadEntryBuilder {
    seq: u32,
    after_offset: Vec<u8>,
    records: Vec<StreamRecord>,
    page_counts: Vec<u32>,
    /// The activation of each page, by its index in the task.
    page_activations: Vec<u32>,
    end: Option<StreamReadEnded>,
}

#[derive(Default)]
pub(super) struct StreamCalls {
    reads: HashMap<u32, Read>,
    appends: HashMap<u32, Append>,
    /// The appends of each Stream that wait for the one before them, in call order. The first one
    /// is the one that runs.
    append_queues: HashMap<(String, String), VecDeque<u32>>,
    requests: Vec<StreamRequest>,
    marker_reads: Vec<ReadEntryBuilder>,
    marker_appends: Vec<StreamAppendEntry>,
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
            stream: stream.clone(),
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
                stream,
                last_offset: attrs.from_offset.clone(),
                from_offset: attrs.from_offset,
                records_in_history: attrs.records_in_history,
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

    pub(super) fn append(&mut self, attrs: AppendToStream) -> Result<()> {
        let seq = attrs.seq;
        let stream = attrs
            .stream
            .ok_or_else(|| fatal!("Stream append {seq} has no Stream"))?;
        let producer = attrs
            .producer
            .ok_or_else(|| fatal!("Stream append {seq} has no Producer"))?;
        if !matches!(producer.kind, Some(Kind::Workflow(_))) {
            return Err(fatal!(
                "Stream append {seq} has a Producer that is not of kind Workflow"
            ));
        }
        if self.appends.contains_key(&seq) {
            return Err(fatal!("Stream append {seq} was issued twice"));
        }
        let call_id = next_call_id();
        let spec = AppendSpec {
            call_id,
            seq,
            stream: stream.clone(),
            producer,
            // The caller's own sequence goes to the Store as given. With none, the append's seq,
            // which grows in a run, lets the Store refuse a resend in one epoch.
            sequence: if attrs.sequence != 0 {
                attrs.sequence
            } else {
                i64::from(seq)
            },
            bodies: attrs.bodies,
        };
        let queue = self
            .append_queues
            .entry((stream.store.clone(), stream.id.clone()))
            .or_default();
        queue.push_back(seq);
        let runs_now = queue.len() == 1;
        self.appends.insert(
            seq,
            Append {
                call_id,
                stream,
                spec: Some(spec),
                resolved: false,
            },
        );
        if runs_now {
            self.dispatch_append(seq);
        }
        Ok(())
    }

    fn dispatch_append(&mut self, seq: u32) {
        if let Some(spec) = self.appends.get_mut(&seq).and_then(|a| a.spec.take()) {
            self.requests.push(StreamRequest::Append(spec));
        }
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
            StreamCallResult::Append(status) => {
                let append = self.appends.get_mut(&seq)?;
                if append.call_id != call_id || append.resolved {
                    return None;
                }
                append.resolved = true;
                let key = (append.stream.store.clone(), append.stream.id.clone());
                if let Some(queue) = self.append_queues.get_mut(&key) {
                    queue.retain(|queued| *queued != seq);
                    if let Some(next) = queue.front().copied() {
                        self.dispatch_append(next);
                    }
                }
                Some(StreamJob::Append(ResolveStreamAppend {
                    seq,
                    status: Some(status),
                }))
            }
        }
    }

    /// Stops every call. A terminal command does it, as does a failed Workflow Task, whose
    /// results must not reach Workflow code.
    pub(super) fn stop_all(&mut self) {
        for read in self.reads.values_mut().filter(|read| read.open) {
            read.open = false;
            self.requests.push(StreamRequest::Cancel(read.call_id));
        }
        for append in self.appends.values_mut().filter(|a| !a.resolved) {
            append.resolved = true;
            append.spec = None;
            self.requests.push(StreamRequest::Cancel(append.call_id));
        }
        self.append_queues.clear();
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

    pub(super) fn open_appends(&self) -> usize {
        self.appends.values().filter(|a| !a.resolved).count()
    }

    /// Notes the Stream jobs of an activation, which Workflow code gets at `activation` in the
    /// Workflow Task. The marker of the task records them.
    pub(super) fn note_delivered(&mut self, jobs: &[OutgoingJob], activation: u32) {
        for job in jobs {
            match &job.variant {
                workflow_activation_job::Variant::ResolveStreamRead(page) => {
                    self.note_page(page, activation);
                }
                workflow_activation_job::Variant::ResolveStreamAppend(result) => {
                    let Some(append) = self.appends.get(&result.seq) else {
                        continue;
                    };
                    let mut entry = StreamAppendEntry {
                        seq: result.seq,
                        stream: Some(append.stream.clone()),
                        activation,
                        ..Default::default()
                    };
                    match &result.status {
                        Some(resolve_stream_append::Status::Appended(appended)) => {
                            entry.first_offset.clone_from(&appended.first_offset);
                            entry.last_offset.clone_from(&appended.last_offset);
                        }
                        Some(resolve_stream_append::Status::Failed(failure)) => {
                            entry.failure = Some(failure.clone());
                        }
                        None => {}
                    }
                    self.marker_appends.push(entry);
                }
                _ => {}
            }
        }
    }

    fn note_page(&mut self, page: &ResolveStreamRead, activation: u32) {
        let Some(read) = self.reads.get_mut(&page.seq) else {
            return;
        };
        let at = match self.marker_reads.iter().position(|e| e.seq == page.seq) {
            Some(at) => at,
            None => {
                self.marker_reads.push(ReadEntryBuilder {
                    seq: page.seq,
                    after_offset: read.last_offset.clone(),
                    records: vec![],
                    page_counts: vec![],
                    page_activations: vec![],
                    end: None,
                });
                self.marker_reads.len() - 1
            }
        };
        let entry = &mut self.marker_reads[at];
        if let Some(last) = page.records.last() {
            entry.page_counts.push(page.records.len() as u32);
            entry.page_activations.push(activation);
            entry.records.extend(page.records.iter().cloned());
            read.last_offset.clone_from(&last.offset);
            let bytes = page.records.iter().map(Message::encoded_len).sum();
            read.flow.delivered(bytes);
        }
        if let Some(StreamReadEnd {
            last_offset,
            stop,
            done,
            failure,
        }) = &page.end
        {
            entry.end = Some(StreamReadEnded {
                last_offset: last_offset.clone(),
                stop: *stop,
                done: *done,
                failure: failure.clone(),
                activation,
            });
        }
    }

    /// True if Workflow code got a Stream result in this Workflow Task.
    pub(super) fn has_marker_entries(&self) -> bool {
        !self.marker_reads.is_empty() || !self.marker_appends.is_empty()
    }

    /// The marker of this Workflow Task, if Workflow code got a Stream result in it. It clears the
    /// entries, since the next task records its own.
    pub(super) fn take_marker(&mut self) -> Option<StreamMarkerData> {
        if !self.has_marker_entries() {
            return None;
        }
        let reads = std::mem::take(&mut self.marker_reads)
            .into_iter()
            .filter_map(|entry| {
                let read = self.reads.get(&entry.seq)?;
                Some(read_entry(read, entry))
            })
            .collect();
        Some(StreamMarkerData {
            reads,
            appends: std::mem::take(&mut self.marker_appends),
        })
    }
}

fn read_entry(read: &Read, entry: ReadEntryBuilder) -> StreamReadEntry {
    let count = entry.records.len() as u32;
    let last_offset = entry
        .records
        .last()
        .map(|record| record.offset.clone())
        .unwrap_or_default();
    let (digest, records) = if read.records_in_history {
        (vec![], entry.records)
    } else if count > 0 {
        (records_digest(&entry.records).to_vec(), vec![])
    } else {
        (vec![], vec![])
    };
    let mut before = 0;
    let page_activations = entry
        .page_activations
        .into_iter()
        .enumerate()
        .map(|(at, activation)| {
            let coded = if at == 0 {
                activation
            } else {
                activation - before
            };
            before = activation;
            coded
        })
        .collect();
    StreamReadEntry {
        seq: entry.seq,
        stream: Some(read.stream.clone()),
        from_offset: read.from_offset.clone(),
        after_offset: entry.after_offset,
        last_offset,
        count,
        digest,
        records,
        page_counts: entry.page_counts,
        page_activations,
        end: entry.end,
    }
}
