//! The model Store of the unit tests of the Stream reads.

use super::manager::MIN_ASK_AGAIN;
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use prost::Message;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::sync::atomic::{AtomicUsize, Ordering};
use temporalio_streams::{
    AppendError, CreateError, DescribeError, ReadError, StreamStore,
    proto::{
        AppendRecordsRequest, AppendRecordsResponse, ClientProducer, CreateStreamRequest,
        CreateStreamResponse, DescribeStreamRequest, DescribeStreamResponse, Producer,
        ReadRecordsRequest, ReadRecordsResponse, StreamRecord, producer::Kind,
    },
    record::field_len,
};

/// One entry of a model Stream, which is a Record or an entry that a read passes but that isn't
/// one.
#[derive(Clone, Debug)]
pub(crate) struct Entry {
    pub(crate) offset: Vec<u8>,
    /// When the entry comes, after the Store starts.
    pub(crate) at: std::time::Duration,
    pub(crate) record: Option<StreamRecord>,
}

/// What a model Store does with the next read.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Fault {
    /// The read fails with this error.
    Fails(ReadError),
    /// The Store gives no answer.
    Silent,
}

/// A Store with one Stream whose reads get pages of any size that the read allows, entries that
/// aren't Records, and faults at random. A read with no entry to give waits for one, up to its
/// `wait`, unless the Store ignores the wait.
pub(crate) struct ModelStore {
    pub(crate) model: Mutex<Model>,
    start: tokio::time::Instant,
    /// The bytes of the Records of all the answers, each as its encoded size.
    pub(crate) answered: AtomicUsize,
}

pub(crate) struct Model {
    pub(crate) rng: StdRng,
    pub(crate) entries: Vec<Entry>,
    /// Returns the fault of each read, if it has one. The test checks each request in it too.
    #[allow(clippy::type_complexity)]
    pub(crate) fault: Box<dyn FnMut(&mut StdRng, &ReadRecordsRequest) -> Option<Fault> + Send>,
    /// Each read answers immediately with what came, as a Store that can't wait does.
    pub(crate) ignores_wait: bool,
    /// The reads that run now, and the maximum that ran at the same time.
    pub(crate) reading: (usize, usize),
    /// The offset that an answer with no Records moved to, where the next read must start.
    pub(crate) moved_to: Option<Vec<u8>>,
    /// The offset of an answer with nothing after it, and the time before which no read from that
    /// offset may come. That's `MIN_ASK_AGAIN` after the ask, or the end of its wait if sooner.
    pub(crate) empty_until: Option<(Vec<u8>, tokio::time::Instant)>,
    /// The answers with nothing that came before the read's wait was up.
    pub(crate) answered_early: usize,
    /// The `last_offset` of the last answer.
    pub(crate) last_offset: Vec<u8>,
    pub(crate) violations: Vec<String>,
}

impl Model {
    /// Returns the model of a Stream with `entries`, whose reads get the faults of `fault`. Its
    /// Store waits as each read asks.
    pub(crate) fn new(
        seed: u64,
        entries: Vec<Entry>,
        fault: impl FnMut(&mut StdRng, &ReadRecordsRequest) -> Option<Fault> + Send + 'static,
    ) -> Self {
        Self {
            rng: StdRng::seed_from_u64(seed),
            entries,
            fault: Box::new(fault),
            ignores_wait: false,
            reading: (0, 0),
            moved_to: None,
            empty_until: None,
            answered_early: 0,
            last_offset: vec![],
            violations: vec![],
        }
    }
}

impl ModelStore {
    pub(crate) fn new(model: Model) -> Self {
        Self {
            model: Mutex::new(model),
            start: tokio::time::Instant::now(),
            answered: AtomicUsize::new(0),
        }
    }

    /// Returns the Records of the Stream after `offset`.
    pub(crate) fn records_after(&self, offset: &[u8]) -> Vec<StreamRecord> {
        self.came_after(offset, None)
    }

    /// Returns the Records of the Stream after `offset` that came by now.
    pub(crate) fn records_came_after(&self, offset: &[u8]) -> Vec<StreamRecord> {
        self.came_after(offset, Some(tokio::time::Instant::now()))
    }

    fn came_after(&self, offset: &[u8], by: Option<tokio::time::Instant>) -> Vec<StreamRecord> {
        let model = self.model.lock();
        (model.entries.iter())
            .filter(|entry| entry.offset.as_slice() > offset)
            .filter(|entry| by.is_none_or(|by| self.start + entry.at <= by))
            .filter_map(|entry| entry.record.clone())
            .collect()
    }

    async fn read(
        &self,
        request: ReadRecordsRequest,
        reading: &mut Reading<'_>,
    ) -> Result<ReadRecordsResponse, ReadError> {
        let asked = tokio::time::Instant::now();
        let (fault, ignores_wait) = {
            let mut model = self.model.lock();
            let Model {
                rng,
                fault,
                ignores_wait,
                moved_to,
                empty_until,
                violations,
                ..
            } = &mut *model;
            if moved_to
                .as_ref()
                .is_some_and(|to| *to != request.from_offset)
            {
                violations.push("A read didn't start where an empty answer moved to".to_string());
            }
            if (empty_until.as_ref())
                .is_some_and(|(offset, until)| *offset == request.from_offset && asked < *until)
            {
                violations.push("A read came too soon after an empty answer".to_string());
            }
            (fault(rng, &request), *ignores_wait)
        };
        match fault {
            Some(Fault::Fails(error)) => return Err(error),
            Some(Fault::Silent) => {
                reading.waits = false;
                std::future::pending().await
            }
            None => {}
        }
        let wait = request.wait.map_or(std::time::Duration::ZERO, |wait| {
            std::time::Duration::try_from(wait).expect("The runtime checks the wait")
        });
        let until = asked
            + if ignores_wait {
                std::time::Duration::ZERO
            } else {
                wait
            };
        let after = |entry: &&Entry| entry.offset.as_slice() > request.from_offset.as_slice();
        // The read waits until an entry after its offset comes, or its wait ends.
        loop {
            let next = (self.model.lock().entries.iter().find(after)).map(|entry| entry.at);
            match next.map(|at| self.start + at) {
                Some(at) if at <= tokio::time::Instant::now() => break,
                Some(at) if at < until => tokio::time::sleep_until(at).await,
                _ => {
                    tokio::time::sleep_until(until).await;
                    break;
                }
            }
        }
        let mut model = self.model.lock();
        let Model {
            rng,
            entries,
            moved_to,
            empty_until,
            answered_early,
            last_offset: last_answer,
            ..
        } = &mut *model;
        let now = tokio::time::Instant::now();
        let later: Vec<&Entry> = entries.iter().filter(after).collect();
        let came = (later.iter())
            .filter(|entry| self.start + entry.at <= now)
            .count();
        // A page of any number of the entries that came, with at most `max_records` Records.
        let mut take = if came == 0 {
            0
        } else {
            rng.random_range(1..=came)
        };
        let max_records = request.max_records.max(1) as usize;
        if let Some((at, _)) = (later.iter().enumerate())
            .filter(|(_, entry)| entry.record.is_some())
            .nth(max_records)
        {
            take = take.min(at);
        }
        // The page has at most `max_bytes` of Records as the runtime counts them, but always its
        // first Record, so the runtime gives it to the caller as it is.
        let (mut bytes, mut kept) = (0, 0);
        for (at, entry) in later[..take].iter().enumerate() {
            let Some(record) = &entry.record else {
                continue;
            };
            bytes += field_len(record.encoded_len());
            if kept > 0 && request.max_bytes > 0 && bytes > request.max_bytes as usize {
                take = at;
                break;
            }
            kept += 1;
        }
        let page = &later[..take];
        let records: Vec<StreamRecord> = (page.iter())
            .filter_map(|entry| entry.record.clone())
            .collect();
        let given: usize = (records.iter())
            .map(|record| field_len(record.encoded_len()))
            .sum();
        self.answered.fetch_add(given, Ordering::Relaxed);
        let last_offset = page
            .last()
            .map_or(request.from_offset, |e| e.offset.clone());
        *moved_to = records.is_empty().then(|| last_offset.clone());
        *empty_until = (take == 0).then(|| (last_offset.clone(), asked + wait.min(MIN_ASK_AGAIN)));
        *answered_early += usize::from(empty_until.is_some() && now < asked + wait);
        last_answer.clone_from(&last_offset);
        Ok(ReadRecordsResponse {
            records,
            last_offset,
        })
    }
}

/// Counts a read of a model Store while it runs. A caller that stops waiting for an answer that
/// comes within the read's wait is a violation.
struct Reading<'a> {
    model: &'a Mutex<Model>,
    waits: bool,
    answered: bool,
}

impl Drop for Reading<'_> {
    fn drop(&mut self) {
        let mut model = self.model.lock();
        model.reading.0 -= 1;
        if self.waits && !self.answered {
            let violation = "A caller stopped waiting for an answer within its wait".to_string();
            model.violations.push(violation);
        }
    }
}

impl StreamStore for ModelStore {
    fn create_stream(
        &self,
        _: CreateStreamRequest,
        _: String,
    ) -> BoxFuture<'_, Result<CreateStreamResponse, CreateError>> {
        unimplemented!("A model Store only reads")
    }

    fn append_records(
        &self,
        _: AppendRecordsRequest,
        _: String,
    ) -> BoxFuture<'_, Result<AppendRecordsResponse, AppendError>> {
        unimplemented!("A model Store only reads")
    }

    fn read_records(
        &self,
        request: ReadRecordsRequest,
    ) -> BoxFuture<'_, Result<ReadRecordsResponse, ReadError>> {
        Box::pin(async move {
            {
                let mut model = self.model.lock();
                model.reading.0 += 1;
                model.reading.1 = model.reading.0.max(model.reading.1);
            }
            let mut reading = Reading {
                model: &self.model,
                waits: true,
                answered: false,
            };
            let answer = self.read(request, &mut reading).await;
            reading.answered = true;
            answer
        })
    }

    fn describe_stream(
        &self,
        _: DescribeStreamRequest,
    ) -> BoxFuture<'_, Result<DescribeStreamResponse, DescribeError>> {
        unimplemented!("A model Store only reads")
    }
}

pub(crate) fn client() -> Producer {
    Producer {
        kind: Some(Kind::Client(ClientProducer {
            namespace: "n".to_string(),
            id: "a".to_string(),
        })),
    }
}
