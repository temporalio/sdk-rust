//! The reader of a read of Workflow code. It asks the Store from its own cursor for one page at a
//! time, and only for what the task in hand can still give Workflow code.

use super::{
    StreamSettings, TaskRoom, TaskState,
    manager::{Backoff, Note, failure},
};
use crate::{
    MAX_WAIT, Reason, StreamError, StreamRuntime,
    proto::{ReadRecordsRequest, StreamRecord},
};
use std::{sync::Arc, time::Duration};
use temporalio_common::protos::coresdk::workflow_activation::StreamReadEnd;
use tokio::{
    sync::{Notify, watch},
    time::Instant,
};

/// How long a read waits for a Record when Workflow code sets no `idle_timeout`.
pub(crate) const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// One answer of the Store, which Workflow code gets whole in one job, or the end of a read.
#[derive(Debug)]
pub(crate) struct Page {
    pub(crate) records: Vec<StreamRecord>,
    /// Set in the last page of the read.
    pub(crate) end: Option<StreamReadEnd>,
}

/// Tells the reader of a read that an activation took the page that it gave. The reader waits for
/// it after each page, so Core holds at most one page of a read.
#[derive(Debug, Default)]
pub(crate) struct PageTaken(Notify);

impl PageTaken {
    pub(crate) fn notify(&self) {
        self.0.notify_one();
    }
}

/// A read to run, and what its reader shares with the run.
#[derive(Debug)]
pub(crate) struct ReadCall {
    /// The request of the next try. Its `from_offset` is the reader's cursor.
    pub(crate) request: ReadRecordsRequest,
    pub(crate) idle_timeout: Duration,
    pub(crate) records_in_history: bool,
    pub(crate) gate: watch::Receiver<TaskState>,
    pub(crate) room: Arc<TaskRoom>,
    pub(crate) taken: Arc<PageTaken>,
}

impl ReadCall {
    /// Returns the bytes that the next page of the read may have. Only Records in History take
    /// the room of the marker.
    pub(crate) fn room_left(&self) -> usize {
        let (task, marker) = self.room.get();
        if self.records_in_history {
            task.min(marker)
        } else {
            task
        }
    }
}

/// Reads until the read ends, and gives each page to `give`. A try that fails with
/// `StorageUnavailable`, or gets no answer by its deadline, notes an outage, and the reader asks
/// again from its cursor after a backoff.
pub(crate) async fn read(
    runtime: &StreamRuntime,
    mut call: ReadCall,
    settings: StreamSettings,
    note: &Note,
    give: impl Fn(Page),
) {
    // A time past the end of the clock never ends the read.
    let idle_timeout = call.idle_timeout;
    let idle_end = |from: Instant| from.checked_add(idle_timeout);
    let mut idle_until = idle_end(Instant::now());
    let mut gap_until = Instant::now();
    let mut backoff = Backoff::new(&settings);
    loop {
        tokio::time::sleep_until(gap_until).await;
        let Some(max_bytes) = room(&mut call, &settings).await else {
            return;
        };
        let idle_left =
            idle_until.map_or(MAX_WAIT, |at| at.saturating_duration_since(Instant::now()));
        let wait = idle_left.min(MAX_WAIT);
        let request = ReadRecordsRequest {
            max_records: settings.read_page_records as i32,
            max_bytes: i64::try_from(max_bytes).unwrap_or(i64::MAX),
            wait: (!wait.is_zero()).then(|| wait.try_into().expect("A wait of at most 60 s")),
            ..call.request.clone()
        };
        // A read that waits for a Record is not slow, so its deadline adds that wait.
        let deadline = settings.try_deadline + wait;
        let answer = match tokio::time::timeout(deadline, runtime.read_records(request)).await {
            Ok(Err(error)) if error.reason != Reason::StorageUnavailable => {
                give(Page {
                    records: vec![],
                    end: Some(StreamReadEnd {
                        done: false,
                        failed: Some(failure(&error)),
                    }),
                });
                return;
            }
            Ok(answer) => answer,
            Err(_) => Err(StreamError::storage_unavailable(format!(
                "The Store gave no answer in {deadline:?}"
            ))),
        };
        let page = match answer {
            Ok(page) => page,
            Err(error) => {
                note.set(Some(&error));
                tokio::time::sleep(backoff.next_wait(rand::random())).await;
                continue;
            }
        };
        note.set(None);
        backoff = Backoff::new(&settings);
        let passed = page.last_offset != call.request.from_offset;
        call.request.from_offset = page.last_offset;
        if page.records.is_empty() && !page.done {
            // A look that found nothing is the last one once the idle time is over. A look that
            // passed entries which aren't Records may have stopped short of a Record.
            if !passed && idle_until.is_some_and(|at| at <= Instant::now()) {
                give(Page {
                    records: vec![],
                    end: Some(StreamReadEnd::default()),
                });
                return;
            }
            continue;
        }
        let done = page.done;
        idle_until = idle_end(Instant::now());
        give(Page {
            records: page.records,
            end: done.then_some(StreamReadEnd { done, failed: None }),
        });
        if done {
            return;
        }
        gap_until = Instant::now() + settings.read_min_gap;
        call.taken.0.notified().await;
    }
}

/// Waits until the task is open and can take more Records, and returns the maximum bytes of the
/// next page. Returns `None` once the run is gone.
async fn room(call: &mut ReadCall, settings: &StreamSettings) -> Option<usize> {
    loop {
        if matches!(*call.gate.borrow_and_update(), TaskState::Open(_)) {
            let room = call.room_left();
            if room > 0 {
                return Some(room.min(settings.read_ahead_bytes));
            }
        }
        call.gate.changed().await.ok()?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        proto::{StreamRef, WorkflowTaskEpoch},
        record::BatchLimits,
        test_store::{Entry, Fault, Model, ModelStore, task},
        workflow::{
            Phase, StreamCalls, StreamRuntimes,
            manager::{Call, Op, Outcome, Request, StreamManager, StreamResolution},
        },
    };
    use parking_lot::Mutex;
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use temporalio_common::protos::{
        coresdk::{
            workflow_activation::workflow_activation_job::Variant,
            workflow_commands::StartStreamRead,
        },
        temporal::api::common::v1::Payload,
    };
    use tokio::{runtime::Handle, sync::mpsc};

    /// What the test, as the run of the read, did last. The Store checks each request against it.
    #[derive(Default)]
    struct Run {
        /// True while a page that the reader gave waits for the run to take it.
        page_out: bool,
        /// When the reader gave its last page with Records.
        last_page: Option<Instant>,
        violations: Vec<String>,
    }

    /// Returns a Stream of up to 40 entries, of which some aren't Records, that come over time.
    fn entries(rng: &mut StdRng) -> Vec<Entry> {
        let mut at = Duration::ZERO;
        (0..rng.random_range(0..40u16))
            .map(|index| {
                at += Duration::from_millis(rng.random_range(0..30));
                let offset = (index * 2 + 1).to_be_bytes().to_vec();
                let record = rng.random_bool(0.75).then(|| StreamRecord {
                    offset: offset.clone(),
                    body: Some(Payload {
                        data: vec![0; rng.random_range(0..100)],
                        ..Default::default()
                    }),
                    producer: Some(crate::test_store::client()),
                });
                Entry { offset, at, record }
            })
            .collect()
    }

    // A reader gives the Records of its Stream after its offset in order, each once, with the
    // right end, for any answers of the Store and any task boundaries. It asks only while the task
    // is open, the run took its last page and the task has room, and only for what fits.
    #[tokio::test(start_paused = true)]
    async fn a_reader_gives_its_stream_in_order_once_and_asks_only_for_what_fits() {
        let (mut ends, mut outages) = ([0; 3], 0);
        for seed in 0..300 {
            let mut rng = StdRng::seed_from_u64(seed);
            let entries = entries(&mut rng);
            let from_offset = match rng.random_range(0..=entries.len()) {
                0 => vec![],
                at => entries[at - 1].offset.clone(),
            };
            let idle_timeout = match rng.random_range(0..4) {
                0 => Duration::ZERO,
                1 => Duration::MAX,
                _ => Duration::from_millis(rng.random_range(1..300)),
            };
            // A read that never ends for idle time ends once its Stream is closed and read.
            let closed = idle_timeout == Duration::MAX || rng.random_bool(0.5);
            let settings = StreamSettings {
                retry_first_wait: Duration::from_millis(1),
                retry_max_wait: Duration::from_millis(5),
                try_deadline: Duration::from_millis(20),
                read_ahead_bytes: rng.random_range(1..2000),
                read_page_records: rng.random_range(1..=10),
                read_min_gap: Duration::from_millis(rng.random_range(0..3) * 20),
                ..Default::default()
            };
            let records_in_history = rng.random_bool(0.5);
            let fails = rng.random_bool(0.3);
            let (gate, room) = (
                watch::Sender::new(TaskState::NoTask),
                Arc::new(TaskRoom::default()),
            );
            let run = Arc::new(Mutex::new(Run::default()));
            let (checked_gate, checked_room, checked_run) =
                (gate.subscribe(), room.clone(), run.clone());
            let started = Instant::now();
            let fault = move |rng: &mut StdRng, request: &ReadRecordsRequest| {
                let mut run = checked_run.lock();
                let (task, marker) = checked_room.get();
                let room = if records_in_history {
                    task.min(marker)
                } else {
                    task
                };
                let gap_left =
                    (run.last_page).is_some_and(|at| at + settings.read_min_gap > Instant::now());
                // The Store waits no longer than the idle time that the read has left.
                let since = run.last_page.unwrap_or(started);
                let idle_left = (since.checked_add(idle_timeout)).map_or(MAX_WAIT, |end| {
                    end.saturating_duration_since(Instant::now())
                });
                let wait = (request.wait).map_or(Duration::ZERO, |wait| wait.try_into().unwrap());
                let rules = [
                    matches!(*checked_gate.borrow(), TaskState::Open(_)),
                    !run.page_out,
                    room > 0,
                    request.max_bytes as usize == room.min(settings.read_ahead_bytes),
                    request.max_records as usize == settings.read_page_records,
                    !gap_left,
                    wait <= idle_left.min(MAX_WAIT),
                ];
                if let Some(rule) = rules.iter().position(|kept| !kept) {
                    run.violations
                        .push(format!("seed {seed}: rule {rule} of a request"));
                }
                match rng.random_range(0..100) {
                    0..=4 => Some(Fault::Fails(StreamError::storage_unavailable("Down"))),
                    5..=7 => Some(Fault::Silent),
                    8 if fails => Some(Fault::Fails(StreamError::new(Reason::NotFound, "Gone"))),
                    _ => None,
                }
            };
            let store = Arc::new(ModelStore::new(Model {
                rng: StdRng::seed_from_u64(seed),
                entries,
                closed,
                fault: Box::new(fault),
                reading: (0, 0),
                moved_to: None,
                violations: vec![],
            }));
            let runtime = StreamRuntime::new(store.clone(), BatchLimits::default());
            let mut runtimes = StreamRuntimes::new([("mem".to_string(), Arc::new(runtime))]);
            runtimes.settings = settings;
            let (tx, mut pages) = mpsc::unbounded_channel();
            let sink = move |_, resolution: StreamResolution| tx.send(resolution).unwrap();
            let manager = StreamManager::new(runtimes, Handle::current(), sink);
            let taken = Arc::new(PageTaken::default());
            let call = ReadCall {
                request: ReadRecordsRequest {
                    r#ref: Some(StreamRef {
                        id: "s".to_string(),
                        store: "mem".to_string(),
                    }),
                    from_offset: from_offset.clone(),
                    ..Default::default()
                },
                idle_timeout,
                records_in_history,
                gate: gate.subscribe(),
                room: room.clone(),
                taken: taken.clone(),
            };
            let op = Op::Read(call);
            manager.dispatch(
                "r",
                vec![Request::Run(Call {
                    call_id: 1,
                    seq: 1,
                    op,
                })],
            );
            let (mut got, mut taken_at) = (vec![], None);
            let end = loop {
                let event = Instant::now() + Duration::from_millis(rng.random_range(1..40));
                tokio::select! {
                    resolution = pages.recv() => {
                        let Outcome::Page(page) = resolution.unwrap().outcome else {
                            panic!("seed {seed}: a read gives only pages");
                        };
                        let mut run = run.lock();
                        if run.page_out {
                            run.violations.push(format!("seed {seed}: two pages wait"));
                        }
                        run.page_out = true;
                        if !page.records.is_empty() {
                            run.last_page = Some(Instant::now());
                        }
                        got.extend(page.records);
                        if let Some(end) = page.end {
                            break end;
                        }
                        let wait = Duration::from_millis(rng.random_range(0..30));
                        taken_at = Some(Instant::now() + wait);
                    }
                    _ = tokio::time::sleep_until(event) => {
                        outages += usize::from(manager.outage("r").is_some());
                        // The run takes the page that waits, or the task changes, at random.
                        if taken_at.is_some_and(|at| at <= Instant::now()) {
                            run.lock().page_out = false;
                            taken.notify();
                            taken_at = None;
                        }
                        let epoch = WorkflowTaskEpoch::default();
                        match rng.random_range(0..10) {
                            0 => drop(gate.send_replace(TaskState::Completing(epoch))),
                            1 => drop(gate.send_replace(TaskState::NoTask)),
                            2..=4 => {
                                let left = |rng: &mut StdRng| match rng.random_range(0..4) {
                                    0 => 0,
                                    1 => rng.random_range(1..200),
                                    _ => usize::MAX,
                                };
                                room.set((left(&mut rng), left(&mut rng)));
                                gate.send_replace(TaskState::Open(epoch));
                            }
                            _ => {}
                        }
                    }
                }
            };
            let violations = [
                run.lock().violations.clone(),
                store.model.lock().violations.clone(),
            ];
            assert_eq!(violations.concat(), Vec::<String>::new());
            let all = store.records_after(&from_offset);
            assert!(all.starts_with(&got), "seed {seed}");
            match (&end.failed, end.done) {
                // Only a Store that can fail the read ends it with a failure.
                (Some(_), _) => {
                    assert!(fails, "seed {seed}: {end:?}");
                    ends[0] += 1;
                }
                (None, true) => {
                    assert!(closed && got == all, "seed {seed}");
                    ends[1] += 1;
                }
                // No Record came for the idle time, and the last look found none.
                (None, false) => {
                    let came = store.records_came_after(&from_offset);
                    assert_eq!(got, came, "seed {seed}");
                    let since = run.lock().last_page;
                    assert!(since.is_none_or(|since| since + idle_timeout <= Instant::now()));
                    ends[2] += 1;
                }
            }
        }
        // The scripts reach each kind of end, and an outage.
        assert!(
            ends.iter().all(|count| *count > 0) && outages > 0,
            "{ends:?} {outages}"
        );
    }

    // A run takes the pages of a real reader through the gate of its task, the room of the task,
    // and the signal that it took a page. Over random task changes, Workflow code gets the Records
    // of the Stream after the read's offset in order and each once, with the end after the last
    // of them, and the marker of each task counts the pages that the task gave. The Store has no
    // outage here, since a retry takes its jitter from `rand::random`, which no seed fixes. The
    // test above covers outages.
    #[tokio::test(start_paused = true)]
    async fn a_run_takes_the_pages_of_a_real_reader_in_order_and_once() {
        let (mut between_tasks, mut over_tasks) = (0, 0);
        for seed in 0..200 {
            let mut rng = StdRng::seed_from_u64(seed);
            let entries = entries(&mut rng);
            let from_offset = match rng.random_range(0..=entries.len()) {
                0 => vec![],
                at => entries[at - 1].offset.clone(),
            };
            let closed = rng.random_bool(0.7);
            // The offset that each read of the Store starts after.
            let asked = Arc::new(Mutex::new(vec![]));
            let kept = asked.clone();
            let store = Arc::new(ModelStore::new(Model {
                rng: StdRng::seed_from_u64(seed),
                entries,
                closed,
                fault: Box::new(move |_, request: &ReadRecordsRequest| {
                    kept.lock().push(request.from_offset.clone());
                    None
                }),
                reading: (0, 0),
                moved_to: None,
                violations: vec![],
            }));
            let runtime = StreamRuntime::new(store.clone(), BatchLimits::default());
            let mut runtimes = StreamRuntimes::new([("mem".to_string(), Arc::new(runtime))]);
            runtimes.settings = StreamSettings {
                read_ahead_bytes: rng.random_range(1..2000),
                read_page_records: rng.random_range(1..=10),
                ..Default::default()
            };
            let (tx, mut resolutions) = mpsc::unbounded_channel();
            let sink = move |_, resolution: StreamResolution| tx.send(resolution).unwrap();
            let manager = StreamManager::new(runtimes.clone(), Handle::current(), sink);
            let mut calls = StreamCalls::new("ns", "r", runtimes, Phase::Live);
            let mut number = 1;
            calls.set_task(TaskState::Open(task(number, 1, 0)));
            let records_in_history = rng.random_bool(0.5);
            let idle = Duration::from_millis(rng.random_range(0..200));
            let read = StartStreamRead {
                seq: 1,
                stream: Some(StreamRef {
                    id: "s".to_string(),
                    store: "mem".to_string(),
                }),
                from_offset: from_offset.clone(),
                idle_timeout: Some(idle.try_into().unwrap()),
                records_in_history,
            };
            calls.start_read(read).unwrap();
            // The pages of the task in hand, and all that Workflow code got.
            let (mut activation, mut given, mut got) = (0, vec![], vec![]);
            let (mut tasks_with_pages, mut steps) = (0, 0);
            let end = loop {
                steps += 1;
                assert!(steps < 100_000, "seed {seed}: the read never ended");
                manager.dispatch("r", calls.take_requests());
                let mut end = None;
                let completes = match rng.random_range(0..10) {
                    // The run waits for a page of the reader, or for time to pass.
                    0..=4 => {
                        let wait = Duration::from_millis(rng.random_range(0..30));
                        tokio::select! {
                            biased;
                            Some(resolution) = resolutions.recv() => calls.resolve(resolution),
                            () = tokio::time::sleep(wait) => {}
                        }
                        false
                    }
                    // An activation takes the pages that fit.
                    5..=8 => {
                        if calls.has_jobs(activation) {
                            for job in calls.take_jobs(activation) {
                                let Variant::ResolveStreamRead(page) = job else {
                                    panic!("seed {seed}: {job:?}");
                                };
                                given.push(page.records.clone());
                                got.extend(page.records);
                                end = page.end;
                            }
                            activation += 1;
                        }
                        false
                    }
                    _ => true,
                };
                if !(completes || end.is_some() || calls.must_complete(activation)) {
                    continue;
                }
                // The task completes, sometimes after a time in which no page reaches Workflow
                // code.
                if rng.random_bool(0.5) {
                    calls.set_task(TaskState::Completing(task(number, 1, 0)));
                    let wait = Duration::from_millis(rng.random_range(0..30));
                    tokio::time::sleep(wait).await;
                    while let Ok(resolution) = resolutions.try_recv() {
                        between_tasks += 1;
                        calls.resolve(resolution);
                    }
                    assert!(!calls.has_jobs(activation), "seed {seed}");
                }
                let marker = calls.take_marker();
                let entry = marker.as_ref().and_then(|marker| marker.reads.first());
                let pages: Vec<&Vec<StreamRecord>> =
                    given.iter().filter(|records| !records.is_empty()).collect();
                let counts: Vec<u32> = pages.iter().map(|page| page.len() as u32).collect();
                let recorded = entry.map_or(vec![], |entry| entry.page_records.clone());
                assert_eq!(recorded, counts, "seed {seed}: task {number}");
                if let Some(entry) = entry.filter(|_| records_in_history) {
                    let records: Vec<StreamRecord> = pages.into_iter().flatten().cloned().collect();
                    assert_eq!(entry.records, records, "seed {seed}: task {number}");
                }
                tasks_with_pages += usize::from(!counts.is_empty());
                if marker.is_some() {
                    calls.marker_recorded().unwrap();
                }
                calls.set_task(TaskState::NoTask);
                if let Some(end) = end {
                    break end;
                }
                (activation, number) = (0, number + 1);
                given.clear();
                let wait = Duration::from_millis(rng.random_range(0..30));
                tokio::time::sleep(wait).await;
                calls.set_task(TaskState::Open(task(number, 1, 0)));
            };
            over_tasks += usize::from(tasks_with_pages > 1);
            assert_eq!(store.model.lock().violations, Vec::<String>::new());
            // Workflow code got each Record of the Stream once a read is done, or else each one
            // before the reader's last look, which found none.
            let all = store.records_after(&from_offset);
            let last = asked.lock().last().cloned().unwrap_or_default();
            let given: Vec<StreamRecord> = (all.iter())
                .filter(|record| end.done || record.offset <= last)
                .cloned()
                .collect();
            assert!(end.failed.is_none() && got == given, "seed {seed}: {end:?}");
            assert!(!calls.needs_next_task(), "seed {seed}");
        }
        // Pages reach the run while it has no task open, and reads go on over several tasks.
        assert!(
            between_tasks > 0 && over_tasks > 0,
            "{between_tasks} {over_tasks}"
        );
    }
}
