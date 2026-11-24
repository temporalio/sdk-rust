//! Runs the Stream calls of Workflow code. Their results arrive as local inputs, and each call
//! writes with the attempt of a Workflow Task.

mod calls;
mod digest;
mod ledger;
mod manager;
mod replay;
#[cfg(test)]
mod test_store;

pub(crate) use calls::{Phase, StreamCalls, is_stream_replay_failure, set_replay_failure_type};
pub(crate) use ledger::{has_ledger, peeked_ledger};
use manager::{Call, Op, Request};
pub(crate) use manager::{StreamManager, StreamResolution};
pub(super) use replay::StreamRecordsReq;
pub(crate) use replay::{LedgerReads, ReplayReader, ReplayReads};

use crate::{
    WorkerConfig,
    worker::workflow::{
        LocalResolution, LocalResolutionMsg, RunBasics, WFT_HEARTBEAT_TIMEOUT_FRACTION,
        WorkflowTaskInfo,
        workflow_stream::{LocalInput, LocalInputs},
    },
};
use futures_util::future::{AbortHandle, AbortRegistration, Abortable};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use temporalio_common::protos::temporal::api::stream::v1::WorkflowTaskAttempt;
use temporalio_streams::StreamRuntime;
use tokio::{runtime::Handle, sync::mpsc::UnboundedSender};
use tracing::Span;

/// Keeps a completion far below the gRPC message size limit and the size of one write to History
/// on the server.
const MAX_LEDGER_BYTES: usize = 256 * 1024;

/// The maximum size of the Records that one Workflow Task gives Workflow code from all of its
/// reads. A replay reads digested Records again in parts of this size by the bytes that History
/// recorded. One ledger over it still makes a separate part.
const MAX_TASK_RECORD_BYTES: usize = 16 * 1024 * 1024;

/// The maximum number of Records in the page of a read whose command has no `max_records`.
const DEFAULT_PAGE_RECORDS: i32 = 256;

/// The maximum size of the Records in the page of a read whose command has no `max_bytes`.
const DEFAULT_PAGE_BYTES: i64 = 4 * 1024 * 1024;

/// The share of the time to a deadline that a read uses. The rest leaves room for the answer of a
/// Store that answers at the end of its wait, before the deadline.
const READ_WAIT_SHARE: f32 = 0.9;

/// The Stream runtimes of a Worker, identified by the name of the Store that each one serves,
/// with the settings of its Stream calls.
#[derive(Clone, Debug, Default)]
pub struct StreamRuntimes {
    runtimes: Arc<HashMap<String, Arc<StreamRuntime>>>,
    /// The settings of the Stream calls.
    pub settings: StreamSettings,
}

impl StreamRuntimes {
    /// Creates the runtimes, identified by Store name, with the default settings.
    pub fn new(runtimes: impl IntoIterator<Item = (String, Arc<StreamRuntime>)>) -> Self {
        Self {
            runtimes: Arc::new(runtimes.into_iter().collect()),
            settings: StreamSettings::default(),
        }
    }

    pub(crate) fn get(&self, store: &str) -> Option<&Arc<StreamRuntime>> {
        self.runtimes.get(store)
    }
}

/// The settings of a Worker's Stream calls. A call tries again after
/// [`AnyReason::StoreUnavailable`](temporalio_streams::AnyReason::StoreUnavailable) or a try with
/// no answer within `try_deadline`. An append or a close has no time limit for that. A read has
/// the read limit of its task, and a replay read has `replay_retry_bound`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamSettings {
    /// The wait after the first failed try, 100 ms by default. Each later wait doubles, and the
    /// actual wait is a random value within 20 % of it. It must be more than 0 and at most
    /// `retry_max_interval`.
    pub retry_initial_interval: Duration,
    /// The maximum wait between two tries before its random part, 2 s by default.
    pub retry_max_interval: Duration,
    /// How long one try waits for the Store, 5 s by default. A read's try also waits as long as the
    /// read asks the Store to wait. It must be more than 0.
    pub try_deadline: Duration,
    /// How long the reads of a Workflow Task can wait, from the start of the task. With `None` it's
    /// half the Workflow Task timeout. The read limit is always before the heartbeat of the task,
    /// so a read alone never makes Core send a heartbeat. Appends and closes have no such limit.
    pub read_time: Option<Duration>,
    /// How long a replay goes on trying a Store after its first try of that Store fails, each time
    /// it reads a part of the Records of History, 3 s by default. No try or wait goes past that
    /// time, and zero is one try. The first try can take `try_deadline` to fail, so the two
    /// together must stay under the Workflow Task timeout.
    pub replay_retry_bound: Duration,
    /// The maximum number of read entries whose Records a replay reads at the same time, 8 by
    /// default. It must be 1 or more.
    pub replay_concurrency: usize,
    /// The bounds that a test sets in place of the constants.
    #[cfg(test)]
    pub(crate) bounds: TestBounds,
}

impl Default for StreamSettings {
    fn default() -> Self {
        Self {
            retry_initial_interval: Duration::from_millis(100),
            retry_max_interval: Duration::from_secs(2),
            try_deadline: Duration::from_secs(5),
            read_time: None,
            replay_retry_bound: Duration::from_secs(3),
            replay_concurrency: 8,
            #[cfg(test)]
            bounds: TestBounds::default(),
        }
    }
}

impl StreamSettings {
    /// Checks the lower bounds of the settings. Nothing bounds a setting from above.
    pub(crate) fn check(&self) -> Result<(), String> {
        let first = self.retry_initial_interval;
        if first.is_zero() || first > self.retry_max_interval {
            return Err(
                "The Stream setting `retry_initial_interval` must be more than 0 and at most \
                 `retry_max_interval`"
                    .to_string(),
            );
        }
        let zeros = [
            ("try_deadline", self.try_deadline.is_zero()),
            (
                "read_time",
                self.read_time.is_some_and(|time| time.is_zero()),
            ),
            ("replay_concurrency", self.replay_concurrency == 0),
        ];
        if let Some((setting, _)) = zeros.iter().find(|(_, zero)| *zero) {
            return Err(format!(
                "The Stream setting `{setting}` must be more than 0"
            ));
        }
        Ok(())
    }

    /// Returns [`MAX_LEDGER_BYTES`], or the bound that a test set.
    pub(crate) fn max_ledger_bytes(&self) -> usize {
        #[cfg(test)]
        let max = self.bounds.max_ledger_bytes;
        #[cfg(not(test))]
        let max = MAX_LEDGER_BYTES;
        max
    }

    /// Returns [`MAX_TASK_RECORD_BYTES`], or the bound that a test set.
    pub(crate) fn max_task_record_bytes(&self) -> usize {
        #[cfg(test)]
        let max = self.bounds.max_task_record_bytes;
        #[cfg(not(test))]
        let max = MAX_TASK_RECORD_BYTES;
        max
    }
}

/// The bounds of the Stream calls that a test sets in place of the constants.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TestBounds {
    pub(crate) max_ledger_bytes: usize,
    pub(crate) max_task_record_bytes: usize,
}

#[cfg(test)]
impl Default for TestBounds {
    fn default() -> Self {
        Self {
            max_ledger_bytes: MAX_LEDGER_BYTES,
            max_task_record_bytes: MAX_TASK_RECORD_BYTES,
        }
    }
}

/// The state of the run's current Workflow Task, as its Stream calls see it.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) enum TaskState {
    /// The run has no task, so no call starts and Workflow code gets no result.
    #[default]
    NoTask,
    /// Calls start, and each call that Workflow code makes writes with the attempt of the task.
    Open(WorkflowTaskAttempt),
    /// The run completes the task as soon as it can. Until the next task, no call starts and
    /// Workflow code gets no live result.
    Completing(WorkflowTaskAttempt),
}

impl TaskState {
    fn is_open(&self) -> bool {
        matches!(self, TaskState::Open(_))
    }

    fn attempt(&self) -> Option<WorkflowTaskAttempt> {
        match self {
            TaskState::Open(attempt) | TaskState::Completing(attempt) => Some(*attempt),
            TaskState::NoTask => None,
        }
    }
}

/// The Stream manager of a Worker, with the channel of local inputs that it and a heartbeat timer
/// send to.
pub(super) struct Streams {
    manager: StreamManager,
    local_tx: UnboundedSender<LocalInput>,
}

impl Streams {
    /// Creates the manager. It runs the calls on the caller's runtime, which is the Worker's.
    pub(super) fn new(config: &WorkerConfig, local_tx: UnboundedSender<LocalInput>) -> Self {
        let results = local_tx.clone();
        let sink = move |run_id, resolution| {
            let input = LocalInputs::LocalResolution(LocalResolutionMsg {
                run_id,
                res: LocalResolution::Stream(resolution),
            });
            let span = Span::current();
            let _ = results.send(LocalInput { input, span });
        };
        Self {
            manager: StreamManager::new(config.stream_runtimes.clone(), Handle::current(), sink),
            local_tx,
        }
    }
}

/// The Stream manager of a Worker for one run, with the id of that run.
pub(super) struct RunStreams {
    streams: Arc<Streams>,
    run_id: String,
}

impl RunStreams {
    pub(super) fn new(streams: Arc<Streams>, basics: &RunBasics) -> Self {
        Self {
            streams,
            run_id: basics.run_id.clone(),
        }
    }

    /// Starts and stops the Stream calls that the machines decided on. A read that starts asks the
    /// Store to wait at most a share of `time_left`, the time that its task has left to its read
    /// limit.
    pub(super) fn dispatch(&self, calls: &mut StreamCalls, time_left: Duration) {
        let mut requests = calls.take_requests();
        if requests.is_empty() {
            return;
        }
        let max_wait = time_left.mul_f32(READ_WAIT_SHARE);
        for request in &mut requests {
            if let Request::Run(Call {
                op: Op::Read(read), ..
            }) = request
            {
                read.wait = read.wait.min(max_wait);
            }
        }
        self.streams.manager.dispatch(&self.run_id, requests);
    }

    pub(super) fn stop(&self, calls: &mut StreamCalls) {
        calls.stop();
        self.cancel();
    }

    /// Stops every call of the run in the manager. The calls of the machines stay as they are.
    pub(super) fn cancel(&self) {
        self.streams.manager.cancel_run(&self.run_id);
    }

    /// Returns the failure of the run's call that waits to try again after an outage.
    pub(super) fn outage(&self) -> Option<String> {
        self.streams.manager.outage(&self.run_id)
    }

    /// Sends the read limit of the task in hand to the run when the limit comes, once per task. It
    /// must be called on the thread of the runs.
    pub(super) fn read_limit_at(&self, limit: &mut ReadLimit) {
        let Some(at) = limit.at.filter(|_| limit.timer.is_none()) else {
            return;
        };
        let (abort, registration) = AbortHandle::new_pair();
        limit.timer = Some(abort);
        let (run_id, generation) = (self.run_id.clone(), limit.generation);
        let local_tx = self.streams.local_tx.clone();
        let span = Span::current();
        let timer = async move {
            tokio::time::sleep_until(at.into()).await;
            let input = LocalInputs::ReadLimit(run_id, generation);
            let _ = local_tx.send(LocalInput { input, span });
        };
        tokio::spawn(Abortable::new(timer, registration));
    }

    /// Sends a heartbeat timeout for the run at `deadline`. A Worker uses this when it has no Local
    /// Activity manager to run the timer. It must be called on the thread of the runs.
    pub(super) fn heartbeat_at(&self, deadline: Instant, abort: AbortRegistration) {
        let run_id = self.run_id.clone();
        let local_tx = self.streams.local_tx.clone();
        let span = Span::current();
        let timer = async move {
            tokio::time::sleep_until(deadline.into()).await;
            let input = LocalInputs::HeartbeatTimeout(run_id);
            let _ = local_tx.send(LocalInput { input, span });
        };
        // The timer runs on the thread of the runs, so a timer that the run aborted sends nothing.
        // A timeout that it sent before the abort arrives before the run's next task.
        tokio::spawn(Abortable::new(timer, abort));
    }
}

/// The read limit of the task in hand. After it, a read of the task asks the Store not to wait,
/// and doesn't hold the task.
#[derive(Debug, Default)]
pub(super) struct ReadLimit {
    /// `None` until the run knows the Workflow Task timeout.
    at: Option<Instant>,
    /// Counts the tasks, so that the timer of an earlier task ends nothing.
    generation: u64,
    timer: Option<AbortHandle>,
}

impl ReadLimit {
    /// Gives the limit to a task that Core got at `start`, with the Workflow Task timeout
    /// `timeout`, if it has none.
    pub(super) fn set(&mut self, settings: &StreamSettings, start: Instant, timeout: Duration) {
        // The limit leaves time for the results of the reads before the heartbeat point.
        let latest = timeout.mul_f32(WFT_HEARTBEAT_TIMEOUT_FRACTION * READ_WAIT_SHARE);
        let time = settings.read_time.unwrap_or(timeout / 2).min(latest);
        self.at.get_or_insert(start + time);
    }

    /// Returns the time that the task has left to its limit.
    pub(super) fn left(&self) -> Duration {
        (self.at).map_or(Duration::ZERO, |at| {
            at.saturating_duration_since(Instant::now())
        })
    }

    /// Clears the limit for the next task, and stops its timer.
    pub(super) fn reset(&mut self) {
        if let Some(timer) = self.timer.take() {
            timer.abort();
        }
        self.at = None;
        self.generation += 1;
    }

    /// Returns true for the timer of the task in hand.
    pub(super) fn expires(&mut self, generation: u64) -> bool {
        generation == self.generation && self.timer.take().is_some()
    }
}

/// Returns the attempt of a Workflow Task, from its poll response.
pub(super) fn task_attempt(info: &WorkflowTaskInfo) -> WorkflowTaskAttempt {
    WorkflowTaskAttempt {
        started_event_id: info.started_event_id,
        attempt: i32::try_from(info.attempt).unwrap_or(i32::MAX),
        started_time: info.started_time.map(Into::into),
    }
}
