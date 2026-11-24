//! Runs the Stream calls of Workflow code. Their results arrive as local inputs, and each call
//! writes with the attempt of a Workflow Task.

mod calls;
mod ledger;
mod manager;

pub(crate) use calls::{Phase, StreamCalls};
pub(crate) use ledger::{has_ledger, peeked_ledger};
pub(crate) use manager::{StreamManager, StreamResolution};

use crate::{
    WorkerConfig,
    worker::workflow::{
        LocalResolution, LocalResolutionMsg, RunBasics, WorkflowTaskInfo,
        workflow_stream::{LocalInput, LocalInputs},
    },
};
use futures_util::future::{AbortRegistration, Abortable};
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

/// The retry settings of a Worker's Stream calls. A call tries again, with no time limit, after
/// [`AnyReason::StoreUnavailable`](temporalio_streams::AnyReason::StoreUnavailable) or a try with
/// no answer within `try_deadline`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamSettings {
    /// The wait after the first failed try, 100 ms by default. Each later wait doubles, and the
    /// actual wait is a random value within 20 % of it. It must be more than 0 and at most
    /// `retry_max_interval`.
    pub retry_initial_interval: Duration,
    /// The maximum wait between two tries before its random part, 2 s by default.
    pub retry_max_interval: Duration,
    /// How long one try waits for the Store, 5 s by default. It must be more than 0.
    pub try_deadline: Duration,
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
        let zeros = [("try_deadline", self.try_deadline.is_zero())];
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
}

/// The bounds of the Stream calls that a test sets in place of the constants.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TestBounds {
    pub(crate) max_ledger_bytes: usize,
}

#[cfg(test)]
impl Default for TestBounds {
    fn default() -> Self {
        Self {
            max_ledger_bytes: MAX_LEDGER_BYTES,
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

    /// Starts and stops the Stream calls that the machines decided on.
    pub(super) fn dispatch(&self, calls: &mut StreamCalls) {
        let requests = calls.take_requests();
        if !requests.is_empty() {
            self.streams.manager.dispatch(&self.run_id, requests);
        }
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

/// Returns the attempt of a Workflow Task, from its poll response.
pub(super) fn task_attempt(info: &WorkflowTaskInfo) -> WorkflowTaskAttempt {
    WorkflowTaskAttempt {
        started_event_id: info.started_event_id,
        attempt: i32::try_from(info.attempt).unwrap_or(i32::MAX),
        started_time: info.started_time.map(Into::into),
    }
}
