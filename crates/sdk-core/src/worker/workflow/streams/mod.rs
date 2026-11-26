//! Runs the Stream calls of Workflow code. Their results arrive as local inputs, and each call
//! writes with the attempt of a Workflow Task.

mod calls;
mod manager;
mod marker;

pub(crate) use calls::{Phase, StreamCalls};
pub(crate) use manager::{StreamManager, StreamResolution};
pub(crate) use marker::{is_stream_marker, marker_details, peeked_marker};

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

/// The Stream runtimes of a Worker, identified by the name of the Store that each one serves,
/// with the settings of its Stream calls. The runtime under the empty name is the default.
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

    /// Returns the Store name that a ref with no Store name uses. That is the empty name if a
    /// runtime has it, else the name of the only runtime.
    pub(crate) fn default_store(&self) -> Option<&str> {
        if self.runtimes.contains_key("") {
            return Some("");
        }
        if self.runtimes.len() != 1 {
            return None;
        }
        self.runtimes.keys().next().map(String::as_str)
    }

    pub(crate) fn count(&self) -> usize {
        self.runtimes.len()
    }
}

/// The retry settings of a Worker's Stream calls. A call tries again after
/// [`AnyReason::StoreUnavailable`](temporalio_streams::AnyReason::StoreUnavailable) or a try with
/// no answer within `try_deadline`. If it still waits to try again at the heartbeat of its Workflow
/// Task, the task fails. A Worker with a setting out of its bounds fails to start.
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
}

impl Default for StreamSettings {
    fn default() -> Self {
        Self {
            retry_initial_interval: Duration::from_millis(100),
            retry_max_interval: Duration::from_secs(2),
            try_deadline: Duration::from_secs(5),
        }
    }
}

impl StreamSettings {
    /// Checks the settings. Each has a lower bound, and `retry_initial_interval` can't be more than
    /// `retry_max_interval`.
    pub(crate) fn check(&self) -> Result<(), String> {
        let first = self.retry_initial_interval;
        if first.is_zero() || first > self.retry_max_interval {
            return Err(
                "The Stream setting `retry_initial_interval` must be more than 0 and at most \
                 `retry_max_interval`"
                    .to_string(),
            );
        }
        if self.try_deadline.is_zero() {
            return Err("The Stream setting `try_deadline` must be more than 0".to_string());
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_settings_check_refuses_each_value_out_of_its_bounds() {
        let settings = StreamSettings::default();
        assert_eq!(settings.check(), Ok(()));
        let first_at_max = StreamSettings {
            retry_initial_interval: settings.retry_max_interval,
            ..settings
        };
        assert_eq!(first_at_max.check(), Ok(()));
        let retry = Err(
            "The Stream setting `retry_initial_interval` must be more than 0 and at most \
             `retry_max_interval`"
                .to_string(),
        );
        let zero_first = StreamSettings {
            retry_initial_interval: Duration::ZERO,
            ..settings
        };
        assert_eq!(zero_first.check(), retry);
        let first_above_max = StreamSettings {
            retry_initial_interval: settings.retry_max_interval + Duration::from_millis(1),
            ..settings
        };
        assert_eq!(first_above_max.check(), retry);
        let zero_deadline = StreamSettings {
            try_deadline: Duration::ZERO,
            ..settings
        };
        assert_eq!(
            zero_deadline.check(),
            Err("The Stream setting `try_deadline` must be more than 0".to_string())
        );
    }
}
