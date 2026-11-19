//! The Stream calls of Workflow code, which Core runs. Its API can change in any release, and a
//! Store needs nothing of it.

mod calls;
mod manager;
mod marker;

pub use calls::{CallError, Phase, StreamCalls};
pub use manager::{Request, StreamManager, StreamResolution};
pub use marker::{is_stream_marker, marker_details, peeked_marker};

use crate::StreamRuntime;
use std::{collections::HashMap, sync::Arc, time::Duration};
use temporalio_common::protos::temporal::api::stream::v1::WorkflowTaskEpoch;
use tokio::sync::watch;

/// The Stream runtimes of a Worker by the name of the Store that each serves, and the settings of
/// its Stream calls.
#[derive(Clone, Debug, Default)]
pub struct StreamRuntimes {
    runtimes: Arc<HashMap<String, Arc<StreamRuntime>>>,
    /// How a call tries the Store.
    pub settings: StreamSettings,
}

impl StreamRuntimes {
    /// The runtimes by Store name, with the default settings.
    pub fn new(runtimes: impl IntoIterator<Item = (String, Arc<StreamRuntime>)>) -> Self {
        Self {
            runtimes: Arc::new(runtimes.into_iter().collect()),
            settings: StreamSettings::default(),
        }
    }

    /// The runtime of the Store `store`.
    pub fn get(&self, store: &str) -> Option<&Arc<StreamRuntime>> {
        self.runtimes.get(store)
    }
}

/// How a Stream call tries its Store. A try that fails with
/// [`Reason::StorageUnavailable`](crate::Reason::StorageUnavailable), or gets no answer by its
/// deadline, is an outage. The call then tries again, with no bound in time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamSettings {
    /// The wait after the first try in an outage. Each later wait doubles, and each is half to all
    /// of its value at random. More than 0, and at most `retry_max_wait`.
    pub retry_first_wait: Duration,
    /// The longest wait between two tries. At most 60 s.
    pub retry_max_wait: Duration,
    /// How long one try waits for the Store. More than 0, and at most 60 s.
    pub try_deadline: Duration,
}

impl Default for StreamSettings {
    fn default() -> Self {
        Self {
            retry_first_wait: Duration::from_millis(100),
            retry_max_wait: Duration::from_secs(2),
            try_deadline: Duration::from_secs(5),
        }
    }
}

const MAX_SETTING_WAIT: Duration = Duration::from_secs(60);

impl StreamSettings {
    /// Fails for a setting out of its bounds.
    pub fn check(&self) -> Result<(), String> {
        if self.retry_first_wait.is_zero() || self.retry_first_wait > self.retry_max_wait {
            return Err(
                "The Stream setting `retry_first_wait` must be more than 0 and at most \
                 `retry_max_wait`"
                    .to_string(),
            );
        }
        if self.retry_max_wait > MAX_SETTING_WAIT {
            return Err("The Stream setting `retry_max_wait` must be at most 60 s".to_string());
        }
        if self.try_deadline.is_zero() || self.try_deadline > MAX_SETTING_WAIT {
            return Err(
                "The Stream setting `try_deadline` must be more than 0 and at most 60 s"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// The Workflow Task that a run has in hand, as its Stream calls see it.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum TaskState {
    /// No call starts, and no result reaches Workflow code.
    #[default]
    NoTask,
    /// Calls start, and a call that Workflow code issues takes the task as its epoch.
    Open(WorkflowTaskEpoch),
    /// The run completes the task at its next chance. No call starts, and no live result reaches
    /// Workflow code before the next task.
    Completing(WorkflowTaskEpoch),
}

/// The one copy of a run's [TaskState].
#[derive(Debug)]
struct RunGate(watch::Sender<TaskState>);

impl RunGate {
    fn new() -> Self {
        Self(watch::Sender::new(TaskState::NoTask))
    }

    fn set(&self, state: TaskState) {
        self.0.send_replace(state);
    }

    fn is_open(&self) -> bool {
        matches!(*self.0.borrow(), TaskState::Open(_))
    }

    fn epoch(&self) -> Option<WorkflowTaskEpoch> {
        match *self.0.borrow() {
            TaskState::Open(epoch) | TaskState::Completing(epoch) => Some(epoch),
            TaskState::NoTask => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_setting_out_of_its_bounds_is_refused() {
        let ms = Duration::from_millis;
        let s = Duration::from_secs;
        let first = "The Stream setting `retry_first_wait` must be more than 0 and at most \
                     `retry_max_wait`";
        let max = "The Stream setting `retry_max_wait` must be at most 60 s";
        let deadline = "The Stream setting `try_deadline` must be more than 0 and at most 60 s";
        let cases = [
            (ms(100), s(2), s(5), None),
            (ms(1), ms(1), ms(1), None),
            (s(60), s(60), s(60), None),
            (ms(0), s(2), s(5), Some(first)),
            (s(3), s(2), s(5), Some(first)),
            (ms(100), s(61), s(5), Some(max)),
            (ms(100), s(2), ms(0), Some(deadline)),
            (ms(100), s(2), s(61), Some(deadline)),
        ];
        for (retry_first_wait, retry_max_wait, try_deadline, error) in cases {
            let settings = StreamSettings {
                retry_first_wait,
                retry_max_wait,
                try_deadline,
            };
            let expected = error.map_or(Ok(()), |error| Err(error.to_string()));
            assert_eq!(settings.check(), expected, "{settings:?}");
        }
    }
}
