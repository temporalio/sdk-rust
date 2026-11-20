//! Runs the Stream calls of Workflow code for Core. This API can change in any release, and a Store
//! doesn't need any of it.

mod calls;
mod digest;
mod manager;
mod marker;
mod reader;
mod replay;

pub use calls::{CallError, Phase, StreamCalls};
pub use manager::{Request, StreamManager, StreamResolution};
pub use marker::{is_stream_marker, marker_details, peeked_marker};
pub use replay::{MarkerReads, ReplayError, ReplayReader, ReplayReads};

use crate::{MAX_RECORDS, StreamRuntime};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_common::protos::temporal::api::stream::v1::WorkflowTaskEpoch;
use tokio::sync::watch;

/// The Stream runtimes of a Worker, keyed by the name of the Store that each one serves, with the
/// settings of its Stream calls.
#[derive(Clone, Debug, Default)]
pub struct StreamRuntimes {
    runtimes: Arc<HashMap<String, Arc<StreamRuntime>>>,
    /// The settings that each call uses to try its Store.
    pub settings: StreamSettings,
}

impl StreamRuntimes {
    /// Creates the runtimes, keyed by Store name, with the default settings.
    pub fn new(runtimes: impl IntoIterator<Item = (String, Arc<StreamRuntime>)>) -> Self {
        Self {
            runtimes: Arc::new(runtimes.into_iter().collect()),
            settings: StreamSettings::default(),
        }
    }

    /// Returns the runtime of the Store `store`.
    pub fn get(&self, store: &str) -> Option<&Arc<StreamRuntime>> {
        self.runtimes.get(store)
    }

    /// Returns true if there's no runtime, so Workflow code on this Worker can't call a Store.
    pub fn is_empty(&self) -> bool {
        self.runtimes.is_empty()
    }
}

/// The settings for how a Stream call tries its Store, and for how a read asks it. A try that fails
/// with [`Reason::StorageUnavailable`](crate::Reason::StorageUnavailable), or gets no answer within
/// its deadline, counts as an outage, and the call then tries again with no limit in time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamSettings {
    /// The wait after the first failed try. Each later wait doubles, and the actual wait is a
    /// random value from half of it to all of it. It must be more than 0 and at most
    /// `retry_max_wait`.
    pub retry_first_wait: Duration,
    /// The maximum wait between two tries. It must be at most 60 seconds.
    pub retry_max_wait: Duration,
    /// How long one try waits for the Store. A read's try also waits as long as the read asks the
    /// Store to wait. It must be more than 0 and at most 60 seconds.
    pub try_deadline: Duration,
    /// The maximum bytes of Records in one page of a read, which is all that waits in Core for the
    /// next activation. It must be more than 0.
    pub read_ahead_bytes: usize,
    /// The maximum number of Records in one page of a read. It must be from 1 to 1000.
    pub read_page_records: usize,
    /// The minimum time between two pages of a read, so that Records which come one at a time reach
    /// Workflow code in fewer activations. Zero is no gap, and it must be at most 1 second.
    pub read_min_gap: Duration,
    /// How long a replay tries a Store that is in an outage, each time it reads the Records for the
    /// next part of History. The time starts at the first try that fails, and no try or wait goes
    /// past it. Zero is one try, and it must be at most 60 seconds. A try can fail after
    /// `try_deadline`, so the two together must stay under the Workflow Task timeout.
    pub replay_retry_bound: Duration,
    /// The maximum number of read entries of one marker whose Records a replay reads at the same
    /// time. It must be from 1 to 64.
    pub replay_concurrency: usize,
}

impl Default for StreamSettings {
    fn default() -> Self {
        Self {
            retry_first_wait: Duration::from_millis(100),
            retry_max_wait: Duration::from_secs(2),
            try_deadline: Duration::from_secs(5),
            read_ahead_bytes: 4 * 1024 * 1024,
            read_page_records: 256,
            read_min_gap: Duration::ZERO,
            replay_retry_bound: Duration::from_secs(3),
            replay_concurrency: 8,
        }
    }
}

const MAX_SETTING_WAIT: Duration = Duration::from_secs(60);
const MAX_READ_MIN_GAP: Duration = Duration::from_secs(1);
const MAX_REPLAY_CONCURRENCY: usize = 64;

impl StreamSettings {
    /// Checks that each setting is within its bounds.
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
        if self.read_ahead_bytes == 0 {
            return Err("The Stream setting `read_ahead_bytes` must be more than 0".to_string());
        }
        if self.read_page_records == 0 || self.read_page_records > MAX_RECORDS {
            return Err(
                "The Stream setting `read_page_records` must be from 1 to 1000".to_string(),
            );
        }
        if self.read_min_gap > MAX_READ_MIN_GAP {
            return Err("The Stream setting `read_min_gap` must be at most 1 s".to_string());
        }
        if self.replay_retry_bound > MAX_SETTING_WAIT {
            return Err("The Stream setting `replay_retry_bound` must be at most 60 s".to_string());
        }
        if self.replay_concurrency == 0 || self.replay_concurrency > MAX_REPLAY_CONCURRENCY {
            return Err("The Stream setting `replay_concurrency` must be from 1 to 64".to_string());
        }
        Ok(())
    }
}

/// The Workflow Task that a run has in hand, as its Stream calls see it.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum TaskState {
    /// No task is in hand, so no call starts and no result reaches Workflow code.
    #[default]
    NoTask,
    /// Calls start, and a call that Workflow code makes takes the task as its epoch.
    Open(WorkflowTaskEpoch),
    /// The run completes the task at its next chance. No call starts, and no live result reaches
    /// Workflow code, before the next task.
    Completing(WorkflowTaskEpoch),
}

/// The one place that holds a run's [`TaskState`].
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

    /// Returns a receiver for a reader, which wakes it each time the task state changes.
    fn subscribe(&self) -> watch::Receiver<TaskState> {
        self.0.subscribe()
    }
}

/// The bytes of Records that the task in hand can still give Workflow code, and that its marker
/// can still hold for reads with `records_in_history`. The run is its one writer, and its readers
/// only read it.
#[derive(Debug, Default)]
pub(crate) struct TaskRoom {
    task: AtomicUsize,
    marker: AtomicUsize,
}

impl TaskRoom {
    fn set(&self, (task, marker): (usize, usize)) {
        self.task.store(task, Ordering::Release);
        self.marker.store(marker, Ordering::Release);
    }

    /// Returns the bytes of Records that the task, and then its marker, can still take.
    fn get(&self) -> (usize, usize) {
        let task = self.task.load(Ordering::Acquire);
        (task, self.marker.load(Ordering::Acquire))
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
                ..StreamSettings::default()
            };
            let expected = error.map_or(Ok(()), |error| Err(error.to_string()));
            assert_eq!(settings.check(), expected, "{settings:?}");
        }
    }
}
