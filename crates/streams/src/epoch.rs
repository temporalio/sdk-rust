//! Producer epochs and their order.
//!
//! A Store keeps an epoch as a key of fixed-width big-endian fields, in the order of comparison.
//! A byte comparison of two keys then orders the epochs field by field. `testdata/epochs.json` has
//! the shared vectors of the keys, in hex.

use crate::{
    StreamError, StreamResult,
    proto::{Producer, WorkflowTaskEpoch, producer::Kind},
};
use prost_types::Timestamp;
use std::cmp::Ordering;

/// The kind of a Producer, which is the case of its epoch. The stored field names and keys hold
/// its number, so each kind keeps its number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProducerKind {
    /// A Workflow run.
    Workflow = 1,
    /// A Local Activity.
    LocalActivity = 2,
    /// An Activity of a Workflow.
    Activity = 3,
    /// A client.
    Client = 4,
    /// A Standalone Activity.
    StandaloneActivity = 5,
}

impl From<&Kind> for ProducerKind {
    fn from(kind: &Kind) -> Self {
        match kind {
            Kind::Workflow(_) => Self::Workflow,
            Kind::LocalActivity(_) => Self::LocalActivity,
            Kind::WorkflowActivity(_) => Self::Activity,
            Kind::StandaloneActivity(_) => Self::StandaloneActivity,
            Kind::Client(_) => Self::Client,
        }
    }
}

/// Refuses a Producer with no kind case.
impl TryFrom<&Producer> for ProducerKind {
    type Error = StreamError;

    fn try_from(producer: &Producer) -> StreamResult<Self> {
        kind_of(producer).ok_or_else(|| StreamError::invalid("The Producer has no kind"))
    }
}

/// The kind case of a Producer, `None` when it has none.
pub fn kind_of(producer: &Producer) -> Option<ProducerKind> {
    producer.kind.as_ref().map(ProducerKind::from)
}

/// The length of a [task_key].
pub const TASK_KEY_LEN: usize = 24;
/// The length of an [epoch_key].
pub const EPOCH_KEY_LEN: usize = 40;

// A negative field is clamped at zero, since its bytes would outrank every real epoch.
fn be64(value: i64) -> [u8; 8] {
    value.max(0).to_be_bytes()
}

fn be32(value: i32) -> [u8; 4] {
    value.max(0).to_be_bytes()
}

fn time_fields(time: Option<&Timestamp>) -> (i64, i32) {
    time.map_or((0, 0), |time| (time.seconds, time.nanos))
}

/// The key of a Workflow Task attempt. It is the started event id, the attempt, and the started
/// seconds and nanos, in 8, 4, 8 and 4 bytes. The epoch keys of the run and of the task's Local
/// Activities start with it.
pub fn task_key(task: &WorkflowTaskEpoch) -> [u8; TASK_KEY_LEN] {
    let (seconds, nanos) = time_fields(task.started_time.as_ref());
    let mut key = [0; TASK_KEY_LEN];
    key[..8].copy_from_slice(&be64(task.started_event_id));
    key[8..12].copy_from_slice(&be32(task.attempt));
    key[12..20].copy_from_slice(&be64(seconds));
    key[20..].copy_from_slice(&be32(nanos));
    key
}

/// The key of an epoch. It is the [task_of] key, then the attempt's scheduled seconds and nanos
/// and the attempt, in 8, 4 and 4 bytes. A field that the case doesn't have is zero, and a Producer
/// with no case is all zeros.
pub fn epoch_key(kind: Option<&Kind>) -> [u8; EPOCH_KEY_LEN] {
    let (scheduled, attempt) = match kind {
        Some(Kind::LocalActivity(local)) => (None, local.epoch.map_or(0, |e| e.attempt)),
        Some(Kind::WorkflowActivity(activity) | Kind::StandaloneActivity(activity)) => {
            let epoch = activity.epoch;
            (
                epoch.and_then(|e| e.attempt_scheduled_time),
                epoch.map_or(0, |e| e.attempt),
            )
        }
        // A client has one epoch for its whole life, so nothing fences it.
        Some(Kind::Client(_)) => (None, 0),
        Some(Kind::Workflow(_)) | None => (None, 0),
    };
    let (seconds, nanos) = time_fields(scheduled.as_ref());
    let mut key = [0; EPOCH_KEY_LEN];
    key[..TASK_KEY_LEN].copy_from_slice(&task_key(&task_of(kind)));
    key[24..32].copy_from_slice(&be64(seconds));
    key[32..36].copy_from_slice(&be32(nanos));
    key[36..].copy_from_slice(&be32(attempt));
    key
}

/// The Workflow Task of an epoch. It is a Workflow's own task, or the task that dispatched a Local
/// Activity, and zero for the other kinds.
pub fn task_of(kind: Option<&Kind>) -> WorkflowTaskEpoch {
    match kind {
        Some(Kind::Workflow(w)) => w.epoch.unwrap_or_default(),
        Some(Kind::LocalActivity(local)) => local.epoch.and_then(|e| e.task).unwrap_or_default(),
        _ => WorkflowTaskEpoch::default(),
    }
}

/// Orders two epochs field by field.
pub fn compare_epochs(a: Option<&Kind>, b: Option<&Kind>) -> Ordering {
    epoch_key(a).cmp(&epoch_key(b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{LocalActivityEpoch, LocalActivityProducer, producer::Kind};

    // Without the clamp, the bytes of a negative field would outrank every real epoch.
    #[test]
    fn a_negative_field_compares_as_zero() {
        let negative = Kind::LocalActivity(LocalActivityProducer {
            epoch: Some(LocalActivityEpoch {
                task: None,
                attempt: -1,
            }),
            ..Default::default()
        });
        assert_eq!(compare_epochs(Some(&negative), None), Ordering::Equal);
    }
}
