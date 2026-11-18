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

/// The kind of a Producer, which is the case of its epoch. The numbers must never change. A Store
/// may keep the number of a kind in what it stores, and a digest that History keeps for good can
/// hash it with the [epoch_key]. A changed number would break the stored data, and the replay of
/// that History.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{
        ActivityEpoch, ActivityProducer, ClientProducer, LocalActivityEpoch, LocalActivityProducer,
        WorkflowProducer,
    };

    // Without the clamp, the bytes of a negative field would outrank every real epoch.
    #[test]
    fn a_negative_field_keys_as_zero() {
        let negative = Kind::LocalActivity(LocalActivityProducer {
            epoch: Some(LocalActivityEpoch {
                task: None,
                attempt: -1,
            }),
            ..Default::default()
        });
        assert_eq!(epoch_key(Some(&negative)), epoch_key(None));
    }

    #[test]
    fn the_numbers_of_the_kinds_never_change() {
        assert_eq!(ProducerKind::Workflow as u8, 1);
        assert_eq!(ProducerKind::LocalActivity as u8, 2);
        assert_eq!(ProducerKind::Activity as u8, 3);
        assert_eq!(ProducerKind::Client as u8, 4);
        assert_eq!(ProducerKind::StandaloneActivity as u8, 5);
    }

    /// A time as a proto Timestamp, from its seconds and nanos.
    fn at(seconds: i64, nanos: i32) -> Option<Timestamp> {
        Some(Timestamp { seconds, nanos })
    }

    fn task(started_event_id: i64, attempt: i32, seconds: i64) -> WorkflowTaskEpoch {
        WorkflowTaskEpoch {
            started_event_id,
            attempt,
            started_time: at(seconds, 0),
        }
    }

    fn local_activity(task: WorkflowTaskEpoch, attempt: i32) -> Kind {
        Kind::LocalActivity(LocalActivityProducer {
            run_id: "r".to_string(),
            epoch: Some(LocalActivityEpoch {
                task: Some(task),
                attempt,
            }),
        })
    }

    fn activity(seconds: i64, nanos: i32, attempt: i32) -> ActivityProducer {
        ActivityProducer {
            run_id: "r".to_string(),
            epoch: Some(ActivityEpoch {
                attempt_scheduled_time: at(seconds, nanos),
                attempt,
            }),
        }
    }

    // A Store fences by comparing keys, so a later epoch of a kind must give a higher key.
    #[test]
    fn a_higher_epoch_gives_a_higher_key_for_each_kind_that_has_one() {
        let workflow = |task| {
            Kind::Workflow(WorkflowProducer {
                run_id: "r".to_string(),
                epoch: Some(task),
            })
        };
        let ascending = [
            // A later started time, a higher attempt, and a later Workflow Task. Each beats the
            // fields after it.
            vec![
                workflow(task(8, 1, 0)),
                workflow(task(8, 1, 100)),
                workflow(task(8, 2, 0)),
                workflow(task(8, 2, 200)),
                workflow(task(9, 1, 0)),
            ],
            // The Workflow Task of a Local Activity comes before its own attempt.
            vec![
                local_activity(task(8, 1, 0), 1),
                local_activity(task(8, 1, 0), 2),
                local_activity(task(8, 1, 100), 1),
                local_activity(task(8, 2, 0), 1),
                local_activity(task(9, 1, 0), 1),
            ],
            // A later scheduled time beats a higher attempt.
            vec![
                Kind::WorkflowActivity(activity(100, 0, 1)),
                Kind::WorkflowActivity(activity(100, 0, 2)),
                Kind::WorkflowActivity(activity(100, 5, 1)),
                Kind::WorkflowActivity(activity(101, 0, 1)),
            ],
            vec![
                Kind::StandaloneActivity(activity(100, 0, 1)),
                Kind::StandaloneActivity(activity(100, 0, 2)),
                Kind::StandaloneActivity(activity(100, 5, 1)),
                Kind::StandaloneActivity(activity(101, 0, 1)),
            ],
        ];
        for kinds in ascending {
            for pair in kinds.windows(2) {
                assert!(
                    epoch_key(Some(&pair[0])) < epoch_key(Some(&pair[1])),
                    "{:?} must key lower than {:?}",
                    pair[0],
                    pair[1]
                );
            }
        }
    }

    #[test]
    fn a_client_has_no_epoch_and_keys_as_zero() {
        let client = Kind::Client(ClientProducer {});
        assert_eq!(epoch_key(Some(&client)), [0; EPOCH_KEY_LEN]);
        assert_eq!(epoch_key(None), [0; EPOCH_KEY_LEN]);
    }
}
