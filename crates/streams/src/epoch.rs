//! The order of Producer epochs.
//!
//! An epoch key has fixed-width big-endian fields in the order of comparison, so a Store orders
//! two epochs of one Producer by comparing their keys as bytes. Keys of different Producers don't
//! compare.

use crate::{
    StreamError, StreamResult,
    proto::{Producer, WorkflowTaskEpoch, producer::Kind},
};
use prost_types::Timestamp;

/// The kind of a Producer. Each kind has its own epoch fields.
///
/// The numbers must never change. A Store can persist them, and Workflow History can keep a digest
/// of them, so a changed number breaks the stored data and the replay of that History.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProducerKind {
    /// A Workflow run.
    Workflow = 1,
    /// A Local Activity.
    LocalActivity = 2,
    /// An Activity that a Workflow scheduled.
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

/// Fails for a Producer with no kind set.
impl TryFrom<&Producer> for ProducerKind {
    type Error = StreamError;

    fn try_from(producer: &Producer) -> StreamResult<Self> {
        kind_of(producer).ok_or_else(|| StreamError::invalid("The Producer has no kind"))
    }
}

/// The kind of a Producer, or `None` if it has no kind set.
pub fn kind_of(producer: &Producer) -> Option<ProducerKind> {
    producer.kind.as_ref().map(ProducerKind::from)
}

/// The length of a [`task_key`] in bytes.
pub const TASK_KEY_LEN: usize = 24;
/// The length of an [`epoch_key`] in bytes.
pub const EPOCH_KEY_LEN: usize = 40;

// Without the clamp, the two's-complement bytes of a negative field sort above every real epoch.
fn be64(value: i64) -> [u8; 8] {
    value.max(0).to_be_bytes()
}

fn be32(value: i32) -> [u8; 4] {
    value.max(0).to_be_bytes()
}

fn time_fields(time: Option<&Timestamp>) -> (i64, i32) {
    time.map_or((0, 0), |time| (time.seconds, time.nanos))
}

/// The key of a Workflow Task attempt. It has the started event id, the attempt, and the started
/// seconds and nanos, in 8, 4, 8 and 4 bytes. A negative field gives zero. The epoch keys of a
/// Workflow and of its Local Activities start with this key, so the same Local Activity call in a
/// later task gives a higher key.
pub fn task_key(task: &WorkflowTaskEpoch) -> [u8; TASK_KEY_LEN] {
    let (seconds, nanos) = time_fields(task.started_time.as_ref());
    let mut key = [0; TASK_KEY_LEN];
    key[..8].copy_from_slice(&be64(task.started_event_id));
    key[8..12].copy_from_slice(&be32(task.attempt));
    key[12..20].copy_from_slice(&be64(seconds));
    key[20..].copy_from_slice(&be32(nanos));
    key
}

/// The key of an epoch. It starts with the [`task_key`] of the task that [`task_of`] returns, then
/// has the attempt's scheduled seconds and nanos and the attempt, in 8, 4 and 4 bytes. A negative
/// field, or a field that the kind doesn't have, gives zero. So a client or a Producer with no
/// kind gives an all-zero key.
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
        // A client has no epoch, so nothing fences it.
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

/// The Workflow Task of an epoch. It's a Workflow's own task, or the task that scheduled a Local
/// Activity. Other kinds give a zero task.
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

    #[test]
    fn a_higher_epoch_gives_a_higher_key_for_each_kind_that_has_one() {
        let workflow = |task| {
            Kind::Workflow(WorkflowProducer {
                run_id: "r".to_string(),
                epoch: Some(task),
            })
        };
        let ascending = [
            // A later Workflow Task beats a higher attempt, and a higher attempt beats a later
            // started time.
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

    // A Store persists the key as its fence, so its bytes must never change.
    #[test]
    fn the_bytes_of_a_key_never_change() {
        let task = WorkflowTaskEpoch {
            started_event_id: 8,
            attempt: 2,
            started_time: at(1_760_000_000, 5),
        };
        let workflow = Kind::Workflow(WorkflowProducer {
            run_id: "r".to_string(),
            epoch: Some(task),
        });
        let local = local_activity(WorkflowTaskEpoch { attempt: 1, ..task }, 3);
        let activity = Kind::WorkflowActivity(activity(1_760_000_001, 999_999_999, 2));
        let client = Kind::Client(ClientProducer {});
        let zero = "0".repeat(80);
        let cases = [
            (
                Some(workflow),
                "0000000000000008000000020000000068e778000000000500000000000000000000000000000000",
            ),
            (
                Some(local),
                "0000000000000008000000010000000068e778000000000500000000000000000000000000000003",
            ),
            (
                Some(activity),
                "0000000000000000000000000000000000000000000000000000000068e778013b9ac9ff00000002",
            ),
            (Some(client), zero.as_str()),
            (None, zero.as_str()),
        ];
        for (kind, key) in cases {
            let hex: String = epoch_key(kind.as_ref())
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            assert_eq!(hex, key, "{kind:?}");
        }
    }
}
