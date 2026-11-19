//! Defines Producer epochs and epoch key construction.
//!
//! An epoch is one attempt of a Producer. [`epoch_key`] turns it into a fixed-size key whose byte
//! order is the order of the attempts, so a Store can compare two epochs of the same Producer
//! with a plain byte comparison. Keys of different Producers aren't comparable.

use crate::{
    StreamError, StreamResult,
    proto::{Producer, producer::Kind},
};

/// The kind of a Producer. Stores may persist these numbers, so they must never change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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
            Kind::Client(_) => Self::Client,
            Kind::StandaloneActivity(_) => Self::StandaloneActivity,
        }
    }
}

/// Fails for a Producer with no kind.
impl TryFrom<&Producer> for ProducerKind {
    type Error = StreamError;

    fn try_from(producer: &Producer) -> StreamResult<Self> {
        let kind = producer.kind.as_ref();
        Ok(kind
            .ok_or_else(|| StreamError::invalid("The Producer has no kind"))?
            .into())
    }
}

/// The length of an [`epoch_key`], in bytes.
pub const EPOCH_KEY_LEN: usize = 40;

/// Encodes a Producer's epoch as a fixed-size key. For two epochs of the same Producer, the byte
/// order of the keys is the order of the attempts.
///
/// The key has these big-endian fields, in the order they're compared:
///
/// 1. The started event id of the Workflow Task (8 bytes)
/// 2. The attempt of that Workflow Task (4 bytes)
/// 3. The seconds of the task's started time (8 bytes)
/// 4. The nanos of the task's started time (4 bytes)
/// 5. The seconds of the Activity attempt's scheduled time (8 bytes)
/// 6. The nanos of the Activity attempt's scheduled time (4 bytes)
/// 7. The attempt of the Activity or Local Activity (4 bytes)
///
/// Fields 1 to 4 are set for a Workflow, where the task is the one that made the write, and for a
/// Local Activity, where it's the task that dispatched it. Fields 5 and 6 are set for an Activity,
/// and field 7 for an Activity or a Local Activity. A field that doesn't apply, or that is
/// negative, is zero, so a client's key is all zeros.
///
/// An Activity's attempts are ordered by the server's clock first. If that clock steps back
/// between two attempts, the newer one gets the lower key, and a Store that fences refuses it
/// with [`Reason::Fenced`](crate::Reason::Fenced).
pub fn epoch_key(kind: Option<&Kind>) -> [u8; EPOCH_KEY_LEN] {
    let (task, scheduled, attempt) = match kind {
        Some(Kind::Workflow(workflow)) => (workflow.epoch, None, 0),
        Some(Kind::LocalActivity(local)) => {
            let epoch = local.epoch.unwrap_or_default();
            (epoch.task, None, epoch.attempt)
        }
        Some(Kind::WorkflowActivity(activity) | Kind::StandaloneActivity(activity)) => {
            let epoch = activity.epoch.unwrap_or_default();
            (None, epoch.attempt_scheduled_time, epoch.attempt)
        }
        Some(Kind::Client(_)) | None => (None, None, 0),
    };
    let task = task.unwrap_or_default();
    let started = task.started_time.unwrap_or_default();
    let scheduled = scheduled.unwrap_or_default();
    let mut key = [0; EPOCH_KEY_LEN];
    key[..8].copy_from_slice(&be64(task.started_event_id));
    key[8..12].copy_from_slice(&be32(task.attempt));
    key[12..20].copy_from_slice(&be64(started.seconds));
    key[20..24].copy_from_slice(&be32(started.nanos));
    key[24..32].copy_from_slice(&be64(scheduled.seconds));
    key[32..36].copy_from_slice(&be32(scheduled.nanos));
    key[36..].copy_from_slice(&be32(attempt));
    key
}

// The big-endian bytes of a negative number sort above every real value, so clamp it to zero.
fn be64(value: i64) -> [u8; 8] {
    value.max(0).to_be_bytes()
}

fn be32(value: i32) -> [u8; 4] {
    value.max(0).to_be_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        proto::WorkflowTaskEpoch,
        test_store::{activity, client, local_activity, standalone_activity, task, workflow},
    };
    use prost_types::Timestamp;

    fn key(producer: &Producer) -> [u8; EPOCH_KEY_LEN] {
        epoch_key(producer.kind.as_ref())
    }

    #[test]
    fn a_higher_epoch_gives_a_higher_key() {
        let activities = [activity, standalone_activity].map(|build| {
            vec![
                build(100, 0, 1),
                build(100, 0, 2),
                build(100, 5, 1),
                build(101, 0, 1),
            ]
        });
        let ascending = [
            vec![
                workflow(Some(task(8, 1, 0))),
                workflow(Some(task(8, 1, 100))),
                workflow(Some(task(8, 2, 0))),
                workflow(Some(task(9, 1, 0))),
            ],
            vec![
                local_activity(task(8, 1, 0), 1),
                local_activity(task(8, 1, 0), 2),
                local_activity(task(8, 1, 100), 1),
                local_activity(task(8, 2, 0), 1),
                local_activity(task(9, 1, 0), 1),
            ],
        ];
        for producers in ascending.iter().chain(&activities) {
            for pair in producers.windows(2) {
                assert!(key(&pair[0]) < key(&pair[1]), "{pair:?}");
            }
        }
        let negative = [
            (
                workflow(Some(task(8, 1, -1))),
                workflow(Some(task(8, 1, 0))),
            ),
            (activity(-1, 0, 1), activity(0, 0, 1)),
        ];
        for (negative, real) in negative {
            assert!(key(&negative) <= key(&real), "{negative:?}");
        }
    }

    #[test]
    fn what_a_store_keeps_never_changes() {
        let task = WorkflowTaskEpoch {
            started_event_id: 8,
            attempt: 2,
            started_time: Some(Timestamp {
                seconds: 1_760_000_000,
                nanos: 5,
            }),
        };
        let kinds = [
            workflow(Some(task)),
            local_activity(task, 1),
            activity(1, 0, 1),
            client(),
            standalone_activity(1, 0, 1),
        ]
        .map(|producer| ProducerKind::try_from(&producer).unwrap() as u8);
        assert_eq!(kinds, [1, 2, 3, 4, 5]);
        let zero = "0".repeat(80);
        let keys = [
            (
                workflow(Some(task)),
                "0000000000000008000000020000000068e778000000000500000000000000000000000000000000",
            ),
            (
                local_activity(WorkflowTaskEpoch { attempt: 1, ..task }, 3),
                "0000000000000008000000010000000068e778000000000500000000000000000000000000000003",
            ),
            (
                activity(1_760_000_001, 999_999_999, 2),
                "0000000000000000000000000000000000000000000000000000000068e778013b9ac9ff00000002",
            ),
            (client(), zero.as_str()),
            (Producer::default(), zero.as_str()),
        ];
        for (producer, expected) in keys {
            let hex: String = key(&producer)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            assert_eq!(hex, expected, "{producer:?}");
        }
    }
}
