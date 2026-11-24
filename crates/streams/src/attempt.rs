//! The keys that a Store keeps for a Producer and for its attempts.
//!
//! [`producer_key`] identifies a Producer, so two keys are equal exactly when their Producers are
//! the same. [`attempt_key`] turns the attempt that made a write into a fixed-size key whose byte
//! order is the order of the attempts, so a Store can compare two attempts of the same Producer
//! with a plain byte comparison. The attempt keys of different Producers aren't comparable.

use crate::proto::{
    ClientProducer, LocalActivityProducer, Producer, StandaloneActivityProducer,
    WorkflowActivityProducer, WorkflowProducer, producer::Kind,
};

/// Encodes what identifies a Producer, which is its kind and the ids of that kind. Two Producers
/// are the same exactly when their keys are equal. The attempt isn't part of the key. Stores keep
/// these bytes, so the layout must never change. A new id of a kind would change every key of that
/// kind that a Store keeps.
///
/// The key has these fields:
///
/// 1. The proto field number of the kind in [`Producer`] (1 byte)
/// 2. Each id of the kind, in the order of its proto fields. An id has two parts:
///    - its length in bytes, as UTF-8, as a big-endian number (4 bytes)
///    - its bytes
///
/// The ids of each kind are:
///
/// - A Workflow has a namespace, a Workflow id and a run id.
/// - A Local Activity has a namespace, a Workflow id, a run id and an Activity id.
/// - An Activity that a Workflow scheduled has a namespace, a Workflow id, a run id and an
///   Activity id.
/// - A Standalone Activity has a namespace, an Activity id and a run id.
/// - A client has a namespace and an id.
///
/// Returns `None` for a Producer with no kind. A read can give one, for a Record whose Producer
/// has a kind that this build doesn't know.
pub fn producer_key(producer: &Producer) -> Option<Vec<u8>> {
    let (number, ids) = kind_ids(producer.kind.as_ref()?);
    let mut key = vec![number];
    for (_, id) in ids {
        key.extend((id.len() as u32).to_be_bytes());
        key.extend(id.as_bytes());
    }
    Some(key)
}

/// Returns the proto field number of `kind` in [`Producer`], and a list of ids as tuples of the
/// form (human readable id description, id value)
pub(crate) fn kind_ids(kind: &Kind) -> (u8, Vec<(&'static str, &str)>) {
    let (namespace_id, workflow, run, activity) =
        ("namespace", "Workflow id", "run id", "Activity id");
    // Each pattern has every field of its kind, so that a new id doesn't compile until it has its
    // place here.
    match kind {
        Kind::Workflow(WorkflowProducer {
            namespace,
            workflow_id,
            run_id,
            attempt: _,
        }) => (
            1,
            vec![
                (namespace_id, namespace.as_str()),
                (workflow, workflow_id.as_str()),
                (run, run_id.as_str()),
            ],
        ),
        Kind::LocalActivity(LocalActivityProducer {
            namespace,
            workflow_id,
            run_id,
            activity_id,
            attempt: _,
        }) => (
            2,
            vec![
                (namespace_id, namespace.as_str()),
                (workflow, workflow_id.as_str()),
                (run, run_id.as_str()),
                (activity, activity_id.as_str()),
            ],
        ),
        Kind::WorkflowActivity(WorkflowActivityProducer {
            namespace,
            workflow_id,
            run_id,
            activity_id,
            attempt: _,
        }) => (
            3,
            vec![
                (namespace_id, namespace.as_str()),
                (workflow, workflow_id.as_str()),
                (run, run_id.as_str()),
                (activity, activity_id.as_str()),
            ],
        ),
        Kind::StandaloneActivity(StandaloneActivityProducer {
            namespace,
            activity_id,
            run_id,
            attempt: _,
        }) => (
            4,
            vec![
                (namespace_id, namespace.as_str()),
                (activity, activity_id.as_str()),
                (run, run_id.as_str()),
            ],
        ),
        Kind::Client(ClientProducer { namespace, id }) => (
            5,
            vec![
                (namespace_id, namespace.as_str()),
                ("client id", id.as_str()),
            ],
        ),
    }
}

/// The length of an [`attempt_key`], in bytes. Stores keep these keys, so it must never change.
pub const ATTEMPT_KEY_LEN: usize = 40;

/// Encodes the attempt of a Producer as a fixed-size key. For two attempts of the same Producer,
/// the byte order of the keys is the order of the attempts. That holds when the nanos of each time
/// are from 0 to 999,999,999, and the runtime refuses a write with other nanos.
///
/// The key has these big-endian fields, in the order they're compared:
///
/// 1. The started event id of the Workflow Task (8 bytes)
/// 2. The attempt number of that Workflow Task (4 bytes)
/// 3. The seconds of the task's started time (8 bytes)
/// 4. The nanos of the task's started time (4 bytes)
/// 5. The seconds of the Activity attempt's scheduled time (8 bytes)
/// 6. The nanos of the Activity attempt's scheduled time (4 bytes)
/// 7. The attempt number of the Activity or Local Activity (4 bytes)
///
/// Fields 1 to 4 are set for a Workflow, where the task is the one that made the write, and for a
/// Local Activity, where it's the task that dispatched it. Fields 5 and 6 are set for an Activity,
/// and field 7 for an Activity or a Local Activity. A field that doesn't apply, or that is
/// negative, is zero, so a client's key is all zeros.
///
/// An Activity's attempts are ordered by the server's clock first. If that clock steps back
/// between two attempts, the later one gets the lower key, and a Store that fences refuses it
/// with [`AppendReason::Fenced`](crate::AppendReason::Fenced).
pub fn attempt_key(kind: Option<&Kind>) -> [u8; ATTEMPT_KEY_LEN] {
    let (task, scheduled, attempt_number) = match kind {
        Some(Kind::Workflow(workflow)) => (workflow.attempt, None, 0),
        Some(Kind::LocalActivity(local)) => {
            let attempt = local.attempt.unwrap_or_default();
            (attempt.task, None, attempt.attempt)
        }
        Some(
            Kind::WorkflowActivity(WorkflowActivityProducer { attempt, .. })
            | Kind::StandaloneActivity(StandaloneActivityProducer { attempt, .. }),
        ) => {
            let attempt = attempt.unwrap_or_default();
            (None, attempt.attempt_scheduled_time, attempt.attempt)
        }
        Some(Kind::Client(_)) | None => (None, None, 0),
    };
    let task = task.unwrap_or_default();
    let started = task.started_time.unwrap_or_default();
    let scheduled = scheduled.unwrap_or_default();
    let mut key = [0; ATTEMPT_KEY_LEN];
    key[..8].copy_from_slice(&be64(task.started_event_id));
    key[8..12].copy_from_slice(&be32(task.attempt));
    key[12..20].copy_from_slice(&be64(started.seconds));
    key[20..24].copy_from_slice(&be32(started.nanos));
    key[24..32].copy_from_slice(&be64(scheduled.seconds));
    key[32..36].copy_from_slice(&be32(scheduled.nanos));
    key[36..].copy_from_slice(&be32(attempt_number));
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
        ids::validate_producer,
        proto::WorkflowTaskAttempt,
        test_store::{
            activity, client, local_activity, set_ids, standalone_activity, task, workflow,
        },
    };
    use prost::Message;
    use prost_types::Timestamp;

    fn key(producer: &Producer) -> [u8; ATTEMPT_KEY_LEN] {
        attempt_key(producer.kind.as_ref())
    }

    /// The parts of an attempt, in the order that the attempts of a Producer compare:
    ///
    /// 1. The started event id of the Workflow Task
    /// 2. The attempt number of that Workflow Task
    /// 3. The started time of that task, in nanoseconds since 1970
    /// 4. The scheduled time of the Activity attempt, in nanoseconds since 1970
    /// 5. The attempt number of the Activity or the Local Activity
    type Order = (i64, i32, i128, i128, i32);

    #[test]
    fn for_the_attempts_that_the_runtime_takes_the_order_of_two_keys_is_their_order() {
        let mut workflows: Vec<(Order, Producer)> = vec![];
        let mut locals = vec![];
        let mut activities = vec![];
        let mut standalones = vec![];
        // Each grid has 1 and 256, which sort the other way round as little-endian bytes. It also
        // has values that the runtime refuses, such as nanos of one second or more.
        let attempts = [-1, 1, 256, i32::MAX];
        for seconds in [-1, 0, 1, 256, i64::MAX] {
            for nanos in [-1, 0, 1, 256, 999_999_999, 1_000_000_000, i32::MAX] {
                // A time before 1970 counts as 1970, as it does in the key.
                let at = i128::from(seconds.max(0)) * 1_000_000_000 + i128::from(nanos);
                for attempt in attempts {
                    let scheduled = (0, 0, 0, at, attempt);
                    activities.push((scheduled, activity(seconds, nanos, attempt)));
                    standalones.push((scheduled, standalone_activity(seconds, nanos, attempt)));
                    for started_event_id in [0, 1, 256, i64::MAX] {
                        let task = WorkflowTaskAttempt {
                            started_event_id,
                            attempt,
                            started_time: Some(Timestamp { seconds, nanos }),
                        };
                        let started = (started_event_id, attempt, at, 0, 0);
                        workflows.push((started, workflow(Some(task))));
                        for local in attempts {
                            let order = (started_event_id, attempt, at, 0, local);
                            locals.push((order, local_activity(task, local)));
                        }
                    }
                }
            }
        }
        for attempts in [workflows, locals, activities, standalones] {
            let taken: Vec<(Order, [u8; ATTEMPT_KEY_LEN])> = attempts
                .iter()
                .filter(|(_, producer)| validate_producer(Some(producer)).is_ok())
                .map(|(order, producer)| (*order, key(producer)))
                .collect();
            for (order, key) in &taken {
                for (other_order, other_key) in &taken {
                    assert_eq!(
                        key.cmp(other_key),
                        order.cmp(other_order),
                        "{order:?} and {other_order:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn two_producers_have_one_key_exactly_when_their_kind_and_ids_are_equal() {
        // Two ids of `a` and `aa` in either order have the same bytes when joined.
        let texts = ["", "a", "aa"];
        let mut producers = vec![];
        for attempt in [1, 2] {
            // The proto field number of each kind, and its ids in the order of their proto fields,
            // as places in the ids of `set_ids`.
            let kinds: [(u8, &[usize], Producer); 5] = [
                (1, &[0, 1, 2], workflow(Some(task(3, attempt, 0)))),
                (2, &[0, 1, 2, 3], local_activity(task(3, 1, 0), attempt)),
                (3, &[0, 1, 2, 3], activity(0, 0, attempt)),
                (4, &[0, 3, 2], standalone_activity(0, 0, attempt)),
                (5, &[0, 3], client()),
            ];
            for (number, order, mut producer) in kinds {
                for at in 0..81 {
                    let ids = [at, at / 3, at / 9, at / 27].map(|place| texts[place % 3]);
                    set_ids(&mut producer, ids);
                    let same: Vec<&str> = order.iter().map(|at| ids[*at]).collect();
                    producers.push(((number, same), producer_key(&producer).unwrap()));
                }
            }
        }
        for (same, key) in &producers {
            for (other_same, other_key) in &producers {
                assert_eq!(
                    key == other_key,
                    same == other_same,
                    "{same:?} and {other_same:?}"
                );
            }
        }
    }

    #[test]
    fn what_a_store_keeps_never_changes() {
        let task = WorkflowTaskAttempt {
            started_event_id: 8,
            attempt: 2,
            started_time: Some(Timestamp {
                seconds: 1_760_000_000,
                nanos: 5,
            }),
        };
        let hex =
            |bytes: &[u8]| -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() };
        // The ids `n`, `w`, `r` and `a` of the test Producers, each after its length.
        let (n, w, r, a) = ("000000016e", "0000000177", "0000000172", "0000000161");
        let activity_attempt =
            "0000000000000000000000000000000000000000000000000000000068e778013b9ac9ff00000002";
        let zero = "0".repeat(80);
        let keys = [
            (
                workflow(Some(task)),
                Some(format!("01{n}{w}{r}")),
                "0000000000000008000000020000000068e778000000000500000000000000000000000000000000",
            ),
            (
                local_activity(WorkflowTaskAttempt { attempt: 1, ..task }, 3),
                Some(format!("02{n}{w}{r}{a}")),
                "0000000000000008000000010000000068e778000000000500000000000000000000000000000003",
            ),
            (
                activity(1_760_000_001, 999_999_999, 2),
                Some(format!("03{n}{w}{r}{a}")),
                activity_attempt,
            ),
            (
                standalone_activity(1_760_000_001, 999_999_999, 2),
                Some(format!("04{n}{a}{r}")),
                activity_attempt,
            ),
            (client(), Some(format!("05{n}{a}")), zero.as_str()),
            (Producer::default(), None, zero.as_str()),
        ];
        for (producer, of_producer, of_attempt) in keys {
            let producer_key = producer_key(&producer);
            assert_eq!(
                producer_key.as_deref().map(hex),
                of_producer,
                "{producer:?}"
            );
            assert_eq!(hex(&key(&producer)), of_attempt, "{producer:?}");
            // The tag of the kind's field is the first byte of the encoded Producer.
            if let Some(key) = producer_key {
                assert_eq!(key[0], producer.encode_to_vec()[0] >> 3, "{producer:?}");
            }
        }
        // The length of an id counts its bytes as UTF-8, and `é` has two.
        let mut non_ascii = client();
        set_ids(&mut non_ascii, ["n", "", "", "é"]);
        let key = producer_key(&non_ascii).unwrap();
        assert_eq!(hex(&key), format!("05{n}00000002c3a9"));
    }
}
