//! Persistent keys for Producer identity and attempt ordering. Compare attempt keys only
//! within one Producer.

use crate::proto::{
    ClientProducer, LocalActivityProducer, Producer, StandaloneActivityProducer,
    WorkflowActivityProducer, WorkflowProducer, producer::Kind,
};

/// Encodes what identifies a Producer, which is its kind and the ids of that kind. Two Producers
/// are the same exactly when their keys are equal. The attempt isn't part of the key. Stores keep
/// these bytes, so the layout must never change.
///
/// The key has these fields:
///
/// 1. The proto field number of the kind in [`Producer`] (1 byte)
/// 2. Each id of the kind, in the order of its proto fields. An id has two parts:
///    - its length in bytes, as a big-endian number (4 bytes)
///    - its bytes, which are UTF-8 for a text id, and big-endian for a number id (8 bytes)
///
/// The key leaves out a number id of zero, which means that the SDK doesn't have the id. So adding
/// a number id to a kind doesn't change the keys of that kind that Stores keep.
///
/// The ids of each kind are:
///
/// - A Workflow has a namespace, a Workflow id and a run id.
/// - A Local Activity has a namespace, a Workflow id, a run id and an Activity id.
/// - An Activity that a Workflow scheduled has a namespace, a Workflow id, a run id, an Activity
///   id, and the id of its scheduled event as a number id.
/// - A Standalone Activity has a namespace, an Activity id and a run id.
/// - A client has a namespace and an id.
///
/// Returns `None` for a Producer with no kind. A read can give one, for a Record whose Producer
/// has a kind that this build doesn't know.
pub fn producer_key(producer: &Producer) -> Option<Vec<u8>> {
    let (number, ids) = kind_ids(producer.kind.as_ref()?);
    let mut key = vec![number];
    let mut push = |bytes: &[u8]| {
        key.extend((bytes.len() as u32).to_be_bytes());
        key.extend(bytes);
    };
    for (_, id) in ids {
        match id {
            Id::Text(text) => push(text.as_bytes()),
            Id::Number(0) => {}
            Id::Number(number) => push(&number.to_be_bytes()),
        }
    }
    Some(key)
}

/// One id of a Producer kind.
pub(crate) enum Id<'a> {
    Text(&'a str),
    Number(i64),
}

/// Returns the proto field number of `kind` in [`Producer`], and a list of ids as tuples of the
/// form (human readable id description, id value)
pub(crate) fn kind_ids(kind: &Kind) -> (u8, Vec<(&'static str, Id<'_>)>) {
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
                (namespace_id, Id::Text(namespace)),
                (workflow, Id::Text(workflow_id)),
                (run, Id::Text(run_id)),
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
                (namespace_id, Id::Text(namespace)),
                (workflow, Id::Text(workflow_id)),
                (run, Id::Text(run_id)),
                (activity, Id::Text(activity_id)),
            ],
        ),
        Kind::WorkflowActivity(WorkflowActivityProducer {
            namespace,
            workflow_id,
            run_id,
            activity_id,
            scheduled_event_id,
            attempt: _,
        }) => (
            3,
            vec![
                (namespace_id, Id::Text(namespace)),
                (workflow, Id::Text(workflow_id)),
                (run, Id::Text(run_id)),
                (activity, Id::Text(activity_id)),
                ("scheduled event id", Id::Number(*scheduled_event_id)),
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
                (namespace_id, Id::Text(namespace)),
                (activity, Id::Text(activity_id)),
                (run, Id::Text(run_id)),
            ],
        ),
        Kind::Client(ClientProducer { namespace, id }) => (
            5,
            vec![
                (namespace_id, Id::Text(namespace)),
                ("client id", Id::Text(id)),
            ],
        ),
    }
}

/// The length of an [`attempt_key`], in bytes. Stores keep these keys, so it must never change.
pub const ATTEMPT_KEY_LEN: usize = 28;

/// Encodes the attempt of a Producer as a fixed-size key. For two attempts of the same Producer,
/// the byte order of the keys is the order of the attempts. That holds when the nanos of each time
/// are from 0 to 999,999,999 and the time is at or after the Unix epoch, as required on writes.
///
/// The layout of the key depends on the kind of the Producer. A Producer has one kind, so all its
/// keys have one layout, and two layouts never meet in a comparison. The fields are big-endian, in
/// the order they're compared, and a field that doesn't apply, or that is negative, is zero.
///
/// For a Workflow, the task is the Workflow Task that made the write, and for a Local Activity,
/// it's the Workflow Task that dispatched it:
///
/// 1. The started event id of the task (8 bytes)
/// 2. The attempt number of the task (4 bytes)
/// 3. The seconds of the task's started time (8 bytes)
/// 4. The nanos of the task's started time (4 bytes)
/// 5. The attempt number of the Local Activity, which is zero for a Workflow (4 bytes)
///
/// For an Activity that a Workflow scheduled, and for a Standalone Activity:
///
/// 1. The seconds of the attempt's scheduled time (8 bytes)
/// 2. The nanos of the attempt's scheduled time (4 bytes)
/// 3. The attempt number (4 bytes)
/// 4. Zeros (12 bytes)
///
/// A client's key is all zeros.
///
/// An Activity's attempts are ordered by the server's clock first. If that clock steps back
/// between two attempts, the later one gets the lower key, and a Store that fences refuses it
/// with [`AppendReason::Fenced`](crate::AppendReason::Fenced).
pub fn attempt_key(kind: Option<&Kind>) -> [u8; ATTEMPT_KEY_LEN] {
    let mut key = [0; ATTEMPT_KEY_LEN];
    let (task, local_attempt) = match kind {
        Some(Kind::Workflow(workflow)) => (workflow.attempt.unwrap_or_default(), 0),
        Some(Kind::LocalActivity(local)) => {
            let attempt = local.attempt.unwrap_or_default();
            (attempt.task.unwrap_or_default(), attempt.attempt)
        }
        Some(
            Kind::WorkflowActivity(WorkflowActivityProducer { attempt, .. })
            | Kind::StandaloneActivity(StandaloneActivityProducer { attempt, .. }),
        ) => {
            let attempt = attempt.unwrap_or_default();
            let scheduled = attempt.attempt_scheduled_time.unwrap_or_default();
            key[..8].copy_from_slice(&be64(scheduled.seconds));
            key[8..12].copy_from_slice(&be32(scheduled.nanos));
            key[12..16].copy_from_slice(&be32(attempt.attempt));
            return key;
        }
        Some(Kind::Client(_)) | None => return key,
    };
    let started = task.started_time.unwrap_or_default();
    key[..8].copy_from_slice(&be64(task.started_event_id));
    key[8..12].copy_from_slice(&be32(task.attempt));
    key[12..20].copy_from_slice(&be64(started.seconds));
    key[20..24].copy_from_slice(&be32(started.nanos));
    key[24..].copy_from_slice(&be32(local_attempt));
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
            activity, client, local_activity, set_ids, standalone_activity, task,
            with_scheduled_event_id, workflow,
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
                let at = i128::from(seconds) * 1_000_000_000 + i128::from(nanos);
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
            let mut taken: Vec<_> = attempts
                .iter()
                .filter(|(_, producer)| validate_producer(Some(producer)).is_ok())
                .map(|(order, producer)| (*order, key(producer)))
                .collect();
            taken.sort_unstable_by_key(|(order, _)| *order);
            for ((order, key), (next_order, next_key)) in taken.iter().zip(taken.iter().skip(1)) {
                assert_eq!(
                    key.cmp(next_key),
                    order.cmp(next_order),
                    "{order:?} and {next_order:?}"
                );
            }
        }
    }

    #[test]
    fn two_producers_have_one_key_exactly_when_their_kind_and_ids_are_equal() {
        // Two ids of `a` and `aa` in either order have the same bytes when joined.
        let texts = ["", "a", "aa"];
        let mut producers = vec![];
        for attempt in [1, 2] {
            // The proto field number of each kind, and its text ids in the order of their proto
            // fields, as places in the ids of `set_ids`.
            let kinds: [(u8, &[usize], Producer); 5] = [
                (1, &[0, 1, 2], workflow(Some(task(3, attempt, 0)))),
                (2, &[0, 1, 2, 3], local_activity(task(3, 1, 0), attempt)),
                (3, &[0, 1, 2, 3], activity(0, 0, attempt)),
                (4, &[0, 3, 2], standalone_activity(0, 0, attempt)),
                (5, &[0, 3], client()),
            ];
            for (number, order, mut producer) in kinds {
                // Only a Workflow's Activity has a scheduled event id, and zero means that the SDK
                // doesn't have it.
                let event_ids: &[i64] = if number == 3 { &[0, 1, -1] } else { &[0] };
                for at in 0..81 {
                    let ids = [at, at / 3, at / 9, at / 27].map(|place| texts[place % 3]);
                    set_ids(&mut producer, ids);
                    let same: Vec<&str> = order.iter().map(|at| ids[*at]).collect();
                    for event_id in event_ids {
                        if number == 3 {
                            producer = with_scheduled_event_id(producer, *event_id);
                        }
                        let key = producer_key(&producer).unwrap();
                        producers.push(((number, same.clone(), *event_id), key));
                    }
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
        // The ids `n`, `w`, `r` and `a` of the test Producers, and the scheduled event id 5, each
        // after its length.
        let (n, w, r, a) = ("000000016e", "0000000177", "0000000172", "0000000161");
        let e = "000000080000000000000005";
        let activity_attempt = "0000000068e778013b9ac9ff00000002000000000000000000000000";
        let zero = "0".repeat(56);
        let keys = [
            (
                workflow(Some(task)),
                Some(format!("01{n}{w}{r}")),
                "0000000000000008000000020000000068e778000000000500000000",
            ),
            (
                local_activity(WorkflowTaskAttempt { attempt: 1, ..task }, 3),
                Some(format!("02{n}{w}{r}{a}")),
                "0000000000000008000000010000000068e778000000000500000003",
            ),
            (
                activity(1_760_000_001, 999_999_999, 2),
                Some(format!("03{n}{w}{r}{a}")),
                activity_attempt,
            ),
            (
                with_scheduled_event_id(activity(1_760_000_001, 999_999_999, 2), 5),
                Some(format!("03{n}{w}{r}{a}{e}")),
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
