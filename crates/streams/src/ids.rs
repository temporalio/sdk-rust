//! Size limits for Stream ids and offsets, and the checks for ids, offsets and Producers. Callers
//! choose the ids and Stores choose the offsets, so this crate treats both as opaque.

use crate::{
    AnyError, AnyReason,
    attempt::{Id, kind_ids},
    proto::{
        Producer, StandaloneActivityProducer, WorkflowActivityProducer, WorkflowTaskAttempt,
        producer::Kind,
    },
};

/// The maximum length of a Stream id, in bytes, as the API of the Stream service sets it.
pub const MAX_STREAM_ID_BYTES: usize = 4096;

/// The maximum nanos of a time in an attempt. [`attempt_key`](crate::attempt::attempt_key) compares
/// the seconds of a time before its nanos, which is the order of the times only when the nanos are
/// less than one second.
const MAX_NANOS: i32 = 999_999_999;

/// The maximum length of an offset, in bytes. Every Store keeps to it, since offsets go into
/// Workflow History and readers keep them.
pub const MAX_OFFSET_BYTES: usize = 1024;

/// Checks that a Stream id has 1 to [`MAX_STREAM_ID_BYTES`] bytes.
pub fn validate_stream_id(id: &str) -> Result<(), AnyError> {
    if id.is_empty() {
        return Err(AnyError::invalid_argument("The Stream id is empty"));
    }
    if id.len() > MAX_STREAM_ID_BYTES {
        return Err(AnyError::invalid_argument(format!(
            "The Stream id has {} bytes, more than {MAX_STREAM_ID_BYTES}",
            id.len()
        )));
    }
    Ok(())
}

/// Checks that an offset has at most [`MAX_OFFSET_BYTES`] bytes. The error has the given reason,
/// and its message starts with `what`, which says where the offset is from.
pub fn validate_offset(offset: &[u8], what: &str, reason: AnyReason) -> Result<(), AnyError> {
    if offset.len() > MAX_OFFSET_BYTES {
        return Err(AnyError::new(
            reason,
            format!(
                "{what} has {} bytes, more than {MAX_OFFSET_BYTES}",
                offset.len()
            ),
        ));
    }
    Ok(())
}

/// Checks the Producer of a write. It needs a kind, every text id of that kind, and the fields of
/// its attempt. A number id can be zero, which means that the SDK doesn't have it, but not
/// negative. Stores and readers tell Producers apart by their ids, and the attempts of a Producer
/// by the fields of the attempt, so both have to be complete. The started time of a Workflow Task
/// is the one optional field, because the server doesn't always send it. Each time of the attempt
/// must be at or after the Unix epoch and have nanos from 0 to 999,999,999.
pub(crate) fn validate_producer(producer: Option<&Producer>) -> Result<&Producer, AnyError> {
    let producer =
        producer.ok_or_else(|| AnyError::invalid_argument("The request has no Producer"))?;
    let kind = producer.kind.as_ref().ok_or_else(|| missing("kind"))?;
    for (name, id) in kind_ids(kind).1 {
        match id {
            Id::Text("") => return Err(missing(name)),
            Id::Number(number) if number < 0 => {
                return Err(AnyError::invalid_argument(format!(
                    "The {name} is {number}, and it must be 0 or more"
                )));
            }
            _ => {}
        }
    }
    match kind {
        Kind::Workflow(workflow) => {
            let task = workflow.attempt.as_ref();
            validate_task(task.ok_or_else(|| missing("Workflow Task attempt"))?)?;
        }
        Kind::LocalActivity(local) => {
            let attempt = local.attempt.as_ref().ok_or_else(|| missing("attempt"))?;
            let task = attempt.task.as_ref();
            validate_task(task.ok_or_else(|| missing("Workflow Task that dispatched it"))?)?;
            at_least_one(attempt.attempt, "Local Activity")?;
        }
        Kind::WorkflowActivity(WorkflowActivityProducer { attempt, .. })
        | Kind::StandaloneActivity(StandaloneActivityProducer { attempt, .. }) => {
            let attempt = attempt.as_ref().ok_or_else(|| missing("attempt"))?;
            let scheduled = attempt.attempt_scheduled_time.as_ref();
            let scheduled = scheduled.ok_or_else(|| missing("attempt scheduled time"))?;
            validate_time(scheduled.seconds, scheduled.nanos, "attempt scheduled time")?;
            at_least_one(attempt.attempt, "Activity")?;
        }
        Kind::Client(_) => {}
    }
    Ok(producer)
}

fn missing(field: &str) -> AnyError {
    AnyError::invalid_argument(format!("The Producer has no {field}"))
}

fn at_least_one(attempt_number: i32, of: &str) -> Result<(), AnyError> {
    if attempt_number < 1 {
        return Err(AnyError::invalid_argument(format!(
            "The {of} attempt number is {attempt_number}, and it must be 1 or more"
        )));
    }
    Ok(())
}

fn validate_task(task: &WorkflowTaskAttempt) -> Result<(), AnyError> {
    // Tasks are ordered by their started event id first, so an attempt without one would sort
    // below every real task.
    if task.started_event_id <= 0 {
        return Err(AnyError::invalid_argument(
            "The Workflow Task attempt has no started event id",
        ));
    }
    if let Some(started) = &task.started_time {
        validate_time(
            started.seconds,
            started.nanos,
            "started time of the Workflow Task",
        )?;
    }
    at_least_one(task.attempt, "Workflow Task")
}

fn validate_time(seconds: i64, nanos: i32, time: &str) -> Result<(), AnyError> {
    // Persisted attempt keys encode negative seconds as zero, which would misorder writes.
    if seconds < 0 {
        return Err(AnyError::invalid_argument(format!(
            "The {time} is before the Unix epoch"
        )));
    }
    if !(0..=MAX_NANOS).contains(&nanos) {
        return Err(AnyError::invalid_argument(format!(
            "The {time} has {nanos} nanos, and it must have 0 to {MAX_NANOS}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        proto::{ActivityAttempt, LocalActivityAttempt, LocalActivityProducer},
        test_store::{
            activity, client, local_activity, producer, set_ids, standalone_activity, task,
            with_scheduled_event_id, workflow,
        },
    };
    use prost_types::Timestamp;

    #[test]
    fn a_stream_id_has_1_to_the_maximum_bytes() {
        for len in [0, 1, MAX_STREAM_ID_BYTES, MAX_STREAM_ID_BYTES + 1] {
            let expected = match len {
                0 => Err(AnyError::invalid_argument("The Stream id is empty")),
                1..=MAX_STREAM_ID_BYTES => Ok(()),
                _ => Err(AnyError::invalid_argument(
                    "The Stream id has 4097 bytes, more than 4096",
                )),
            };
            assert_eq!(validate_stream_id(&"s".repeat(len)), expected);
        }
    }

    #[test]
    fn the_producer_of_a_write_needs_the_ids_and_the_attempt_of_its_kind() {
        let ids = ["n", "w", "r", "a"];
        // Each id of a kind, as its place in `ids` and the words of its error.
        let workflow_run = [(0, "namespace"), (1, "Workflow id"), (2, "run id")];
        let of_activity = [workflow_run.as_slice(), &[(3, "Activity id")]].concat();
        let kinds: [(Producer, &[(usize, &str)]); 6] = [
            (workflow(Some(task(3, 1, 0))), &workflow_run),
            (local_activity(task(3, 1, 0), 1), &of_activity),
            (activity(0, 0, 1), &of_activity),
            (with_scheduled_event_id(activity(0, 0, 1), 5), &of_activity),
            (
                standalone_activity(0, 0, 1),
                &[(0, "namespace"), (3, "Activity id"), (2, "run id")],
            ),
            (client(), &[(0, "namespace"), (3, "client id")]),
        ];
        for (producer, needed) in kinds {
            assert_eq!(validate_producer(Some(&producer)), Ok(&producer));
            for (at, id) in needed {
                let mut ids = ids;
                ids[*at] = "";
                let mut without = producer.clone();
                set_ids(&mut without, ids);
                let error = AnyError::invalid_argument(format!("The Producer has no {id}"));
                assert_eq!(validate_producer(Some(&without)), Err(error));
            }
        }

        let no_time = |started_event_id, attempt| {
            Some(WorkflowTaskAttempt {
                started_event_id,
                attempt,
                started_time: None,
            })
        };
        let local = |attempt| {
            producer(Kind::LocalActivity(LocalActivityProducer {
                attempt,
                ..Default::default()
            }))
        };
        let dispatched = |task, attempt| Some(LocalActivityAttempt { task, attempt });
        let standalone = |attempt| {
            producer(Kind::StandaloneActivity(StandaloneActivityProducer {
                attempt,
                ..Default::default()
            }))
        };
        let scheduled = |attempt_scheduled_time, attempt| {
            Some(ActivityAttempt {
                attempt_scheduled_time,
                attempt,
            })
        };
        let time = Some(Timestamp::default());
        let nanos = |nanos| Some(Timestamp { seconds: 1, nanos });
        let task_at = |started_time| {
            Some(WorkflowTaskAttempt {
                started_event_id: 3,
                attempt: 1,
                started_time,
            })
        };
        // The started time of a Workflow Task is optional.
        for producer in [workflow(no_time(3, 1)), local(dispatched(no_time(3, 1), 1))] {
            assert_eq!(validate_producer(Some(&producer)), Ok(&producer));
        }
        let started_event_id = "The Workflow Task attempt has no started event id";
        let cases = [
            (
                Some(standalone_activity(-1, 0, 1)),
                "The attempt scheduled time is before the Unix epoch",
            ),
            (
                Some(workflow(Some(task(1, 1, -1)))),
                "The started time of the Workflow Task is before the Unix epoch",
            ),
            (None, "The request has no Producer"),
            (
                Some(with_scheduled_event_id(activity(0, 0, 1), -1)),
                "The scheduled event id is -1, and it must be 0 or more",
            ),
            (Some(Producer::default()), "The Producer has no kind"),
            (
                Some(workflow(None)),
                "The Producer has no Workflow Task attempt",
            ),
            (Some(workflow(no_time(0, 1))), started_event_id),
            (
                Some(workflow(no_time(3, 0))),
                "The Workflow Task attempt number is 0, and it must be 1 or more",
            ),
            (Some(local(None)), "The Producer has no attempt"),
            (
                Some(local(dispatched(None, 1))),
                "The Producer has no Workflow Task that dispatched it",
            ),
            (Some(local(dispatched(no_time(0, 1), 1))), started_event_id),
            (
                Some(local(dispatched(no_time(3, 1), 0))),
                "The Local Activity attempt number is 0, and it must be 1 or more",
            ),
            (Some(standalone(None)), "The Producer has no attempt"),
            (
                Some(standalone(scheduled(None, 1))),
                "The Producer has no attempt scheduled time",
            ),
            (
                Some(standalone(scheduled(time, 0))),
                "The Activity attempt number is 0, and it must be 1 or more",
            ),
            (
                Some(standalone(scheduled(nanos(-1), 1))),
                "The attempt scheduled time has -1 nanos, and it must have 0 to 999999999",
            ),
            (
                Some(workflow(task_at(nanos(1_000_000_000)))),
                "The started time of the Workflow Task has 1000000000 nanos, and it must have 0 to \
                 999999999",
            ),
            (
                Some(local(dispatched(task_at(nanos(-1)), 1))),
                "The started time of the Workflow Task has -1 nanos, and it must have 0 to \
                 999999999",
            ),
        ];
        for (producer, message) in cases {
            let error = AnyError::invalid_argument(message);
            assert_eq!(
                validate_producer(producer.as_ref()),
                Err(error),
                "{producer:?}"
            );
        }
    }
}
