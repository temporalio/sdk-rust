//! Size limits for Stream ids, Store names, Producer ids and offsets, and the checks that enforce
//! them. Callers choose the ids and Stores choose the offsets, so this crate treats both as opaque.

use crate::{
    AnyError, AnyReason,
    proto::{Producer, StreamRef, WorkflowTaskEpoch, producer::Kind},
};

/// The maximum length of a Stream id, in bytes.
pub const MAX_STREAM_ID_BYTES: usize = 4096;

/// The maximum length of a Producer id, in bytes. A Store keeps every Producer id for as long as
/// its Stream exists, so the id needs a bound.
pub const MAX_PRODUCER_ID_BYTES: usize = 4096;

/// The maximum length of an offset, in bytes. Offsets end up in Workflow History, so they need a
/// bound too.
pub const MAX_OFFSET_BYTES: usize = 1024;

/// The maximum length of a Store name in a Workflow call, in bytes. A marker records the Store of
/// each call, so the name needs a bound.
pub const MAX_STORE_NAME_BYTES: usize = 4096;

pub(crate) fn check_ref(stream: Option<&StreamRef>) -> Result<&StreamRef, AnyError> {
    let stream = stream.ok_or_else(|| AnyError::invalid("The request has no Stream ref"))?;
    check_id("Stream", &stream.id, MAX_STREAM_ID_BYTES)?;
    Ok(stream)
}

#[cfg(feature = "workflow")]
pub(crate) fn check_store_name(store: &str) -> Result<(), AnyError> {
    if store.len() > MAX_STORE_NAME_BYTES {
        return Err(AnyError::invalid(format!(
            "The Store name has {} bytes, more than {MAX_STORE_NAME_BYTES}",
            store.len()
        )));
    }
    Ok(())
}

fn check_id(of: &str, id: &str, max: usize) -> Result<(), AnyError> {
    if id.is_empty() {
        return Err(AnyError::invalid(format!("The {of} id is empty")));
    }
    if id.len() > max {
        return Err(AnyError::invalid(format!(
            "The {of} id has {} bytes, more than {max}",
            id.len()
        )));
    }
    Ok(())
}

pub(crate) fn check_offset(offset: &[u8], what: &str, reason: AnyReason) -> Result<(), AnyError> {
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

/// Checks that a Producer has an id and the fields that its kind requires. Stores and readers tell
/// a Producer's attempts apart by its id and epoch, so both have to be complete. The started time
/// of a Workflow Task is the one optional field, because the server doesn't always send it.
pub(crate) fn check_producer(producer: Option<&Producer>) -> Result<&Producer, AnyError> {
    let producer = producer.ok_or_else(|| AnyError::invalid("The request has no Producer"))?;
    check_id("Producer", &producer.id, MAX_PRODUCER_ID_BYTES)?;
    match producer.kind.as_ref() {
        Some(Kind::Workflow(workflow)) => {
            if workflow.run_id.is_empty() {
                return Err(missing("run id"));
            }
            let task = workflow.epoch.as_ref();
            check_task(task.ok_or_else(|| missing("Workflow Task as its epoch"))?)?;
        }
        Some(Kind::LocalActivity(local)) => {
            if local.run_id.is_empty() {
                return Err(missing("run id"));
            }
            let epoch = local.epoch.as_ref().ok_or_else(|| missing("epoch"))?;
            let task = epoch.task.as_ref();
            check_task(task.ok_or_else(|| missing("Workflow Task that dispatched it"))?)?;
            at_least_one(epoch.attempt, "Local Activity")?;
        }
        Some(Kind::WorkflowActivity(activity)) if activity.run_id.is_empty() => {
            return Err(missing("run id"));
        }
        Some(Kind::StandaloneActivity(activity)) if !activity.run_id.is_empty() => {
            return Err(AnyError::invalid(
                "The Producer of a Standalone Activity has a run id",
            ));
        }
        Some(Kind::WorkflowActivity(activity) | Kind::StandaloneActivity(activity)) => {
            let epoch = activity.epoch.as_ref().ok_or_else(|| missing("epoch"))?;
            if epoch.attempt_scheduled_time.is_none() {
                return Err(missing("attempt scheduled time"));
            }
            at_least_one(epoch.attempt, "Activity")?;
        }
        Some(Kind::Client(_)) => {}
        None => return Err(missing("kind")),
    }
    Ok(producer)
}

fn missing(field: &str) -> AnyError {
    AnyError::invalid(format!("The Producer has no {field}"))
}

fn at_least_one(attempt: i32, of: &str) -> Result<(), AnyError> {
    if attempt < 1 {
        return Err(AnyError::invalid(format!(
            "The {of} attempt is {attempt}, and it must be 1 or more"
        )));
    }
    Ok(())
}

fn check_task(task: &WorkflowTaskEpoch) -> Result<(), AnyError> {
    // Tasks are ordered by their started event id first, so an epoch without one would sort below
    // every real task.
    if task.started_event_id <= 0 {
        return Err(AnyError::invalid(
            "The Workflow Task of the epoch has no started event id",
        ));
    }
    at_least_one(task.attempt, "Workflow Task")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        proto::{
            ActivityEpoch, ActivityProducer, LocalActivityEpoch, LocalActivityProducer,
            WorkflowProducer,
        },
        test_store::client,
    };
    use prost_types::Timestamp;

    #[test]
    fn a_producer_needs_the_fields_of_its_kind() {
        let task = |started_event_id, attempt| {
            Some(WorkflowTaskEpoch {
                started_event_id,
                attempt,
                started_time: None,
            })
        };
        let workflow = |run_id: &str, epoch| Producer {
            id: "wf:r".to_string(),
            kind: Some(Kind::Workflow(WorkflowProducer {
                run_id: run_id.to_string(),
                epoch,
            })),
        };
        let local = |run_id: &str, epoch| Producer {
            id: "la:r:k".to_string(),
            kind: Some(Kind::LocalActivity(LocalActivityProducer {
                run_id: run_id.to_string(),
                epoch,
            })),
        };
        let dispatched = |task, attempt| Some(LocalActivityEpoch { task, attempt });
        let activity = |kind: fn(ActivityProducer) -> Kind, run_id: &str, epoch| Producer {
            id: "act:a".to_string(),
            kind: Some(kind(ActivityProducer {
                run_id: run_id.to_string(),
                epoch,
            })),
        };
        let standalone = |epoch| activity(Kind::StandaloneActivity, "", epoch);
        let scheduled = |attempt_scheduled_time, attempt| {
            Some(ActivityEpoch {
                attempt_scheduled_time,
                attempt,
            })
        };
        let time = Some(Timestamp::default());
        let least = [
            workflow("r", task(3, 1)),
            local("r", dispatched(task(3, 1), 1)),
            activity(Kind::WorkflowActivity, "r", scheduled(time, 1)),
            standalone(scheduled(time, 1)),
            client(),
        ];
        for producer in &least {
            assert_eq!(check_producer(Some(producer)), Ok(producer));
        }
        let started_event_id = "The Workflow Task of the epoch has no started event id";
        let cases = [
            (None, "The request has no Producer"),
            (Some(workflow("", task(3, 1))), "The Producer has no run id"),
            (
                Some(workflow("r", None)),
                "The Producer has no Workflow Task as its epoch",
            ),
            (Some(workflow("r", task(0, 1))), started_event_id),
            (
                Some(workflow("r", task(3, 0))),
                "The Workflow Task attempt is 0, and it must be 1 or more",
            ),
            (
                Some(Producer {
                    kind: None,
                    ..client()
                }),
                "The Producer has no kind",
            ),
            (
                Some(local("", dispatched(task(3, 1), 1))),
                "The Producer has no run id",
            ),
            (Some(local("r", None)), "The Producer has no epoch"),
            (
                Some(local("r", dispatched(None, 1))),
                "The Producer has no Workflow Task that dispatched it",
            ),
            (
                Some(local("r", dispatched(task(0, 1), 1))),
                started_event_id,
            ),
            (
                Some(local("r", dispatched(task(3, 1), 0))),
                "The Local Activity attempt is 0, and it must be 1 or more",
            ),
            (
                Some(activity(Kind::WorkflowActivity, "", scheduled(time, 1))),
                "The Producer has no run id",
            ),
            (
                Some(activity(Kind::StandaloneActivity, "r", scheduled(time, 1))),
                "The Producer of a Standalone Activity has a run id",
            ),
            (Some(standalone(None)), "The Producer has no epoch"),
            (
                Some(standalone(scheduled(None, 1))),
                "The Producer has no attempt scheduled time",
            ),
            (
                Some(standalone(scheduled(time, 0))),
                "The Activity attempt is 0, and it must be 1 or more",
            ),
        ];
        for (producer, message) in cases {
            let error = AnyError::invalid(message);
            assert_eq!(
                check_producer(producer.as_ref()),
                Err(error),
                "{producer:?}"
            );
        }
    }
}
