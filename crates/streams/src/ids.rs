//! The checks of Stream ids and Producers, and the Redis key layout. A caller builds both kinds of
//! id, so the crate treats each as opaque text. `testdata/ids.json` has the shared vectors of the
//! key layout.
//!
//! ```text
//! <prefix>:{<stream id>}:meta     the create key, the fence, dedupe and close state
//! <prefix>:{<stream id>}:log      the log, one entry per Record
//! <prefix>:{<stream id>}:closed   the entry that a close adds, which wakes a waiting read
//! ```
//!
//! A key has no Temporal namespace, so a Stream id is unique in one Store and key prefix. The part
//! in braces is the Redis Cluster hash tag, so that the keys are in one slot for one script. Each
//! part is percent-encoded, so a `:` or a brace in an id can't make two Streams share a key.

use crate::{
    StreamError, StreamResult,
    epoch::ProducerKind,
    proto::{Producer, WorkflowTaskEpoch, producer::Kind},
};

/// The longest Stream id, in bytes.
pub const MAX_STREAM_ID_BYTES: usize = 4096;

/// The longest Producer id, in bytes. A caller builds the id from names that Temporal limits to
/// 1000 characters, so an id with two of them still fits. A Store keeps the id in one meta field of
/// each Producer for as long as the Stream lives. Each Record of a batch holds its Producer, so the
/// size check counts the Producer once for each Record against
/// [MAX_BATCH_BYTES](crate::store::MAX_BATCH_BYTES).
pub const MAX_PRODUCER_ID_BYTES: usize = 4096;

/// The key prefix when the Store options set none.
pub const DEFAULT_KEY_PREFIX: &str = "temporal-streams";

/// Percent-encodes every byte but the unreserved ones, as Python's `quote(text, safe="")` does.
pub fn encode_part(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Refuses an empty or too long Stream id.
pub fn check_stream_id(id: &str) -> StreamResult<()> {
    if id.is_empty() {
        return Err(StreamError::invalid("The Stream id is empty"));
    }
    if id.len() > MAX_STREAM_ID_BYTES {
        return Err(StreamError::invalid(format!(
            "The Stream id has {} bytes, more than {MAX_STREAM_ID_BYTES}",
            id.len()
        )));
    }
    Ok(())
}

/// Refuses a Producer with no id, an id over [MAX_PRODUCER_ID_BYTES], or no kind. The id is opaque
/// text, so the Store requires no shape of it. It names one Producer in one Stream. A Store keeps
/// the fence of a Producer by kind and id, and an epoch has no run. So the id of a run-based kind
/// must hold its run id. A caller that uses one id in two runs gets `FENCED` in the second run when
/// that run's epoch is lower.
pub fn check_producer(producer: &Producer) -> StreamResult<()> {
    if producer.id.is_empty() {
        return Err(StreamError::invalid("The Producer id is empty"));
    }
    if producer.id.len() > MAX_PRODUCER_ID_BYTES {
        return Err(StreamError::invalid(format!(
            "The Producer id has {} bytes, more than {MAX_PRODUCER_ID_BYTES}",
            producer.id.len()
        )));
    }
    ProducerKind::try_from(producer)?;
    Ok(())
}

fn missing(what: &str) -> StreamResult<()> {
    Err(StreamError::invalid(format!("The Producer has no {what}")))
}

fn at_least_one(attempt: i32, of: &str) -> StreamResult<()> {
    if attempt < 1 {
        return Err(StreamError::invalid(format!(
            "The {of} attempt is {attempt}, and it must be 1 or more"
        )));
    }
    Ok(())
}

/// Refuses a Workflow Task epoch with no started event id, or with an attempt below 1.
fn check_task(task: &WorkflowTaskEpoch) -> StreamResult<()> {
    // Without its started event, a later task would have the epoch of an earlier one.
    if task.started_event_id <= 0 {
        return Err(StreamError::invalid(
            "The Workflow Task of the epoch has no started event id",
        ));
    }
    at_least_one(task.attempt, "Workflow Task")
}

/// Refuses a Producer that lacks a field the API requires. A Workflow Producer only closes a
/// Stream, so it may send no epoch, but an epoch that it sends must order. A Workflow Task's
/// started time stays optional, since Core leaves it empty when the server gave the task none.
pub fn check_required(producer: &Producer) -> StreamResult<()> {
    match producer.kind.as_ref() {
        Some(Kind::Workflow(workflow)) => {
            if workflow.run_id.is_empty() {
                return missing("run id");
            }
            workflow.epoch.as_ref().map_or(Ok(()), check_task)
        }
        Some(Kind::LocalActivity(local)) => {
            if local.run_id.is_empty() {
                return missing("run id");
            }
            let Some(epoch) = local.epoch.as_ref() else {
                return missing("epoch");
            };
            let Some(task) = epoch.task.as_ref() else {
                return missing("Workflow Task that dispatched it");
            };
            check_task(task)?;
            at_least_one(epoch.attempt, "Local Activity")
        }
        Some(Kind::WorkflowActivity(activity) | Kind::StandaloneActivity(activity)) => {
            let Some(epoch) = activity.epoch.as_ref() else {
                return missing("epoch");
            };
            if epoch.attempt_scheduled_time.is_none() {
                return missing("attempt scheduled time");
            }
            at_least_one(epoch.attempt, "Activity")
        }
        Some(Kind::Client(_)) => Ok(()),
        None => missing("kind"),
    }
}

/// The keys of one Stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamKeys {
    base: String,
}

impl StreamKeys {
    /// The keys of `stream_id` under `prefix`.
    pub fn new(prefix: &str, stream_id: &str) -> Self {
        Self {
            base: format!("{}:{{{}}}", encode_part(prefix), encode_part(stream_id)),
        }
    }

    /// The hash with the format version, the fence, dedupe and close state.
    pub fn meta(&self) -> String {
        format!("{}:meta", self.base)
    }

    /// The log.
    pub fn log(&self) -> String {
        format!("{}:log", self.base)
    }

    /// The key that a close adds one entry to. A close writes no log entry, so a waiting read also
    /// waits on this key, and the close wakes it.
    pub fn closed(&self) -> String {
        format!("{}:closed", self.base)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Reason,
        proto::{
            ActivityEpoch, ActivityProducer, ClientProducer, LocalActivityEpoch,
            LocalActivityProducer, WorkflowProducer, WorkflowTaskEpoch,
        },
    };
    use prost_types::Timestamp;

    fn producer(kind: ProducerKind, id: &str, run_id: &str) -> Producer {
        let run_id = run_id.to_string();
        let kind = match kind {
            ProducerKind::Workflow => Kind::Workflow(WorkflowProducer {
                run_id,
                ..Default::default()
            }),
            ProducerKind::LocalActivity => Kind::LocalActivity(LocalActivityProducer {
                run_id,
                ..Default::default()
            }),
            ProducerKind::Activity => Kind::WorkflowActivity(ActivityProducer {
                run_id,
                ..Default::default()
            }),
            ProducerKind::StandaloneActivity => Kind::StandaloneActivity(ActivityProducer {
                run_id,
                ..Default::default()
            }),
            ProducerKind::Client => Kind::Client(ClientProducer::default()),
        };
        Producer {
            id: id.to_string(),
            kind: Some(kind),
        }
    }

    #[test]
    fn a_producer_id_is_any_non_empty_text_up_to_the_limit() {
        for id in ["x", "wf:a|b", "client:{é} %", "a:b:c:d:e:f"] {
            check_producer(&producer(ProducerKind::Client, id, "")).unwrap();
        }
        check_producer(&producer(
            ProducerKind::Workflow,
            &"a".repeat(MAX_PRODUCER_ID_BYTES),
            "r",
        ))
        .unwrap();
        let no_kind = Producer {
            kind: None,
            ..producer(ProducerKind::Workflow, "wf:x", "x")
        };
        let too_long = producer(
            ProducerKind::Client,
            &"a".repeat(MAX_PRODUCER_ID_BYTES + 1),
            "",
        );
        let empty = producer(ProducerKind::Client, "", "");
        for bad in [no_kind, too_long, empty] {
            assert_eq!(
                check_producer(&bad).unwrap_err().reason,
                Reason::Invalid,
                "{bad:?}"
            );
        }
    }

    #[test]
    fn a_stream_id_is_bounded() {
        check_stream_id("a").unwrap();
        check_stream_id(&"a".repeat(MAX_STREAM_ID_BYTES)).unwrap();
        assert!(check_stream_id("").is_err());
        assert!(check_stream_id(&"a".repeat(MAX_STREAM_ID_BYTES + 1)).is_err());
    }

    /// A Local Activity Producer with every field the API requires.
    fn local_activity() -> Producer {
        Producer {
            id: "la:r:k".to_string(),
            kind: Some(Kind::LocalActivity(LocalActivityProducer {
                run_id: "r".to_string(),
                epoch: Some(LocalActivityEpoch {
                    task: Some(WorkflowTaskEpoch {
                        started_event_id: 3,
                        attempt: 1,
                        started_time: None,
                    }),
                    attempt: 1,
                }),
            })),
        }
    }

    /// An Activity Producer with every field the API requires.
    fn activity() -> Producer {
        Producer {
            id: "act:r:a:s".to_string(),
            kind: Some(Kind::WorkflowActivity(ActivityProducer {
                run_id: "r".to_string(),
                epoch: Some(ActivityEpoch {
                    attempt_scheduled_time: Some(Timestamp::default()),
                    attempt: 1,
                }),
            })),
        }
    }

    /// A client Producer with every field the API requires.
    fn client() -> Producer {
        Producer {
            id: "client:n:p".to_string(),
            kind: Some(Kind::Client(ClientProducer {})),
        }
    }

    fn local_epoch(producer: &mut Producer) -> &mut LocalActivityEpoch {
        let Some(Kind::LocalActivity(local)) = producer.kind.as_mut() else {
            unreachable!()
        };
        local.epoch.as_mut().unwrap()
    }

    fn activity_epoch(producer: &mut Producer) -> &mut ActivityEpoch {
        let Some(Kind::WorkflowActivity(activity)) = producer.kind.as_mut() else {
            unreachable!()
        };
        activity.epoch.as_mut().unwrap()
    }

    fn refused(producer: &Producer) -> String {
        let error = check_required(producer).unwrap_err();
        assert_eq!(error.reason, Reason::Invalid);
        error.message
    }

    #[test]
    fn a_producer_with_its_required_fields_passes() {
        // A Workflow Producer only closes, with no epoch, and a standalone Activity has no run.
        let workflow = producer(ProducerKind::Workflow, "wf:r", "r");
        let mut standalone = activity();
        let Some(Kind::WorkflowActivity(kind)) = standalone.kind.take() else {
            unreachable!()
        };
        standalone.kind = Some(Kind::StandaloneActivity(ActivityProducer {
            run_id: String::new(),
            ..kind
        }));
        for producer in [workflow, local_activity(), activity(), standalone, client()] {
            check_required(&producer).unwrap();
        }
    }

    #[test]
    fn a_workflow_producer_needs_its_run_id() {
        let workflow = producer(ProducerKind::Workflow, "wf:", "");
        assert!(refused(&workflow).contains("run id"));
    }

    /// A Workflow Producer with `epoch`.
    fn workflow_with(epoch: WorkflowTaskEpoch) -> Producer {
        Producer {
            id: "wf:r".to_string(),
            kind: Some(Kind::Workflow(WorkflowProducer {
                run_id: "r".to_string(),
                epoch: Some(epoch),
            })),
        }
    }

    #[test]
    fn a_workflow_epoch_passes_with_its_required_fields() {
        let epoch = WorkflowTaskEpoch {
            started_event_id: 3,
            attempt: 1,
            started_time: None,
        };
        check_required(&workflow_with(epoch)).unwrap();
    }

    #[test]
    fn a_workflow_epoch_needs_its_started_event_id() {
        let epoch = WorkflowTaskEpoch {
            attempt: 1,
            ..Default::default()
        };
        assert!(refused(&workflow_with(epoch)).contains("started event id"));
    }

    #[test]
    fn a_workflow_epoch_attempt_is_1_or_more() {
        let epoch = WorkflowTaskEpoch {
            started_event_id: 3,
            ..Default::default()
        };
        assert!(refused(&workflow_with(epoch)).contains("Workflow Task attempt"));
    }

    #[test]
    fn a_local_activity_producer_needs_its_run_id() {
        let mut local = local_activity();
        let Some(Kind::LocalActivity(kind)) = local.kind.as_mut() else {
            unreachable!()
        };
        kind.run_id.clear();
        assert!(refused(&local).contains("run id"));
    }

    #[test]
    fn a_local_activity_producer_needs_its_epoch() {
        let mut local = local_activity();
        let Some(Kind::LocalActivity(kind)) = local.kind.as_mut() else {
            unreachable!()
        };
        kind.epoch = None;
        assert!(refused(&local).contains("epoch"));
    }

    #[test]
    fn a_local_activity_epoch_needs_its_workflow_task() {
        let mut local = local_activity();
        local_epoch(&mut local).task = None;
        assert!(refused(&local).contains("Workflow Task"));
    }

    #[test]
    fn a_local_activity_task_needs_its_started_event_id() {
        let mut local = local_activity();
        local_epoch(&mut local)
            .task
            .as_mut()
            .unwrap()
            .started_event_id = 0;
        assert!(refused(&local).contains("Workflow Task"));
    }

    #[test]
    fn a_local_activity_task_attempt_is_1_or_more() {
        let mut local = local_activity();
        local_epoch(&mut local).task.as_mut().unwrap().attempt = 0;
        assert!(refused(&local).contains("Workflow Task attempt"));
    }

    #[test]
    fn a_local_activity_attempt_is_1_or_more() {
        let mut local = local_activity();
        local_epoch(&mut local).attempt = 0;
        assert!(refused(&local).contains("Local Activity attempt"));
    }

    #[test]
    fn an_activity_producer_needs_its_epoch() {
        let mut act = activity();
        let Some(Kind::WorkflowActivity(kind)) = act.kind.as_mut() else {
            unreachable!()
        };
        kind.epoch = None;
        assert!(refused(&act).contains("epoch"));
    }

    #[test]
    fn an_activity_epoch_needs_its_scheduled_time() {
        let mut act = activity();
        activity_epoch(&mut act).attempt_scheduled_time = None;
        assert!(refused(&act).contains("scheduled time"));
    }

    #[test]
    fn an_activity_attempt_is_1_or_more() {
        let mut act = activity();
        activity_epoch(&mut act).attempt = 0;
        assert!(refused(&act).contains("Activity attempt"));
    }
}
