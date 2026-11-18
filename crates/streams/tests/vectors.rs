//! Checks the epoch keys against `testdata/epochs.json`. `examples/gen_vectors.rs` writes that
//! file without the crate's code, so the expected keys come from a second implementation.

use prost_types::Timestamp;
use serde::Deserialize;
use temporalio_streams::{
    epoch::{epoch_key, task_key, task_of},
    proto::{
        ActivityEpoch, ActivityProducer, ClientProducer, LocalActivityEpoch, LocalActivityProducer,
        WorkflowProducer, WorkflowTaskEpoch, producer::Kind,
    },
};

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CaseKind {
    Workflow,
    LocalActivity,
    Activity,
    Client,
    Empty,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Epoch {
    task_started_event_id: i64,
    task_attempt: i32,
    task_started_time: Option<(i64, i32)>,
    attempt_scheduled_time: Option<(i64, i32)>,
    attempt: i32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    kind: CaseKind,
    epoch: Epoch,
    key: String,
    task_key: String,
}

fn timestamp(time: Option<(i64, i32)>) -> Option<Timestamp> {
    time.map(|(seconds, nanos)| Timestamp { seconds, nanos })
}

impl Case {
    fn producer_kind(&self) -> Option<Kind> {
        let epoch = &self.epoch;
        let task = WorkflowTaskEpoch {
            started_event_id: epoch.task_started_event_id,
            attempt: epoch.task_attempt,
            started_time: timestamp(epoch.task_started_time),
        };
        match self.kind {
            CaseKind::Workflow => Some(Kind::Workflow(WorkflowProducer {
                epoch: Some(task),
                ..Default::default()
            })),
            CaseKind::LocalActivity => Some(Kind::LocalActivity(LocalActivityProducer {
                epoch: Some(LocalActivityEpoch {
                    task: Some(task),
                    attempt: epoch.attempt,
                }),
                ..Default::default()
            })),
            CaseKind::Activity => Some(Kind::WorkflowActivity(ActivityProducer {
                epoch: Some(ActivityEpoch {
                    attempt_scheduled_time: timestamp(epoch.attempt_scheduled_time),
                    attempt: epoch.attempt,
                }),
                ..Default::default()
            })),
            CaseKind::Client => Some(Kind::Client(ClientProducer {})),
            CaseKind::Empty => None,
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn epoch_keys_match() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/epochs.json");
    let cases: Vec<Case> = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    for case in cases {
        let kind = case.producer_kind();
        assert_eq!(hex(&epoch_key(kind.as_ref())), case.key);
        assert_eq!(hex(&task_key(&task_of(kind.as_ref()))), case.task_key);
    }
}
