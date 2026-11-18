//! The golden vectors of the epoch keys. `examples/gen_vectors.rs` writes them from a second
//! implementation, so this test checks the keys against values that the crate did not compute.

use prost_types::Timestamp;
use serde_json::Value;
use temporalio_streams::{
    epoch::{epoch_key, task_key, task_of},
    proto::{
        ActivityEpoch, ActivityProducer, ClientProducer, LocalActivityEpoch, LocalActivityProducer,
        WorkflowProducer, WorkflowTaskEpoch, producer::Kind,
    },
};

fn text<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field].as_str().unwrap()
}

fn time(value: &Value) -> Option<Timestamp> {
    let pair = value.as_array()?;
    Some(Timestamp {
        seconds: pair[0].as_i64().unwrap(),
        nanos: pair[1].as_i64().unwrap() as i32,
    })
}

fn number(value: &Value, field: &str) -> i64 {
    value[field].as_i64().unwrap_or(0)
}

/// The Producer kind of a vector, from its flat fields. The case takes the fields of its kind.
/// `empty` has no case.
fn kind_of(kind: &str, value: &Value) -> Option<Kind> {
    let task = WorkflowTaskEpoch {
        started_event_id: number(value, "task_started_event_id"),
        attempt: number(value, "task_attempt") as i32,
        started_time: time(&value["task_started_time"]),
    };
    let attempt = number(value, "attempt") as i32;
    match kind {
        "workflow" => Some(Kind::Workflow(WorkflowProducer {
            epoch: Some(task),
            ..Default::default()
        })),
        "local_activity" => Some(Kind::LocalActivity(LocalActivityProducer {
            epoch: Some(LocalActivityEpoch {
                task: Some(task),
                attempt,
            }),
            ..Default::default()
        })),
        "activity" => Some(Kind::WorkflowActivity(ActivityProducer {
            epoch: Some(ActivityEpoch {
                attempt_scheduled_time: time(&value["attempt_scheduled_time"]),
                attempt,
            }),
            ..Default::default()
        })),
        "client" => Some(Kind::Client(ClientProducer {})),
        "empty" => None,
        other => panic!("unknown kind {other}"),
    }
}

#[test]
fn epoch_keys_match() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/epochs.json");
    let cases: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    for case in cases.as_array().unwrap() {
        let kind = kind_of(text(case, "kind"), &case["epoch"]);
        assert_eq!(epoch_key(kind.as_ref()).to_vec(), hex(text(case, "key")));
        assert_eq!(
            task_key(&task_of(kind.as_ref())).to_vec(),
            hex(text(case, "task_key"))
        );
    }
}

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}
