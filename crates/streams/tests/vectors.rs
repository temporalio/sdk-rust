//! The shared test vectors, which `examples/gen_vectors.rs` writes from a second implementation.
//! The crate's README says which other side reads each file.

use prost_types::Timestamp;
use serde_json::Value;
use temporalio_common::protos::temporal::api::common::v1::{
    Payload, payload::ExternalPayloadDetails,
};
use temporalio_streams::{
    digest::batch_digest,
    epoch::{epoch_key, task_key, task_of},
    ids::StreamKeys,
    offset::{entry_id_of_offset, offset_of_entry_id},
    proto::{
        ActivityEpoch, ActivityProducer, AppendRecord, ClientProducer, LocalActivityEpoch,
        LocalActivityProducer, WorkflowProducer, WorkflowTaskEpoch, producer::Kind,
    },
};

fn load(name: &str) -> Value {
    let path = format!("{}/testdata/{name}", env!("CARGO_MANIFEST_DIR"));
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

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

/// The Producer kind of a vector, from its flat fields and its run. The case takes the fields of
/// its kind, and a run-based kind carries the run. `empty` has no case.
fn kind_of(kind: &str, value: &Value, run_id: &str) -> Option<Kind> {
    let task = WorkflowTaskEpoch {
        started_event_id: number(value, "task_started_event_id"),
        attempt: number(value, "task_attempt") as i32,
        started_time: time(&value["task_started_time"]),
    };
    let attempt = number(value, "attempt") as i32;
    let run_id = run_id.to_string();
    let activity = ActivityProducer {
        run_id: run_id.clone(),
        epoch: Some(ActivityEpoch {
            attempt_scheduled_time: time(&value["attempt_scheduled_time"]),
            attempt,
        }),
    };
    match kind {
        "workflow" => Some(Kind::Workflow(WorkflowProducer {
            run_id,
            epoch: Some(task),
        })),
        "local_activity" => Some(Kind::LocalActivity(LocalActivityProducer {
            run_id,
            epoch: Some(LocalActivityEpoch {
                task: Some(task),
                attempt,
            }),
        })),
        "activity" => Some(Kind::WorkflowActivity(activity)),
        "standalone_activity" => Some(Kind::StandaloneActivity(activity)),
        "client" => Some(Kind::Client(ClientProducer {})),
        "empty" => None,
        other => panic!("unknown kind {other}"),
    }
}

#[test]
fn stream_ids_and_keys_match() {
    let vectors = load("ids.json");
    for case in vectors["streams"].as_array().unwrap() {
        let keys = StreamKeys::new(text(case, "prefix"), text(case, "stream_id"));
        assert_eq!(keys.meta(), text(case, "meta"));
        assert_eq!(keys.log(), text(case, "log"));
        assert_eq!(keys.closed(), text(case, "closed"));
    }
}

#[test]
fn offsets_match() {
    for case in load("offsets.json").as_array().unwrap() {
        let offset = number(case, "offset");
        assert_eq!(offset_of_entry_id(text(case, "entry_id")).unwrap(), offset);
        assert_eq!(entry_id_of_offset(offset), text(case, "entry_id"));
    }
}

#[test]
fn epoch_keys_match() {
    for case in load("epochs.json").as_array().unwrap() {
        let kind = kind_of(text(case, "kind"), &case["epoch"], "");
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

#[test]
fn batch_digests_match() {
    for case in load("digests.json").as_array().unwrap() {
        let records: Vec<_> = case["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| AppendRecord {
                // A record with none of these keys has no body.
                body: ["metadata", "data", "external_payloads"]
                    .iter()
                    .any(|key| r.get(key).is_some())
                    .then(|| Payload {
                        metadata: r["metadata"]
                            .as_object()
                            .into_iter()
                            .flatten()
                            .map(|(key, value)| (key.clone(), hex(value.as_str().unwrap())))
                            .collect(),
                        data: hex(r["data"].as_str().unwrap_or_default()),
                        external_payloads: r["external_payloads"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .map(|size| ExternalPayloadDetails {
                                size_bytes: size.as_i64().unwrap(),
                            })
                            .collect(),
                    }),
            })
            .collect();
        assert_eq!(
            batch_digest(&records).to_vec(),
            hex(text(case, "digest")),
            "{}",
            text(case, "name")
        );
    }
}
