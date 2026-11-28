//! The `core_stream_append` marker of an activation, which records the results of appends that
//! Workflow code got in that activation.

use super::manager::failure_of;
use crate::worker::workflow::WFMachinesError;
use serde::Serialize;
use std::collections::HashMap;
use temporalio_common::protos::{
    constants::STREAM_APPEND_MARKER_NAME,
    coresdk::{
        AsJsonPayloadExt, FromJsonPayloadExt,
        external_data::{StreamAppendMarkerData, StreamAppendResult, stream_append_result},
        workflow_activation::{ResolveStreamAppend, StreamAppended, resolve_stream_append},
    },
    temporal::api::{
        common::v1::{Payload, Payloads},
        history::v1::{HistoryEvent, MarkerRecordedEventAttributes, history_event},
    },
};
use temporalio_streams::proto::StreamErrorReason;

/// The maximum activation index that Core takes from a marker entry. A replay adds one to the
/// index of an entry for each later activation, and the bound prevents an overflow of that sum.
const MAX_ACTIVATION: u64 = u32::MAX as u64;

/// The maximum size of the data of one marker, half the limit of the server for one event. A
/// marker holds each result of its activation, so many results with long Store messages can pass
/// it.
const MAX_MARKER_DATA_BYTES: usize = 1024 * 1024;

/// The key of the marker's details that holds its data. It can't change, because History keeps
/// the markers that earlier releases wrote.
const MARKER_DATA_KEY: &str = "data";

/// Returns the job that gives Workflow code the result of an append in a marker. A replay and a
/// live run both build their jobs from the entry, so both give Workflow code the same job for it.
pub(crate) fn append_job(entry: &StreamAppendResult) -> ResolveStreamAppend {
    let status = entry.result.as_ref().map(|result| match result {
        stream_append_result::Result::Appended(offsets) => {
            resolve_stream_append::Status::Appended(StreamAppended {
                first_offset: offsets.first_offset.clone(),
                last_offset: offsets.last_offset.clone(),
            })
        }
        stream_append_result::Result::Failure(failure) => {
            resolve_stream_append::Status::Failed(failure_of(failure))
        }
    });
    ResolveStreamAppend {
        seq: entry.seq,
        status,
    }
}

/// Returns `value` as the JSON payload of a marker. The data of a marker has no map, so its JSON
/// can always be written.
fn json(value: &impl Serialize) -> Payload {
    (value.as_json_payload()).expect("The data of a Stream marker has a JSON form")
}

/// Returns the details of the marker command that records `data`. Data over
/// [`MAX_MARKER_DATA_BYTES`] fails the Workflow Task, because the server would refuse its event.
pub(crate) fn marker_details(
    data: &StreamAppendMarkerData,
) -> Result<HashMap<String, Payloads>, WFMachinesError> {
    let payload = json(data);
    let size = payload.data.len();
    if size > MAX_MARKER_DATA_BYTES {
        return Err(WFMachinesError::Stream(format!(
            "StreamMarkerTooLarge: The data of a Stream marker has {size} bytes, more than \
             {MAX_MARKER_DATA_BYTES}"
        )));
    }
    Ok(HashMap::from([(
        MARKER_DATA_KEY.to_string(),
        payload.into(),
    )]))
}

pub(crate) fn is_stream_marker(event: &HistoryEvent) -> bool {
    marker_of(event).is_some()
}

/// Decodes a `core_stream_append` marker event, and returns `None` for any other event. A marker
/// that doesn't decode, or that has a result which Core can't give Workflow code, is an error.
pub(crate) fn peeked_marker(
    event: &HistoryEvent,
) -> Option<Result<StreamAppendMarkerData, WFMachinesError>> {
    marker_of(event).map(decode)
}

fn marker_of(event: &HistoryEvent) -> Option<&MarkerRecordedEventAttributes> {
    match event.attributes.as_ref()? {
        history_event::Attributes::MarkerRecordedEventAttributes(marker)
            if marker.marker_name == STREAM_APPEND_MARKER_NAME =>
        {
            Some(marker)
        }
        _ => None,
    }
}

fn decode(
    marker: &MarkerRecordedEventAttributes,
) -> Result<StreamAppendMarkerData, WFMachinesError> {
    let bad = |why: String| WFMachinesError::Fatal(format!("The Stream marker {why}"));
    let payload = marker
        .details
        .get(MARKER_DATA_KEY)
        .and_then(|payloads| payloads.payloads.first())
        .ok_or_else(|| bad("has no data".to_string()))?;
    let data = StreamAppendMarkerData::from_json_payload(payload)
        .map_err(|error| bad(format!("doesn't decode: {error}")))?;
    if let Some(append) = data.appends.iter().find(|append| append.result.is_none()) {
        return Err(bad(format!("has no result of append {}", append.seq)));
    }
    // Core builds the failure of a call from its reason, and a later version can have a reason
    // that this one doesn't know.
    let mut failures = (data.appends.iter()).filter_map(|append| match &append.result {
        Some(stream_append_result::Result::Failure(failure)) => Some(failure),
        _ => None,
    });
    if let Some(failure) =
        failures.find(|failure| StreamErrorReason::from_str_name(&failure.reason).is_none())
    {
        return Err(bad(format!(
            "has a failure with the reason `{}`, which this Worker doesn't know",
            failure.reason
        )));
    }
    // A replay sets its activation index to the activation of an entry, and each later activation
    // of the task adds one to it.
    let mut activations = data.appends.iter().map(|append| append.activation_index);
    if let Some(activation) = activations.find(|at| *at > MAX_ACTIVATION) {
        return Err(bad(format!(
            "has an entry at activation {activation}, above the maximum of {MAX_ACTIVATION}"
        )));
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::{super::manager::recorded, *};
    use serde_json::{Value, json};
    use temporalio_common::protos::coresdk::external_data::{StreamAppendOffsets, StreamFailure};
    use temporalio_streams::{AppendError, AppendReason};

    fn event(details: HashMap<String, Payloads>) -> HistoryEvent {
        HistoryEvent {
            attributes: Some(history_event::Attributes::MarkerRecordedEventAttributes(
                MarkerRecordedEventAttributes {
                    marker_name: STREAM_APPEND_MARKER_NAME.to_string(),
                    details,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    // History keeps the markers of earlier releases, so the JSON of the data and the key of its
    // detail can't change.
    #[test]
    fn the_details_of_a_known_marker_never_change() {
        let offset = |at: u64| at.to_be_bytes().to_vec();
        let appended = stream_append_result::Result::Appended(StreamAppendOffsets {
            first_offset: offset(42),
            last_offset: offset(43),
        });
        let fenced = AppendError::operation(AppendReason::Fenced, "A later attempt wrote");
        let failed = stream_append_result::Result::Failure(recorded(&fenced));
        let entry = |seq, stream_id: &str, result| StreamAppendResult {
            seq,
            stream_id: stream_id.to_string(),
            store: "redis".to_string(),
            activation_index: 1,
            result: Some(result),
        };
        let marker = StreamAppendMarkerData {
            appends: vec![entry(3, "orders", appended), entry(4, "audit", failed)],
        };

        let details = marker_details(&marker).unwrap();
        let keys: Vec<&str> = details.keys().map(String::as_str).collect();
        assert_eq!(keys, ["data"]);
        let encoding = |name: &str| HashMap::from([("encoding".to_string(), name.into())]);
        let [data] = &details["data"].payloads[..] else {
            panic!("The data is one payload");
        };
        assert_eq!(data.metadata, encoding("json/plain"));
        let json = [
            r#"{"appends":[{"seq":3,"stream_id":"orders","store":"redis","activation_index":1,"#,
            r#""result":{"appended":{"first_offset":"000000000000002a","#,
            r#""last_offset":"000000000000002b"}}},"#,
            r#"{"seq":4,"stream_id":"audit","store":"redis","activation_index":1,"#,
            r#""result":{"failure":{"reason":"STREAM_ERROR_REASON_FENCED","#,
            r#""message":"A later attempt wrote"}}}]}"#,
        ];
        assert_eq!(std::str::from_utf8(&data.data).unwrap(), json.concat());
        assert_eq!(peeked_marker(&event(details)), Some(Ok(marker)));
    }

    // The server refuses an event over 2 MB, so the data of a marker can have at most 1 MiB.
    #[test]
    fn a_marker_with_more_than_1_mib_of_data_fails_its_task() {
        let marker = |message: usize| StreamAppendMarkerData {
            appends: vec![StreamAppendResult {
                seq: 1,
                stream_id: "s".to_string(),
                store: "redis".to_string(),
                activation_index: 0,
                result: Some(stream_append_result::Result::Failure(StreamFailure {
                    reason: "STREAM_ERROR_REASON_INVALID_ARGUMENT".to_string(),
                    message: "x".repeat(message),
                })),
            }],
        };
        let rest = json(&marker(0)).data.len();
        let full = marker(1024 * 1024 - rest);
        assert_eq!(json(&full).data.len(), 1024 * 1024);
        assert!(marker_details(&full).is_ok());
        let over = marker(1024 * 1024 - rest + 1);
        let error = "StreamMarkerTooLarge: The data of a Stream marker has 1048577 bytes, more \
                     than 1048576";
        assert_eq!(
            marker_details(&over),
            Err(WFMachinesError::Stream(error.to_string()))
        );
    }

    // A marker records a failed call by the name of its reason, and a replay reads that name.
    #[test]
    fn the_names_of_the_reasons_never_change() {
        let names: Vec<&str> = (0..)
            .map_while(|number| StreamErrorReason::try_from(number).ok())
            .map(|reason| reason.as_str_name())
            .collect();
        let pinned = [
            "STREAM_ERROR_REASON_UNSPECIFIED",
            "STREAM_ERROR_REASON_NOT_FOUND",
            "STREAM_ERROR_REASON_EXPIRED",
            "STREAM_ERROR_REASON_FENCED",
            "STREAM_ERROR_REASON_FORMAT_TOO_NEW",
            "STREAM_ERROR_REASON_OFFSET_TOO_HIGH",
            "STREAM_ERROR_REASON_TOO_LARGE",
            "STREAM_ERROR_REASON_INVALID_ARGUMENT",
            "STREAM_ERROR_REASON_STORE_UNAVAILABLE",
            "STREAM_ERROR_REASON_UNSUPPORTED",
            "STREAM_ERROR_REASON_ALREADY_EXISTS",
            "STREAM_ERROR_REASON_STORE_CORRUPT",
        ];
        assert_eq!(names, pinned);
    }

    fn fatal(event: HistoryEvent) -> String {
        match peeked_marker(&event) {
            Some(Err(WFMachinesError::Fatal(message))) => message,
            other => panic!("expected a fatal marker error, got {other:?}"),
        }
    }

    #[test]
    fn marker_entries_accept_boundary_values_and_reject_invalid_data() {
        let append = StreamAppendMarkerData {
            appends: vec![StreamAppendResult {
                seq: 99,
                stream_id: "é\n\"".into(),
                store: "mem".into(),
                activation_index: MAX_ACTIVATION,
                result: Some(stream_append_result::Result::Appended(
                    StreamAppendOffsets {
                        first_offset: vec![],
                        last_offset: vec![0xff; 1024],
                    },
                )),
            }],
        };
        let details = marker_details(&append).unwrap();
        let data = serde_json::to_value(&append).unwrap();
        assert_eq!(peeked_marker(&event(details)), Some(Ok(append)));
        let entry = &data["appends"][0];
        let details = |payload: Payload| HashMap::from([("data".to_string(), payload.into())]);
        let encoded = |entry: &Value| details(json(&json!({"appends": [entry]})));
        assert_eq!(
            fatal(event(HashMap::new())),
            "The Stream marker has no data"
        );
        let mut missing_encoding = json(&data);
        missing_encoding.metadata.clear();
        let broken_json = Payload {
            data: b"{".to_vec(),
            ..json(&data)
        };
        for payload in [missing_encoding, broken_json] {
            assert!(
                fatal(event(details(payload))).starts_with("The Stream marker doesn't decode:")
            );
        }
        for hex in ["f", "gg"] {
            let mut broken = entry.clone();
            *broken.pointer_mut("/result/appended/first_offset").unwrap() = json!(hex);
            assert!(
                fatal(event(encoded(&broken))).starts_with("The Stream marker doesn't decode:")
            );
        }

        let mut broken = entry.clone();
        broken["result"] = Value::Null;
        assert_eq!(
            fatal(event(encoded(&broken))),
            "The Stream marker has no result of append 99"
        );
        let mut broken = entry.clone();
        broken["activation_index"] = json!(MAX_ACTIVATION + 1);
        assert_eq!(
            fatal(event(encoded(&broken))),
            "The Stream marker has an entry at activation 4294967296, above the maximum of \
             4294967295"
        );
        let mut broken = entry.clone();
        broken["result"] = json!({"failure": {"reason": "FUTURE", "message": "unknown"}});
        assert_eq!(
            fatal(event(encoded(&broken))),
            "The Stream marker has a failure with the reason `FUTURE`, which this Worker \
             doesn't know"
        );
    }
}
