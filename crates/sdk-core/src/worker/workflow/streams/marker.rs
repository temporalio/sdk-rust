//! The Stream markers of an activation. A `core_stream_append` marker records the results of the
//! appends that Workflow code got in that activation, and a `core_stream_read` marker the pages of
//! its reads, without their Records.

use super::manager::failure_of;
use crate::worker::workflow::WFMachinesError;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashMap;
use temporalio_common::protos::{
    constants::{STREAM_APPEND_MARKER_NAME, STREAM_READ_MARKER_NAME},
    coresdk::{
        AsJsonPayloadExt, FromJsonPayloadExt,
        external_data::{
            StreamAppendMarkerData, StreamAppendResult, StreamFailure, StreamReadMarkerData,
            stream_append_result, stream_read_result,
        },
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

/// The data of a Stream marker in History.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PeekedMarker {
    Append(StreamAppendMarkerData),
    Read(StreamReadMarkerData),
}

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
    data: &impl Serialize,
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

pub(crate) fn is_read_marker(event: &HistoryEvent) -> bool {
    marker_of(event).is_some_and(|marker| marker.marker_name == STREAM_READ_MARKER_NAME)
}

/// Decodes a Stream marker event, and returns `None` for any other event. A marker that doesn't
/// decode, or that has a result which Core can't give Workflow code, is an error.
pub(crate) fn peeked_marker(event: &HistoryEvent) -> Option<Result<PeekedMarker, WFMachinesError>> {
    let marker = marker_of(event)?;
    Some(match marker.marker_name.as_str() {
        STREAM_READ_MARKER_NAME => decode_read(marker).map(PeekedMarker::Read),
        _ => decode_append(marker).map(PeekedMarker::Append),
    })
}

pub(crate) fn peeked_read_marker(
    event: &HistoryEvent,
) -> Option<Result<StreamReadMarkerData, WFMachinesError>> {
    marker_of(event)
        .filter(|marker| marker.marker_name == STREAM_READ_MARKER_NAME)
        .map(decode_read)
}

fn marker_of(event: &HistoryEvent) -> Option<&MarkerRecordedEventAttributes> {
    match event.attributes.as_ref()? {
        history_event::Attributes::MarkerRecordedEventAttributes(marker)
            if marker.marker_name == STREAM_APPEND_MARKER_NAME
                || marker.marker_name == STREAM_READ_MARKER_NAME =>
        {
            Some(marker)
        }
        _ => None,
    }
}

fn bad(why: String) -> WFMachinesError {
    WFMachinesError::Fatal(format!("The Stream marker {why}"))
}

fn data<T: DeserializeOwned>(marker: &MarkerRecordedEventAttributes) -> Result<T, WFMachinesError> {
    let payload = marker
        .details
        .get(MARKER_DATA_KEY)
        .and_then(|payloads| payloads.payloads.first())
        .ok_or_else(|| bad("has no data".to_string()))?;
    T::from_json_payload(payload).map_err(|error| bad(format!("doesn't decode: {error}")))
}

/// One entry of a marker, as the checks of a decode see it.
struct Entry<'a> {
    kind: &'static str,
    seq: u32,
    has_result: bool,
    failure: Option<&'a StreamFailure>,
    activation: u64,
}

fn decode_append(
    marker: &MarkerRecordedEventAttributes,
) -> Result<StreamAppendMarkerData, WFMachinesError> {
    let data: StreamAppendMarkerData = data(marker)?;
    check(data.appends.iter().map(|append| Entry {
        kind: "append",
        seq: append.seq,
        has_result: append.result.is_some(),
        failure: match &append.result {
            Some(stream_append_result::Result::Failure(failure)) => Some(failure),
            _ => None,
        },
        activation: append.activation_index,
    }))?;
    Ok(data)
}

fn decode_read(
    marker: &MarkerRecordedEventAttributes,
) -> Result<StreamReadMarkerData, WFMachinesError> {
    let data: StreamReadMarkerData = data(marker)?;
    check(data.reads.iter().map(|read| Entry {
        kind: "read",
        seq: read.seq,
        has_result: read.result.is_some(),
        failure: match &read.result {
            Some(stream_read_result::Result::Failure(failure)) => Some(failure),
            _ => None,
        },
        activation: read.activation_index,
    }))?;
    Ok(data)
}

fn check<'a>(entries: impl Iterator<Item = Entry<'a>> + Clone) -> Result<(), WFMachinesError> {
    if let Some(entry) = entries.clone().find(|entry| !entry.has_result) {
        return Err(bad(format!(
            "has no result of {} {}",
            entry.kind, entry.seq
        )));
    }
    // Core builds the failure of a call from its reason, and a later version can have a reason
    // that this one doesn't know.
    let mut failures = entries.clone().filter_map(|entry| entry.failure);
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
    let mut activations = entries.map(|entry| entry.activation);
    if let Some(activation) = activations.find(|at| *at > MAX_ACTIVATION) {
        return Err(bad(format!(
            "has an entry at activation {activation}, above the maximum of {MAX_ACTIVATION}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{super::manager::recorded, *};
    use serde_json::{Value, json};
    use temporalio_common::protos::coresdk::external_data::{
        StreamAppendOffsets, StreamReadPage, StreamReadResult,
    };
    use temporalio_streams::{AppendError, AppendReason};

    fn event(marker: &PeekedMarker) -> HistoryEvent {
        let (name, details) = match marker {
            PeekedMarker::Append(data) => (STREAM_APPEND_MARKER_NAME, marker_details(data)),
            PeekedMarker::Read(data) => (STREAM_READ_MARKER_NAME, marker_details(data)),
        };
        event_of(name, details.unwrap())
    }

    fn event_of(name: &str, details: HashMap<String, Payloads>) -> HistoryEvent {
        HistoryEvent {
            attributes: Some(history_event::Attributes::MarkerRecordedEventAttributes(
                MarkerRecordedEventAttributes {
                    marker_name: name.to_string(),
                    details,
                    ..Default::default()
                },
            )),
            ..Default::default()
        }
    }

    /// Returns the text of the `data` detail of `details`, and checks that it's the only detail.
    fn json_of(details: &HashMap<String, Payloads>) -> String {
        let keys: Vec<&str> = details.keys().map(String::as_str).collect();
        assert_eq!(keys, ["data"]);
        let encoding = |name: &str| HashMap::from([("encoding".to_string(), name.into())]);
        let [data] = &details["data"].payloads[..] else {
            panic!("The data is one payload");
        };
        assert_eq!(data.metadata, encoding("json/plain"));
        String::from_utf8(data.data.clone()).unwrap()
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
        let json = [
            r#"{"appends":[{"seq":3,"stream_id":"orders","store":"redis","activation_index":1,"#,
            r#""result":{"appended":{"first_offset":"000000000000002a","#,
            r#""last_offset":"000000000000002b"}}},"#,
            r#"{"seq":4,"stream_id":"audit","store":"redis","activation_index":1,"#,
            r#""result":{"failure":{"reason":"STREAM_ERROR_REASON_FENCED","#,
            r#""message":"A later attempt wrote"}}}]}"#,
        ];
        assert_eq!(json_of(&marker_details(&marker).unwrap()), json.concat());
        let marker = PeekedMarker::Append(marker);
        assert_eq!(peeked_marker(&event(&marker)), Some(Ok(marker)));

        let page = stream_read_result::Result::Page(StreamReadPage {
            record_count: 2,
            digest: vec![0xab; 16],
            record_bytes: 40,
            last_offset: offset(43),
            wait_cut: true,
        });
        let failed = stream_read_result::Result::Failure(StreamFailure {
            reason: "STREAM_ERROR_REASON_EXPIRED".to_string(),
            message: "Trimmed".to_string(),
        });
        let entry = |seq, from_offset, result| StreamReadResult {
            seq,
            stream_id: "orders".to_string(),
            store: "redis".to_string(),
            activation_index: 2,
            from_offset,
            result: Some(result),
        };
        let marker = StreamReadMarkerData {
            reads: vec![entry(5, offset(41), page), entry(6, vec![], failed)],
        };
        let json = [
            r#"{"reads":[{"seq":5,"stream_id":"orders","store":"redis","activation_index":2,"#,
            r#""from_offset":"0000000000000029","result":{"page":{"record_count":2,"#,
            r#""digest":"abababababababababababababababab","record_bytes":40,"#,
            r#""last_offset":"000000000000002b","wait_cut":true}}},"#,
            r#"{"seq":6,"stream_id":"orders","store":"redis","activation_index":2,"#,
            r#""from_offset":"","result":{"failure":{"#,
            r#""reason":"STREAM_ERROR_REASON_EXPIRED","message":"Trimmed"}}}]}"#,
        ];
        assert_eq!(json_of(&marker_details(&marker).unwrap()), json.concat());
        let marker = PeekedMarker::Read(marker);
        let event = event(&marker);
        assert_eq!(peeked_marker(&event), Some(Ok(marker.clone())));
        let PeekedMarker::Read(data) = marker else {
            unreachable!("The marker is a read marker");
        };
        assert_eq!(peeked_read_marker(&event), Some(Ok(data)));
    }

    // The server refuses an event over 2 MB, so the data of a marker of either kind can have at
    // most 1 MiB.
    #[test]
    fn a_marker_with_more_than_1_mib_of_data_fails_its_task() {
        fn check<T: Serialize>(marker: impl Fn(String) -> T) {
            let rest = json(&marker(String::new())).data.len();
            let full = marker("x".repeat(1024 * 1024 - rest));
            assert_eq!(json(&full).data.len(), 1024 * 1024);
            assert!(marker_details(&full).is_ok());
            let over = marker("x".repeat(1024 * 1024 - rest + 1));
            let error = "StreamMarkerTooLarge: The data of a Stream marker has 1048577 bytes, \
                         more than 1048576";
            assert_eq!(
                marker_details(&over),
                Err(WFMachinesError::Stream(error.to_string()))
            );
        }
        let failure = |message| StreamFailure {
            reason: "STREAM_ERROR_REASON_INVALID_ARGUMENT".to_string(),
            message,
        };
        check(|message| StreamAppendMarkerData {
            appends: vec![StreamAppendResult {
                seq: 1,
                stream_id: "s".to_string(),
                store: "redis".to_string(),
                activation_index: 0,
                result: Some(stream_append_result::Result::Failure(failure(message))),
            }],
        });
        check(|message| StreamReadMarkerData {
            reads: vec![StreamReadResult {
                seq: 1,
                stream_id: "s".to_string(),
                store: "redis".to_string(),
                activation_index: 0,
                from_offset: vec![],
                result: Some(stream_read_result::Result::Failure(failure(message))),
            }],
        });
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
        let read = StreamReadMarkerData {
            reads: vec![StreamReadResult {
                seq: 99,
                stream_id: "é\n\"".into(),
                store: "mem".into(),
                from_offset: vec![],
                activation_index: MAX_ACTIVATION,
                result: Some(stream_read_result::Result::Page(StreamReadPage {
                    last_offset: vec![0xff; 1024],
                    ..Default::default()
                })),
            }],
        };
        let cases = [
            (
                PeekedMarker::Append(append),
                "append",
                "/result/appended/first_offset",
            ),
            (PeekedMarker::Read(read), "read", "/from_offset"),
        ];
        for (marker, kind, offset) in cases {
            assert_eq!(peeked_marker(&event(&marker)), Some(Ok(marker.clone())));
            let (name, data) = match marker {
                PeekedMarker::Append(data) => {
                    (STREAM_APPEND_MARKER_NAME, serde_json::to_value(data))
                }
                PeekedMarker::Read(data) => (STREAM_READ_MARKER_NAME, serde_json::to_value(data)),
            };
            check_bad_entries(name, kind, offset, data.unwrap());
        }
    }

    fn check_bad_entries(name: &str, kind: &str, offset: &str, data: Value) {
        let key = format!("{kind}s");
        let entry = &data[&key][0];
        let event = |details| event_of(name, details);
        let details = |payload: Payload| HashMap::from([("data".to_string(), payload.into())]);
        let encoded = |entry: &Value| details(json(&json!({(&key): [entry]})));
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
            *broken.pointer_mut(offset).unwrap() = json!(hex);
            assert!(
                fatal(event(encoded(&broken))).starts_with("The Stream marker doesn't decode:")
            );
        }

        let mut broken = entry.clone();
        broken["result"] = Value::Null;
        assert_eq!(
            fatal(event(encoded(&broken))),
            format!("The Stream marker has no result of {kind} 99")
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
