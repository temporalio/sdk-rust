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

/// Returns true if `event` is a `core_stream_append` marker.
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
    use super::super::manager::recorded;
    use super::*;
    use rand::{RngExt, SeedableRng, rngs::StdRng};
    use temporalio_common::protos::coresdk::external_data::{StreamAppendOffsets, StreamFailure};
    use temporalio_streams::{AppendError, AppendReason, StreamError};

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

    /// Returns text of 1 to `max` characters that JSON escapes or has in several bytes, such as a
    /// quote, a control character or a character of another script.
    fn text(rng: &mut StdRng, max: usize) -> String {
        let chars = [
            'a', '"', '\\', '/', '\n', '\0', '\u{7f}', 'é', '€', '𝄞', '\u{2028}',
        ];
        (0..rng.random_range(1..=max))
            .map(|_| chars[rng.random_range(0..chars.len())])
            .collect()
    }

    /// Returns an offset of 0 to 1024 bytes of any value.
    fn offset(rng: &mut StdRng) -> Vec<u8> {
        let len = match rng.random_range(0..10) {
            0 => 0,
            1 => 1024,
            _ => rng.random_range(1..=16),
        };
        (0..len).map(|_| rng.random()).collect()
    }

    /// Returns the failure of a call, as the manager records it for an error of any reason.
    fn failure(rng: &mut StdRng) -> StreamFailure {
        let reasons: Vec<StreamErrorReason> = (0..)
            .map_while(|number| StreamErrorReason::try_from(number).ok())
            .collect();
        let reason = reasons[rng.random_range(0..reasons.len())];
        let message = match rng.random_bool(0.9) {
            true => text(rng, 40),
            false => text(rng, 5000),
        };
        recorded(&StreamError { reason, message })
    }

    /// Returns the marker of an activation with random results.
    fn valid_marker(rng: &mut StdRng) -> StreamAppendMarkerData {
        let activation_index = match rng.random_bool(0.9) {
            true => rng.random_range(0..10),
            false => MAX_ACTIVATION,
        };
        let appends = (1..=rng.random_range(1..4))
            .map(|seq| StreamAppendResult {
                seq,
                stream_id: text(rng, 12),
                store: text(rng, 12),
                activation_index,
                result: Some(match rng.random_bool(0.5) {
                    true => stream_append_result::Result::Appended(StreamAppendOffsets {
                        first_offset: offset(rng),
                        last_offset: offset(rng),
                    }),
                    false => stream_append_result::Result::Failure(failure(rng)),
                }),
            })
            .collect();
        StreamAppendMarkerData { appends }
    }

    #[test]
    fn a_marker_reads_back_as_it_was_written() {
        for seed in 0..1000 {
            let mut rng = StdRng::seed_from_u64(seed);
            let marker = valid_marker(&mut rng);
            let read_back = peeked_marker(&event(marker_details(&marker).unwrap()));
            assert_eq!(read_back, Some(Ok(marker)), "seed {seed}");
        }
    }

    /// Breaks one rule of `marker`. Returns the details of the marker event and the reason that
    /// the decode gives, or the start of it where the rest is the text of the JSON parser.
    fn corrupt(
        rng: &mut StdRng,
        mut marker: StreamAppendMarkerData,
    ) -> (HashMap<String, Payloads>, String) {
        let why = match rng.random_range(0..5) {
            0 => return (HashMap::new(), "has no data".to_string()),
            1 => {
                let mut details = marker_details(&marker).unwrap();
                let payload = &mut details.get_mut(MARKER_DATA_KEY).unwrap().payloads[0];
                let text = String::from_utf8(payload.data.clone()).unwrap();
                // One more character makes the hex of an offset odd, and a `g` isn't hex.
                let has_offset = "\"first_offset\":\"";
                match rng.random_range(0..3) {
                    0 => payload.metadata.clear(),
                    1 if text.contains(has_offset) => {
                        let not_hex = ["\"first_offset\":\"0", "\"first_offset\":\"0g"];
                        let not_hex = not_hex[rng.random_range(0..2)];
                        payload.data = text.replacen(has_offset, not_hex, 1).into_bytes();
                    }
                    _ => payload.data.truncate(text.len() - 1),
                }
                return (details, "doesn't decode: ".to_string());
            }
            2 => {
                marker.appends.push(StreamAppendResult {
                    seq: 99,
                    ..Default::default()
                });
                "has no result of append 99".to_string()
            }
            3 => {
                let activation = rng.random_range(MAX_ACTIVATION + 1..=u64::MAX);
                marker.appends.push(StreamAppendResult {
                    seq: 99,
                    activation_index: activation,
                    result: Some(stream_append_result::Result::Appended(Default::default())),
                    ..Default::default()
                });
                format!("has an entry at activation {activation}, above the maximum of 4294967295")
            }
            _ => {
                let later = StreamFailure {
                    reason: "STREAM_ERROR_REASON_LATER".to_string(),
                    message: "A later version of Core has this reason".to_string(),
                };
                marker.appends.push(StreamAppendResult {
                    seq: 99,
                    result: Some(stream_append_result::Result::Failure(later)),
                    ..Default::default()
                });
                "has a failure with the reason `STREAM_ERROR_REASON_LATER`, which this Worker \
                 doesn't know"
                    .to_string()
            }
        };
        (marker_details(&marker).unwrap(), why)
    }

    #[test]
    fn a_bad_marker_is_fatal() {
        for seed in 0..1000 {
            let mut rng = StdRng::seed_from_u64(seed);
            let valid = valid_marker(&mut rng);
            let (details, why) = corrupt(&mut rng, valid);
            let Some(Err(WFMachinesError::Fatal(error))) = peeked_marker(&event(details)) else {
                panic!("seed {seed}: the marker decoded");
            };
            let expected = format!("The Stream marker {why}");
            match why.ends_with(": ") {
                true => assert!(error.starts_with(&expected), "seed {seed}: {error}"),
                false => assert_eq!(error, expected, "seed {seed}"),
            }
        }
    }
}
