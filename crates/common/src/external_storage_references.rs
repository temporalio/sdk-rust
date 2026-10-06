//! The on-the-wire form of a payload that has been offloaded to external storage.
//!
//! An offloaded payload is replaced by one whose data is the proto JSON form of an
//! `ExternalStorageReference`. This shape is part of the wire contract and must not be changed
//! unilaterally. The driver contracts these serve live in `temporalio-common-wasm`; the format
//! lives here because proto JSON support and the payload visitor that will drive it do.

// Crate-private until there is a consumer; nothing offloads or restores payloads yet.
#![allow(dead_code)]

use std::collections::HashMap;

use temporalio_common_wasm::external_storage::ExternalStorageError;

use crate::protos::temporal::api::{
    common::v1::{Payload, payload::ExternalPayloadDetails},
    sdk::v1::ExternalStorageReference,
};

const REFERENCE_ENCODING: &str = "json/protobuf";
// Spelled out rather than taken from `prost::Name` so that a proto-side rename fails
// `reference_message_type_matches_the_proto` instead of silently changing what we put on the wire.
const REFERENCE_MESSAGE_TYPE: &str = "temporal.api.sdk.v1.ExternalStorageReference";

/// Whether the given payload is a reference to externally stored data.
pub(crate) fn is_reference(payload: &Payload) -> bool {
    payload.metadata.get("encoding").map(Vec::as_slice) == Some(REFERENCE_ENCODING.as_bytes())
        && payload.metadata.get("messageType").map(Vec::as_slice)
            == Some(REFERENCE_MESSAGE_TYPE.as_bytes())
}

/// Parse the reference from the given payload, which must be one.
pub(crate) fn parse_reference(
    payload: &Payload,
) -> Result<ExternalStorageReference, ExternalStorageError> {
    if !is_reference(payload) {
        return Err(ExternalStorageError::InvalidReference(
            "payload is not an external storage reference".into(),
        ));
    }
    serde_json::from_slice(&payload.data)
        .map_err(|e| ExternalStorageError::InvalidReference(Box::new(e)))
}

/// Create the reference payload that replaces an offloaded payload on the wire.
///
/// This shape is part of the wire contract and must not be changed unilaterally.
pub(crate) fn create_reference_payload(
    driver_name: &str,
    claim_data: HashMap<String, String>,
    original_size_bytes: i64,
) -> Result<Payload, ExternalStorageError> {
    let reference = ExternalStorageReference {
        driver_name: driver_name.to_owned(),
        claim_data,
    };
    let data = serde_json::to_vec(&reference)
        .map_err(|e| ExternalStorageError::InvalidReference(Box::new(e)))?;
    Ok(Payload {
        metadata: HashMap::from([
            (
                "encoding".to_owned(),
                REFERENCE_ENCODING.as_bytes().to_vec(),
            ),
            (
                "messageType".to_owned(),
                REFERENCE_MESSAGE_TYPE.as_bytes().to_vec(),
            ),
        ]),
        data,
        // The original size is kept so the server and UI can report what the payload would have
        // been without having to fetch it from storage.
        external_payloads: vec![ExternalPayloadDetails {
            size_bytes: original_size_bytes,
        }],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_payload_uses_the_cross_sdk_wire_shape() {
        let payload = create_reference_payload(
            "aws.s3driver",
            HashMap::from([("bucket".to_owned(), "b".to_owned())]),
            385,
        )
        .unwrap();

        // Proto JSON, so camel case. Serde's own derive would emit `driver_name` and break every
        // other SDK's reader.
        let body = String::from_utf8(payload.data.clone()).unwrap();
        assert!(body.contains("\"driverName\""), "body was {body}");
        assert!(body.contains("\"claimData\""), "body was {body}");
        assert!(!body.contains("driver_name"), "body was {body}");

        assert_eq!(
            payload.metadata.get("encoding").map(Vec::as_slice),
            Some(REFERENCE_ENCODING.as_bytes())
        );
        assert_eq!(payload.external_payloads[0].size_bytes, 385);
        assert!(is_reference(&payload));

        let reference = parse_reference(&payload).unwrap();
        assert_eq!(reference.driver_name, "aws.s3driver");
        assert_eq!(reference.claim_data["bucket"], "b");
    }

    #[test]
    fn reference_written_by_another_sdk_parses() {
        // Verbatim from the Go SDK's TestClaimDeserialization_OtherSdk_ProtoJSON fixture: compact
        // and differently ordered, which is what another SDK actually puts on the wire.
        let data = br#"{"claimData":{"bucket":"test-bucket","hash_algorithm":"sha256","key":"v0/ns/default"},"driverName":"aws.s3driver"}"#;
        let payload = Payload {
            metadata: HashMap::from([
                (
                    "encoding".to_owned(),
                    REFERENCE_ENCODING.as_bytes().to_vec(),
                ),
                (
                    "messageType".to_owned(),
                    REFERENCE_MESSAGE_TYPE.as_bytes().to_vec(),
                ),
            ]),
            data: data.to_vec(),
            external_payloads: vec![],
        };

        let reference = parse_reference(&payload).unwrap();

        assert_eq!(reference.driver_name, "aws.s3driver");
        assert_eq!(reference.claim_data["bucket"], "test-bucket");
        assert_eq!(reference.claim_data.len(), 3);
    }

    #[test]
    fn reference_message_type_matches_the_proto() {
        assert_eq!(
            REFERENCE_MESSAGE_TYPE,
            <ExternalStorageReference as prost::Name>::full_name()
        );
    }

    #[test]
    fn ordinary_payload_is_not_a_reference() {
        let payload = Payload {
            metadata: HashMap::from([("encoding".to_owned(), b"json/plain".to_vec())]),
            data: b"{}".to_vec(),
            external_payloads: vec![],
        };

        assert!(!is_reference(&payload));
        assert!(matches!(
            parse_reference(&payload),
            Err(ExternalStorageError::InvalidReference(_))
        ));
    }
}
