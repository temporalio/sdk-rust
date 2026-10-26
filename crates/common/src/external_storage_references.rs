//! The on-the-wire form of a payload that has been offloaded to external storage.
//!
//! An offloaded payload is replaced by one whose data is the proto JSON form of an
//! `ExternalStorageReference`. This shape is part of the wire contract and must not be changed
//! unilaterally. Unlike the driver contracts, it is defined here rather than in
//! `temporalio-common-wasm`, because proto JSON support and the payload visitor that will drive it
//! are here.

// Only the runner creates or parses references, and nothing calls it yet.
#![allow(dead_code)]

use std::collections::HashMap;

use crate::{
    data_converters::ExternalStorageError,
    payload_visitor::{PayloadVisitable, payload_visitor},
    protos::temporal::api::{
        common::v1::{Payload, payload::ExternalPayloadDetails},
        sdk::v1::ExternalStorageReference,
    },
};

const REFERENCE_ENCODING: &str = "json/protobuf";
// Spelled out rather than taken from `prost::Name` so that a proto-side rename fails
// `reference_message_type_matches_the_proto` instead of silently changing what we put on the wire.
const REFERENCE_MESSAGE_TYPE: &str = "temporal.api.sdk.v1.ExternalStorageReference";

/// Whether the given payload is a plain external storage reference and so has to be retrieved.
/// Reads metadata only, so one that needs no retrieval is not touched.
///
/// Plain means the reference exactly as [`create_reference_payload`] built it, before any payload
/// codec has run over it. A codec rewrites the metadata, so an encoded reference is not recognized
/// here and has to be decoded first.
///
/// Both the `encoding` and the `messageType` metadata must match.
pub(crate) fn is_plain_reference(payload: &Payload) -> bool {
    payload.metadata.get("encoding").map(Vec::as_slice) == Some(REFERENCE_ENCODING.as_bytes())
        && payload.metadata.get("messageType").map(Vec::as_slice)
            == Some(REFERENCE_MESSAGE_TYPE.as_bytes())
}

/// Fail if any payload in the message is a reference, for a caller with no external storage to
/// retrieve it with.
pub(crate) async fn reject_references_in<M: PayloadVisitable + Send>(
    message: &mut M,
) -> Result<(), ExternalStorageError> {
    let mut finder = ReferenceFinder { found: false };
    message
        .visit_payloads_mut(&mut finder)
        .await
        .map_err(|e| ExternalStorageError::Walk(Box::new(e)))?;
    if finder.found {
        return Err(ExternalStorageError::NotConfigured);
    }
    Ok(())
}

struct ReferenceFinder {
    found: bool,
}

payload_visitor!(ReferenceFinder, |this, payloads| {
    this.found |= payloads.iter().any(is_plain_reference);
    payloads
});

/// Parse the reference from the given payload, which must be a plain reference.
pub(crate) fn parse_reference(
    payload: &Payload,
) -> Result<ExternalStorageReference, ExternalStorageError> {
    if !is_plain_reference(payload) {
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
    use crate::protos::temporal::api::common::v1::Payloads;

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
        assert!(is_plain_reference(&payload));

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
    fn the_right_encoding_with_another_message_type_is_not_a_reference() {
        let payload = Payload {
            metadata: HashMap::from([
                ("encoding".to_owned(), b"json/protobuf".to_vec()),
                (
                    "messageType".to_owned(),
                    b"temporal.api.common.v1.Payload".to_vec(),
                ),
            ]),
            data: b"{}".to_vec(),
            external_payloads: vec![],
        };

        assert!(!is_plain_reference(&payload));
    }

    #[test]
    fn the_reference_message_type_with_another_encoding_is_not_a_reference() {
        let payload = Payload {
            metadata: HashMap::from([
                ("encoding".to_owned(), b"binary/protobuf".to_vec()),
                (
                    "messageType".to_owned(),
                    b"temporal.api.sdk.v1.ExternalStorageReference".to_vec(),
                ),
            ]),
            data: b"{}".to_vec(),
            external_payloads: vec![],
        };

        assert!(!is_plain_reference(&payload));
    }

    #[test]
    fn a_created_reference_is_detected() {
        let payload = create_reference_payload(
            "mem",
            HashMap::from([("key".to_owned(), "k".to_owned())]),
            10,
        )
        .unwrap();

        assert!(is_plain_reference(&payload));
    }

    #[tokio::test]
    async fn a_reference_with_no_storage_to_retrieve_it_is_tmprl1105() {
        let ordinary = Payload {
            data: b"fine".to_vec(),
            ..Default::default()
        };
        let reference = create_reference_payload(
            "mem",
            HashMap::from([("key".to_owned(), "a".to_owned())]),
            99,
        )
        .unwrap();
        let mut message = Payloads {
            payloads: vec![ordinary.clone(), reference],
        };

        let err = reject_references_in(&mut message).await.unwrap_err();

        assert!(matches!(err, ExternalStorageError::NotConfigured));
        assert!(err.to_string().contains("TMPRL1105"));
        reject_references_in(&mut Payloads {
            payloads: vec![ordinary],
        })
        .await
        .unwrap();
    }

    #[test]
    fn ordinary_payload_is_not_a_reference() {
        let payload = Payload {
            metadata: HashMap::from([("encoding".to_owned(), b"json/plain".to_vec())]),
            data: b"{}".to_vec(),
            external_payloads: vec![],
        };

        assert!(!is_plain_reference(&payload));
        assert!(matches!(
            parse_reference(&payload),
            Err(ExternalStorageError::InvalidReference(_))
        ));
    }
}
