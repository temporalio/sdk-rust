//! Applies the payload codec to the references that replace offloaded payloads, so a reference's
//! claim data is protected the same way an inline payload is.
//!
//! An encoded reference is unrecognizable by its encoding, so it carries a marker that survives the
//! codec. Inbound, marked payloads are decoded back to plain references before retrieval. Outbound,
//! plain references are encoded and marked after storing, only when
//! [`ExternalStorage::encodes_references`] is set.
//!
//! These passes visit the same fields as the codec passes in [`crate::payload_visitor`], one
//! reference at a time, since the codec is never run concurrently on one message.

use temporalio_common_wasm::data_converters::{
    PayloadCodec, PayloadConversionError, SerializationContextData,
};

use crate::{
    data_converters::ExternalStorageError,
    external_storage::references::{is_marked_reference, is_plain_reference, mark_reference},
    payload_visitor::{PayloadFieldCollectable, transformable_fields},
};

/// Replace every marked reference in a message with the plain reference it encodes, so it can be
/// retrieved. Returns whether the message holds any reference to retrieve, so a caller can skip
/// walking it again when it holds none.
///
/// On error the message is left partly processed and must not be used.
pub(crate) async fn decode_references_in<M: PayloadFieldCollectable + Send>(
    message: &mut M,
    codec: &(dyn PayloadCodec + Send + Sync),
    context: &SerializationContextData,
) -> Result<bool, PayloadConversionError> {
    let mut found = false;
    for field in transformable_fields(message) {
        for payload in field.into_payloads_mut() {
            if !is_marked_reference(payload) {
                found |= is_plain_reference(payload);
                continue;
            }
            // Decoded exactly as it appears in history, marker included, since that is also what a
            // codec server receives.
            let encoded = std::mem::take(payload);
            let external_payloads = encoded.external_payloads.clone();
            let mut decoded = codec.decode(context, vec![encoded]).await?;
            let mut reference = match decoded.pop() {
                Some(reference) if decoded.is_empty() && is_plain_reference(&reference) => {
                    reference
                }
                _ => return Err(ExternalStorageError::UndecodableReference.into()),
            };
            if reference.external_payloads.is_empty() {
                reference.external_payloads = external_payloads;
            }
            *payload = reference;
            found = true;
        }
    }
    Ok(found)
}

/// Encode and mark every plain reference in a message.
///
/// On error the message is left partly processed and must not be used.
pub(crate) async fn encode_references_in<M: PayloadFieldCollectable + Send>(
    message: &mut M,
    codec: &(dyn PayloadCodec + Send + Sync),
    context: &SerializationContextData,
) -> Result<(), PayloadConversionError> {
    for field in transformable_fields(message) {
        for payload in field.into_payloads_mut() {
            if !is_plain_reference(payload) {
                continue;
            }
            let reference = std::mem::take(payload);
            // The server reads offloaded sizes from the outermost payload, and a codec that builds
            // a new payload drops them.
            let external_payloads = reference.external_payloads.clone();
            let mut encoded = codec.encode(context, vec![reference]).await?;
            let mut encoded = match (encoded.pop(), encoded.len()) {
                (Some(encoded), 0) => encoded,
                (popped, remaining) => {
                    return Err(PayloadConversionError::EncodingError(
                        format!(
                            "payload codec returned {} payloads for one external storage reference",
                            remaining + usize::from(popped.is_some())
                        )
                        .into(),
                    ));
                }
            };
            mark_reference(&mut encoded);
            if encoded.external_payloads.is_empty() {
                encoded.external_payloads = external_payloads;
            }
            *payload = encoded;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_converters::{ActivitySerializationContext, DefaultPayloadCodec},
        external_storage::references::create_reference_payload,
        protos::temporal::api::common::v1::{
            Payload, Payloads, SearchAttributes, payload::ExternalPayloadDetails,
        },
    };
    use futures::future::{self, BoxFuture};
    use prost::Message;
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    const WRAPPED: &[u8] = b"binary/wrapped";

    /// Wraps each whole payload in a new one, keeping nothing of the original's metadata or
    /// `external_payloads`, and records the batch size and context of every call.
    #[derive(Clone, Default)]
    struct WrappingCodec {
        calls: Arc<Mutex<Vec<(usize, String)>>>,
    }

    impl WrappingCodec {
        fn record(&self, context: &SerializationContextData, payloads: &[Payload]) {
            self.calls
                .lock()
                .unwrap()
                .push((payloads.len(), format!("{context:?}")));
        }
    }

    impl PayloadCodec for WrappingCodec {
        fn encode(
            &self,
            context: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            self.record(context, &payloads);
            let encoded = payloads
                .iter()
                .map(|p| Payload {
                    metadata: HashMap::from([("encoding".to_owned(), WRAPPED.to_vec())]),
                    data: p.encode_to_vec(),
                    external_payloads: vec![],
                })
                .collect();
            Box::pin(future::ready(Ok(encoded)))
        }

        fn decode(
            &self,
            context: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            self.record(context, &payloads);
            let decoded = payloads
                .into_iter()
                .map(|p| {
                    if p.metadata.get("encoding").map(Vec::as_slice) == Some(WRAPPED) {
                        Payload::decode(p.data.as_slice())
                            .map_err(|e| PayloadConversionError::EncodingError(e.into()))
                    } else {
                        Ok(p)
                    }
                })
                .collect();
            Box::pin(future::ready(decoded))
        }
    }

    /// Returns the given number of empty payloads, whatever it is given.
    struct MiscountingCodec(usize);

    impl PayloadCodec for MiscountingCodec {
        fn encode(
            &self,
            _: &SerializationContextData,
            _: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            Box::pin(future::ready(Ok(vec![Payload::default(); self.0])))
        }

        fn decode(
            &self,
            _: &SerializationContextData,
            _: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            Box::pin(future::ready(Ok(vec![Payload::default(); self.0])))
        }
    }

    fn reference(key: &str) -> Payload {
        create_reference_payload(
            "mem",
            HashMap::from([("key".to_owned(), key.to_owned())]),
            1234,
        )
        .unwrap()
    }

    fn ordinary(data: &str) -> Payload {
        Payload {
            metadata: HashMap::from([("encoding".to_owned(), b"json/plain".to_vec())]),
            data: data.as_bytes().to_vec(),
            external_payloads: vec![],
        }
    }

    fn activity_context() -> SerializationContextData {
        SerializationContextData::Activity(ActivitySerializationContext::new())
    }

    #[tokio::test]
    async fn encoded_references_round_trip_and_ordinary_payloads_are_untouched() {
        let codec = WrappingCodec::default();
        let context = activity_context();
        let mut message = Payloads {
            payloads: vec![ordinary("inline"), reference("a"), reference("b")],
        };

        encode_references_in(&mut message, &codec, &context)
            .await
            .unwrap();

        assert_eq!(message.payloads[0], ordinary("inline"));
        for encoded in &message.payloads[1..] {
            assert!(is_marked_reference(encoded));
            assert!(!is_plain_reference(encoded));
            assert_eq!(
                encoded.external_payloads,
                vec![ExternalPayloadDetails { size_bytes: 1234 }]
            );
        }

        let any_references = decode_references_in(&mut message, &codec, &context)
            .await
            .unwrap();

        assert!(any_references);

        assert_eq!(
            message.payloads,
            vec![ordinary("inline"), reference("a"), reference("b")]
        );
        let calls = codec.calls.lock().unwrap();
        assert_eq!(calls.len(), 4, "one call per reference each way");
        for (batch, call_context) in calls.iter() {
            assert_eq!(*batch, 1);
            assert_eq!(*call_context, format!("{context:?}"));
        }
    }

    #[tokio::test]
    async fn a_reference_written_marked_but_unencoded_reads() {
        let mut marked = reference("a");
        mark_reference(&mut marked);
        let mut message = Payloads {
            payloads: vec![marked.clone()],
        };

        decode_references_in(&mut message, &DefaultPayloadCodec, &activity_context())
            .await
            .unwrap();

        assert_eq!(message.payloads, vec![marked]);
    }

    #[tokio::test]
    async fn only_a_message_holding_a_reference_reports_one_to_retrieve() {
        let codec = WrappingCodec::default();
        let mut ordinary_only = Payloads {
            payloads: vec![ordinary("a"), ordinary("b")],
        };
        let mut plain = Payloads {
            payloads: vec![ordinary("a"), reference("b")],
        };

        assert!(
            !decode_references_in(&mut ordinary_only, &codec, &activity_context())
                .await
                .unwrap()
        );
        assert!(
            decode_references_in(&mut plain, &codec, &activity_context())
                .await
                .unwrap()
        );
        assert!(codec.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_codec_decodes_the_reference_exactly_as_it_was_written() {
        let mut message = Payloads {
            payloads: vec![reference("a")],
        };
        encode_references_in(&mut message, &WrappingCodec::default(), &activity_context())
            .await
            .unwrap();
        let written = message.payloads[0].clone();
        let seen = Arc::new(Mutex::new(None));

        struct Spy(Arc<Mutex<Option<Payload>>>);
        impl PayloadCodec for Spy {
            fn encode(
                &self,
                _: &SerializationContextData,
                payloads: Vec<Payload>,
            ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
                Box::pin(future::ready(Ok(payloads)))
            }
            fn decode(
                &self,
                context: &SerializationContextData,
                payloads: Vec<Payload>,
            ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
                *self.0.lock().unwrap() = payloads.first().cloned();
                WrappingCodec::default().decode(context, payloads)
            }
        }

        decode_references_in(&mut message, &Spy(Arc::clone(&seen)), &activity_context())
            .await
            .unwrap();

        assert_eq!(seen.lock().unwrap().as_ref(), Some(&written));
    }

    #[tokio::test]
    async fn a_marked_payload_the_codec_cannot_turn_into_a_reference_is_tmprl1106() {
        let mut message = Payloads {
            payloads: vec![reference("a")],
        };
        encode_references_in(&mut message, &WrappingCodec::default(), &activity_context())
            .await
            .unwrap();

        // Without the codec that wrote it, the marked payload stays encoded.
        let err = decode_references_in(
            &mut message.clone(),
            &DefaultPayloadCodec,
            &activity_context(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            PayloadConversionError::ExternalStorage(ExternalStorageError::UndecodableReference)
        ));
        assert!(err.to_string().contains("TMPRL1106"));

        for returned in [0, 2] {
            let err = decode_references_in(
                &mut message.clone(),
                &MiscountingCodec(returned),
                &activity_context(),
            )
            .await
            .unwrap_err();
            assert!(
                matches!(
                    err,
                    PayloadConversionError::ExternalStorage(
                        ExternalStorageError::UndecodableReference
                    )
                ),
                "codec returning {returned} payloads"
            );
        }
    }

    #[tokio::test]
    async fn a_codec_returning_the_wrong_number_of_payloads_on_encode_is_an_error() {
        for returned in [0, 2] {
            let err = encode_references_in(
                &mut Payloads {
                    payloads: vec![reference("a")],
                },
                &MiscountingCodec(returned),
                &activity_context(),
            )
            .await
            .unwrap_err();
            assert!(
                matches!(err, PayloadConversionError::EncodingError(_)),
                "codec returning {returned} payloads"
            );
        }
    }

    #[tokio::test]
    async fn search_attributes_are_left_alone() {
        let codec = WrappingCodec::default();
        let mut attributes = SearchAttributes {
            indexed_fields: HashMap::from([("attr".to_owned(), reference("a"))]),
        };

        encode_references_in(&mut attributes, &codec, &activity_context())
            .await
            .unwrap();

        assert_eq!(attributes.indexed_fields["attr"], reference("a"));
        assert!(codec.calls.lock().unwrap().is_empty());
    }
}
