//! The external storage passes the SDK and client run over the messages they exchange with the
//! server, beside the payload codec passes in [`crate::payload_visitor`].
//!
//! These exist in every build, while the storage they drive only exists with the `experimental`
//! feature. Callers cannot tell whether that feature is enabled on this crate, so nothing here
//! exposes a feature-gated type.

#[cfg(feature = "experimental")]
use crate::data_converters::{
    StorageDriverActivityInfo, StorageDriverTargetInfo, StorageDriverWorkflowInfo,
};
use crate::{
    data_converters::{DataConverter, PayloadConversionError, SerializationContextData},
    payload_visitor::PayloadFieldCollectable,
    protos::coresdk::common::ExternalStorageMetrics,
};
use std::{collections::BTreeSet, time::Instant};

#[cfg(feature = "experimental")]
mod reference_codec;
mod references;
#[cfg(feature = "experimental")]
mod runner;

/// What one or more store or retrieve passes moved, and the time they took, reported to Core on a
/// workflow completion. Only a pass that moved at least one payload has metrics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageOperationMetrics {
    totals: StorageOperationTotals,
    /// From the earliest pass's start to the latest pass's end, so merging is correct whether the
    /// passes ran one after another or concurrently.
    start: Instant,
    end: Instant,
}

/// What a pass moved, summed over the fields it visited.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct StorageOperationTotals {
    payload_count: u64,
    total_size_bytes: u64,
    driver_names: BTreeSet<String>,
}

impl StorageOperationTotals {
    fn add(&mut self, other: Self) {
        self.payload_count += other.payload_count;
        self.total_size_bytes += other.total_size_bytes;
        self.driver_names.extend(other.driver_names);
    }
}

impl StorageOperationMetrics {
    /// Combine two passes' metrics, in any order.
    pub fn merge(mut self, other: Self) -> Self {
        self.totals.add(other.totals);
        self.start = self.start.min(other.start);
        self.end = self.end.max(other.end);
        self
    }

    /// The metrics in Core's form.
    pub fn into_proto(self) -> ExternalStorageMetrics {
        ExternalStorageMetrics {
            payload_count: self.totals.payload_count,
            total_size_bytes: self.totals.total_size_bytes,
            total_duration: (self.end - self.start).try_into().ok(),
            driver_names: self.totals.driver_names.into_iter().collect(),
        }
    }
}

/// Offload every eligible payload in an outbound message to the data converter's external storage.
/// Runs after [`encode_payloads`](crate::payload_visitor::encode_payloads), since the size
/// threshold applies to encoded payloads. Does nothing without the `experimental` feature. Returns
/// metrics only when a payload was offloaded.
///
/// `namespace` is the namespace of the client or worker performing the store, given to drivers and
/// selectors. An empty string means unknown. `context` must be the one the message's payloads were
/// encoded with, since the references that replace them may be encoded too.
///
/// On error, reported as [`PayloadConversionError::ExternalStorage`], the message is left partly
/// processed and must not be sent.
pub async fn store_external_payloads<M: PayloadFieldCollectable + Send>(
    msg: &mut M,
    data_converter: &DataConverter,
    context: &SerializationContextData,
    namespace: &str,
    target: Option<StoreTarget>,
) -> Result<Option<StorageOperationMetrics>, PayloadConversionError> {
    #[cfg(feature = "experimental")]
    let metrics = {
        let storage = data_converter.external_storage();
        let metrics = runner::store_in(
            storage,
            msg,
            (!namespace.is_empty()).then_some(namespace),
            target.as_ref().map(|t| &t.info),
        )
        .await?;
        if storage.is_some_and(|storage| storage.encodes_references()) {
            reference_codec::encode_references_in(msg, data_converter.codec(), context).await?;
        }
        metrics
    };
    #[cfg(not(feature = "experimental"))]
    let metrics = {
        let _ = (msg, data_converter, context, namespace, target);
        None
    };
    Ok(metrics)
}

/// The execution an outbound message's payloads are stored on behalf of, given to
/// [`store_external_payloads`]. Callers build this rather than the driver-facing target type,
/// which only exists with the `experimental` feature.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreTarget {
    #[cfg(feature = "experimental")]
    info: StorageDriverTargetInfo,
}

impl StoreTarget {
    /// A workflow target. An empty string, which is how a proto leaves a field unset, means
    /// unknown.
    pub fn workflow(namespace: &str, id: &str, run_id: &str, workflow_type: &str) -> Self {
        #[cfg(feature = "experimental")]
        return Self {
            info: StorageDriverTargetInfo::Workflow(
                StorageDriverWorkflowInfo::builder()
                    .namespace(namespace.to_owned())
                    .maybe_id(non_empty(id))
                    .maybe_run_id(non_empty(run_id))
                    .maybe_workflow_type(non_empty(workflow_type))
                    .build(),
            ),
        };
        #[cfg(not(feature = "experimental"))]
        {
            let _ = (namespace, id, run_id, workflow_type);
            Self {}
        }
    }

    /// A standalone activity target. An empty string, which is how a proto leaves a field unset,
    /// means unknown.
    pub fn activity(namespace: &str, id: &str, run_id: &str, activity_type: &str) -> Self {
        #[cfg(feature = "experimental")]
        return Self {
            info: StorageDriverTargetInfo::Activity(
                StorageDriverActivityInfo::builder()
                    .namespace(namespace.to_owned())
                    .maybe_id(non_empty(id))
                    .maybe_run_id(non_empty(run_id))
                    .maybe_activity_type(non_empty(activity_type))
                    .build(),
            ),
        };
        #[cfg(not(feature = "experimental"))]
        {
            let _ = (namespace, id, run_id, activity_type);
            Self {}
        }
    }

    /// An activity addressed by id: its workflow when a workflow id is given, otherwise the
    /// standalone activity. An empty string, which is how a proto leaves a field unset, means
    /// unknown.
    pub fn activity_by_id(
        namespace: &str,
        workflow_id: &str,
        run_id: &str,
        activity_id: &str,
    ) -> Self {
        if workflow_id.is_empty() {
            Self::activity(namespace, activity_id, run_id, "")
        } else {
            Self::workflow(namespace, workflow_id, run_id, "")
        }
    }
}

#[cfg(feature = "experimental")]
fn non_empty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}

/// Replace every external storage reference in an inbound message with the payload it points at.
/// Runs before [`decode_payloads`](crate::payload_visitor::decode_payloads), since a driver returns
/// a payload as it was before offloading. Without external storage to retrieve them, which includes
/// every build without the `experimental` feature, a reference is an error instead. Failures are
/// reported as [`PayloadConversionError::ExternalStorage`], or as the codec's own error when
/// decoding an encoded reference fails. Returns metrics only when a payload was retrieved.
///
/// `context` must be the one the message's payloads are decoded with, since a reference may have
/// been encoded with the payload codec too.
///
/// On error the message is left partly processed and must not be used.
pub async fn retrieve_external_payloads<M: PayloadFieldCollectable + Send>(
    msg: &mut M,
    data_converter: &DataConverter,
    context: &SerializationContextData,
) -> Result<Option<StorageOperationMetrics>, PayloadConversionError> {
    #[cfg(feature = "experimental")]
    let metrics = {
        match data_converter.external_storage() {
            // Retrieval fails on any reference, so decoding one first would only add codec calls.
            None => {
                references::reject_references_in(msg)?;
                None
            }
            Some(storage) => {
                let any_references =
                    reference_codec::decode_references_in(msg, data_converter.codec(), context)
                        .await?;
                if any_references {
                    runner::retrieve_in(Some(storage), msg).await?
                } else {
                    None
                }
            }
        }
    };
    #[cfg(not(feature = "experimental"))]
    let metrics = {
        let _ = (data_converter, context);
        references::reject_references_in(msg)?;
        None
    };
    Ok(metrics)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_converters::ExternalStorageError, protos::temporal::api::common::v1::Payloads,
    };
    use std::{collections::HashMap, time::Duration};
    #[cfg(feature = "experimental")]
    use {
        crate::{
            data_converters::{
                DefaultFailureConverter, ExternalStorage, PayloadCodec, PayloadConverter,
                WorkflowSerializationContext,
            },
            protos::temporal::api::common::v1::Payload,
        },
        futures::future::{self, BoxFuture},
        std::sync::{Arc, Mutex},
    };

    #[test]
    fn merged_passes_report_the_time_they_spanned() {
        let start = Instant::now();
        let pass = |from: u64, to: u64| StorageOperationMetrics {
            totals: StorageOperationTotals {
                payload_count: 1,
                ..Default::default()
            },
            start: start + Duration::from_millis(from),
            end: start + Duration::from_millis(to),
        };

        let proto = pass(50, 150).merge(pass(0, 100)).into_proto();
        assert_eq!(proto.payload_count, 2);
        assert_eq!(
            proto.total_duration,
            Duration::from_millis(150).try_into().ok()
        );
    }

    #[tokio::test]
    async fn unretrievable_reference_is_a_payload_conversion_error() {
        let reference = references::create_reference_payload(
            "mem",
            HashMap::from([("key".to_owned(), "a".to_owned())]),
            99,
        )
        .unwrap();
        let mut message = Payloads {
            payloads: vec![reference],
        };

        let err = retrieve_external_payloads(
            &mut message,
            &DataConverter::default(),
            &SerializationContextData::None,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            PayloadConversionError::ExternalStorage(ExternalStorageError::NotConfigured)
        ));
        assert!(err.to_string().contains("TMPRL1105"));
    }

    /// Reverses each payload's data, and marks it so decoding can tell what it encoded.
    #[cfg(feature = "experimental")]
    struct ReversingCodec;

    #[cfg(feature = "experimental")]
    impl PayloadCodec for ReversingCodec {
        fn encode(
            &self,
            _: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            let encoded = payloads
                .into_iter()
                .map(|mut p| {
                    p.data.reverse();
                    p.metadata.insert("reversed".to_owned(), b"1".to_vec());
                    p
                })
                .collect();
            Box::pin(future::ready(Ok(encoded)))
        }

        fn decode(
            &self,
            _: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            let decoded = payloads
                .into_iter()
                .map(|mut p| {
                    if p.metadata.remove("reversed").is_some() {
                        p.data.reverse();
                    }
                    p
                })
                .collect();
            Box::pin(future::ready(Ok(decoded)))
        }
    }

    /// Fails the test if the payload codec is ever asked to decode.
    #[cfg(feature = "experimental")]
    struct NoDecodeCodec;

    #[cfg(feature = "experimental")]
    impl PayloadCodec for NoDecodeCodec {
        fn encode(
            &self,
            _: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            Box::pin(future::ready(Ok(payloads)))
        }

        fn decode(
            &self,
            _: &SerializationContextData,
            _: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            panic!("nothing in the message needed decoding");
        }
    }

    #[cfg(feature = "experimental")]
    fn converter_storing_everything(encode_references: bool) -> DataConverter {
        let driver = Arc::new(runner::tests::RoundTripDriver {
            stored: Arc::new(Mutex::new(HashMap::new())),
        });
        DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter::default(),
            ReversingCodec,
        )
        .with_external_storage(
            ExternalStorage::new(driver)
                .unwrap()
                .with_payload_size_threshold(0)
                .with_encoded_references(encode_references),
        )
    }

    #[cfg(feature = "experimental")]
    fn workflow_context() -> SerializationContextData {
        SerializationContextData::Workflow(WorkflowSerializationContext::new())
    }

    #[cfg(feature = "experimental")]
    async fn stored(data_converter: &DataConverter, value: &str) -> Payloads {
        let mut message = Payloads {
            payloads: vec![
                data_converter
                    .to_payload(&workflow_context(), &value.to_owned())
                    .await
                    .unwrap(),
            ],
        };
        store_external_payloads(&mut message, data_converter, &workflow_context(), "", None)
            .await
            .unwrap();
        message
    }

    #[cfg(feature = "experimental")]
    #[tokio::test]
    async fn references_are_written_plain_unless_encoding_them_is_enabled() {
        let plain = stored(&converter_storing_everything(false), "value").await;
        assert!(references::is_plain_reference(&plain.payloads[0]));
        assert!(!references::is_marked_reference(&plain.payloads[0]));

        let encoded = stored(&converter_storing_everything(true), "value").await;
        assert!(references::is_marked_reference(&encoded.payloads[0]));
        assert_eq!(encoded.payloads[0].metadata["reversed"], b"1");
        let mut unreversed = encoded.payloads[0].data.clone();
        unreversed.reverse();
        assert_eq!(unreversed, plain.payloads[0].data);
        assert_eq!(
            encoded.payloads[0].external_payloads,
            plain.payloads[0].external_payloads
        );
    }

    #[cfg(feature = "experimental")]
    #[tokio::test]
    async fn encoded_references_are_read_whether_or_not_encoding_them_is_enabled() {
        let writer = converter_storing_everything(true);
        let mut message = stored(&writer, "value").await;
        // The reader shares the writer's driver, but not its setting.
        let reader = DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter::default(),
            ReversingCodec,
        )
        .with_external_storage(
            ExternalStorage::new(Arc::clone(&writer.external_storage().unwrap().drivers()[0]))
                .unwrap(),
        );

        retrieve_external_payloads(&mut message, &reader, &workflow_context())
            .await
            .unwrap();
        crate::payload_visitor::decode_payloads(&mut message, reader.codec(), &workflow_context())
            .await
            .unwrap();

        let value: String = reader
            .from_payload(&workflow_context(), message.payloads.remove(0))
            .await
            .unwrap();
        assert_eq!(value, "value");
    }

    #[cfg(feature = "experimental")]
    #[tokio::test]
    async fn an_encoded_reference_with_no_storage_to_retrieve_it_is_tmprl1105() {
        let mut message = stored(&converter_storing_everything(true), "value").await;
        let without_storage = DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter::default(),
            ReversingCodec,
        );

        let err = retrieve_external_payloads(&mut message, &without_storage, &workflow_context())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            PayloadConversionError::ExternalStorage(ExternalStorageError::NotConfigured)
        ));
    }

    #[cfg(feature = "experimental")]
    #[tokio::test]
    async fn only_marked_references_reach_the_codec_on_retrieval() {
        let writer = converter_storing_everything(false);
        let reader = DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter::default(),
            NoDecodeCodec,
        )
        .with_external_storage(
            ExternalStorage::new(Arc::clone(&writer.external_storage().unwrap().drivers()[0]))
                .unwrap(),
        );
        let ordinary = Payload {
            data: b"inline".to_vec(),
            ..Default::default()
        };

        let mut without_references = Payloads {
            payloads: vec![ordinary.clone()],
        };
        let metrics =
            retrieve_external_payloads(&mut without_references, &reader, &workflow_context())
                .await
                .unwrap();
        assert_eq!(metrics, None);
        assert_eq!(without_references.payloads, vec![ordinary]);

        let mut plain = stored(&writer, "value").await;
        let metrics = retrieve_external_payloads(&mut plain, &reader, &workflow_context())
            .await
            .unwrap();
        assert_eq!(metrics.unwrap().into_proto().payload_count, 1);
        assert!(!references::is_plain_reference(&plain.payloads[0]));
    }
}
