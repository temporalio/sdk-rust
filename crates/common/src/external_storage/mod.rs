//! The external storage passes the SDK and client run over the messages they exchange with the
//! server, beside the payload codec passes in [`crate::payload_visitor`].
//!
//! These exist in every build, while the storage they drive only exists with the `experimental`
//! feature. Callers cannot tell whether that feature is enabled on this crate, so nothing here
//! exposes a feature-gated type.

use crate::{
    data_converters::{DataConverter, PayloadConversionError},
    payload_visitor::PayloadVisitable,
};

mod references;
#[cfg(feature = "experimental")]
mod runner;

/// Offload every eligible payload in an outbound message to the data converter's external storage.
/// Runs after [`encode_payloads`](crate::payload_visitor::encode_payloads), since the size
/// threshold applies to encoded payloads. Does nothing without the `experimental` feature.
///
/// On error, reported as [`PayloadConversionError::ExternalStorage`], the message is left partly
/// processed and must not be sent.
pub async fn store_external_payloads<M: PayloadVisitable + Send>(
    msg: &mut M,
    data_converter: &DataConverter,
) -> Result<(), PayloadConversionError> {
    // Stored without a target until the worker and client can name the execution a message is for.
    #[cfg(feature = "experimental")]
    runner::store_in(data_converter.external_storage(), msg, None).await?;
    #[cfg(not(feature = "experimental"))]
    let _ = (msg, data_converter);
    Ok(())
}

/// Replace every external storage reference in an inbound message with the payload it points at.
/// Runs before [`decode_payloads`](crate::payload_visitor::decode_payloads), since a driver returns
/// a payload as it was before offloading. Without external storage to retrieve them, which includes
/// every build without the `experimental` feature, a reference is an error instead. Failures are
/// reported as [`PayloadConversionError::ExternalStorage`].
///
/// On error the message is left partly processed and must not be used.
pub async fn retrieve_external_payloads<M: PayloadVisitable + Send>(
    msg: &mut M,
    data_converter: &DataConverter,
) -> Result<(), PayloadConversionError> {
    #[cfg(feature = "experimental")]
    runner::retrieve_in(data_converter.external_storage(), msg).await?;
    #[cfg(not(feature = "experimental"))]
    {
        let _ = data_converter;
        references::reject_references_in(msg).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_converters::ExternalStorageError, protos::temporal::api::common::v1::Payloads,
    };
    use std::collections::HashMap;

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

        let err = retrieve_external_payloads(&mut message, &DataConverter::default())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            PayloadConversionError::ExternalStorage(ExternalStorageError::NotConfigured)
        ));
        assert!(err.to_string().contains("TMPRL1105"));
    }
}
