use super::{
    PayloadConversionError, PayloadConverter, SerializationContext, TemporalDeserializable,
    TemporalSerializable,
};
use crate::protos::temporal::api::common::v1::Payload;
use std::error::Error;

/// Associates a model with its canonical serializer-facing representation.
///
/// Encoding uses `Transfer::try_from(&model)` and decoding uses `Model::try_from(transfer)`.
/// Infallible directions may implement [`From`] instead, which also supplies [`TryFrom`].
/// The conversions must be deterministic, safe to call concurrently, and perform no I/O or
/// workflow commands.
///
/// Prefer `#[derive(temporalio_macros::TemporalTransfer)]` to implement this trait together with
/// [`TemporalSerializable`] and [`TemporalDeserializable`]. The model itself must not implement
/// the corresponding Serde traits, whose blanket Temporal implementations would conflict.
/// The transfer type may use Serde or another supported payload representation.
///
/// Transfer conversion runs once per top-level payload value, before the configured payload
/// serializer when encoding and after it when decoding. Nested fields do not automatically use
/// their transfer mappings. Failure detail values participate, but memo values do not.
pub trait HasTransferType:
    Sized + TemporalSerializable + TemporalDeserializable + TryFrom<Self::Transfer>
{
    /// The representation passed to the configured payload serializer.
    type Transfer: TemporalSerializable
        + TemporalDeserializable
        + for<'a> TryFrom<&'a Self>
        + 'static;
}

/// Keeps native serializer dispatch available after erasing the transfer value's type.
pub struct TransferValue(Box<dyn ErasedTransferValue>);

impl TransferValue {
    /// Serializes the transfer value using the context's payload converter.
    pub fn to_payload(
        &self,
        converter: &PayloadConverter,
        context: &SerializationContext<'_>,
    ) -> Result<Payload, PayloadConversionError> {
        self.0.to_payload(converter, context)
    }

    /// Preserves the transfer representation's multiple-payload behavior.
    pub fn to_payloads(
        &self,
        converter: &PayloadConverter,
        context: &SerializationContext<'_>,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        self.0.to_payloads(converter, context)
    }
}

trait ErasedTransferValue {
    fn to_payload(
        &self,
        converter: &PayloadConverter,
        context: &SerializationContext<'_>,
    ) -> Result<Payload, PayloadConversionError>;
    fn to_payloads(
        &self,
        converter: &PayloadConverter,
        context: &SerializationContext<'_>,
    ) -> Result<Vec<Payload>, PayloadConversionError>;
}

impl<T: TemporalSerializable + 'static> ErasedTransferValue for T {
    fn to_payload(
        &self,
        converter: &PayloadConverter,
        context: &SerializationContext<'_>,
    ) -> Result<Payload, PayloadConversionError> {
        converter.to_payload_without_transfer(context, self)
    }

    fn to_payloads(
        &self,
        converter: &PayloadConverter,
        context: &SerializationContext<'_>,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        converter.to_payloads_without_transfer(context, self)
    }
}

/// Converts a borrowed model without requiring the model to be cloned.
pub fn to_transfer_value<'a, T>(value: &'a T) -> Result<TransferValue, PayloadConversionError>
where
    T: HasTransferType,
    <T::Transfer as TryFrom<&'a T>>::Error: Error + Send + Sync + 'static,
{
    let transfer = T::Transfer::try_from(value)
        .map_err(|error| PayloadConversionError::EncodingError(Box::new(error)))?;
    Ok(TransferValue(Box::new(transfer)))
}

/// Preserves the transfer type's decoder without erasing the resulting model.
pub struct TransferDecoder<T> {
    decode: fn(
        &PayloadConverter,
        &SerializationContext<'_>,
        Payload,
    ) -> Result<T, PayloadConversionError>,
    decode_payloads: fn(
        &PayloadConverter,
        &SerializationContext<'_>,
        Vec<Payload>,
    ) -> Result<T, PayloadConversionError>,
}

impl<T> TransferDecoder<T> {
    /// Selects the model's declared transfer type and reconstruction function.
    pub fn for_model() -> Self
    where
        T: HasTransferType,
        <T as TryFrom<T::Transfer>>::Error: Error + Send + Sync + 'static,
    {
        Self {
            decode: |converter, context, payload| {
                let transfer: T::Transfer =
                    converter.from_payload_without_transfer(context, payload)?;
                T::try_from(transfer)
                    .map_err(|error| PayloadConversionError::EncodingError(Box::new(error)))
            },
            decode_payloads: |converter, context, payloads| {
                let transfer: T::Transfer =
                    converter.from_payloads_without_transfer(context, payloads)?;
                T::try_from(transfer)
                    .map_err(|error| PayloadConversionError::EncodingError(Box::new(error)))
            },
        }
    }

    /// Deserializes the transfer representation and reconstructs the requested model.
    pub fn decode(
        &self,
        converter: &PayloadConverter,
        context: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<T, PayloadConversionError> {
        (self.decode)(converter, context, payload)
    }

    /// Reconstructs a model from its transfer representation's payload sequence.
    pub fn decode_payloads(
        &self,
        converter: &PayloadConverter,
        context: &SerializationContext<'_>,
        payloads: Vec<Payload>,
    ) -> Result<T, PayloadConversionError> {
        (self.decode_payloads)(converter, context, payloads)
    }
}

#[cfg(test)]
mod tests;
