use super::{
    GenericPayloadConverter, PayloadConversionError, SerializationContext, TemporalDeserializable,
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
/// Currently this API declares the transfer relationship and supplies adapters only. Normal
/// payload-conversion entry points do not yet invoke transfer conversion.
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
        context: &SerializationContext<'_>,
    ) -> Result<Payload, PayloadConversionError> {
        self.0.to_payload(context)
    }
}

trait ErasedTransferValue {
    fn to_payload(
        &self,
        context: &SerializationContext<'_>,
    ) -> Result<Payload, PayloadConversionError>;
}

impl<T: TemporalSerializable + 'static> ErasedTransferValue for T {
    fn to_payload(
        &self,
        context: &SerializationContext<'_>,
    ) -> Result<Payload, PayloadConversionError> {
        context.converter.to_payload(context, self)
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
    decode: fn(&SerializationContext<'_>, Payload) -> Result<T, PayloadConversionError>,
}

impl<T: HasTransferType> TransferDecoder<T> {
    /// Selects the model's declared transfer type and reconstruction function.
    pub fn for_model() -> Self
    where
        <T as TryFrom<T::Transfer>>::Error: Error + Send + Sync + 'static,
    {
        Self {
            decode: |context, payload| {
                let transfer: T::Transfer = context.converter.from_payload(context, payload)?;
                T::try_from(transfer)
                    .map_err(|error| PayloadConversionError::EncodingError(Box::new(error)))
            },
        }
    }

    /// Deserializes the transfer representation and reconstructs the requested model.
    pub fn decode(
        &self,
        context: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<T, PayloadConversionError> {
        (self.decode)(context, payload)
    }
}
