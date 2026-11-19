use std::{error::Error, fmt::Display, marker::PhantomData, str::FromStr};
use temporalio_common::{
    data_converters::{
        GenericPayloadConverter, HasTransferType, PayloadConversionError, PayloadConverter,
        SerializationContext, SerializationContextData, TemporalDeserializable, TemporalSerializable,
    },
    protos::temporal::api::common::v1::Payload,
};
use temporalio_common as renamed_common;
use temporalio_macros::TemporalTransfer;

#[derive(Debug, PartialEq, TemporalTransfer)]
#[temporal(transfer_type = String)]
struct UserId(u64);

impl From<&UserId> for String {
    fn from(value: &UserId) -> Self {
        value.0.to_string()
    }
}

impl TryFrom<String> for UserId {
    type Error = std::num::ParseIntError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse().map(Self)
    }
}

#[derive(Debug, PartialEq, TemporalTransfer)]
#[temporal(transfer_type = String)]
struct Label<T = String>
where
    T: Display + FromStr,
{
    value: T,
}

impl<T: Display + FromStr> From<&Label<T>> for String {
    fn from(value: &Label<T>) -> Self {
        value.value.to_string()
    }
}

impl<T: Display + FromStr> TryFrom<String> for Label<T> {
    type Error = T::Err;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse().map(|value| Self { value })
    }
}

#[derive(Debug, PartialEq, TemporalTransfer)]
#[temporal(transfer_type = Vec<T>)]
struct Items<T>(Vec<T>);

impl<T: Clone> From<&Items<T>> for Vec<T> {
    fn from(value: &Items<T>) -> Self {
        value.0.clone()
    }
}

impl<T> From<Vec<T>> for Items<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value)
    }
}

#[derive(Debug, PartialEq, TemporalTransfer)]
#[temporal(transfer_type = [u8; N])]
struct Fixed<const N: usize>([u8; N]);

impl<const N: usize> From<&Fixed<N>> for [u8; N] {
    fn from(value: &Fixed<N>) -> Self {
        value.0
    }
}

impl<const N: usize> From<[u8; N]> for Fixed<N> {
    fn from(value: [u8; N]) -> Self {
        Self(value)
    }
}

#[derive(Debug, PartialEq, TemporalTransfer)]
#[temporal(transfer_type = bool, crate = renamed_common)]
enum Switch {
    On,
    Off,
}

impl From<&Switch> for bool {
    fn from(value: &Switch) -> Self {
        matches!(value, Switch::On)
    }
}

impl From<bool> for Switch {
    fn from(value: bool) -> Self {
        if value {
            Self::On
        } else {
            Self::Off
        }
    }
}

#[derive(Debug, PartialEq, TemporalTransfer)]
#[temporal(transfer_type = String)]
struct Tagged<'__temporal_transfer>(u64, PhantomData<&'__temporal_transfer ()>);

impl From<&Tagged<'_>> for String {
    fn from(value: &Tagged<'_>) -> Self {
        value.0.to_string()
    }
}

impl TryFrom<String> for Tagged<'_> {
    type Error = std::num::ParseIntError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse().map(|value| Self(value, PhantomData))
    }
}

#[derive(TemporalTransfer)]
#[temporal(transfer_type = String)]
struct CannotEncode;

impl TryFrom<&CannotEncode> for String {
    type Error = std::io::Error;

    fn try_from(_: &CannotEncode) -> Result<Self, Self::Error> {
        Err(std::io::Error::other("cannot encode model"))
    }
}

impl From<String> for CannotEncode {
    fn from(_: String) -> Self {
        Self
    }
}

fn encode<T: HasTransferType>(value: &T) -> Result<Payload, PayloadConversionError> {
    let converter = PayloadConverter::default();
    let context = SerializationContext::new(&SerializationContextData::None, &converter);
    value.to_transfer_value()?.unwrap().to_payload(&context)
}

fn decode<T: HasTransferType>(payload: Payload) -> Result<T, PayloadConversionError> {
    let converter = PayloadConverter::default();
    let context = SerializationContext::new(&SerializationContextData::None, &converter);
    T::transfer_decoder().unwrap().decode(&context, payload)
}

fn round_trip<T: HasTransferType>(value: &T) -> T {
    decode(encode(value).unwrap()).unwrap()
}

fn assert_compatible<T: HasTransferType>() {
    fn accepts_serialization<T: TemporalSerializable + TemporalDeserializable>() {}
    accepts_serialization::<T>();
}

fn main() {
    assert_compatible::<UserId>();

    let id = UserId(42);
    let payload = encode(&id).unwrap();
    assert_eq!(payload.metadata["encoding"], b"json/plain");
    assert_eq!(payload.data, br#""42""#);
    assert_eq!(decode::<UserId>(payload).unwrap(), id);

    assert_eq!(round_trip(&Label { value: 17u32 }), Label { value: 17u32 });
    assert_eq!(
        round_trip(&Label {
            value: "label".to_owned(),
        })
        .value,
        "label",
    );
    assert_eq!(round_trip(&Fixed([1, 2, 3])), Fixed([1, 2, 3]));
    assert_eq!(round_trip(&Switch::On), Switch::On);
    assert_eq!(round_trip(&Tagged(7, PhantomData)), Tagged(7, PhantomData));

    let bytes = Items(vec![1u8, 2, 3]);
    let payload = encode(&bytes).unwrap();
    assert_eq!(payload.metadata["encoding"], b"binary/plain");
    assert_eq!(payload.data, [1, 2, 3]);
    assert_eq!(decode::<Items<u8>>(payload).unwrap(), bytes);

    let error = encode(&CannotEncode).unwrap_err();
    assert_eq!(error.source().unwrap().to_string(), "cannot encode model");
    assert!(error.source().unwrap().is::<std::io::Error>());

    let converter = PayloadConverter::serde_json();
    let context = SerializationContext::new(&SerializationContextData::None, &converter);
    let payload = converter
        .to_payload(&context, &"not an integer".to_owned())
        .unwrap();
    let error = decode::<UserId>(payload).unwrap_err();
    assert!(error.source().unwrap().is::<std::num::ParseIntError>());
}
