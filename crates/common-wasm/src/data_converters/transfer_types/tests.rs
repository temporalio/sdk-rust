use crate::{
    Memo, MemoValue,
    data_converters::{
        __private, ActivitySerializationContext, DataConverter, DefaultFailureConverter,
        GenericPayloadConverter, HasTransferType, MultiArgs2, NoopDecodeHint, PayloadCodec,
        PayloadConversionError, PayloadConverter, ProstSerializable, RawValue,
        SerializationContext, SerializationContextData, TemporalDeserializable,
        TemporalSerializable, WorkflowSerializationContext,
    },
    error::{ApplicationFailure, IncomingError, OutgoingError, OutgoingWorkflowError},
    protos::temporal::api::{
        common::v1::{Memo as ProtoMemo, Payload, Payloads, WorkflowExecution},
        failure::v1::{CanceledFailureInfo, Failure, TimeoutFailureInfo, failure::FailureInfo},
    },
};
use futures::future::BoxFuture;
use std::{
    collections::HashMap,
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use temporalio_macros::TemporalTransfer;

#[derive(Debug, PartialEq, TemporalTransfer)]
#[temporal(transfer_type = String, crate = crate)]
struct Id(u64);

impl From<&Id> for String {
    fn from(value: &Id) -> Self {
        value.0.to_string()
    }
}

impl TryFrom<String> for Id {
    type Error = std::num::ParseIntError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse().map(Self)
    }
}

#[derive(Debug, PartialEq, TemporalTransfer)]
#[temporal(transfer_type = ProstSerializable<WorkflowExecution>, crate = crate)]
struct ProtoModel(String);

impl From<&ProtoModel> for ProstSerializable<WorkflowExecution> {
    fn from(value: &ProtoModel) -> Self {
        Self(WorkflowExecution {
            workflow_id: value.0.clone(),
            run_id: "transfer".to_owned(),
        })
    }
}

impl From<ProstSerializable<WorkflowExecution>> for ProtoModel {
    fn from(value: ProstSerializable<WorkflowExecution>) -> Self {
        Self(value.0.workflow_id)
    }
}

#[derive(Debug, PartialEq)]
struct NativeModel(u64);

impl HasTransferType for NativeModel {
    type Transfer = String;
}

impl From<&NativeModel> for String {
    fn from(value: &NativeModel) -> Self {
        format!("wire-{}", value.0)
    }
}

impl TryFrom<String> for NativeModel {
    type Error = std::num::ParseIntError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.strip_prefix("wire-").unwrap_or("").parse().map(Self)
    }
}

impl TemporalSerializable for NativeModel {
    fn to_transfer_value(
        &self,
    ) -> Result<Option<__private::TransferValue>, PayloadConversionError> {
        __private::to_transfer_value(self).map(Some)
    }

    fn to_payload(
        &self,
        context: &SerializationContext<'_>,
    ) -> Result<Payload, PayloadConversionError> {
        context.converter.to_payload(context, &self.0)
    }
}

impl TemporalDeserializable for NativeModel {
    fn transfer_decoder() -> Option<__private::TransferDecoder<Self>> {
        Some(__private::TransferDecoder::for_model())
    }

    fn from_payload(
        context: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<Self, PayloadConversionError> {
        context.converter.from_payload(context, payload).map(Self)
    }
}

#[derive(Debug, PartialEq, TemporalTransfer)]
#[temporal(transfer_type = NativeModel, crate = crate)]
struct Outer(u64);

impl From<&Outer> for NativeModel {
    fn from(value: &Outer) -> Self {
        Self(value.0)
    }
}

impl From<NativeModel> for Outer {
    fn from(value: NativeModel) -> Self {
        Self(value.0)
    }
}

#[derive(TemporalTransfer)]
#[temporal(transfer_type = String, crate = crate)]
struct Counted(Arc<AtomicUsize>);

impl From<&Counted> for String {
    fn from(value: &Counted) -> Self {
        value.0.fetch_add(1, Ordering::SeqCst);
        "counted".to_owned()
    }
}

impl From<String> for Counted {
    fn from(_: String) -> Self {
        Self(Arc::new(AtomicUsize::new(0)))
    }
}

#[derive(TemporalTransfer)]
#[temporal(transfer_type = String, crate = crate)]
struct Failing(Arc<AtomicUsize>);

impl TryFrom<&Failing> for String {
    type Error = PayloadConversionError;

    fn try_from(value: &Failing) -> Result<Self, Self::Error> {
        value.0.fetch_add(1, Ordering::SeqCst);
        Err(PayloadConversionError::WrongEncoding)
    }
}

impl TryFrom<String> for Failing {
    type Error = PayloadConversionError;

    fn try_from(_: String) -> Result<Self, Self::Error> {
        Err(PayloadConversionError::WrongEncoding)
    }
}

#[derive(Debug, PartialEq, TemporalTransfer)]
#[temporal(transfer_type = (), crate = crate)]
struct Empty;

impl From<&Empty> for () {
    fn from(_: &Empty) -> Self {}
}

impl From<()> for Empty {
    fn from(_: ()) -> Self {
        Self
    }
}

#[test]
fn transfer_values_use_configured_serializer() {
    for converter in [PayloadConverter::default(), PayloadConverter::serde_json()] {
        let context = SerializationContext::new(&SerializationContextData::None, &converter);
        let payload = converter.to_payload(&context, &Id(42)).unwrap();
        assert_eq!(payload.metadata["encoding"], b"json/plain");
        assert_eq!(payload.data, br#""42""#);
        assert_eq!(
            converter.from_payload::<Id>(&context, payload).unwrap(),
            Id(42)
        );
        let payloads = converter.to_payloads(&context, &Id(43)).unwrap();
        assert_eq!(payloads.len(), 1);
        assert_eq!(
            converter.from_payloads::<Id>(&context, payloads).unwrap(),
            Id(43)
        );
        assert!(converter.from_payloads::<Id>(&context, vec![]).is_err());
        let payload = converter.to_payload(&context, &"44").unwrap();
        assert!(
            converter
                .from_payloads::<Id>(&context, vec![payload.clone(), payload])
                .is_err()
        );
    }
}

#[test]
fn transfer_to_protobuf_retains_native_dispatch() {
    let converter = PayloadConverter::UseWrappers;
    let nested_converter = PayloadConverter::serde_json();
    let context = SerializationContext::new(&SerializationContextData::None, &nested_converter);
    let payload = converter
        .to_payload(&context, &ProtoModel("id".to_owned()))
        .unwrap();
    assert_eq!(payload.metadata["encoding"], b"binary/protobuf");
    assert_eq!(
        converter
            .from_payload::<ProtoModel>(&context, payload)
            .unwrap(),
        ProtoModel("id".to_owned()),
    );
}

#[test]
fn transfer_step_does_not_chain_through_intermediate_model() {
    let converter = PayloadConverter::default();
    let context = SerializationContext::new(&SerializationContextData::None, &converter);
    let direct = converter.to_payload(&context, &NativeModel(7)).unwrap();
    assert_eq!(direct.data, br#""wire-7""#);
    let payload = converter.to_payload(&context, &Outer(7)).unwrap();
    assert_eq!(payload.data, b"7");
    assert_eq!(
        converter.from_payload::<Outer>(&context, payload).unwrap(),
        Outer(7)
    );
    let payloads = converter.to_payloads(&context, &Outer(8)).unwrap();
    assert_eq!(payloads[0].data, b"8");
    assert_eq!(
        converter
            .from_payloads::<Outer>(&context, payloads)
            .unwrap(),
        Outer(8)
    );
}

#[test]
fn transfer_conversion_runs_once_before_composite_dispatch() {
    let converter = PayloadConverter::composite([
        PayloadConverter::UseWrappers,
        PayloadConverter::composite([
            PayloadConverter::UseWrappers,
            PayloadConverter::serde_json(),
        ]),
    ]);
    let context = SerializationContext::new(&SerializationContextData::None, &converter);
    let calls = Arc::new(AtomicUsize::new(0));
    converter
        .to_payload(&context, &Counted(calls.clone()))
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    converter
        .to_payloads(&context, &Counted(calls.clone()))
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    let failing = Failing(calls.clone());
    let error = converter.to_payload(&context, &failing).unwrap_err();
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(matches!(error, PayloadConversionError::EncodingError(_)));
    assert!(error.source().unwrap().is::<PayloadConversionError>());
    assert!(converter.to_payloads(&context, &failing).is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    let payload = converter.to_payload(&context, &"wire").unwrap();
    let Err(error) = converter.from_payload::<Failing>(&context, payload.clone()) else {
        panic!("transfer decoding must fail");
    };
    assert!(matches!(error, PayloadConversionError::EncodingError(_)));
    let Err(error) = converter.from_payloads::<Failing>(&context, vec![payload]) else {
        panic!("transfer decoding must fail");
    };
    assert!(matches!(error, PayloadConversionError::EncodingError(_)));
}

#[test]
fn transfer_multi_args_preserve_each_top_level_value_and_raw_payloads() {
    let converter = PayloadConverter::default();
    let context = SerializationContext::new(&SerializationContextData::None, &converter);
    let values = MultiArgs2(Id(1), ProtoModel("two".to_owned()));
    let payloads = converter.to_payloads(&context, &values).unwrap();
    assert_eq!(payloads.len(), 2);
    assert_eq!(payloads[0].data, br#""1""#);
    assert_eq!(payloads[1].metadata["encoding"], b"binary/protobuf");
    assert_eq!(
        converter
            .from_payloads::<MultiArgs2<Id, ProtoModel>>(&context, payloads.clone())
            .unwrap(),
        values,
    );
    let raw = converter
        .from_payloads::<RawValue>(&context, payloads.clone())
        .unwrap();
    assert_eq!(converter.to_payloads(&context, &raw).unwrap(), payloads);
}

#[test]
fn transfer_preserves_native_empty_payload_sequence() {
    let converter = PayloadConverter::default();
    let context = SerializationContext::new(&SerializationContextData::None, &converter);
    assert_eq!(
        converter.to_payload(&context, &Empty).unwrap().metadata["encoding"],
        b"binary/null"
    );
    let payloads = converter.to_payloads(&context, &Empty).unwrap();
    assert!(payloads.is_empty());
    assert_eq!(
        converter
            .from_payloads::<Empty>(&context, payloads)
            .unwrap(),
        Empty
    );
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Envelope {
    #[serde(with = "native_field")]
    value: NativeModel,
}

mod native_field {
    use super::NativeModel;

    pub(super) fn serialize<S: serde::Serializer>(
        value: &NativeModel,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&value.0, serializer)
    }

    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<NativeModel, D::Error> {
        serde::Deserialize::deserialize(deserializer).map(NativeModel)
    }
}

#[test]
fn transfer_does_not_inspect_nested_fields() {
    let converter = PayloadConverter::default();
    let context = SerializationContext::new(&SerializationContextData::None, &converter);
    let payload = converter
        .to_payload(
            &context,
            &Envelope {
                value: NativeModel(7),
            },
        )
        .unwrap();
    assert_eq!(payload.data, br#"{"value":7}"#);
    assert_eq!(
        converter
            .from_payload::<Envelope>(&context, payload)
            .unwrap()
            .value,
        NativeModel(7)
    );
}

#[test]
fn transfer_mappings_are_bypassed_for_memos() {
    let converter = PayloadConverter::default();
    let context = SerializationContext::new(&SerializationContextData::None, &converter);
    let payload = converter
        .to_payload(&context, &MemoValue::new(NativeModel(7)))
        .unwrap();
    assert_eq!(payload.data, b"7");
    let memo = Memo::from_raw(
        Some(ProtoMemo {
            fields: HashMap::from([("value".to_owned(), payload)]),
        }),
        converter.clone(),
        SerializationContextData::None,
    );
    assert_eq!(
        memo.get::<NativeModel>("value").unwrap(),
        Some(NativeModel(7))
    );
    let payload = converter.to_payload(&context, &"7").unwrap();
    let memo = Memo::from_raw(
        Some(ProtoMemo {
            fields: HashMap::from([("value".to_owned(), payload)]),
        }),
        converter.clone(),
        SerializationContextData::None,
    );
    assert!(memo.get::<Id>("value").is_err());
    assert!(
        converter
            .to_payload(&context, &MemoValue::new(Id(7)))
            .is_err()
    );
}

struct XorCodec;

impl PayloadCodec for XorCodec {
    fn encode(
        &self,
        data: &SerializationContextData,
        mut payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        assert!(matches!(data, SerializationContextData::Activity(_)));
        for payload in &mut payloads {
            assert!(serde_json::from_slice::<String>(&payload.data).is_ok());
            payload.data.iter_mut().for_each(|byte| *byte ^= 0xff);
        }
        Box::pin(async move { Ok(payloads) })
    }

    fn decode(
        &self,
        data: &SerializationContextData,
        mut payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
        assert!(matches!(data, SerializationContextData::Activity(_)));
        for payload in &mut payloads {
            assert!(serde_json::from_slice::<String>(&payload.data).is_err());
            payload.data.iter_mut().for_each(|byte| *byte ^= 0xff);
        }
        Box::pin(async move { Ok(payloads) })
    }
}

#[tokio::test]
async fn transfer_runs_inside_codec_and_is_exposed_by_serialization_context() {
    let converter = DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter::default(),
        XorCodec,
    );
    let data = SerializationContextData::Activity(ActivitySerializationContext::new());
    let payload = converter.to_payload(&data, &Id(7)).await.unwrap();
    assert_eq!(
        converter.from_payload::<Id>(&data, payload).await.unwrap(),
        Id(7)
    );
    let values = MultiArgs2(Id(8), Id(9));
    let payloads = converter.to_payloads(&data, &values).await.unwrap();
    assert_eq!(
        converter
            .from_payloads::<MultiArgs2<Id, Id>>(&data, payloads)
            .await
            .unwrap(),
        values
    );

    let context = SerializationContext::new(&data, converter.payload_converter());
    let payload = context.converter.to_payload(&context, &Id(10)).unwrap();
    assert_eq!(
        context
            .converter
            .from_payload::<Id>(&context, payload)
            .unwrap(),
        Id(10)
    );
    let raw = RawValue::from_value(&Id(11), converter.payload_converter());
    assert_eq!(raw.to_value::<Id>(converter.payload_converter()), Id(11));
}

#[test]
fn transfer_values_round_trip_through_failure_details() {
    let converter = DataConverter::default();
    let context = SerializationContextData::Workflow(WorkflowSerializationContext::new());
    let error = ApplicationFailure::builder(anyhow::anyhow!("failed"))
        .details(ProtoModel("detail".to_owned()))
        .build();
    let failure = converter.to_failure(
        &context,
        OutgoingError::Workflow(OutgoingWorkflowError::Application(Box::new(error))),
    );
    let Some(FailureInfo::ApplicationFailureInfo(info)) = &failure.failure_info else {
        panic!("expected application failure");
    };
    assert_eq!(
        info.details.as_ref().unwrap().payloads[0].metadata["encoding"],
        b"binary/protobuf"
    );
    let IncomingError::Application(error) = converter
        .to_error(&context, failure, NoopDecodeHint)
        .unwrap()
    else {
        panic!("expected application error");
    };
    assert_eq!(
        error.details::<ProtoModel>().unwrap(),
        Some(ProtoModel("detail".to_owned()))
    );

    let pc = converter.payload_converter();
    let ctx = SerializationContext::new(&context, pc);
    let details = Payloads {
        payloads: pc.to_payloads(&ctx, &Id(12)).unwrap(),
    };
    let cancelled = Failure {
        failure_info: Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
            details: Some(details.clone()),
            ..Default::default()
        })),
        ..Default::default()
    };
    let IncomingError::Cancelled(error) = converter
        .to_error(&context, cancelled, NoopDecodeHint)
        .unwrap()
    else {
        panic!("expected cancellation");
    };
    assert_eq!(error.details::<Id>().unwrap(), Some(Id(12)));
    let timeout = Failure {
        failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
            last_heartbeat_details: Some(details),
            ..Default::default()
        })),
        ..Default::default()
    };
    let IncomingError::Timeout(error) = converter
        .to_error(&context, timeout, NoopDecodeHint)
        .unwrap()
    else {
        panic!("expected timeout");
    };
    assert_eq!(error.last_heartbeat_details::<Id>().unwrap(), Some(Id(12)));
}
