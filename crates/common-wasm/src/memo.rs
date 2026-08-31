use crate::{
    data_converters::{
        GenericPayloadConverter, PayloadConversionError, PayloadConverter, SerializationContext,
        SerializationContextData, TemporalDeserializable, TemporalSerializable,
    },
    protos::temporal::api::common::v1::{Memo as ProtoMemo, Payload},
};
use std::{collections::BTreeMap, sync::Arc};

/// A collection of memo payloads that can be deserialized into typed values.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Memo {
    raw: ProtoMemo,
    payload_converter: PayloadConverter,
    context: SerializationContextData,
}

impl Memo {
    /// Construct a memo with the payload converter and serialization context associated with its
    /// source.
    #[doc(hidden)]
    pub fn from_raw(
        raw: Option<ProtoMemo>,
        payload_converter: PayloadConverter,
        context: SerializationContextData,
    ) -> Self {
        Self {
            raw: raw.unwrap_or_default(),
            payload_converter,
            context,
        }
    }

    /// Decode a memo value as `T`, returning `None` when the key is absent.
    pub fn get<T: TemporalDeserializable + 'static>(
        &self,
        key: &str,
    ) -> Result<Option<T>, PayloadConversionError> {
        let Some(payload) = self.raw.fields.get(key) else {
            return Ok(None);
        };
        self.payload_converter
            .from_payload(
                &SerializationContext::new(&self.context, &self.payload_converter),
                payload.clone(),
            )
            .map(Some)
    }

    /// Returns whether the memo contains `key`.
    pub fn contains_key(&self, key: &str) -> bool {
        self.raw.fields.contains_key(key)
    }

    /// Returns the number of memo entries.
    pub fn len(&self) -> usize {
        self.raw.fields.len()
    }

    /// Returns whether the memo has no entries.
    pub fn is_empty(&self) -> bool {
        self.raw.fields.is_empty()
    }

    /// Iterates over memo keys.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.raw.fields.keys().map(String::as_str)
    }

    /// Returns the underlying payload without applying payload conversion.
    pub fn raw_value(&self, key: &str) -> Option<&Payload> {
        self.raw.fields.get(key)
    }

    /// Access the underlying memo protobuf.
    pub fn raw(&self) -> &ProtoMemo {
        &self.raw
    }

    /// Consume this wrapper and return the underlying memo protobuf.
    pub fn into_raw(self) -> ProtoMemo {
        self.raw
    }
}

trait SerializableMemoValue: Send + Sync {
    fn to_payload(
        &self,
        context: &SerializationContext<'_>,
    ) -> Result<Payload, PayloadConversionError>;
}

impl<T> SerializableMemoValue for T
where
    T: TemporalSerializable + Send + Sync + 'static,
{
    fn to_payload(
        &self,
        context: &SerializationContext<'_>,
    ) -> Result<Payload, PayloadConversionError> {
        context.converter.to_payload(context, self)
    }
}

/// A typed value used in a workflow memo update.
#[derive(Clone, derive_more::Debug)]
#[non_exhaustive]
pub struct MemoValue {
    #[debug(skip)]
    value: Arc<dyn SerializableMemoValue>,
}

impl MemoValue {
    /// Create a memo value that will be serialized with the workflow's data converter.
    pub fn new<T: TemporalSerializable + Send + Sync + 'static>(value: T) -> Self {
        Self {
            value: Arc::new(value),
        }
    }
}

impl TemporalSerializable for MemoValue {
    fn to_payload(
        &self,
        context: &SerializationContext<'_>,
    ) -> Result<Payload, PayloadConversionError> {
        self.value.to_payload(context)
    }
}

/// A complete set of memo values for a new workflow execution.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct MemoValues {
    values: BTreeMap<String, MemoValue>,
}

impl MemoValues {
    /// Create an empty set of memo values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add or replace a memo value.
    pub fn insert<T>(&mut self, key: impl Into<String>, value: T) -> &mut Self
    where
        T: TemporalSerializable + Send + Sync + 'static,
    {
        self.values.insert(key.into(), MemoValue::new(value));
        self
    }

    /// Returns the value for `key`, if present.
    pub fn get(&self, key: &str) -> Option<&MemoValue> {
        self.values.get(key)
    }

    /// Iterates over the memo entries in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &MemoValue)> {
        self.values.iter().map(|(key, value)| (key.as_str(), value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_converters::WorkflowSerializationContext;
    use std::collections::HashMap;

    #[test]
    fn memo_decodes_serialized_values() {
        let payload_converter = PayloadConverter::default();
        let context_data = SerializationContextData::Workflow(WorkflowSerializationContext::new());
        let context = SerializationContext::new(&context_data, &payload_converter);
        let payload = payload_converter.to_payload(&context, &7_u32).unwrap();
        let raw = ProtoMemo {
            fields: HashMap::from([("count".to_owned(), payload.clone())]),
        };
        let memo = Memo::from_raw(
            Some(raw.clone()),
            payload_converter,
            SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        );

        assert_eq!(memo.get::<u32>("count").unwrap(), Some(7));
        assert_eq!(memo.get::<u32>("missing").unwrap(), None);
        assert_eq!(memo.raw_value("count"), Some(&payload));
        assert_eq!(memo.into_raw(), raw);
    }

    #[test]
    fn memo_reports_deserialization_errors() {
        let payload_converter = PayloadConverter::default();
        let context_data = SerializationContextData::Workflow(WorkflowSerializationContext::new());
        let context = SerializationContext::new(&context_data, &payload_converter);
        let payload = payload_converter.to_payload(&context, &7_u32).unwrap();
        let memo = Memo::from_raw(
            Some(ProtoMemo {
                fields: HashMap::from([("count".to_owned(), payload)]),
            }),
            payload_converter,
            SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        );

        assert!(memo.get::<String>("count").is_err());
    }

    #[test]
    fn memo_values_serialize_heterogeneous_values() {
        let payload_converter = PayloadConverter::default();
        let mut values = MemoValues::new();
        values
            .insert("count", 7_u32)
            .insert("label", "hello".to_string());

        let context_data = SerializationContextData::Workflow(WorkflowSerializationContext::new());
        let context = SerializationContext::new(&context_data, &payload_converter);
        let fields = values
            .iter()
            .map(|(key, value)| {
                (
                    key.to_owned(),
                    payload_converter.to_payload(&context, value).unwrap(),
                )
            })
            .collect();

        let memo = Memo::from_raw(
            Some(ProtoMemo { fields }),
            payload_converter.clone(),
            SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        );

        assert_eq!(memo.get::<u32>("count").unwrap(), Some(7));
        assert_eq!(
            memo.get::<String>("label").unwrap(),
            Some("hello".to_string())
        );
    }
}
