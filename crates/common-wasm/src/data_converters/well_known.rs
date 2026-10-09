use crate::protos::{ENCODING_PAYLOAD_KEY, temporal::api::common::v1::Payload};
use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

pub(super) const BINARY_PLAIN_ENCODING_VAL: &str = "binary/plain";
pub(super) const BINARY_NULL_ENCODING_VAL: &str = "binary/null";

#[derive(Clone, Copy)]
pub(super) enum WellKnownType {
    Unit,
    Bytes,
    OptionalBytes,
}

impl WellKnownType {
    pub(super) fn of<T: 'static>() -> Option<Self> {
        let type_id = TypeId::of::<T>();
        if type_id == TypeId::of::<()>() {
            Some(Self::Unit)
        } else if type_id == TypeId::of::<Vec<u8>>() {
            Some(Self::Bytes)
        } else if type_id == TypeId::of::<Option<Vec<u8>>>() {
            Some(Self::OptionalBytes)
        } else {
            None
        }
    }

    pub(super) fn to_payload<T: 'static>(self, value: &T) -> Payload {
        let value = value as &dyn Any;
        match self {
            Self::Unit => binary_null_payload(),
            Self::Bytes => binary_plain_payload(value.downcast_ref::<Vec<u8>>().unwrap().clone()),
            Self::OptionalBytes => match value.downcast_ref::<Option<Vec<u8>>>().unwrap() {
                Some(bytes) => binary_plain_payload(bytes.clone()),
                None => binary_null_payload(),
            },
        }
    }

    pub(super) fn to_payloads<T: 'static>(self, value: &T) -> Vec<Payload> {
        match self {
            Self::Unit => Vec::new(),
            _ => vec![self.to_payload(value)],
        }
    }

    pub(super) fn try_from_payload<T: 'static>(self, payload: Payload) -> Result<T, Payload> {
        match self {
            Self::Unit if is_binary_null_payload(&payload) => Ok(downcast_well_known(())),
            Self::Bytes if is_binary_plain_payload(&payload) => {
                Ok(downcast_well_known(payload.data))
            }
            Self::OptionalBytes if is_binary_plain_payload(&payload) => {
                Ok(downcast_well_known(Some(payload.data)))
            }
            Self::OptionalBytes if is_binary_null_payload(&payload) => {
                Ok(downcast_well_known(None::<Vec<u8>>))
            }
            _ => Err(payload),
        }
    }

    pub(super) fn try_from_payloads<T: 'static>(
        self,
        mut payloads: Vec<Payload>,
    ) -> Result<T, Vec<Payload>> {
        if matches!(self, Self::Unit) && payloads.is_empty() {
            return Ok(downcast_well_known(()));
        }
        if payloads.len() != 1 {
            return Err(payloads);
        }
        match self.try_from_payload(payloads.pop().unwrap()) {
            Ok(value) => Ok(value),
            Err(payload) => Err(vec![payload]),
        }
    }
}

fn downcast_well_known<T: 'static>(value: impl Any) -> T {
    let value: Box<dyn Any> = Box::new(value);
    *value.downcast::<T>().ok().unwrap()
}

fn binary_plain_payload(data: Vec<u8>) -> Payload {
    Payload {
        metadata: HashMap::from([(
            ENCODING_PAYLOAD_KEY.to_string(),
            BINARY_PLAIN_ENCODING_VAL.as_bytes().to_vec(),
        )]),
        data,
        external_payloads: vec![],
    }
}

fn is_binary_plain_payload(payload: &Payload) -> bool {
    payload
        .metadata
        .get(ENCODING_PAYLOAD_KEY)
        .is_some_and(|encoding| encoding == BINARY_PLAIN_ENCODING_VAL.as_bytes())
}

pub(super) fn binary_null_payload() -> Payload {
    Payload {
        metadata: HashMap::from([(
            ENCODING_PAYLOAD_KEY.to_string(),
            BINARY_NULL_ENCODING_VAL.as_bytes().to_vec(),
        )]),
        data: vec![],
        external_payloads: vec![],
    }
}

pub(super) fn is_binary_null_payload(payload: &Payload) -> bool {
    payload.data.is_empty()
        && payload
            .metadata
            .get(ENCODING_PAYLOAD_KEY)
            .is_some_and(|encoding| encoding == BINARY_NULL_ENCODING_VAL.as_bytes())
}
