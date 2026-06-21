//! Type-safe search attribute APIs for the Temporal Rust SDK.
//!
//! Search attributes are key-value pairs attached to workflows that enable
//! server-side filtering via visibility queries. This module provides a typed
//! layer over the raw proto payloads so that attribute values are checked at
//! compile time.
//!
//! # Example
//!
//! ```
//! use temporalio_common_wasm::search_attributes::SearchAttributeKey;
//!
//! const MY_BOOL: SearchAttributeKey<bool> = SearchAttributeKey::bool("my_bool");
//! const MY_KW: SearchAttributeKey<String> = SearchAttributeKey::keyword("my_keyword");
//!
//! let update = MY_BOOL.value_set(true);
//! let unset = MY_KW.value_unset();
//! ```

use std::collections::HashMap;
use std::marker::PhantomData;

use prost_types::Timestamp;

use crate::protos::temporal::api::common::v1::{
    Payload, SearchAttributes as ProtoSearchAttributes,
};
use crate::protos::temporal::api::enums::v1::IndexedValueType;
use crate::protos::{ENCODING_PAYLOAD_KEY, JSON_ENCODING_VAL};

/// Metadata key for the search attribute value type, matching Go SDK convention.
const TYPE_METADATA_KEY: &str = "type";

/// Errors arising from search attribute serialization or deserialization.
#[derive(Debug, thiserror::Error)]
pub enum SearchAttributeError {
    /// JSON serialization failed.
    #[error("failed to serialize search attribute value: {0}")]
    Serialization(#[from] serde_json::Error),

    /// The payload is missing required metadata or has an unexpected encoding.
    #[error("invalid search attribute payload: {reason}")]
    InvalidPayload {
        /// Description of what was wrong with the payload.
        reason: String,
    },

    /// A timestamp value could not be formatted or parsed as RFC3339.
    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(String),
}

// ---------------------------------------------------------------------------
// SearchAttributeValue trait
// ---------------------------------------------------------------------------

mod private {
    pub trait Sealed {}
    impl Sealed for bool {}
    impl Sealed for i64 {}
    impl Sealed for f64 {}
    impl Sealed for String {}
    impl Sealed for prost_types::Timestamp {}
    impl Sealed for Vec<String> {}
}

/// A value type that can be stored as a Temporal search attribute.
///
/// This trait is sealed and implemented for: `bool`, `i64`, `f64`, `String`,
/// [`prost_types::Timestamp`], and `Vec<String>`.
pub trait SearchAttributeValue: private::Sealed + Clone + Sized {
    /// Encode this value into a search attribute [`Payload`].
    fn to_search_attribute_payload(
        &self,
        indexed_value_type: IndexedValueType,
    ) -> Result<Payload, SearchAttributeError>;

    /// Decode a value from a search attribute [`Payload`].
    fn from_search_attribute_payload(payload: &Payload) -> Result<Self, SearchAttributeError>;

    /// The default [`IndexedValueType`] for this Rust type.
    fn default_indexed_value_type() -> IndexedValueType;
}

fn type_metadata_str(ivt: IndexedValueType) -> &'static str {
    match ivt {
        IndexedValueType::Bool => "Bool",
        IndexedValueType::Int => "Int",
        IndexedValueType::Double => "Double",
        IndexedValueType::Keyword => "Keyword",
        IndexedValueType::Text => "Text",
        IndexedValueType::Datetime => "Datetime",
        IndexedValueType::KeywordList => "KeywordList",
        IndexedValueType::Unspecified => "Unspecified",
    }
}

fn build_payload(
    json_bytes: Vec<u8>,
    indexed_value_type: IndexedValueType,
) -> Payload {
    let mut metadata = HashMap::with_capacity(2);
    metadata.insert(
        ENCODING_PAYLOAD_KEY.to_string(),
        JSON_ENCODING_VAL.as_bytes().to_vec(),
    );
    metadata.insert(
        TYPE_METADATA_KEY.to_string(),
        type_metadata_str(indexed_value_type).as_bytes().to_vec(),
    );
    Payload {
        metadata,
        data: json_bytes,
        ..Default::default()
    }
}

fn validate_encoding(payload: &Payload) -> Result<(), SearchAttributeError> {
    let encoding = payload.metadata.get(ENCODING_PAYLOAD_KEY).ok_or_else(|| {
        SearchAttributeError::InvalidPayload {
            reason: "missing encoding metadata".into(),
        }
    })?;
    if encoding.as_slice() != JSON_ENCODING_VAL.as_bytes() {
        return Err(SearchAttributeError::InvalidPayload {
            reason: format!(
                "expected encoding '{}', got '{}'",
                JSON_ENCODING_VAL,
                String::from_utf8_lossy(encoding)
            ),
        });
    }
    Ok(())
}

// --- bool ---

impl SearchAttributeValue for bool {
    fn to_search_attribute_payload(
        &self,
        indexed_value_type: IndexedValueType,
    ) -> Result<Payload, SearchAttributeError> {
        Ok(build_payload(serde_json::to_vec(self)?, indexed_value_type))
    }

    fn from_search_attribute_payload(payload: &Payload) -> Result<Self, SearchAttributeError> {
        validate_encoding(payload)?;
        Ok(serde_json::from_slice(&payload.data)?)
    }

    fn default_indexed_value_type() -> IndexedValueType {
        IndexedValueType::Bool
    }
}

// --- i64 ---

impl SearchAttributeValue for i64 {
    fn to_search_attribute_payload(
        &self,
        indexed_value_type: IndexedValueType,
    ) -> Result<Payload, SearchAttributeError> {
        Ok(build_payload(serde_json::to_vec(self)?, indexed_value_type))
    }

    fn from_search_attribute_payload(payload: &Payload) -> Result<Self, SearchAttributeError> {
        validate_encoding(payload)?;
        Ok(serde_json::from_slice(&payload.data)?)
    }

    fn default_indexed_value_type() -> IndexedValueType {
        IndexedValueType::Int
    }
}

// --- f64 ---

impl SearchAttributeValue for f64 {
    fn to_search_attribute_payload(
        &self,
        indexed_value_type: IndexedValueType,
    ) -> Result<Payload, SearchAttributeError> {
        Ok(build_payload(serde_json::to_vec(self)?, indexed_value_type))
    }

    fn from_search_attribute_payload(payload: &Payload) -> Result<Self, SearchAttributeError> {
        validate_encoding(payload)?;
        Ok(serde_json::from_slice(&payload.data)?)
    }

    fn default_indexed_value_type() -> IndexedValueType {
        IndexedValueType::Double
    }
}

// --- String ---

impl SearchAttributeValue for String {
    fn to_search_attribute_payload(
        &self,
        indexed_value_type: IndexedValueType,
    ) -> Result<Payload, SearchAttributeError> {
        Ok(build_payload(serde_json::to_vec(self)?, indexed_value_type))
    }

    fn from_search_attribute_payload(payload: &Payload) -> Result<Self, SearchAttributeError> {
        validate_encoding(payload)?;
        Ok(serde_json::from_slice(&payload.data)?)
    }

    fn default_indexed_value_type() -> IndexedValueType {
        IndexedValueType::Keyword
    }
}

// --- Timestamp ---
// Temporal wire format expects RFC3339 strings for Datetime search attributes.

fn timestamp_to_rfc3339(ts: &Timestamp) -> String {
    use std::fmt::Write;

    let total_secs = ts.seconds;
    // 86400 seconds per day, epoch is 1970-01-01
    let (days_from_epoch, day_secs) = {
        let mut d = total_secs.div_euclid(86400);
        let mut s = total_secs.rem_euclid(86400);
        if s < 0 {
            s += 86400;
            d -= 1;
        }
        (d, s as u64)
    };

    let hours = day_secs / 3600;
    let minutes = (day_secs % 3600) / 60;
    let seconds = day_secs % 60;

    // Civil date from days since epoch using a well-known algorithm
    let z = days_from_epoch + 719468;
    let era = z.div_euclid(146097);
    let doe = z.rem_euclid(146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    let nanos = ts.nanos.max(0) as u32;
    let mut buf = String::with_capacity(30);
    if nanos == 0 {
        write!(buf, "{y:04}-{m:02}-{d:02}T{hours:02}:{minutes:02}:{seconds:02}Z").unwrap();
    } else {
        write!(
            buf,
            "{y:04}-{m:02}-{d:02}T{hours:02}:{minutes:02}:{seconds:02}.{nanos:09}Z"
        )
        .unwrap();
    }
    buf
}

fn rfc3339_to_timestamp(s: &str) -> Result<Timestamp, SearchAttributeError> {
    let s = s.trim_matches('"');

    let parse_err = |msg: &str| SearchAttributeError::InvalidTimestamp(msg.to_string());

    if s.len() < 20 {
        return Err(parse_err("string too short for RFC3339"));
    }

    let year: i64 = s[0..4].parse().map_err(|_| parse_err("invalid year"))?;
    let month: u64 = s[5..7].parse().map_err(|_| parse_err("invalid month"))?;
    let day: u64 = s[8..10].parse().map_err(|_| parse_err("invalid day"))?;
    let hour: u64 = s[11..13].parse().map_err(|_| parse_err("invalid hour"))?;
    let min: u64 = s[14..16].parse().map_err(|_| parse_err("invalid minute"))?;
    let sec: u64 = s[17..19].parse().map_err(|_| parse_err("invalid second"))?;

    // Parse optional fractional seconds
    let nanos: i32 = if s.len() > 20 && s.as_bytes()[19] == b'.' {
        let frac_end = s[20..]
            .find(|c: char| c == 'Z' || c == '+' || c == '-')
            .unwrap_or(s.len() - 20);
        let frac_str = &s[20..20 + frac_end];
        let padded = format!("{frac_str:0<9}");
        padded[..9]
            .parse()
            .map_err(|_| parse_err("invalid fractional seconds"))?
    } else {
        0
    };

    // Convert civil date to days since epoch
    let y = if month <= 2 { year - 1 } else { year };
    let m = if month <= 2 { month + 9 } else { month - 3 };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400) as u64;
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe as i64 - 719468;

    let seconds = days * 86400 + (hour * 3600 + min * 60 + sec) as i64;

    Ok(Timestamp { seconds, nanos })
}

impl SearchAttributeValue for Timestamp {
    fn to_search_attribute_payload(
        &self,
        indexed_value_type: IndexedValueType,
    ) -> Result<Payload, SearchAttributeError> {
        let rfc3339 = timestamp_to_rfc3339(self);
        Ok(build_payload(
            serde_json::to_vec(&rfc3339)?,
            indexed_value_type,
        ))
    }

    fn from_search_attribute_payload(payload: &Payload) -> Result<Self, SearchAttributeError> {
        validate_encoding(payload)?;
        let s: String = serde_json::from_slice(&payload.data)?;
        rfc3339_to_timestamp(&s)
    }

    fn default_indexed_value_type() -> IndexedValueType {
        IndexedValueType::Datetime
    }
}

// --- Vec<String> ---

impl SearchAttributeValue for Vec<String> {
    fn to_search_attribute_payload(
        &self,
        indexed_value_type: IndexedValueType,
    ) -> Result<Payload, SearchAttributeError> {
        Ok(build_payload(serde_json::to_vec(self)?, indexed_value_type))
    }

    fn from_search_attribute_payload(payload: &Payload) -> Result<Self, SearchAttributeError> {
        validate_encoding(payload)?;
        Ok(serde_json::from_slice(&payload.data)?)
    }

    fn default_indexed_value_type() -> IndexedValueType {
        IndexedValueType::KeywordList
    }
}

// ---------------------------------------------------------------------------
// SearchAttributeKey
// ---------------------------------------------------------------------------

/// A typed handle for a named search attribute, carrying its value type at the
/// type level. Construct via the const factory methods such as
/// [`SearchAttributeKey::bool`], [`SearchAttributeKey::keyword`], etc.
#[derive(Debug, Clone)]
pub struct SearchAttributeKey<T: SearchAttributeValue> {
    name: &'static str,
    indexed_value_type: IndexedValueType,
    _marker: PhantomData<T>,
}

impl<T: SearchAttributeValue> SearchAttributeKey<T> {
    /// Returns the attribute name used as the key in the proto map.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Returns the [`IndexedValueType`] configured for this key.
    pub fn indexed_value_type(&self) -> IndexedValueType {
        self.indexed_value_type
    }

    /// Create a [`SearchAttributeUpdate`] that sets the attribute to the given value.
    pub fn value_set(&self, val: T) -> SearchAttributeUpdate {
        let payload = val
            .to_search_attribute_payload(self.indexed_value_type)
            .expect("search attribute serialization should not fail for supported types");
        SearchAttributeUpdate {
            name: self.name.to_string(),
            payload: Some(payload),
        }
    }

    /// Create a [`SearchAttributeUpdate`] that removes this attribute.
    pub fn value_unset(&self) -> SearchAttributeUpdate {
        SearchAttributeUpdate {
            name: self.name.to_string(),
            payload: None,
        }
    }
}

impl SearchAttributeKey<bool> {
    /// Create a key for a `Bool`-typed search attribute.
    pub const fn bool(name: &'static str) -> Self {
        Self {
            name,
            indexed_value_type: IndexedValueType::Bool,
            _marker: PhantomData,
        }
    }
}

impl SearchAttributeKey<i64> {
    /// Create a key for an `Int`-typed search attribute.
    pub const fn int(name: &'static str) -> Self {
        Self {
            name,
            indexed_value_type: IndexedValueType::Int,
            _marker: PhantomData,
        }
    }
}

impl SearchAttributeKey<f64> {
    /// Create a key for a `Double`-typed search attribute.
    pub const fn float(name: &'static str) -> Self {
        Self {
            name,
            indexed_value_type: IndexedValueType::Double,
            _marker: PhantomData,
        }
    }
}

impl SearchAttributeKey<String> {
    /// Create a key for a `Keyword`-typed search attribute.
    pub const fn keyword(name: &'static str) -> Self {
        Self {
            name,
            indexed_value_type: IndexedValueType::Keyword,
            _marker: PhantomData,
        }
    }

    /// Create a key for a `Text`-typed search attribute.
    pub const fn text(name: &'static str) -> Self {
        Self {
            name,
            indexed_value_type: IndexedValueType::Text,
            _marker: PhantomData,
        }
    }
}

impl SearchAttributeKey<Timestamp> {
    /// Create a key for a `Datetime`-typed search attribute.
    pub const fn datetime(name: &'static str) -> Self {
        Self {
            name,
            indexed_value_type: IndexedValueType::Datetime,
            _marker: PhantomData,
        }
    }
}

impl SearchAttributeKey<Vec<String>> {
    /// Create a key for a `KeywordList`-typed search attribute.
    pub const fn keyword_list(name: &'static str) -> Self {
        Self {
            name,
            indexed_value_type: IndexedValueType::KeywordList,
            _marker: PhantomData,
        }
    }
}

// ---------------------------------------------------------------------------
// SearchAttributeUpdate
// ---------------------------------------------------------------------------

/// A pending mutation to a single search attribute. `None` payload means the
/// attribute should be removed.
#[derive(Debug, Clone)]
pub struct SearchAttributeUpdate {
    pub(crate) name: String,
    pub(crate) payload: Option<Payload>,
}

impl SearchAttributeUpdate {
    /// Returns the attribute name being updated.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns `true` if this update removes the attribute.
    pub fn is_unset(&self) -> bool {
        self.payload.is_none()
    }
}

// ---------------------------------------------------------------------------
// TypedSearchAttributes
// ---------------------------------------------------------------------------

/// A collection of search attribute payloads, providing type-safe access via
/// [`SearchAttributeKey`].
#[derive(Debug, Clone, Default)]
pub struct TypedSearchAttributes {
    fields: HashMap<String, Payload>,
}

impl TypedSearchAttributes {
    /// Construct from an iterator of [`SearchAttributeUpdate`]s.
    ///
    /// Updates with `None` payloads remove any existing entry for that key.
    pub fn new(updates: impl IntoIterator<Item = SearchAttributeUpdate>) -> Self {
        let mut fields = HashMap::new();
        for update in updates {
            match update.payload {
                Some(payload) => {
                    fields.insert(update.name, payload);
                }
                None => {
                    fields.remove(&update.name);
                }
            }
        }
        Self { fields }
    }

    /// Retrieve a typed value. Returns `None` if the key is absent or
    /// deserialization fails (graceful degradation — no panic on type mismatch).
    pub fn get<T: SearchAttributeValue>(&self, key: &SearchAttributeKey<T>) -> Option<T> {
        let payload = self.fields.get(key.name())?;
        T::from_search_attribute_payload(payload).ok()
    }

    /// Returns `true` if a payload exists for the given key.
    pub fn contains_key<T: SearchAttributeValue>(&self, key: &SearchAttributeKey<T>) -> bool {
        self.fields.contains_key(key.name())
    }

    /// Returns true if there are no search attributes.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Returns the number of search attributes.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Convert to the proto wire representation.
    pub fn to_proto(&self) -> ProtoSearchAttributes {
        ProtoSearchAttributes {
            indexed_fields: self.fields.clone(),
        }
    }

    /// Construct from the proto wire representation by cloning the inner map.
    pub fn from_proto(attrs: &ProtoSearchAttributes) -> Self {
        Self {
            fields: attrs.indexed_fields.clone(),
        }
    }

    /// Convert to the proto representation, producing empty-data payloads for
    /// entries that were unset. This is used when building an upsert command
    /// that needs to explicitly clear attributes on the server.
    pub fn updates_to_proto(
        updates: impl IntoIterator<Item = SearchAttributeUpdate>,
    ) -> ProtoSearchAttributes {
        let mut indexed_fields = HashMap::new();
        for update in updates {
            let payload = update.payload.unwrap_or_else(|| Payload {
                metadata: HashMap::new(),
                data: Vec::new(),
                ..Default::default()
            });
            indexed_fields.insert(update.name, payload);
        }
        ProtoSearchAttributes { indexed_fields }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOOL_KEY: SearchAttributeKey<bool> = SearchAttributeKey::bool("my_bool");
    const INT_KEY: SearchAttributeKey<i64> = SearchAttributeKey::int("my_int");
    const FLOAT_KEY: SearchAttributeKey<f64> = SearchAttributeKey::float("my_float");
    const KW_KEY: SearchAttributeKey<String> = SearchAttributeKey::keyword("my_keyword");
    const TEXT_KEY: SearchAttributeKey<String> = SearchAttributeKey::text("my_text");
    const DT_KEY: SearchAttributeKey<Timestamp> = SearchAttributeKey::datetime("my_datetime");
    const KWL_KEY: SearchAttributeKey<Vec<String>> =
        SearchAttributeKey::keyword_list("my_keyword_list");

    fn assert_payload_metadata(payload: &Payload, expected_type: &str) {
        assert_eq!(
            payload.metadata.get(ENCODING_PAYLOAD_KEY).unwrap(),
            JSON_ENCODING_VAL.as_bytes()
        );
        assert_eq!(
            payload.metadata.get(TYPE_METADATA_KEY).unwrap(),
            expected_type.as_bytes()
        );
    }

    #[test]
    fn round_trip_bool() {
        let val = true;
        let payload = val
            .to_search_attribute_payload(IndexedValueType::Bool)
            .unwrap();
        assert_payload_metadata(&payload, "Bool");
        assert_eq!(
            bool::from_search_attribute_payload(&payload).unwrap(),
            true
        );
    }

    #[test]
    fn round_trip_int() {
        let val: i64 = -42;
        let payload = val
            .to_search_attribute_payload(IndexedValueType::Int)
            .unwrap();
        assert_payload_metadata(&payload, "Int");
        assert_eq!(i64::from_search_attribute_payload(&payload).unwrap(), -42);
    }

    #[test]
    fn round_trip_double() {
        let val: f64 = 3.14;
        let payload = val
            .to_search_attribute_payload(IndexedValueType::Double)
            .unwrap();
        assert_payload_metadata(&payload, "Double");
        let decoded = f64::from_search_attribute_payload(&payload).unwrap();
        assert!((decoded - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn round_trip_keyword() {
        let val = "hello".to_string();
        let payload = val
            .to_search_attribute_payload(IndexedValueType::Keyword)
            .unwrap();
        assert_payload_metadata(&payload, "Keyword");
        assert_eq!(
            String::from_search_attribute_payload(&payload).unwrap(),
            "hello"
        );
    }

    #[test]
    fn round_trip_text() {
        let val = "some long text".to_string();
        let payload = val
            .to_search_attribute_payload(IndexedValueType::Text)
            .unwrap();
        assert_payload_metadata(&payload, "Text");
        assert_eq!(
            String::from_search_attribute_payload(&payload).unwrap(),
            "some long text"
        );
    }

    #[test]
    fn round_trip_datetime() {
        let ts = Timestamp {
            seconds: 1_700_000_000,
            nanos: 123_456_789,
        };
        let payload = ts
            .to_search_attribute_payload(IndexedValueType::Datetime)
            .unwrap();
        assert_payload_metadata(&payload, "Datetime");

        let json_str: String = serde_json::from_slice(&payload.data).unwrap();
        assert!(json_str.ends_with('Z'));
        assert!(json_str.contains('T'));

        let decoded = Timestamp::from_search_attribute_payload(&payload).unwrap();
        assert_eq!(decoded.seconds, ts.seconds);
        assert_eq!(decoded.nanos, ts.nanos);

        let attrs = TypedSearchAttributes::new([DT_KEY.value_set(ts.clone())]);
        let got = attrs.get(&DT_KEY).unwrap();
        assert_eq!(got.seconds, ts.seconds);
        assert_eq!(got.nanos, ts.nanos);
    }

    #[test]
    fn round_trip_datetime_no_nanos() {
        let ts = Timestamp {
            seconds: 0,
            nanos: 0,
        };
        let payload = ts
            .to_search_attribute_payload(IndexedValueType::Datetime)
            .unwrap();
        let decoded = Timestamp::from_search_attribute_payload(&payload).unwrap();
        assert_eq!(decoded.seconds, 0);
        assert_eq!(decoded.nanos, 0);
    }

    #[test]
    fn round_trip_keyword_list() {
        let val = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let payload = val
            .to_search_attribute_payload(IndexedValueType::KeywordList)
            .unwrap();
        assert_payload_metadata(&payload, "KeywordList");
        assert_eq!(
            Vec::<String>::from_search_attribute_payload(&payload).unwrap(),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn typed_search_attributes_new_and_get() {
        let attrs = TypedSearchAttributes::new([
            BOOL_KEY.value_set(true),
            INT_KEY.value_set(99),
            FLOAT_KEY.value_set(2.72),
            KW_KEY.value_set("kw_val".into()),
            TEXT_KEY.value_set("text_val".into()),
            KWL_KEY.value_set(vec!["x".into(), "y".into()]),
        ]);

        assert_eq!(attrs.len(), 6);
        assert!(!attrs.is_empty());
        assert_eq!(attrs.get(&BOOL_KEY), Some(true));
        assert_eq!(attrs.get(&INT_KEY), Some(99));
        assert!((attrs.get(&FLOAT_KEY).unwrap() - 2.72).abs() < f64::EPSILON);
        assert_eq!(attrs.get(&KW_KEY), Some("kw_val".into()));
        assert_eq!(attrs.get(&TEXT_KEY), Some("text_val".into()));
        assert_eq!(
            attrs.get(&KWL_KEY),
            Some(vec!["x".to_string(), "y".to_string()])
        );
    }

    #[test]
    fn to_proto_from_proto_round_trip() {
        let attrs = TypedSearchAttributes::new([
            BOOL_KEY.value_set(false),
            INT_KEY.value_set(7),
        ]);

        let proto = attrs.to_proto();
        assert_eq!(proto.indexed_fields.len(), 2);

        let restored = TypedSearchAttributes::from_proto(&proto);
        assert_eq!(restored.get(&BOOL_KEY), Some(false));
        assert_eq!(restored.get(&INT_KEY), Some(7));
    }

    #[test]
    fn value_unset_removes_entry() {
        let attrs = TypedSearchAttributes::new([
            BOOL_KEY.value_set(true),
            BOOL_KEY.value_unset(),
        ]);
        assert!(attrs.is_empty());
        assert_eq!(attrs.get(&BOOL_KEY), None);
    }

    #[test]
    fn keyword_vs_text_disambiguation() {
        let kw_update = KW_KEY.value_set("same_value".into());
        let text_update = TEXT_KEY.value_set("same_value".into());

        let kw_payload = kw_update.payload.as_ref().unwrap();
        let text_payload = text_update.payload.as_ref().unwrap();

        assert_eq!(
            kw_payload.metadata.get(TYPE_METADATA_KEY).unwrap(),
            b"Keyword"
        );
        assert_eq!(
            text_payload.metadata.get(TYPE_METADATA_KEY).unwrap(),
            b"Text"
        );

        assert_eq!(KW_KEY.indexed_value_type(), IndexedValueType::Keyword);
        assert_eq!(TEXT_KEY.indexed_value_type(), IndexedValueType::Text);
    }

    #[test]
    fn get_returns_none_for_missing_key() {
        let attrs = TypedSearchAttributes::default();
        assert_eq!(attrs.get(&BOOL_KEY), None);
        assert!(!attrs.contains_key(&INT_KEY));
    }

    #[test]
    fn get_returns_none_for_type_mismatch() {
        let attrs = TypedSearchAttributes::new([BOOL_KEY.value_set(true)]);
        // Try to read the bool payload as an i64 — should gracefully return None
        let mismatched_key = SearchAttributeKey::<i64>::int("my_bool");
        assert_eq!(attrs.get(&mismatched_key), None);
    }

    #[test]
    fn updates_to_proto_includes_empty_payload_for_unset() {
        let proto =
            TypedSearchAttributes::updates_to_proto([BOOL_KEY.value_set(true), INT_KEY.value_unset()]);

        let bool_payload = proto.indexed_fields.get("my_bool").unwrap();
        assert!(!bool_payload.data.is_empty());

        let int_payload = proto.indexed_fields.get("my_int").unwrap();
        assert!(int_payload.data.is_empty());
        assert!(int_payload.metadata.is_empty());
    }

    #[test]
    fn contains_key_returns_true_when_present() {
        let attrs = TypedSearchAttributes::new([INT_KEY.value_set(42)]);
        assert!(attrs.contains_key(&INT_KEY));
    }

    #[test]
    fn timestamp_rfc3339_format() {
        let ts = Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        };
        let rfc = timestamp_to_rfc3339(&ts);
        assert_eq!(rfc, "2023-11-14T22:13:20Z");
    }

    #[test]
    fn timestamp_rfc3339_with_nanos() {
        let ts = Timestamp {
            seconds: 1_700_000_000,
            nanos: 500_000_000,
        };
        let rfc = timestamp_to_rfc3339(&ts);
        assert_eq!(rfc, "2023-11-14T22:13:20.500000000Z");

        let parsed = rfc3339_to_timestamp(&rfc).unwrap();
        assert_eq!(parsed.seconds, ts.seconds);
        assert_eq!(parsed.nanos, ts.nanos);
    }

    #[test]
    fn search_attribute_update_accessors() {
        let set = BOOL_KEY.value_set(true);
        assert_eq!(set.name(), "my_bool");
        assert!(!set.is_unset());

        let unset = BOOL_KEY.value_unset();
        assert_eq!(unset.name(), "my_bool");
        assert!(unset.is_unset());
    }
}
