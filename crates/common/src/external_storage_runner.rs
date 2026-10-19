//! Stores large payloads in external storage and fetches them back.
//!
//! Outbound, a payload at or above the configured size threshold is handed to a storage driver and
//! replaced by a reference to what the driver stored. Inbound, references are swapped back for the
//! payloads they point at. [`ExternalStorageRunner`] runs either direction over a whole message,
//! visiting every payload-bearing field except search attributes.
//!
//! Each direction is two passes over the message, and the runner's methods sequence them, so a
//! caller only picks a direction. The codec pass walks fields one at a time, because a codec must
//! not be entered from two fields at once. The driver pass is separate for that reason: it makes
//! one call per driver and is free to visit fields concurrently.

// Crate-private until a worker or client calls it.
#![allow(dead_code)]

use futures::future;
use std::{collections::HashMap, sync::Arc};

use prost::Message;
use temporalio_common_wasm::data_converters::{
    ExternalStorage, ExternalStorageError, PayloadCodec, PayloadConversionError,
    SerializationContextData, StorageDriver, StorageDriverClaim, StorageDriverLimiter,
    StorageDriverRetrieveContext, StorageDriverSelectContext, StorageDriverStoreContext,
    StorageDriverTargetInfo,
};

use crate::{
    external_storage_references::{
        create_reference_payload, has_reference_marker, is_plain_reference, mark_reference,
        needs_retrieval, parse_reference, strip_for_decode,
    },
    payload_visitor::{
        AsyncPayloadVisitor, PayloadField, PayloadFieldData, PayloadVisitable, should_transform,
    },
    protos::temporal::api::common::v1::Payload,
};

/// Drives external storage for the payloads reachable from one activation or request.
///
/// Build one per message and share it across that message's calls, so the message-scoped limiter
/// spans them all. Each call takes its own target and serialization context: a worker completion
/// holds many commands that offload to different targets under different contexts (an activity's
/// for `ScheduleActivity`, a child workflow's for `StartChildWorkflowExecution`, and so on), so
/// the caller matches on the command and calls once per command. A uniform message, such as a
/// client request, is a single call.
pub(crate) struct ExternalStorageRunner<'a> {
    storage: &'a ExternalStorage,
    store_limiter: &'a Arc<dyn StorageDriverLimiter<Payload>>,
    retrieve_limiter: &'a Arc<dyn StorageDriverLimiter<StorageDriverClaim>>,
}

impl<'a> ExternalStorageRunner<'a> {
    pub(crate) fn new(
        storage: &'a ExternalStorage,
        store_limiter: &'a Arc<dyn StorageDriverLimiter<Payload>>,
        retrieve_limiter: &'a Arc<dyn StorageDriverLimiter<StorageDriverClaim>>,
    ) -> Self {
        Self {
            storage,
            store_limiter,
            retrieve_limiter,
        }
    }

    /// Replace every eligible payload in a message with a reference.
    pub(crate) async fn store_in<M: PayloadVisitable + Send>(
        &self,
        message: &mut M,
        target: Option<&StorageDriverTargetInfo>,
        codec: &(dyn PayloadCodec + Send + Sync),
        serialization_context: &SerializationContextData,
    ) -> Result<(), ExternalStorageError> {
        let mut store = StorePayloadsVisitor {
            storage: self.storage,
            target,
            limiter: self.store_limiter,
            error: None,
        };
        let walked = message.visit_payloads_mut(&mut store).await;
        finish(store.error, walked)?;

        if should_encode_references() {
            let mut encode = EncodeReferencesVisitor {
                codec,
                serialization_context,
                error: None,
            };
            let walked = message.visit_payloads_mut(&mut encode).await;
            finish(encode.error, walked)?;
        }
        Ok(())
    }

    /// Replace every reference in a message with the payload it points at.
    pub(crate) async fn retrieve_in<M: PayloadVisitable + Send>(
        &self,
        message: &mut M,
        codec: &(dyn PayloadCodec + Send + Sync),
        serialization_context: &SerializationContextData,
    ) -> Result<(), ExternalStorageError> {
        let mut decode = DecodeReferencesVisitor {
            codec,
            serialization_context,
            error: None,
        };
        let walked = message.visit_payloads_mut(&mut decode).await;
        finish(decode.error, walked)?;

        let mut retrieve = RetrievePayloadsVisitor {
            storage: self.storage,
            limiter: self.retrieve_limiter,
            error: None,
        };
        let walked = message.visit_payloads_mut(&mut retrieve).await;
        finish(retrieve.error, walked)
    }
}

/// Fail if any payload needs retrieval. For a caller with no external storage configured, which
/// must not hand a reference to application code as though it were the application's own data.
pub(crate) fn reject_references(payloads: &[Payload]) -> Result<(), ExternalStorageError> {
    if payloads.iter().any(needs_retrieval) {
        return Err(ExternalStorageError::NotConfigured);
    }
    Ok(())
}

/// Decode every marked reference so that each is left in plain form, ready for
/// [`retrieve_payloads`]. Run this sequentially: it is the only phase that calls the codec.
async fn decode_references(
    payloads: &mut [Payload],
    codec: &(dyn PayloadCodec + Send + Sync),
    serialization_context: &SerializationContextData,
) -> Result<(), ExternalStorageError> {
    for payload in payloads.iter_mut() {
        if !has_reference_marker(payload) {
            continue;
        }
        let mut decoded = codec
            .decode(serialization_context, vec![strip_for_decode(payload)])
            .await
            .map_err(|e| ExternalStorageError::Codec(Box::new(e)))?
            .into_iter()
            .next()
            .ok_or(ExternalStorageError::UndecodableReference)?;
        if !is_plain_reference(&decoded) {
            return Err(ExternalStorageError::UndecodableReference);
        }
        if decoded.external_payloads.is_empty() {
            decoded.external_payloads = payload.external_payloads.clone();
        }
        *payload = decoded;
    }
    Ok(())
}

/// Replace every payload that needs retrieval with the payload the driver returns for it.
///
/// Every reference must already be in plain form; run [`decode_references`] first when a codec is
/// configured.
///
/// Each retrieved payload is left exactly as the driver returned it: the codec-encoded form
/// `store` was given, which the normal decode path then handles like any inline payload.
async fn retrieve_payloads(
    payloads: &mut [Payload],
    storage: &ExternalStorage,
    limiter: &Arc<dyn StorageDriverLimiter<StorageDriverClaim>>,
) -> Result<(), ExternalStorageError> {
    let mut by_driver: HashMap<String, Vec<(usize, StorageDriverClaim)>> = HashMap::new();
    for (index, payload) in payloads.iter().enumerate() {
        if !needs_retrieval(payload) {
            continue;
        }
        // Still marked, so it was never decoded: either no codec is configured or the decode
        // phase did not run.
        if !is_plain_reference(payload) {
            return Err(ExternalStorageError::UndecodableReference);
        }
        let reference = parse_reference(payload)?;
        by_driver
            .entry(reference.driver_name)
            .or_default()
            .push((index, StorageDriverClaim::new(reference.claim_data)));
    }

    // Resolve every driver first, so an unregistered name fails before any request is made.
    let mut work = Vec::with_capacity(by_driver.len());
    for (driver_name, group) in by_driver {
        let driver = storage
            .driver(&driver_name)
            .ok_or_else(|| ExternalStorageError::UnregisteredDriver(driver_name.clone()))?;
        work.push((driver_name, driver, group));
    }

    let retrieved_groups =
        future::try_join_all(work.into_iter().map(|(driver_name, driver, group)| {
            let limiter = Arc::clone(limiter);
            async move {
                let context = StorageDriverRetrieveContext::builder()
                    .limiter(limiter)
                    .build();
                let claims: Vec<_> = group.iter().map(|(_, claim)| claim.clone()).collect();
                let retrieved = driver.retrieve(&context, claims).await?;
                if retrieved.len() != group.len() {
                    return Err(ExternalStorageError::DriverContract {
                        driver: driver_name,
                        requested: group.len(),
                        returned: retrieved.len(),
                    });
                }
                Ok::<_, ExternalStorageError>(
                    group
                        .into_iter()
                        .map(|(index, _)| index)
                        .zip(retrieved)
                        .collect::<Vec<_>>(),
                )
            }
        }))
        .await?;

    for group in retrieved_groups {
        for (index, payload) in group {
            payloads[index] = payload;
        }
    }
    Ok(())
}

/// The visitor reports only that it failed, so the typed error it captured is preferred over the
/// placeholder that stopped the walk.
fn finish(
    captured: Option<ExternalStorageError>,
    walked: Result<(), PayloadConversionError>,
) -> Result<(), ExternalStorageError> {
    if let Some(error) = captured {
        return Err(error);
    }
    walked.map_err(|e| ExternalStorageError::Walk(Box::new(e)))
}

fn stop_walk() -> PayloadConversionError {
    PayloadConversionError::EncodingError("external storage pass failed".into())
}

struct DecodeReferencesVisitor<'a> {
    codec: &'a (dyn PayloadCodec + Send + Sync),
    serialization_context: &'a SerializationContextData,
    error: Option<ExternalStorageError>,
}

impl AsyncPayloadVisitor for DecodeReferencesVisitor<'_> {
    fn visit<'a>(
        &'a mut self,
        field: PayloadField<'a>,
    ) -> futures::future::BoxFuture<'a, Result<(), PayloadConversionError>> {
        Box::pin(async move {
            if !should_transform(field.path) {
                return Ok(());
            }
            let codec = self.codec;
            let context = self.serialization_context;
            let result = match field.data {
                PayloadFieldData::Single(payload) => {
                    decode_references(std::slice::from_mut(payload), codec, context).await
                }
                PayloadFieldData::Repeated(payloads) => {
                    decode_references(payloads, codec, context).await
                }
                PayloadFieldData::Payloads(message) => {
                    decode_references(&mut message.payloads, codec, context).await
                }
            };
            match result {
                Ok(()) => Ok(()),
                Err(e) => {
                    self.error = Some(e);
                    Err(stop_walk())
                }
            }
        })
    }
}

struct EncodeReferencesVisitor<'a> {
    codec: &'a (dyn PayloadCodec + Send + Sync),
    serialization_context: &'a SerializationContextData,
    error: Option<ExternalStorageError>,
}

impl AsyncPayloadVisitor for EncodeReferencesVisitor<'_> {
    fn visit<'a>(
        &'a mut self,
        field: PayloadField<'a>,
    ) -> futures::future::BoxFuture<'a, Result<(), PayloadConversionError>> {
        Box::pin(async move {
            if !should_transform(field.path) {
                return Ok(());
            }
            let codec = self.codec;
            let context = self.serialization_context;
            let result = match field.data {
                PayloadFieldData::Single(payload) => {
                    encode_references(std::slice::from_mut(payload), codec, context).await
                }
                PayloadFieldData::Repeated(payloads) => {
                    encode_references(payloads, codec, context).await
                }
                PayloadFieldData::Payloads(message) => {
                    encode_references(&mut message.payloads, codec, context).await
                }
            };
            match result {
                Ok(()) => Ok(()),
                Err(e) => {
                    self.error = Some(e);
                    Err(stop_walk())
                }
            }
        })
    }
}

struct StorePayloadsVisitor<'a> {
    storage: &'a ExternalStorage,
    target: Option<&'a StorageDriverTargetInfo>,
    limiter: &'a Arc<dyn StorageDriverLimiter<Payload>>,
    error: Option<ExternalStorageError>,
}
impl AsyncPayloadVisitor for StorePayloadsVisitor<'_> {
    fn visit<'a>(
        &'a mut self,
        field: PayloadField<'a>,
    ) -> futures::future::BoxFuture<'a, Result<(), PayloadConversionError>> {
        Box::pin(async move {
            if !should_transform(field.path) {
                return Ok(());
            }
            let result = match field.data {
                PayloadFieldData::Single(payload) => {
                    store_payloads(
                        std::slice::from_mut(payload),
                        self.storage,
                        self.target,
                        self.limiter,
                    )
                    .await
                }
                PayloadFieldData::Repeated(payloads) => {
                    store_payloads(payloads, self.storage, self.target, self.limiter).await
                }
                PayloadFieldData::Payloads(message) => {
                    store_payloads(
                        &mut message.payloads,
                        self.storage,
                        self.target,
                        self.limiter,
                    )
                    .await
                }
            };
            match result {
                Ok(()) => Ok(()),
                Err(e) => {
                    self.error = Some(e);
                    Err(stop_walk())
                }
            }
        })
    }
}

struct RetrievePayloadsVisitor<'a> {
    storage: &'a ExternalStorage,
    limiter: &'a Arc<dyn StorageDriverLimiter<StorageDriverClaim>>,
    error: Option<ExternalStorageError>,
}

impl AsyncPayloadVisitor for RetrievePayloadsVisitor<'_> {
    fn visit<'a>(
        &'a mut self,
        field: PayloadField<'a>,
    ) -> futures::future::BoxFuture<'a, Result<(), PayloadConversionError>> {
        Box::pin(async move {
            if !should_transform(field.path) {
                return Ok(());
            }
            let result = match field.data {
                PayloadFieldData::Single(payload) => {
                    retrieve_payloads(std::slice::from_mut(payload), self.storage, self.limiter)
                        .await
                }
                PayloadFieldData::Repeated(payloads) => {
                    retrieve_payloads(payloads, self.storage, self.limiter).await
                }
                PayloadFieldData::Payloads(message) => {
                    retrieve_payloads(&mut message.payloads, self.storage, self.limiter).await
                }
            };
            match result {
                Ok(()) => Ok(()),
                Err(e) => {
                    self.error = Some(e);
                    Err(stop_walk())
                }
            }
        })
    }
}

/// Writing marked, codec-encoded references stays off until a release that can read them has been
/// out long enough for fleets to upgrade: a worker on an earlier version cannot read one. Reading
/// is always on. Flip this in a later release to start writing them.
const ENCODE_STORAGE_REFERENCES: bool = false;

/// Whether the store side marks and encodes references. The production value is the constant above;
/// tests flip it per-thread with [`EncodeReferencesGate`].
#[cfg(not(test))]
fn should_encode_references() -> bool {
    ENCODE_STORAGE_REFERENCES
}

#[cfg(test)]
thread_local! {
    static ENCODE_OVERRIDE: std::cell::Cell<bool> =
        const { std::cell::Cell::new(ENCODE_STORAGE_REFERENCES) };
}

#[cfg(test)]
fn should_encode_references() -> bool {
    ENCODE_OVERRIDE.with(std::cell::Cell::get)
}

/// One driver and the payloads it takes: each payload's index, the payload, and its pre-offload
/// proto size.
type DriverGroup = (Arc<dyn StorageDriver>, Vec<(usize, Payload, i64)>);

/// Replace every eligible payload with a reference, marking each one it creates when writing is
/// enabled so [`encode_references`] can find them without re-examining encodings.
async fn store_payloads(
    payloads: &mut [Payload],
    storage: &ExternalStorage,
    target: Option<&StorageDriverTargetInfo>,
    limiter: &Arc<dyn StorageDriverLimiter<Payload>>,
) -> Result<(), ExternalStorageError> {
    let select_context = StorageDriverSelectContext::builder()
        .maybe_target(target.cloned())
        .build();

    let mut by_driver: HashMap<String, DriverGroup> = HashMap::new();
    for (index, payload) in payloads.iter().enumerate() {
        // Measured after the codec has run, so a codec that compresses can take a payload back
        // below the threshold.
        let size = payload.encoded_len();
        if size < storage.payload_size_threshold() {
            continue;
        }
        let Some(driver) = storage
            .driver_selector()
            .select_driver(&select_context, payload)
        else {
            continue;
        };
        // Checked by identity, so a driver built to look like a registered one is rejected.
        if !storage.drivers().iter().any(|d| Arc::ptr_eq(d, &driver)) {
            return Err(ExternalStorageError::UnregisteredSelection(
                driver.name().to_owned(),
            ));
        }
        by_driver
            .entry(driver.name().to_owned())
            .or_insert_with(|| (Arc::clone(&driver), Vec::new()))
            .1
            .push((index, payload.clone(), size as i64));
    }

    let stored =
        future::try_join_all(by_driver.into_iter().map(|(driver_name, (driver, group))| {
            let limiter = Arc::clone(limiter);
            let target = target.cloned();
            async move {
                let context = StorageDriverStoreContext::builder()
                    .maybe_target(target)
                    .limiter(limiter)
                    .build();
                let to_store: Vec<_> = group
                    .iter()
                    .map(|(_, payload, _)| payload.clone())
                    .collect();
                let claims = driver.store(&context, to_store).await?;
                if claims.len() != group.len() {
                    return Err(ExternalStorageError::DriverContract {
                        driver: driver_name,
                        requested: group.len(),
                        returned: claims.len(),
                    });
                }
                let mut replaced = Vec::with_capacity(group.len());
                for ((index, _, size), claim) in group.into_iter().zip(claims) {
                    let mut reference =
                        create_reference_payload(&driver_name, claim.claim_data, size)?;
                    if should_encode_references() {
                        // So the reference is marked even with no codec; the encode pass re-marks
                        // it when there is one.
                        mark_reference(&mut reference);
                    }
                    replaced.push((index, reference));
                }
                Ok::<_, ExternalStorageError>(replaced)
            }
        }))
        .await?;

    for group in stored {
        for (index, reference) in group {
            payloads[index] = reference;
        }
    }
    Ok(())
}

/// Encode the references [`store_payloads`] created and mark them, so a reader finds them
/// without decoding anything. Run this sequentially: it is the only outbound phase that calls the
/// codec.
async fn encode_references(
    payloads: &mut [Payload],
    codec: &(dyn PayloadCodec + Send + Sync),
    serialization_context: &SerializationContextData,
) -> Result<(), ExternalStorageError> {
    for payload in payloads.iter_mut() {
        if !has_reference_marker(payload) {
            continue;
        }
        let reference = strip_for_decode(payload);
        let mut encoded = codec
            .encode(serialization_context, vec![reference.clone()])
            .await
            .map_err(|e| ExternalStorageError::Codec(Box::new(e)))?
            .into_iter()
            .next()
            .ok_or_else(|| {
                ExternalStorageError::Codec("no payload returned when encoding a reference".into())
            })?;
        if encoded.external_payloads.is_empty() {
            encoded.external_payloads = std::mem::take(&mut payload.external_payloads);
        }
        mark_reference(&mut encoded);
        *payload = encoded;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        external_storage_references::{create_reference_payload, mark_reference},
        protos::temporal::api::{
            common::v1::{Header, Memo, Payloads, SearchAttributes},
            workflowservice::v1::SignalWithStartWorkflowExecutionRequest,
        },
    };
    use futures::future::{self, BoxFuture};
    use std::sync::{Mutex, atomic::Ordering};
    use temporalio_common_wasm::data_converters::{
        PayloadConversionError, StorageDriver, StorageDriverPermit, StorageDriverSelector,
    };

    /// Turns the write side on for the current test thread and restores the default on drop, so
    /// the store path can be exercised in the form it will ship in once writing is enabled.
    struct EncodeReferencesGate;

    impl EncodeReferencesGate {
        fn on() -> Self {
            ENCODE_OVERRIDE.with(|c| c.set(true));
            EncodeReferencesGate
        }
    }

    impl Drop for EncodeReferencesGate {
        fn drop(&mut self) {
            ENCODE_OVERRIDE.with(|c| c.set(ENCODE_STORAGE_REFERENCES));
        }
    }

    /// Returns a payload per claim and records the batches it was called with.
    struct FakeDriver {
        name: &'static str,
        batches: Arc<Mutex<Vec<usize>>>,
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
        peak_in_flight: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl StorageDriver for FakeDriver {
        fn name(&self) -> &str {
            self.name
        }
        fn driver_type(&self) -> &str {
            "test.fakedriver"
        }
        fn store(
            &self,
            _context: &temporalio_common_wasm::data_converters::StorageDriverStoreContext,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<StorageDriverClaim>, ExternalStorageError>> {
            self.batches.lock().unwrap().push(payloads.len());
            let in_flight = Arc::clone(&self.in_flight);
            let peak = Arc::clone(&self.peak_in_flight);
            Box::pin(async move {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                // Stand in for network I/O, letting another driver make progress.
                for _ in 0..4 {
                    tokio::task::yield_now().await;
                }
                in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(payloads
                    .into_iter()
                    .map(|p| {
                        StorageDriverClaim::new(HashMap::from([(
                            "key".to_owned(),
                            String::from_utf8(p.data).unwrap(),
                        )]))
                    })
                    .collect())
            })
        }
        fn retrieve(
            &self,
            _context: &StorageDriverRetrieveContext,
            claims: Vec<StorageDriverClaim>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, ExternalStorageError>> {
            self.batches.lock().unwrap().push(claims.len());
            let in_flight = Arc::clone(&self.in_flight);
            let peak = Arc::clone(&self.peak_in_flight);
            Box::pin(async move {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                // Stand in for network I/O, letting another driver make progress.
                for _ in 0..4 {
                    tokio::task::yield_now().await;
                }
                in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(claims
                    .into_iter()
                    .map(|c| Payload {
                        metadata: HashMap::from([("encoding".to_owned(), b"json/plain".to_vec())]),
                        data: c.claim_data["key"].clone().into_bytes(),
                        external_payloads: vec![],
                    })
                    .collect())
            })
        }
    }

    struct PassThroughLimiter;
    impl<Item: Sync> StorageDriverLimiter<Item> for PassThroughLimiter {
        fn acquire<'a>(
            &'a self,
            _item: &'a Item,
        ) -> BoxFuture<'a, Result<StorageDriverPermit, ExternalStorageError>> {
            Box::pin(future::ready(Ok(StorageDriverPermit::new(Box::new(())))))
        }
    }

    /// A pass-through codec, returning every payload unchanged. Stands in for the wrong codec, or
    /// none, so a marked payload decodes to something that is not a reference.
    struct IdentityCodec;
    impl PayloadCodec for IdentityCodec {
        fn encode(
            &self,
            _c: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            Box::pin(future::ready(Ok(payloads)))
        }
        fn decode(
            &self,
            _c: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            Box::pin(future::ready(Ok(payloads)))
        }
    }

    /// Reverses the bytes on encode and back on decode, so an encoded reference no longer looks
    /// like a plain one.
    struct ReversingCodec;
    impl PayloadCodec for ReversingCodec {
        fn encode(
            &self,
            _c: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            Box::pin(future::ready(Ok(payloads
                .into_iter()
                .map(|mut p| {
                    p.data.reverse();
                    p.metadata
                        .insert("encoding".to_owned(), b"binary/reversed".to_vec());
                    p
                })
                .collect())))
        }
        fn decode(
            &self,
            _c: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            Box::pin(future::ready(Ok(payloads
                .into_iter()
                .map(|mut p| {
                    p.data.reverse();
                    p.metadata
                        .insert("encoding".to_owned(), b"json/protobuf".to_vec());
                    p.metadata.insert(
                        "messageType".to_owned(),
                        b"temporal.api.sdk.v1.ExternalStorageReference".to_vec(),
                    );
                    p
                })
                .collect())))
        }
    }

    fn storage(name: &'static str, batches: Arc<Mutex<Vec<usize>>>) -> ExternalStorage {
        ExternalStorage::new(Arc::new(FakeDriver {
            name,
            batches,
            in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            peak_in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }))
        .unwrap()
    }

    /// Never selects a driver. Retrieval ignores the selector, so this stands in where
    /// `with_selector` needs an argument it will not use.
    struct NeverSelects;
    impl temporalio_common_wasm::data_converters::StorageDriverSelector for NeverSelects {
        fn select_driver(
            &self,
            _context: &temporalio_common_wasm::data_converters::StorageDriverSelectContext,
            _payload: &Payload,
        ) -> Option<Arc<dyn StorageDriver>> {
            None
        }
    }

    fn limiter() -> Arc<dyn StorageDriverLimiter<StorageDriverClaim>> {
        Arc::new(PassThroughLimiter)
    }

    fn reference(driver: &str, key: &str) -> Payload {
        create_reference_payload(
            driver,
            HashMap::from([("key".to_owned(), key.to_owned())]),
            99,
        )
        .unwrap()
    }

    fn inline(data: &str) -> Payload {
        Payload {
            metadata: HashMap::from([("encoding".to_owned(), b"json/plain".to_vec())]),
            data: data.as_bytes().to_vec(),
            external_payloads: vec![],
        }
    }

    #[tokio::test]
    async fn references_are_replaced_and_inline_payloads_untouched() {
        let batches = Arc::new(Mutex::new(vec![]));
        let storage = storage("mem", Arc::clone(&batches));
        let mut payloads = vec![
            inline("first"),
            reference("mem", "a"),
            inline("third"),
            reference("mem", "b"),
        ];

        retrieve_payloads(&mut payloads, &storage, &limiter())
            .await
            .unwrap();

        assert_eq!(payloads[0].data, b"first", "inline payload untouched");
        assert_eq!(payloads[1].data, b"a");
        assert_eq!(payloads[2].data, b"third", "inline payload untouched");
        assert_eq!(payloads[3].data, b"b");
        // One call for the driver, carrying both of its claims, not one call per payload.
        assert_eq!(*batches.lock().unwrap(), vec![2]);
    }

    #[tokio::test]
    async fn two_drivers_retrieve_concurrently() {
        // Both drivers share the counters, so the peak is across the pair.
        let in_flight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let make = |name: &'static str| -> Arc<dyn StorageDriver> {
            Arc::new(FakeDriver {
                name,
                batches: Arc::new(Mutex::new(vec![])),
                in_flight: Arc::clone(&in_flight),
                peak_in_flight: Arc::clone(&peak),
            })
        };
        let storage =
            ExternalStorage::with_selector(vec![make("a"), make("b")], Arc::new(NeverSelects))
                .unwrap();
        let mut payloads = vec![reference("a", "one"), reference("b", "two")];

        retrieve_payloads(&mut payloads, &storage, &limiter())
            .await
            .unwrap();

        assert_eq!(payloads[0].data, b"one");
        assert_eq!(payloads[1].data, b"two");
        assert_eq!(
            peak.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "both drivers were in flight at once"
        );
    }

    #[tokio::test]
    async fn retrieve_in_decodes_a_marked_reference_before_reading_it() {
        let storage = storage("mem", Arc::new(Mutex::new(vec![])));
        let store_lim = store_limiter();
        let retrieve_lim = limiter();
        let runner = ExternalStorageRunner::new(&storage, &store_lim, &retrieve_lim);
        let mut encoded = ReversingCodec
            .encode(&SerializationContextData::None, vec![reference("mem", "z")])
            .await
            .unwrap()
            .remove(0);
        mark_reference(&mut encoded);
        let mut request = SignalWithStartWorkflowExecutionRequest {
            input: Some(Payloads {
                payloads: vec![encoded],
            }),
            ..Default::default()
        };

        runner
            .retrieve_in(
                &mut request,
                &ReversingCodec,
                &SerializationContextData::None,
            )
            .await
            .unwrap();

        assert_eq!(request.input.unwrap().payloads[0].data, b"z");
    }

    #[tokio::test]
    async fn retrieve_in_reports_tmprl1106_when_a_marked_payload_decodes_to_a_non_reference() {
        // The case a reader actually hits: a marked payload whose codec does not recover a
        // reference, e.g. the wrong codec or none at all. IdentityCodec stands in for a codec that
        // leaves the payload as-is, so the decode produces opaque bytes rather than a reference.
        let storage = storage("mem", Arc::new(Mutex::new(vec![])));
        let store_lim = store_limiter();
        let retrieve_lim = limiter();
        let runner = ExternalStorageRunner::new(&storage, &store_lim, &retrieve_lim);
        let mut payload = inline("opaque");
        mark_reference(&mut payload);
        let mut request = SignalWithStartWorkflowExecutionRequest {
            input: Some(Payloads {
                payloads: vec![payload],
            }),
            ..Default::default()
        };

        let err = runner
            .retrieve_in(
                &mut request,
                &IdentityCodec,
                &SerializationContextData::None,
            )
            .await
            .unwrap_err();

        assert!(matches!(err, ExternalStorageError::UndecodableReference));
        assert!(err.to_string().contains("TMPRL1106"));
    }

    #[tokio::test]
    async fn decoding_restores_external_payloads_the_strip_removed() {
        // strip_for_decode clears external_payloads before the codec runs, so the codec never
        // carries the recorded size. Decode restores it from the original marked payload.
        let mut encoded = ReversingCodec
            .encode(&SerializationContextData::None, vec![reference("mem", "z")])
            .await
            .unwrap()
            .remove(0);
        encoded.external_payloads = vec![
            crate::protos::temporal::api::common::v1::payload::ExternalPayloadDetails {
                size_bytes: 99,
            },
        ];
        mark_reference(&mut encoded);
        let mut payloads = vec![encoded];

        decode_references(
            &mut payloads,
            &ReversingCodec,
            &SerializationContextData::None,
        )
        .await
        .unwrap();

        assert_eq!(payloads[0].external_payloads[0].size_bytes, 99);
    }

    // ---- offload ----

    /// Stores whatever it is given, returning a claim keyed by position.
    struct StoringDriver {
        name: &'static str,
        stored: Arc<Mutex<Vec<Payload>>>,
    }

    impl StorageDriver for StoringDriver {
        fn name(&self) -> &str {
            self.name
        }
        fn driver_type(&self) -> &str {
            "test.storingdriver"
        }
        fn store(
            &self,
            _context: &StorageDriverStoreContext,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<StorageDriverClaim>, ExternalStorageError>> {
            let stored = Arc::clone(&self.stored);
            Box::pin(async move {
                let mut held = stored.lock().unwrap();
                let claims = payloads
                    .into_iter()
                    .map(|p| {
                        held.push(p);
                        StorageDriverClaim::new(HashMap::from([(
                            "key".to_owned(),
                            format!("k{}", held.len() - 1),
                        )]))
                    })
                    .collect();
                Ok(claims)
            })
        }
        fn retrieve(
            &self,
            _context: &StorageDriverRetrieveContext,
            _claims: Vec<StorageDriverClaim>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, ExternalStorageError>> {
            Box::pin(future::ready(Ok(vec![])))
        }
    }

    /// Returns an empty batch regardless of input, so store and retrieve both violate the
    /// one-result-per-input contract.
    struct MiscountingDriver {
        name: &'static str,
    }
    impl StorageDriver for MiscountingDriver {
        fn name(&self) -> &str {
            self.name
        }
        fn driver_type(&self) -> &str {
            "test.miscounting"
        }
        fn store(
            &self,
            _context: &StorageDriverStoreContext,
            _payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<StorageDriverClaim>, ExternalStorageError>> {
            Box::pin(future::ready(Ok(vec![])))
        }
        fn retrieve(
            &self,
            _context: &StorageDriverRetrieveContext,
            _claims: Vec<StorageDriverClaim>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, ExternalStorageError>> {
            Box::pin(future::ready(Ok(vec![])))
        }
    }

    /// Routes each payload to the driver keyed by its first data byte, declining any other.
    struct SelectByFirstByte(HashMap<u8, Arc<dyn StorageDriver>>);
    impl StorageDriverSelector for SelectByFirstByte {
        fn select_driver(
            &self,
            _context: &StorageDriverSelectContext,
            payload: &Payload,
        ) -> Option<Arc<dyn StorageDriver>> {
            payload
                .data
                .first()
                .and_then(|b| self.0.get(b))
                .map(Arc::clone)
        }
    }

    /// Returns the one driver it was built with, whether or not that driver is registered.
    struct FixedSelector(Arc<dyn StorageDriver>);
    impl StorageDriverSelector for FixedSelector {
        fn select_driver(
            &self,
            _context: &StorageDriverSelectContext,
            _payload: &Payload,
        ) -> Option<Arc<dyn StorageDriver>> {
            Some(Arc::clone(&self.0))
        }
    }

    fn store_limiter() -> Arc<dyn StorageDriverLimiter<Payload>> {
        Arc::new(PassThroughLimiter)
    }

    fn filled(byte: u8, bytes: usize) -> Payload {
        Payload {
            metadata: HashMap::from([("encoding".to_owned(), b"json/plain".to_vec())]),
            data: vec![byte; bytes],
            external_payloads: vec![],
        }
    }

    fn sized(bytes: usize) -> Payload {
        filled(b'x', bytes)
    }

    fn storage_with_threshold(
        threshold: usize,
        stored: Arc<Mutex<Vec<Payload>>>,
    ) -> ExternalStorage {
        ExternalStorage::new(Arc::new(StoringDriver {
            name: "mem",
            stored,
        }))
        .unwrap()
        .with_payload_size_threshold(threshold)
    }

    #[tokio::test]
    async fn a_payload_at_the_threshold_is_offloaded_and_a_smaller_one_is_not() {
        let small = sized(8);
        let at_threshold = sized(64);
        let threshold = at_threshold.encoded_len();
        assert!(small.encoded_len() < threshold);
        let stored = Arc::new(Mutex::new(vec![]));
        let storage = storage_with_threshold(threshold, Arc::clone(&stored));
        let mut payloads = vec![small.clone(), at_threshold.clone()];

        store_payloads(&mut payloads, &storage, None, &store_limiter())
            .await
            .unwrap();

        assert_eq!(payloads[0], small, "below the threshold, left inline");
        assert!(
            crate::external_storage_references::is_plain_reference(&payloads[1]),
            "only the one at the threshold was offloaded"
        );
        // What the driver was handed is byte-identical to what history would have held inline.
        assert_eq!(stored.lock().unwrap()[0], at_threshold);
    }

    #[tokio::test]
    async fn the_reference_records_the_pre_offload_size() {
        let stored = Arc::new(Mutex::new(vec![]));
        let storage = storage_with_threshold(1, Arc::clone(&stored));
        let payload = sized(100);
        let size = payload.encoded_len() as i64;
        let mut payloads = vec![payload];

        store_payloads(&mut payloads, &storage, None, &store_limiter())
            .await
            .unwrap();

        assert_eq!(payloads[0].external_payloads[0].size_bytes, size);
    }

    #[tokio::test]
    async fn a_driver_the_selector_invented_is_rejected() {
        let stored = Arc::new(Mutex::new(vec![]));
        let registered: Arc<dyn StorageDriver> = Arc::new(StoringDriver {
            name: "mem",
            stored: Arc::clone(&stored),
        });
        // Same name, different instance.
        let impostor: Arc<dyn StorageDriver> = Arc::new(StoringDriver {
            name: "mem",
            stored: Arc::clone(&stored),
        });
        let storage =
            ExternalStorage::with_selector(vec![registered], Arc::new(FixedSelector(impostor)))
                .unwrap()
                .with_payload_size_threshold(1);
        let mut payloads = vec![sized(50)];

        let err = store_payloads(&mut payloads, &storage, None, &store_limiter())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            ExternalStorageError::UnregisteredSelection(name) if name == "mem"
        ));
    }

    /// Encodes without preserving `external_payloads`, as a codec that rebuilds the payload would.
    struct DroppingCodec;
    impl PayloadCodec for DroppingCodec {
        fn encode(
            &self,
            _c: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            Box::pin(future::ready(Ok(payloads
                .into_iter()
                .map(|p| Payload {
                    metadata: HashMap::from([("encoding".to_owned(), b"binary/dropped".to_vec())]),
                    data: p.data,
                    external_payloads: vec![],
                })
                .collect())))
        }
        fn decode(
            &self,
            _c: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            Box::pin(future::ready(Ok(payloads)))
        }
    }

    /// Returns nothing at all, which no codec should do.
    struct EmptyCodec;
    impl PayloadCodec for EmptyCodec {
        fn encode(
            &self,
            _c: &SerializationContextData,
            _payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            Box::pin(future::ready(Ok(vec![])))
        }
        fn decode(
            &self,
            _c: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            Box::pin(future::ready(Ok(payloads)))
        }
    }

    #[tokio::test]
    async fn a_store_driver_returning_the_wrong_number_of_claims_is_an_error() {
        let driver: Arc<dyn StorageDriver> = Arc::new(MiscountingDriver { name: "mem" });
        let storage = ExternalStorage::with_selector(
            vec![Arc::clone(&driver)],
            Arc::new(FixedSelector(driver)),
        )
        .unwrap()
        .with_payload_size_threshold(1);
        let mut payloads = vec![sized(50)];

        let err = store_payloads(&mut payloads, &storage, None, &store_limiter())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            ExternalStorageError::DriverContract {
                driver,
                requested: 1,
                returned: 0,
            } if driver == "mem"
        ));
    }

    #[tokio::test]
    async fn a_retrieve_driver_returning_the_wrong_number_of_payloads_is_an_error() {
        let storage = ExternalStorage::new(Arc::new(MiscountingDriver { name: "mem" })).unwrap();
        let mut payloads = vec![reference("mem", "a")];

        let err = retrieve_payloads(&mut payloads, &storage, &limiter())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            ExternalStorageError::DriverContract {
                driver,
                requested: 1,
                returned: 0,
            } if driver == "mem"
        ));
    }

    #[tokio::test]
    async fn a_payload_the_selector_declines_stays_inline() {
        let stored = Arc::new(Mutex::new(vec![]));
        let driver: Arc<dyn StorageDriver> = Arc::new(StoringDriver { name: "a", stored });
        // Routes a payload beginning with b'a' and declines one beginning with b'z'.
        let storage = ExternalStorage::with_selector(
            vec![Arc::clone(&driver)],
            Arc::new(SelectByFirstByte(HashMap::from([(b'a', driver)]))),
        )
        .unwrap()
        .with_payload_size_threshold(1);
        let declined = filled(b'z', 50);
        let mut payloads = vec![filled(b'a', 50), declined.clone()];

        store_payloads(&mut payloads, &storage, None, &store_limiter())
            .await
            .unwrap();

        assert!(
            is_plain_reference(&payloads[0]),
            "the routed payload was offloaded"
        );
        assert_eq!(
            payloads[1], declined,
            "the declined payload was left inline"
        );
    }

    #[tokio::test]
    async fn payloads_are_grouped_by_driver_and_restored_in_order() {
        let batches_a = Arc::new(Mutex::new(vec![]));
        let batches_b = Arc::new(Mutex::new(vec![]));
        let make =
            |name: &'static str, batches: &Arc<Mutex<Vec<usize>>>| -> Arc<dyn StorageDriver> {
                Arc::new(FakeDriver {
                    name,
                    batches: Arc::clone(batches),
                    in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                    peak_in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                })
            };
        let a = make("a", &batches_a);
        let b = make("b", &batches_b);
        let storage = ExternalStorage::with_selector(
            vec![Arc::clone(&a), Arc::clone(&b)],
            Arc::new(SelectByFirstByte(HashMap::from([(b'a', a), (b'b', b)]))),
        )
        .unwrap()
        .with_payload_size_threshold(1);
        // Interleaved, so grouping by driver and restoring to original order both matter.
        let mut payloads = vec![
            filled(b'a', 50),
            filled(b'b', 50),
            filled(b'a', 50),
            filled(b'b', 50),
        ];

        store_payloads(&mut payloads, &storage, None, &store_limiter())
            .await
            .unwrap();

        for p in &payloads {
            assert!(is_plain_reference(p));
        }
        // One call per driver, each carrying that driver's two payloads.
        assert_eq!(*batches_a.lock().unwrap(), vec![2]);
        assert_eq!(*batches_b.lock().unwrap(), vec![2]);
        // References landed back on the positions they came from.
        let driver_at = |i: usize| parse_reference(&payloads[i]).unwrap().driver_name;
        assert_eq!(driver_at(0), "a");
        assert_eq!(driver_at(1), "b");
        assert_eq!(driver_at(2), "a");
        assert_eq!(driver_at(3), "b");
    }

    #[tokio::test]
    async fn a_threshold_of_zero_offloads_every_payload() {
        let stored = Arc::new(Mutex::new(vec![]));
        let storage = storage_with_threshold(0, Arc::clone(&stored));
        let mut payloads = vec![sized(1), sized(8)];

        store_payloads(&mut payloads, &storage, None, &store_limiter())
            .await
            .unwrap();

        assert!(is_plain_reference(&payloads[0]));
        assert!(is_plain_reference(&payloads[1]));
        assert_eq!(stored.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn only_marked_references_are_encoded() {
        // Both are shaped like references, but only the store pass marks what it created, so a
        // payload a user happened to send in that shape is left alone.
        let untouched = reference("mem", "user-sent-this-shape");
        let mut payloads = vec![untouched.clone(), reference("mem", "just-stored")];
        mark_reference(&mut payloads[1]);

        encode_references(
            &mut payloads,
            &ReversingCodec,
            &SerializationContextData::None,
        )
        .await
        .unwrap();

        assert_eq!(
            payloads[0], untouched,
            "the payload the store pass did not mark is left alone"
        );
        assert!(
            !is_plain_reference(&payloads[1]),
            "the marked reference was actually run through the codec"
        );
        assert!(has_reference_marker(&payloads[1]));
    }

    #[tokio::test]
    async fn an_encoded_reference_decodes_back_to_what_was_encoded() {
        let plain = reference("mem", "claim");
        let mut payloads = vec![plain.clone()];
        mark_reference(&mut payloads[0]);

        encode_references(
            &mut payloads,
            &ReversingCodec,
            &SerializationContextData::None,
        )
        .await
        .unwrap();
        assert!(has_reference_marker(&payloads[0]));
        assert!(!is_plain_reference(&payloads[0]), "the codec ran");

        decode_references(
            &mut payloads,
            &ReversingCodec,
            &SerializationContextData::None,
        )
        .await
        .unwrap();

        assert_eq!(payloads[0], plain, "marker and encoding both undone");
    }

    #[tokio::test]
    async fn a_codec_that_drops_external_payloads_does_not_lose_the_size() {
        let mut payloads = vec![
            create_reference_payload(
                "mem",
                HashMap::from([("key".to_owned(), "k".to_owned())]),
                4096,
            )
            .unwrap(),
        ];
        mark_reference(&mut payloads[0]);

        encode_references(
            &mut payloads,
            &DroppingCodec,
            &SerializationContextData::None,
        )
        .await
        .unwrap();

        assert_eq!(payloads[0].external_payloads[0].size_bytes, 4096);
    }

    #[tokio::test]
    async fn a_codec_that_returns_nothing_is_reported_as_an_encode_failure() {
        let mut payloads = vec![reference("mem", "claim")];
        mark_reference(&mut payloads[0]);

        let err = encode_references(&mut payloads, &EmptyCodec, &SerializationContextData::None)
            .await
            .unwrap_err();

        // A codec failure, not TMPRL1106: that tells the reader to check their codec on the way in.
        assert!(matches!(err, ExternalStorageError::Codec(_)));
        assert!(err.to_string().contains("encoding a reference"));
    }

    // ---- through the runner ----

    /// Keeps what it stored, so a message can make the full outbound then inbound trip.
    struct RoundTripDriver {
        stored: Arc<Mutex<HashMap<String, Payload>>>,
    }

    impl StorageDriver for RoundTripDriver {
        fn name(&self) -> &str {
            "mem"
        }
        fn driver_type(&self) -> &str {
            "test.roundtrip"
        }
        fn store(
            &self,
            _context: &StorageDriverStoreContext,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<StorageDriverClaim>, ExternalStorageError>> {
            let stored = Arc::clone(&self.stored);
            Box::pin(async move {
                let mut held = stored.lock().unwrap();
                Ok(payloads
                    .into_iter()
                    .map(|p| {
                        let key = format!("k{}", held.len());
                        held.insert(key.clone(), p);
                        StorageDriverClaim::new(HashMap::from([("key".to_owned(), key)]))
                    })
                    .collect())
            })
        }
        fn retrieve(
            &self,
            _context: &StorageDriverRetrieveContext,
            claims: Vec<StorageDriverClaim>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, ExternalStorageError>> {
            let stored = Arc::clone(&self.stored);
            Box::pin(async move {
                let held = stored.lock().unwrap();
                claims
                    .into_iter()
                    .map(|c| {
                        held.get(&c.claim_data["key"])
                            .cloned()
                            .ok_or(ExternalStorageError::UndecodableReference)
                    })
                    .collect()
            })
        }
    }

    #[tokio::test]
    async fn a_message_makes_the_round_trip_through_the_runner() {
        // With writing enabled, store marks and encodes and retrieve decodes, so the round trip
        // exercises the codec in both directions.
        let _gate = EncodeReferencesGate::on();
        let stored = Arc::new(Mutex::new(HashMap::new()));
        let driver: Arc<dyn StorageDriver> = Arc::new(RoundTripDriver {
            stored: Arc::clone(&stored),
        });
        let storage = ExternalStorage::with_selector(
            vec![Arc::clone(&driver)],
            Arc::new(FixedSelector(driver)),
        )
        .unwrap()
        .with_payload_size_threshold(1);
        let store_lim = store_limiter();
        let retrieve_lim = limiter();
        let runner = ExternalStorageRunner::new(&storage, &store_lim, &retrieve_lim);
        let mut request = request_with_large_payloads();
        let before = request.clone();

        runner
            .store_in(
                &mut request,
                None,
                &ReversingCodec,
                &SerializationContextData::None,
            )
            .await
            .unwrap();
        // Marked and encoded on the way out, so no longer a plain reference.
        let stored_input = &request.input.as_ref().unwrap().payloads[0];
        assert!(has_reference_marker(stored_input));
        assert!(!is_plain_reference(stored_input));

        runner
            .retrieve_in(
                &mut request,
                &ReversingCodec,
                &SerializationContextData::None,
            )
            .await
            .unwrap();

        // Every field is back to what it was, search attributes included because they never left.
        assert_eq!(request, before);
    }

    #[tokio::test]
    async fn the_runner_reports_a_driver_it_does_not_know() {
        let batches = Arc::new(Mutex::new(vec![]));
        let storage = storage("mem", Arc::clone(&batches));
        let store_lim = store_limiter();
        let retrieve_lim = limiter();
        let runner = ExternalStorageRunner::new(&storage, &store_lim, &retrieve_lim);
        // A reference to the registered driver sits beside the unknown one.
        let mut request = SignalWithStartWorkflowExecutionRequest {
            input: Some(Payloads {
                payloads: vec![reference("mem", "a"), reference("nope", "b")],
            }),
            ..Default::default()
        };

        let err = runner
            .retrieve_in(
                &mut request,
                &ReversingCodec,
                &SerializationContextData::None,
            )
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            ExternalStorageError::UnregisteredDriver(name) if name == "nope"
        ));
        // Drivers are resolved before any request, so the registered one was never called.
        assert!(batches.lock().unwrap().is_empty());
    }

    // ---- over a real message graph ----

    /// A request with a reference in every payload-bearing field the walk reaches, including
    /// search attributes, which must be left alone.
    fn request_with_references() -> SignalWithStartWorkflowExecutionRequest {
        SignalWithStartWorkflowExecutionRequest {
            input: Some(Payloads {
                payloads: vec![reference("mem", "input-0"), reference("mem", "input-1")],
            }),
            signal_input: Some(Payloads {
                payloads: vec![reference("mem", "signal-0")],
            }),
            memo: Some(Memo {
                fields: HashMap::from([("m".to_owned(), reference("mem", "memo-0"))]),
            }),
            header: Some(Header {
                fields: HashMap::from([("h".to_owned(), reference("mem", "header-0"))]),
            }),
            search_attributes: Some(SearchAttributes {
                indexed_fields: HashMap::from([("s".to_owned(), reference("mem", "sa-0"))]),
            }),
            ..Default::default()
        }
    }

    /// The same request, but with payloads large enough to store rather than references.
    fn request_with_large_payloads() -> SignalWithStartWorkflowExecutionRequest {
        SignalWithStartWorkflowExecutionRequest {
            input: Some(Payloads {
                payloads: vec![filled(b'a', 500), filled(b'b', 500)],
            }),
            signal_input: Some(Payloads {
                payloads: vec![filled(b'c', 500)],
            }),
            memo: Some(Memo {
                fields: HashMap::from([("m".to_owned(), filled(b'd', 500))]),
            }),
            header: Some(Header {
                fields: HashMap::from([("h".to_owned(), filled(b'e', 500))]),
            }),
            search_attributes: Some(SearchAttributes {
                indexed_fields: HashMap::from([("s".to_owned(), filled(b'f', 500))]),
            }),
            ..Default::default()
        }
    }

    async fn run_store_pass(
        request: &mut SignalWithStartWorkflowExecutionRequest,
        stored: Arc<Mutex<Vec<Payload>>>,
    ) {
        let storage = storage_with_threshold(1, stored);
        let store_lim = store_limiter();
        let retrieve_lim = limiter();
        let runner = ExternalStorageRunner::new(&storage, &store_lim, &retrieve_lim);
        // Writing is off by default, so this runs the store pass only.
        runner
            .store_in(
                request,
                None,
                &ReversingCodec,
                &SerializationContextData::None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn every_other_payload_bearing_field_is_stored() {
        let stored = Arc::new(Mutex::new(vec![]));
        let mut request = request_with_large_payloads();
        let search_attributes = request.search_attributes.clone();

        run_store_pass(&mut request, Arc::clone(&stored)).await;

        for payload in [
            &request.input.as_ref().unwrap().payloads[0],
            &request.input.as_ref().unwrap().payloads[1],
            &request.signal_input.as_ref().unwrap().payloads[0],
            &request.memo.as_ref().unwrap().fields["m"],
            &request.header.as_ref().unwrap().fields["h"],
        ] {
            assert!(is_plain_reference(payload));
        }
        // The five stored payloads, and not the search attribute.
        assert_eq!(stored.lock().unwrap().len(), 5);
        // The server indexes search attributes literally, so they are left exactly as they were.
        assert_eq!(request.search_attributes, search_attributes);
    }

    #[tokio::test]
    async fn the_runner_leaves_stored_references_plain_while_writing_is_off() {
        // Through the runner's store_in, which also runs the (gated) encode pass. With the gate
        // off, references stay plain and unmarked so a worker that cannot read an encoded one
        // still finds them.
        let stored = Arc::new(Mutex::new(vec![]));
        let storage = storage_with_threshold(1, Arc::clone(&stored));
        let store_lim = store_limiter();
        let retrieve_lim = limiter();
        let runner = ExternalStorageRunner::new(&storage, &store_lim, &retrieve_lim);
        let mut request = request_with_large_payloads();

        runner
            .store_in(
                &mut request,
                None,
                &ReversingCodec,
                &SerializationContextData::None,
            )
            .await
            .unwrap();

        let input = &request.input.as_ref().unwrap().payloads[0];
        assert!(is_plain_reference(input));
        assert!(!has_reference_marker(input));
    }

    #[tokio::test]
    async fn every_other_payload_bearing_field_is_retrieved() {
        let batches = Arc::new(Mutex::new(vec![]));
        let storage = storage("mem", Arc::clone(&batches));
        let store_lim = store_limiter();
        let retrieve_lim = limiter();
        let runner = ExternalStorageRunner::new(&storage, &store_lim, &retrieve_lim);
        let mut request = request_with_references();
        let search_attributes = request.search_attributes.clone();

        runner
            .retrieve_in(
                &mut request,
                &ReversingCodec,
                &SerializationContextData::None,
            )
            .await
            .unwrap();

        // Search attributes are indexed literally, so they are left untouched and still reference
        // external storage afterward.
        assert_eq!(request.search_attributes, search_attributes);
        assert!(crate::external_storage_references::needs_retrieval(
            &request.search_attributes.as_ref().unwrap().indexed_fields["s"]
        ));
        let input = request.input.unwrap().payloads;
        assert_eq!(input[0].data, b"input-0");
        assert_eq!(input[1].data, b"input-1");
        assert_eq!(request.signal_input.unwrap().payloads[0].data, b"signal-0");
        assert_eq!(request.memo.unwrap().fields["m"].data, b"memo-0");
        assert_eq!(request.header.unwrap().fields["h"].data, b"header-0");

        // One call per driver per payload-bearing field: input (2 claims), signal_input, memo,
        // header. Search attributes are skipped, so there is no fifth call.
        let mut sizes = batches.lock().unwrap().clone();
        sizes.sort_unstable();
        assert_eq!(sizes, vec![1, 1, 1, 2]);
    }

    #[tokio::test]
    async fn the_codec_is_never_entered_concurrently() {
        struct BarrierCodec {
            inside: Arc<std::sync::atomic::AtomicUsize>,
            overlapped: Arc<std::sync::atomic::AtomicBool>,
        }
        impl PayloadCodec for BarrierCodec {
            fn encode(
                &self,
                _c: &SerializationContextData,
                payloads: Vec<Payload>,
            ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
                Box::pin(future::ready(Ok(payloads)))
            }
            fn decode(
                &self,
                _c: &SerializationContextData,
                payloads: Vec<Payload>,
            ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
                let inside = Arc::clone(&self.inside);
                let overlapped = Arc::clone(&self.overlapped);
                Box::pin(async move {
                    if inside.fetch_add(1, Ordering::SeqCst) > 0 {
                        overlapped.store(true, Ordering::SeqCst);
                    }
                    // A yield here is where a concurrent caller would slip in.
                    tokio::task::yield_now().await;
                    inside.fetch_sub(1, Ordering::SeqCst);
                    Ok(payloads
                        .into_iter()
                        .map(|mut p| {
                            p.metadata
                                .insert("encoding".to_owned(), b"json/protobuf".to_vec());
                            p.metadata.insert(
                                "messageType".to_owned(),
                                b"temporal.api.sdk.v1.ExternalStorageReference".to_vec(),
                            );
                            p
                        })
                        .collect())
                })
            }
        }

        let overlapped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let codec = BarrierCodec {
            inside: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            overlapped: Arc::clone(&overlapped),
        };
        let mut request = request_with_references();
        // Mark every reference so each one goes through the codec.
        for p in request.input.as_mut().unwrap().payloads.iter_mut() {
            mark_reference(p);
        }
        for p in request.signal_input.as_mut().unwrap().payloads.iter_mut() {
            mark_reference(p);
        }
        mark_reference(request.memo.as_mut().unwrap().fields.get_mut("m").unwrap());
        mark_reference(
            request
                .header
                .as_mut()
                .unwrap()
                .fields
                .get_mut("h")
                .unwrap(),
        );

        let storage = storage("mem", Arc::new(Mutex::new(vec![])));
        let store_lim = store_limiter();
        let retrieve_lim = limiter();
        let runner = ExternalStorageRunner::new(&storage, &store_lim, &retrieve_lim);
        runner
            .retrieve_in(&mut request, &codec, &SerializationContextData::None)
            .await
            .unwrap();

        assert!(
            !overlapped.load(std::sync::atomic::Ordering::SeqCst),
            "the codec must be entered one call at a time"
        );
    }

    #[test]
    fn rejecting_references_without_storage_is_tmprl1105() {
        assert!(reject_references(&[inline("fine")]).is_ok());

        let err = reject_references(&[reference("mem", "a")]).unwrap_err();
        assert!(matches!(err, ExternalStorageError::NotConfigured));
        assert!(err.to_string().contains("TMPRL1105"));
    }
}
