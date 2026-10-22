//! Stores large payloads in external storage and fetches them back.
//!
//! Outbound, a payload at or above the configured size threshold is handed to a storage driver and
//! replaced by a reference to what the driver stored. Inbound, references are swapped back for the
//! payloads they point at. [`ExternalStorageRunner`] runs either direction over a whole message,
//! visiting every payload-bearing field except search attributes, which the server has to be able
//! to read.
//!
//! References are stored and read in plain form, so a reference's claim data (its driver name and
//! claim keys) is written to history unencrypted. The payload codec is not applied to references,
//! and the configured concurrency limits are not enforced.

// Crate-private until a worker or client calls it.
#![allow(dead_code)]

use futures::future;
use std::{collections::HashMap, sync::Arc};

use prost::Message;
use temporalio_common_wasm::data_converters::{
    ExternalStorage, ExternalStorageError, PayloadConversionError, StorageDriver,
    StorageDriverClaim, StorageDriverLimiter, StorageDriverRetrieveContext,
    StorageDriverSelectContext, StorageDriverStoreContext, StorageDriverTargetInfo,
};

use crate::{
    external_storage_references::{create_reference_payload, is_plain_reference, parse_reference},
    payload_visitor::{
        AsyncPayloadVisitor, PayloadField, PayloadFieldData, PayloadVisitable, should_transform,
    },
    protos::temporal::api::common::v1::Payload,
};

/// Drives external storage for the payloads reachable from one activation or request.
///
/// A worker completion holds many commands that offload to different targets, so the caller
/// matches on the command and calls `store_in` once per command with that command's target. A
/// uniform message, such as a client request, is a single call.
pub(crate) struct ExternalStorageRunner<'a> {
    storage: &'a ExternalStorage,
    store_limiter: Arc<dyn StorageDriverLimiter<Payload>>,
    retrieve_limiter: Arc<dyn StorageDriverLimiter<StorageDriverClaim>>,
}

impl<'a> ExternalStorageRunner<'a> {
    pub(crate) fn new(
        storage: &'a ExternalStorage,
        store_limiter: Arc<dyn StorageDriverLimiter<Payload>>,
        retrieve_limiter: Arc<dyn StorageDriverLimiter<StorageDriverClaim>>,
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
    ) -> Result<(), ExternalStorageError> {
        let mut store = StorePayloadsVisitor {
            storage: self.storage,
            target,
            limiter: &self.store_limiter,
            error: None,
        };
        let walked = message.visit_payloads_mut(&mut store).await;
        finish(store.error, walked)
    }

    /// Replace every reference in a message with the payload it points at.
    pub(crate) async fn retrieve_in<M: PayloadVisitable + Send>(
        &self,
        message: &mut M,
    ) -> Result<(), ExternalStorageError> {
        let mut retrieve = RetrievePayloadsVisitor {
            storage: self.storage,
            limiter: &self.retrieve_limiter,
            error: None,
        };
        let walked = message.visit_payloads_mut(&mut retrieve).await;
        finish(retrieve.error, walked)
    }
}

/// Fail if any payload needs retrieval. For a caller with no external storage configured, which
/// must not hand a reference to application code as though it were the application's own data.
pub(crate) fn reject_references(payloads: &[Payload]) -> Result<(), ExternalStorageError> {
    if payloads.iter().any(is_plain_reference) {
        return Err(ExternalStorageError::NotConfigured);
    }
    Ok(())
}

/// Replace every reference with the payload the driver returns for it.
async fn retrieve_payloads(
    payloads: &mut [Payload],
    storage: &ExternalStorage,
    limiter: &Arc<dyn StorageDriverLimiter<StorageDriverClaim>>,
) -> Result<(), ExternalStorageError> {
    let mut by_driver: HashMap<String, Vec<(usize, StorageDriverClaim)>> = HashMap::new();
    for (index, payload) in payloads.iter().enumerate() {
        if !is_plain_reference(payload) {
            continue;
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

/// One driver and the payloads it takes: each payload's index, the payload, and its pre-offload
/// proto size.
type DriverGroup = (Arc<dyn StorageDriver>, Vec<(usize, Payload, i64)>);

/// Replace every eligible payload with a reference.
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
                    let reference = create_reference_payload(&driver_name, claim.claim_data, size)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        external_storage_references::create_reference_payload,
        protos::temporal::api::{
            common::v1::{Header, Memo, Payloads, SearchAttributes},
            workflowservice::v1::SignalWithStartWorkflowExecutionRequest,
        },
    };
    use futures::future::{self, BoxFuture};
    use std::sync::Mutex;
    use temporalio_common_wasm::data_converters::{
        StorageDriver, StorageDriverPermit, StorageDriverSelector,
    };

    /// Echoes each claim back as a payload (and each payload as a claim), recording the batch
    /// sizes it was called with. The in-flight counters let one test observe overlap.
    struct RecordingDriver {
        name: &'static str,
        batches: Arc<Mutex<Vec<usize>>>,
        // Released once every driver sharing it is in a request, so one test can prove overlap.
        barrier: Arc<tokio::sync::Barrier>,
    }

    impl RecordingDriver {
        // The common case: a one-party barrier that releases at once, so the driver does not wait.
        // The overlap test shares a wider barrier across drivers instead.
        fn new(name: &'static str, batches: Arc<Mutex<Vec<usize>>>) -> Self {
            Self {
                name,
                batches,
                barrier: Arc::new(tokio::sync::Barrier::new(1)),
            }
        }
    }

    impl StorageDriver for RecordingDriver {
        fn name(&self) -> &str {
            self.name
        }
        fn driver_type(&self) -> &str {
            "test.recordingdriver"
        }
        fn store(
            &self,
            _context: &temporalio_common_wasm::data_converters::StorageDriverStoreContext,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<StorageDriverClaim>, ExternalStorageError>> {
            self.batches.lock().unwrap().push(payloads.len());
            let barrier = Arc::clone(&self.barrier);
            Box::pin(async move {
                // Block until every driver sharing this barrier is also in a request, proving the
                // requests overlap. A one-party barrier releases immediately.
                barrier.wait().await;
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
            let barrier = Arc::clone(&self.barrier);
            Box::pin(async move {
                // Block until every driver sharing this barrier is also in a request, proving the
                // requests overlap. A one-party barrier releases immediately.
                barrier.wait().await;
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

    fn storage(name: &'static str, batches: Arc<Mutex<Vec<usize>>>) -> ExternalStorage {
        ExternalStorage::new(Arc::new(RecordingDriver::new(name, batches))).unwrap()
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
        // Both drivers share this barrier, so each retrieve blocks until the other is also
        // running — only possible if they overlap. The timeout fails a sequential regression
        // instead of hanging on it.
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let make = |name: &'static str| -> Arc<dyn StorageDriver> {
            Arc::new(RecordingDriver {
                name,
                batches: Arc::new(Mutex::new(vec![])),
                barrier: Arc::clone(&barrier),
            })
        };
        let storage =
            ExternalStorage::with_selector(vec![make("a"), make("b")], Arc::new(NeverSelects))
                .unwrap();
        let mut payloads = vec![reference("a", "one"), reference("b", "two")];

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            retrieve_payloads(&mut payloads, &storage, &limiter()),
        )
        .await
        .expect("drivers did not run concurrently")
        .unwrap();

        assert_eq!(payloads[0].data, b"one");
        assert_eq!(payloads[1].data, b"two");
    }

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
                Arc::new(RecordingDriver::new(name, Arc::clone(batches)))
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
                        held.get(&c.claim_data["key"]).cloned().ok_or_else(|| {
                            ExternalStorageError::InvalidReference(
                                "stored payload not found".into(),
                            )
                        })
                    })
                    .collect()
            })
        }
    }

    #[tokio::test]
    async fn a_message_makes_the_round_trip_through_the_runner() {
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
        let runner = ExternalStorageRunner::new(&storage, store_limiter(), limiter());
        let mut request = request_with_large_payloads();
        let before = request.clone();

        runner.store_in(&mut request, None).await.unwrap();
        assert!(is_plain_reference(
            &request.input.as_ref().unwrap().payloads[0]
        ));

        runner.retrieve_in(&mut request).await.unwrap();

        // Every field is back to what it was, search attributes included because they never left.
        assert_eq!(request, before);
    }

    #[tokio::test]
    async fn the_runner_reports_a_driver_it_does_not_know() {
        let batches = Arc::new(Mutex::new(vec![]));
        let storage = storage("mem", Arc::clone(&batches));
        let runner = ExternalStorageRunner::new(&storage, store_limiter(), limiter());
        // A reference to the registered driver sits beside the unknown one.
        let mut request = SignalWithStartWorkflowExecutionRequest {
            input: Some(Payloads {
                payloads: vec![reference("mem", "a"), reference("nope", "b")],
            }),
            ..Default::default()
        };

        let err = runner.retrieve_in(&mut request).await.unwrap_err();

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
        let runner = ExternalStorageRunner::new(&storage, store_limiter(), limiter());
        runner.store_in(request, None).await.unwrap();
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
    async fn every_other_payload_bearing_field_is_retrieved() {
        let batches = Arc::new(Mutex::new(vec![]));
        let storage = storage("mem", Arc::clone(&batches));
        let runner = ExternalStorageRunner::new(&storage, store_limiter(), limiter());
        let mut request = request_with_references();
        let search_attributes = request.search_attributes.clone();

        runner.retrieve_in(&mut request).await.unwrap();

        // Search attributes are indexed literally, so they are left untouched and still reference
        // external storage afterward.
        assert_eq!(request.search_attributes, search_attributes);
        assert!(crate::external_storage_references::is_plain_reference(
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

    #[test]
    fn rejecting_references_without_storage_is_tmprl1105() {
        assert!(reject_references(&[inline("fine")]).is_ok());

        let err = reject_references(&[reference("mem", "a")]).unwrap_err();
        assert!(matches!(err, ExternalStorageError::NotConfigured));
        assert!(err.to_string().contains("TMPRL1105"));
    }
}
