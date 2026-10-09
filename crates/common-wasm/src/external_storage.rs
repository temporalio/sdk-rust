//! External storage contracts: offloading large payloads to a user-supplied driver, which replaces
//! them on the wire with a small reference so the payload data never reaches the Temporal server.
//!
//! Import these from `temporalio_common::external_storage`, which re-exports them, so that they can
//! later move out of this crate without breaking imports. They sit beside
//! [`crate::data_converters::DataConverter`] because the configuration is expected to hang off it,
//! the way it does in the SDKs that have already shipped external storage. The reference wire
//! format lives in `temporalio-common`, alongside the payload visitor and the proto JSON support it
//! needs. Nothing here is wired into the SDK yet.

use crate::protos::temporal::api::common::v1::Payload;
use futures::future::BoxFuture;
use std::{collections::HashMap, fmt, sync::Arc};

/// Shared with every other SDK, so the same payload offloads regardless of which one wrote it.
const DEFAULT_PAYLOAD_SIZE_THRESHOLD: usize = 256 * 1024;

/// Driver-defined reference to an externally stored payload, used to retrieve it later.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct StorageDriverClaim {
    /// Data the driver needs to retrieve the payload. This is written into history, so it must
    /// contain everything required for retrieval and must not contain secrets.
    pub claim_data: HashMap<String, String>,
}

impl StorageDriverClaim {
    /// Create a claim from the data a driver needs to retrieve the payload later.
    pub fn new(claim_data: HashMap<String, String>) -> Self {
        Self { claim_data }
    }
}

/// Identity of the workflow a payload is being stored on behalf of. Also used for workflow
/// activities, which store against their owning workflow rather than themselves.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StorageDriverWorkflowInfo {
    /// Workflow namespace.
    pub namespace: String,
    /// Workflow id, when known.
    pub id: Option<String>,
    /// Run id, when known. Absent when the run is not yet determined, such as when starting a child
    /// workflow or continuing as new.
    pub run_id: Option<String>,
    /// Workflow type name, when known. Named `workflow_type` because `type` is a reserved word.
    pub workflow_type: Option<String>,
}

/// Identity of a standalone activity a payload is being stored on behalf of.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StorageDriverActivityInfo {
    /// Activity namespace.
    pub namespace: String,
    /// Activity id, when known.
    pub id: Option<String>,
    /// Run id, when known.
    pub run_id: Option<String>,
    /// Activity type name, when known. Named `activity_type` because `type` is a reserved word.
    pub activity_type: Option<String>,
}

/// The execution a payload is being stored on behalf of.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StorageDriverTargetInfo {
    /// Stored on behalf of a workflow.
    Workflow(StorageDriverWorkflowInfo),
    /// Stored on behalf of a standalone activity.
    Activity(StorageDriverActivityInfo),
}

/// Context given to [`StorageDriver::store`].
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct StorageDriverStoreContext {
    /// The execution the payloads are being stored on behalf of, when there is one.
    pub target: Option<StorageDriverTargetInfo>,
}

/// Context given to [`StorageDriverSelector::select_driver`].
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct StorageDriverSelectContext {
    /// The execution the payload is being stored on behalf of, when there is one.
    pub target: Option<StorageDriverTargetInfo>,
}

/// Context given to [`StorageDriver::retrieve`].
///
/// This deliberately carries no target. A payload must be retrievable from its
/// [`StorageDriverClaim`] alone, since the same payload can be read from a different execution than
/// the one that stored it.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct StorageDriverRetrieveContext {}

/// Stores and retrieves payloads in an external storage system.
///
/// Implementations are called concurrently and must be thread safe.
pub trait StorageDriver: Send + Sync {
    /// Name of this driver instance, unique among the drivers registered on one [`ExternalStorage`].
    /// This is written into history alongside every payload the driver stores and routes retrieval
    /// back to the same driver, so renaming a deployed driver makes everything it stored
    /// unretrievable.
    fn name(&self) -> &str;

    /// Identifier for this driver implementation, for example `aws.s3driver`. Unlike [`Self::name`]
    /// this is identical across every instance of the same implementation and across SDK languages.
    /// Named `driver_type` because `type` is a reserved word.
    fn driver_type(&self) -> &str;

    /// Store the given payloads, returning one claim per payload in the same order.
    fn store(
        &self,
        context: &StorageDriverStoreContext,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<StorageDriverClaim>, ExternalStorageError>>;

    /// Retrieve the payloads for the given claims, returning one payload per claim in the same
    /// order.
    fn retrieve(
        &self,
        context: &StorageDriverRetrieveContext,
        claims: Vec<StorageDriverClaim>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, ExternalStorageError>>;
}

/// Chooses which driver stores a payload, or returns `None` to pass the payload through.
pub trait StorageDriverSelector: Send + Sync {
    /// Select the driver for this payload.
    fn select_driver(
        &self,
        context: &StorageDriverSelectContext,
        payload: &Payload,
    ) -> Option<Arc<dyn StorageDriver>>;
}

/// Failures raised by the external storage machinery.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ExternalStorageError {
    /// A reference to externally stored data was found, but no external storage is configured to
    /// retrieve it.
    #[error(
        "[TMPRL1105] Encountered a reference to a payload in external storage, but no external \
         storage is configured to retrieve it"
    )]
    NotConfigured,
    /// A payload is marked as a reference but its contents cannot be read as one.
    #[error("Invalid external storage reference: {0}")]
    InvalidReference(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// The configuration is not usable.
    #[error("Invalid external storage configuration: {0}")]
    InvalidConfiguration(String),
    /// A driver failed to store or retrieve.
    #[error("Storage driver error: {0}")]
    Driver(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Configuration for offloading large payloads to an external storage system.
///
/// This and the driver and selector traits it holds are defined in this crate only so that it can
/// be configured on a [`DataConverter`](crate::data_converters::DataConverter). Drivers are
/// implemented and invoked outside of WASM, by the worker and client, never from workflow code.
///
/// Validated when constructed rather than when used, so a misconfiguration surfaces at startup
/// instead of on the first payload large enough to offload.
#[derive(Clone)]
pub struct ExternalStorage {
    drivers: Vec<Arc<dyn StorageDriver>>,
    drivers_by_name: HashMap<String, Arc<dyn StorageDriver>>,
    driver_selector: Arc<dyn StorageDriverSelector>,
    payload_size_threshold: usize,
}

impl fmt::Debug for ExternalStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalStorage")
            .field(
                "drivers",
                &self.drivers.iter().map(|d| d.name()).collect::<Vec<_>>(),
            )
            .field("payload_size_threshold", &self.payload_size_threshold)
            .finish_non_exhaustive()
    }
}

impl ExternalStorage {
    /// Configure external storage with a single driver that stores every eligible payload.
    pub fn new(driver: Arc<dyn StorageDriver>) -> Result<Self, ExternalStorageError> {
        let selected = Arc::clone(&driver);
        Self::with_selector(vec![driver], Arc::new(SingleDriverSelector(selected)))
    }

    /// Configure external storage with a selector choosing among several drivers.
    ///
    /// A selector is required here rather than optional, so that registering several drivers with
    /// no way to route between them cannot be expressed at all.
    pub fn with_selector(
        drivers: Vec<Arc<dyn StorageDriver>>,
        driver_selector: Arc<dyn StorageDriverSelector>,
    ) -> Result<Self, ExternalStorageError> {
        if drivers.is_empty() {
            return Err(ExternalStorageError::InvalidConfiguration(
                "at least one driver is required".to_owned(),
            ));
        }
        let mut drivers_by_name = HashMap::with_capacity(drivers.len());
        for driver in &drivers {
            let name = driver.name();
            // The name is the routing key written into history, so a driver without one can never
            // be resolved on the retrieval side.
            if name.is_empty() {
                return Err(ExternalStorageError::InvalidConfiguration(
                    "driver name cannot be empty".to_owned(),
                ));
            }
            if drivers_by_name
                .insert(name.to_owned(), Arc::clone(driver))
                .is_some()
            {
                return Err(ExternalStorageError::InvalidConfiguration(format!(
                    "multiple drivers given with name '{name}'"
                )));
            }
        }
        Ok(Self {
            drivers,
            drivers_by_name,
            driver_selector,
            payload_size_threshold: DEFAULT_PAYLOAD_SIZE_THRESHOLD,
        })
    }

    /// Set the minimum encoded payload size, in bytes, that is offloaded. Payloads at or above this
    /// size are offloaded; smaller ones are left inline. Zero offloads every payload. Defaults to
    /// 256 KiB.
    #[must_use]
    pub fn with_payload_size_threshold(mut self, payload_size_threshold: usize) -> Self {
        self.payload_size_threshold = payload_size_threshold;
        self
    }

    /// The registered drivers.
    pub fn drivers(&self) -> &[Arc<dyn StorageDriver>] {
        &self.drivers
    }

    /// The selector choosing which driver stores each payload.
    pub fn driver_selector(&self) -> &Arc<dyn StorageDriverSelector> {
        &self.driver_selector
    }

    /// The minimum encoded payload size, in bytes, that is offloaded.
    pub fn payload_size_threshold(&self) -> usize {
        self.payload_size_threshold
    }

    /// The driver registered under the given name, if any.
    pub fn driver(&self, name: &str) -> Option<&Arc<dyn StorageDriver>> {
        self.drivers_by_name.get(name)
    }
}

struct SingleDriverSelector(Arc<dyn StorageDriver>);

impl StorageDriverSelector for SingleDriverSelector {
    fn select_driver(
        &self,
        _context: &StorageDriverSelectContext,
        _payload: &Payload,
    ) -> Option<Arc<dyn StorageDriver>> {
        Some(Arc::clone(&self.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future;

    struct FakeDriver(&'static str);

    impl StorageDriver for FakeDriver {
        fn name(&self) -> &str {
            self.0
        }
        fn driver_type(&self) -> &str {
            "test.fakedriver"
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

    struct NullSelector;

    impl StorageDriverSelector for NullSelector {
        fn select_driver(
            &self,
            _context: &StorageDriverSelectContext,
            _payload: &Payload,
        ) -> Option<Arc<dyn StorageDriver>> {
            None
        }
    }

    fn driver(name: &'static str) -> Arc<dyn StorageDriver> {
        Arc::new(FakeDriver(name))
    }

    #[test]
    fn configuration_is_validated_when_built() {
        assert!(matches!(
            ExternalStorage::with_selector(vec![], Arc::new(NullSelector)),
            Err(ExternalStorageError::InvalidConfiguration(_))
        ));
        assert!(matches!(
            ExternalStorage::with_selector(vec![driver("")], Arc::new(NullSelector)),
            Err(ExternalStorageError::InvalidConfiguration(_))
        ));
        assert!(matches!(
            ExternalStorage::with_selector(
                vec![driver("dup"), driver("dup")],
                Arc::new(NullSelector)
            ),
            Err(ExternalStorageError::InvalidConfiguration(_))
        ));
    }
}
