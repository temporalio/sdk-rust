//! Contains traits for and default implementations of data converters, codecs, and other
//! serialization related functionality.

// A module of its own rather than a re-export of the one in `temporalio-common-wasm`, so that the
// external storage error, which this crate raises, is published beside the driver contracts it
// belongs with.
pub use temporalio_common_wasm::data_converters::*;

/// Failures raised while storing payloads in, or retrieving them from, external storage.
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
    /// A driver failed to store or retrieve.
    #[error("Storage driver error: {0}")]
    Driver(#[source] anyhow::Error),
    /// Walking the message's payloads failed, outside any storage operation.
    #[error("External storage payload walk failed: {0}")]
    Walk(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// A reference names a driver that is not registered.
    #[error("No storage driver is registered under the name '{0}'")]
    UnregisteredDriver(String),
    /// The selector returned a driver that is not one of the registered instances.
    #[error("Selector returned a driver that is not registered: '{0}'")]
    UnregisteredSelection(String),
    /// A driver returned a different number of results than it was given inputs. A store is given
    /// payloads and returns claims; a retrieve is given claims and returns payloads. Either way
    /// there must be one result per input, in order.
    #[error("Storage driver '{driver}' returned {returned} results for {requested} inputs")]
    DriverContract {
        /// Name of the driver.
        driver: String,
        /// How many inputs it was given.
        requested: usize,
        /// How many results it returned.
        returned: usize,
    },
}

#[cfg(feature = "experimental")]
impl From<StorageDriverError> for ExternalStorageError {
    fn from(error: StorageDriverError) -> Self {
        Self::Driver(error.into_source())
    }
}
