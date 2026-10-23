//! Contains traits for and default implementations of data converters, codecs, and other
//! serialization related functionality.

// A module of its own rather than a re-export of the one in `temporalio-common-wasm`, so that the
// external storage error, which only the runner in this crate raises, is published beside the
// driver contracts it belongs with.
pub use temporalio_common_wasm::data_converters::*;

#[cfg(feature = "experimental")]
pub use crate::external_storage_runner::ExternalStorageError;
