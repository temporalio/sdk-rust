//! External storage: offloading large payloads to a user-supplied driver, which replaces them on
//! the wire with a small reference so the payload data never reaches the Temporal server.
//!
//! This is the path to import these types from. They are defined in `temporalio-common-wasm` only
//! because [`crate::data_converters::DataConverter`], which the configuration hangs off, is defined
//! there; re-exporting them here leaves them free to move out of that crate without breaking
//! imports.

mod references;

pub use temporalio_common_wasm::external_storage::*;
