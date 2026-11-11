//! The Store under a Stream runtime.

/// Keeps the Records and the state of Streams. A [crate::StreamRuntime] serves the Stream service
/// on one Store, which a bridge or a Store crate, such as `temporalio-streams-redis`, gives it.
pub trait Store: Send + Sync + 'static {}
