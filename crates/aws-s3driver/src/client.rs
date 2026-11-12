use futures_util::future::BoxFuture;
use std::{collections::HashMap, error::Error};

/// S3 object operations used by [`S3StorageDriver`](crate::S3StorageDriver). Implement this to use
/// an S3 library other than the AWS SDK.
///
/// Implementations are called concurrently and must be thread safe.
pub trait S3StorageDriverClient: Send + Sync {
    /// Upload data to the given bucket and key.
    fn put_object<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        data: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), Box<dyn Error + Send + Sync>>>;

    /// Check whether an object exists at the given bucket and key.
    fn object_exists<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
    ) -> BoxFuture<'a, Result<bool, Box<dyn Error + Send + Sync>>>;

    /// Download the object at the given bucket and key.
    fn get_object<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Vec<u8>, Box<dyn Error + Send + Sync>>>;

    /// Client diagnostic information, such as the region, that the driver adds to error messages.
    fn describe(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}
