use crate::client::{BoxError, S3StorageDriverClient};
use futures_util::future::{BoxFuture, try_join_all};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{collections::HashSet, sync::Arc};
use temporalio_common::{
    data_converters::{
        ExternalStorageError, StorageDriver, StorageDriverClaim, StorageDriverLimiter,
        StorageDriverRetrieveContext, StorageDriverStoreContext, StorageDriverTargetInfo,
    },
    protos::temporal::api::common::v1::Payload,
};

const DRIVER_TYPE: &str = "aws.s3driver";
const DEFAULT_MAX_PAYLOAD_SIZE: usize = 50 * 1024 * 1024;
const CLAIM_BUCKET: &str = "bucket";
const CLAIM_KEY: &str = "key";
const CLAIM_HASH_ALGORITHM: &str = "hash_algorithm";
const CLAIM_HASH_VALUE: &str = "hash_value";
const NULL_SEGMENT: &str = "null";
const SAFE_SPECIAL_CHARACTERS: &[u8] = b"!-_.*'()";

type BucketSelector = dyn Fn(&StorageDriverStoreContext, &Payload) -> String + Send + Sync;

/// Errors raised by [`S3StorageDriver`], returned inside [`ExternalStorageError::Driver`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum S3StorageDriverError {
    /// A payload is larger than the configured maximum.
    #[error("Payload size {size} bytes exceeds the configured max payload size of {max} bytes")]
    PayloadTooLarge {
        /// Serialized payload size, in bytes.
        size: usize,
        /// Configured maximum, in bytes.
        max: usize,
    },
    /// A claim has no bucket or key.
    #[error(
        "S3StorageDriver claim is missing required location information: claim data must \
         contain 'bucket' and 'key'"
    )]
    MissingLocation,
    /// A claim has no content hash.
    #[error(
        "S3StorageDriver claim is missing required content hash information [bucket={bucket}, \
         key={key}]: claim data must contain 'hash_algorithm' and 'hash_value'"
    )]
    MissingHash {
        /// Bucket from the claim.
        bucket: String,
        /// Key from the claim.
        key: String,
    },
    /// A claim uses a hash algorithm other than SHA-256.
    #[error(
        "S3StorageDriver unsupported hash algorithm [bucket={bucket}, key={key}]: expected \
         sha256, got {hash_algorithm}"
    )]
    UnsupportedHashAlgorithm {
        /// Bucket from the claim.
        bucket: String,
        /// Key from the claim.
        key: String,
        /// Hash algorithm from the claim.
        hash_algorithm: String,
    },
    /// The client failed to store an object.
    #[error("S3StorageDriver store failed [bucket={bucket}, key={key}{client_context}]")]
    Store {
        /// Bucket of the object.
        bucket: String,
        /// Key of the object.
        key: String,
        /// Client diagnostic information, formatted for the message.
        client_context: String,
        /// The client error.
        #[source]
        source: BoxError,
    },
    /// The client failed to retrieve an object.
    #[error("S3StorageDriver retrieve failed [bucket={bucket}, key={key}{client_context}]")]
    Retrieve {
        /// Bucket of the object.
        bucket: String,
        /// Key of the object.
        key: String,
        /// Client diagnostic information, formatted for the message.
        client_context: String,
        /// The client error.
        #[source]
        source: BoxError,
    },
    /// A retrieved object does not match the hash in its claim.
    #[error(
        "S3StorageDriver integrity check failed [bucket={bucket}, key={key}]: expected \
         sha256:{expected}, got sha256:{actual}"
    )]
    IntegrityCheck {
        /// Bucket of the object.
        bucket: String,
        /// Key of the object.
        key: String,
        /// Hash from the claim.
        expected: String,
        /// Hash of the retrieved data.
        actual: String,
    },
}

impl From<S3StorageDriverError> for ExternalStorageError {
    fn from(err: S3StorageDriverError) -> Self {
        ExternalStorageError::Driver(Box::new(err))
    }
}

/// Storage driver that stores and retrieves payloads in Amazon S3.
///
/// Payloads are content addressed by a SHA-256 hash of their serialized bytes, so storing the same
/// payload twice for the same execution uploads it once. Object keys are built from the namespace
/// and the workflow or activity the payload is stored for, percent-encoded to S3's safe character
/// set.
///
/// This API is experimental and may change in the future.
#[derive(Clone)]
pub struct S3StorageDriver {
    client: Arc<dyn S3StorageDriverClient>,
    bucket_selector: Arc<BucketSelector>,
    name: String,
    max_payload_size: usize,
}

impl S3StorageDriver {
    /// Create a driver that stores every payload in one bucket.
    pub fn new(client: Arc<dyn S3StorageDriverClient>, bucket: impl Into<String>) -> Self {
        let bucket = bucket.into();
        Self::with_bucket_selector(client, move |_, _| bucket.clone())
    }

    /// Create a driver that chooses the bucket for each payload.
    pub fn with_bucket_selector(
        client: Arc<dyn S3StorageDriverClient>,
        bucket_selector: impl Fn(&StorageDriverStoreContext, &Payload) -> String + Send + Sync + 'static,
    ) -> Self {
        Self {
            client,
            bucket_selector: Arc::new(bucket_selector),
            name: DRIVER_TYPE.to_owned(),
            max_payload_size: DEFAULT_MAX_PAYLOAD_SIZE,
        }
    }

    /// Set the driver instance name written into history. Defaults to `aws.s3driver`. Only change
    /// this when registering multiple S3 drivers with different configurations on the same
    /// external storage.
    pub fn with_driver_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the maximum serialized payload size, in bytes. Storing a larger payload fails. Defaults
    /// to 50 MiB.
    pub fn with_max_payload_size(
        mut self,
        max_payload_size: usize,
    ) -> Result<Self, ExternalStorageError> {
        if max_payload_size == 0 {
            return Err(ExternalStorageError::InvalidConfiguration(
                "max payload size must be at least 1".to_owned(),
            ));
        }
        self.max_payload_size = max_payload_size;
        Ok(self)
    }

    fn stored_object(
        &self,
        context: &StorageDriverStoreContext,
        payload: Payload,
        context_segments: &str,
    ) -> Result<StoredObject, S3StorageDriverError> {
        let bucket = (self.bucket_selector)(context, &payload);
        let data = payload.encode_to_vec();
        if data.len() > self.max_payload_size {
            return Err(S3StorageDriverError::PayloadTooLarge {
                size: data.len(),
                max: self.max_payload_size,
            });
        }
        let hash_value = sha256_hex(&data);
        Ok(StoredObject {
            key: format!("v0{context_segments}/d/sha256/{hash_value}"),
            payload,
            bucket,
            hash_value,
            data,
        })
    }
}

impl StorageDriver for S3StorageDriver {
    fn name(&self) -> &str {
        &self.name
    }

    fn driver_type(&self) -> &str {
        DRIVER_TYPE
    }

    fn store(
        &self,
        context: &StorageDriverStoreContext,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Result<Vec<StorageDriverClaim>, ExternalStorageError>> {
        let context_segments = context_key_segments(context.target.as_ref());
        let objects: Result<Vec<_>, _> = payloads
            .into_iter()
            .map(|payload| self.stored_object(context, payload, &context_segments))
            .collect();
        let client = self.client.clone();
        let limiter = context.limiter.clone();
        Box::pin(async move {
            let objects = objects?;
            let claims = objects.iter().map(StoredObject::claim).collect();
            let mut seen = HashSet::new();
            let uploads = objects
                .into_iter()
                .filter(|object| seen.insert((object.bucket.clone(), object.key.clone())))
                .map(|object| upload_if_absent(client.as_ref(), limiter.as_ref(), object));
            try_join_all(uploads).await?;
            Ok(claims)
        })
    }

    fn retrieve(
        &self,
        context: &StorageDriverRetrieveContext,
        claims: Vec<StorageDriverClaim>,
    ) -> BoxFuture<'static, Result<Vec<Payload>, ExternalStorageError>> {
        let client = self.client.clone();
        let limiter = context.limiter.clone();
        Box::pin(async move {
            for claim in &claims {
                validate_claim(claim)?;
            }
            try_join_all(
                claims
                    .iter()
                    .map(|claim| download(client.as_ref(), limiter.as_ref(), claim)),
            )
            .await
        })
    }
}

struct StoredObject {
    payload: Payload,
    bucket: String,
    key: String,
    hash_value: String,
    data: Vec<u8>,
}

impl StoredObject {
    fn claim(&self) -> StorageDriverClaim {
        StorageDriverClaim::new(
            [
                (CLAIM_BUCKET, self.bucket.as_str()),
                (CLAIM_KEY, self.key.as_str()),
                (CLAIM_HASH_ALGORITHM, "sha256"),
                (CLAIM_HASH_VALUE, self.hash_value.as_str()),
            ]
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect(),
        )
    }
}

async fn upload_if_absent(
    client: &dyn S3StorageDriverClient,
    limiter: &dyn StorageDriverLimiter<Payload>,
    object: StoredObject,
) -> Result<(), ExternalStorageError> {
    let _permit = limiter.acquire(&object.payload).await?;
    let StoredObject {
        bucket, key, data, ..
    } = object;
    let result: Result<(), BoxError> = async {
        if !client.object_exists(&bucket, &key).await? {
            client.put_object(&bucket, &key, data).await?;
        }
        Ok(())
    }
    .await;
    result.map_err(|source| {
        S3StorageDriverError::Store {
            client_context: client_context(client),
            bucket,
            key,
            source,
        }
        .into()
    })
}

async fn download(
    client: &dyn S3StorageDriverClient,
    limiter: &dyn StorageDriverLimiter<StorageDriverClaim>,
    claim: &StorageDriverClaim,
) -> Result<Payload, ExternalStorageError> {
    let bucket = &claim.claim_data[CLAIM_BUCKET];
    let key = &claim.claim_data[CLAIM_KEY];
    let data = {
        let _permit = limiter.acquire(claim).await?;
        client
            .get_object(bucket, key)
            .await
            .map_err(|source| S3StorageDriverError::Retrieve {
                bucket: bucket.clone(),
                key: key.clone(),
                client_context: client_context(client),
                source,
            })?
    };
    let expected = &claim.claim_data[CLAIM_HASH_VALUE];
    let actual = sha256_hex(&data);
    if actual != *expected {
        return Err(S3StorageDriverError::IntegrityCheck {
            bucket: bucket.clone(),
            key: key.clone(),
            expected: expected.clone(),
            actual,
        }
        .into());
    }
    Payload::decode(data.as_slice()).map_err(|err| ExternalStorageError::Driver(Box::new(err)))
}

fn validate_claim(claim: &StorageDriverClaim) -> Result<(), S3StorageDriverError> {
    let value = |name| claim.claim_data.get(name).filter(|value| !value.is_empty());
    let (Some(bucket), Some(key)) = (value(CLAIM_BUCKET), value(CLAIM_KEY)) else {
        return Err(S3StorageDriverError::MissingLocation);
    };
    let (Some(hash_algorithm), Some(_)) = (value(CLAIM_HASH_ALGORITHM), value(CLAIM_HASH_VALUE))
    else {
        return Err(S3StorageDriverError::MissingHash {
            bucket: bucket.clone(),
            key: key.clone(),
        });
    };
    if hash_algorithm != "sha256" {
        return Err(S3StorageDriverError::UnsupportedHashAlgorithm {
            bucket: bucket.clone(),
            key: key.clone(),
            hash_algorithm: hash_algorithm.clone(),
        });
    }
    Ok(())
}

fn sha256_hex(data: &[u8]) -> String {
    Sha256::digest(data)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn context_key_segments(target: Option<&StorageDriverTargetInfo>) -> String {
    match target {
        Some(StorageDriverTargetInfo::Workflow(workflow)) => format!(
            "/ns/{}/wt/{}/wi/{}/ri/{}",
            encode_key_segment(Some(workflow.namespace.as_str())),
            encode_key_segment(workflow.workflow_type.as_deref()),
            encode_key_segment(workflow.id.as_deref()),
            encode_key_segment(workflow.run_id.as_deref()),
        ),
        Some(StorageDriverTargetInfo::Activity(activity)) => format!(
            "/ns/{}/at/{}/ai/{}/ri/{}",
            encode_key_segment(Some(activity.namespace.as_str())),
            encode_key_segment(activity.activity_type.as_deref()),
            encode_key_segment(activity.id.as_deref()),
            encode_key_segment(activity.run_id.as_deref()),
        ),
        _ => String::new(),
    }
}

/// Escapes every UTF-8 byte outside S3's safe character set, so keys are portable across S3
/// implementations and identical to the keys other SDK languages build.
fn encode_key_segment(value: Option<&str>) -> String {
    match value {
        None | Some("") => NULL_SEGMENT.to_owned(),
        Some(value) => value
            .bytes()
            .map(|byte| {
                if byte.is_ascii_alphanumeric() || SAFE_SPECIAL_CHARACTERS.contains(&byte) {
                    char::from(byte).to_string()
                } else {
                    format!("%{byte:02X}")
                }
            })
            .collect(),
    }
}

fn client_context(client: &dyn S3StorageDriverClient) -> String {
    client
        .describe()
        .iter()
        .map(|(key, value)| format!(", {key}={value}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::future::{self, FutureExt};
    use std::{
        collections::HashMap,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use temporalio_common::data_converters::{
        StorageDriverActivityInfo, StorageDriverPermit, StorageDriverWorkflowInfo,
    };
    use tokio::{
        sync::{OwnedSemaphorePermit, Semaphore, watch},
        time::{Duration, timeout},
    };

    struct RecordingLimiter<Item> {
        items: Mutex<Vec<Item>>,
    }

    impl<Item> RecordingLimiter<Item> {
        fn new() -> Self {
            Self {
                items: Mutex::new(Vec::new()),
            }
        }
    }

    impl<Item: Clone + Send + Sync> StorageDriverLimiter<Item> for RecordingLimiter<Item> {
        fn acquire<'a>(
            &'a self,
            item: &'a Item,
        ) -> BoxFuture<'a, Result<StorageDriverPermit, ExternalStorageError>> {
            self.items.lock().unwrap().push(item.clone());
            future::ready(Ok(StorageDriverPermit::new(Box::new(())))).boxed()
        }
    }

    /// Counts the permits it has handed out that have not yet been dropped.
    struct LivePermitCountingLimiter(Arc<AtomicUsize>);

    struct LivePermit(Arc<AtomicUsize>);

    impl Drop for LivePermit {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl<Item: Send + Sync> StorageDriverLimiter<Item> for LivePermitCountingLimiter {
        fn acquire<'a>(
            &'a self,
            _: &'a Item,
        ) -> BoxFuture<'a, Result<StorageDriverPermit, ExternalStorageError>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            let permit = StorageDriverPermit::new(Box::new(LivePermit(self.0.clone())));
            future::ready(Ok(permit)).boxed()
        }
    }

    struct PermitCheckingS3Client {
        inner: FakeS3Client,
        live_permits: Arc<AtomicUsize>,
        request_count: AtomicUsize,
        requests_without_permit_count: AtomicUsize,
    }

    impl PermitCheckingS3Client {
        fn record_request(&self) {
            self.request_count.fetch_add(1, Ordering::SeqCst);
            if self.live_permits.load(Ordering::SeqCst) == 0 {
                self.requests_without_permit_count
                    .fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    impl S3StorageDriverClient for PermitCheckingS3Client {
        fn put_object<'a>(
            &'a self,
            bucket: &'a str,
            key: &'a str,
            data: Vec<u8>,
        ) -> BoxFuture<'a, Result<(), BoxError>> {
            self.record_request();
            self.inner.put_object(bucket, key, data)
        }

        fn object_exists<'a>(
            &'a self,
            bucket: &'a str,
            key: &'a str,
        ) -> BoxFuture<'a, Result<bool, BoxError>> {
            self.record_request();
            self.inner.object_exists(bucket, key)
        }

        fn get_object<'a>(
            &'a self,
            bucket: &'a str,
            key: &'a str,
        ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>> {
            self.record_request();
            self.inner.get_object(bucket, key)
        }
    }

    struct SemaphoreLimiter(Arc<Semaphore>);

    impl StorageDriverLimiter<Payload> for SemaphoreLimiter {
        fn acquire<'a>(
            &'a self,
            _: &'a Payload,
        ) -> BoxFuture<'a, Result<StorageDriverPermit, ExternalStorageError>> {
            async move {
                let permit: OwnedSemaphorePermit = self.0.clone().acquire_owned().await.unwrap();
                Ok(StorageDriverPermit::new(Box::new(permit)))
            }
            .boxed()
        }
    }

    /// Records the most existence checks in flight at once, holding each one until the expected
    /// number are in flight together.
    struct ConcurrencyTrackingS3Client {
        expected_concurrency: usize,
        expected_concurrency_reached: watch::Sender<bool>,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        request_count: AtomicUsize,
    }

    impl S3StorageDriverClient for ConcurrencyTrackingS3Client {
        fn put_object<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
            _: Vec<u8>,
        ) -> BoxFuture<'a, Result<(), BoxError>> {
            future::ready(Ok(())).boxed()
        }

        fn object_exists<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
        ) -> BoxFuture<'a, Result<bool, BoxError>> {
            async move {
                self.request_count.fetch_add(1, Ordering::SeqCst);
                let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
                if in_flight == self.expected_concurrency {
                    self.expected_concurrency_reached.send_replace(true);
                }
                let _ = self
                    .expected_concurrency_reached
                    .subscribe()
                    .wait_for(|reached| *reached)
                    .await;
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(false)
            }
            .boxed()
        }

        fn get_object<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
        ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>> {
            unimplemented!()
        }
    }

    #[derive(Default)]
    struct FakeS3Client {
        objects: Mutex<HashMap<(String, String), Vec<u8>>>,
        put_count: AtomicUsize,
    }

    impl S3StorageDriverClient for FakeS3Client {
        fn put_object<'a>(
            &'a self,
            bucket: &'a str,
            key: &'a str,
            data: Vec<u8>,
        ) -> BoxFuture<'a, Result<(), BoxError>> {
            self.put_count.fetch_add(1, Ordering::SeqCst);
            self.objects
                .lock()
                .unwrap()
                .insert((bucket.to_owned(), key.to_owned()), data);
            future::ready(Ok(())).boxed()
        }

        fn object_exists<'a>(
            &'a self,
            bucket: &'a str,
            key: &'a str,
        ) -> BoxFuture<'a, Result<bool, BoxError>> {
            let exists = self
                .objects
                .lock()
                .unwrap()
                .contains_key(&(bucket.to_owned(), key.to_owned()));
            future::ready(Ok(exists)).boxed()
        }

        fn get_object<'a>(
            &'a self,
            bucket: &'a str,
            key: &'a str,
        ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>> {
            let data = self.objects.lock().unwrap()[&(bucket.to_owned(), key.to_owned())].clone();
            future::ready(Ok(data)).boxed()
        }
    }

    #[derive(Default)]
    struct FailingS3Client {
        request_count: AtomicUsize,
    }

    impl FailingS3Client {
        fn fail<'a, T: Send + 'a>(&self) -> BoxFuture<'a, Result<T, BoxError>> {
            self.request_count.fetch_add(1, Ordering::SeqCst);
            future::ready(Err("access denied".into())).boxed()
        }
    }

    impl S3StorageDriverClient for FailingS3Client {
        fn put_object<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
            _: Vec<u8>,
        ) -> BoxFuture<'a, Result<(), BoxError>> {
            self.fail()
        }

        fn object_exists<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
        ) -> BoxFuture<'a, Result<bool, BoxError>> {
            self.fail()
        }

        fn get_object<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
        ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>> {
            self.fail()
        }

        fn describe(&self) -> HashMap<String, String> {
            HashMap::from([("client_region".to_owned(), "us-west-2".to_owned())])
        }
    }

    /// Fails the request for one key once every other request is in flight, and counts the other
    /// requests that are dropped before they finish.
    struct FailOneS3Client {
        failing_key_suffix: String,
        other_requests_started: Semaphore,
        canceled_count: Arc<AtomicUsize>,
    }

    struct CountOnDrop(Arc<AtomicUsize>);

    impl Drop for CountOnDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl S3StorageDriverClient for FailOneS3Client {
        fn put_object<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
            _: Vec<u8>,
        ) -> BoxFuture<'a, Result<(), BoxError>> {
            future::ready(Ok(())).boxed()
        }

        fn object_exists<'a>(
            &'a self,
            _: &'a str,
            key: &'a str,
        ) -> BoxFuture<'a, Result<bool, BoxError>> {
            async move {
                if key.ends_with(&self.failing_key_suffix) {
                    let _ = self.other_requests_started.acquire_many(2).await;
                    return Err("fail".into());
                }
                let _guard = CountOnDrop(self.canceled_count.clone());
                self.other_requests_started.add_permits(1);
                future::pending().await
            }
            .boxed()
        }

        fn get_object<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
        ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>> {
            unimplemented!()
        }
    }

    fn payload(data: &str) -> Payload {
        Payload {
            metadata: HashMap::from([("encoding".to_owned(), b"json/plain".to_vec())]),
            data: data.as_bytes().to_vec(),
            ..Default::default()
        }
    }

    fn hash_of(data: &str) -> String {
        sha256_hex(&payload(data).encode_to_vec())
    }

    fn workflow_target(
        namespace: &str,
        id: Option<&str>,
        run_id: Option<&str>,
        workflow_type: Option<&str>,
    ) -> StorageDriverTargetInfo {
        let mut info = StorageDriverWorkflowInfo::default();
        info.namespace = namespace.to_owned();
        info.id = id.map(str::to_owned);
        info.run_id = run_id.map(str::to_owned);
        info.workflow_type = workflow_type.map(str::to_owned);
        StorageDriverTargetInfo::Workflow(info)
    }

    fn my_workflow() -> StorageDriverTargetInfo {
        workflow_target(
            "my-ns",
            Some("my-workflow-id"),
            Some("my-run-id"),
            Some("MyWorkflow"),
        )
    }

    fn store_context(target: Option<StorageDriverTargetInfo>) -> StorageDriverStoreContext {
        store_context_with_limiter(target, Arc::new(RecordingLimiter::new()))
    }

    fn store_context_with_limiter(
        target: Option<StorageDriverTargetInfo>,
        limiter: Arc<dyn StorageDriverLimiter<Payload>>,
    ) -> StorageDriverStoreContext {
        StorageDriverStoreContext::builder()
            .maybe_target(target)
            .limiter(limiter)
            .build()
    }

    fn retrieve_context() -> StorageDriverRetrieveContext {
        StorageDriverRetrieveContext::builder()
            .limiter(Arc::new(RecordingLimiter::new()))
            .build()
    }

    fn claim(
        bucket: Option<&str>,
        key: Option<&str>,
        hash_algorithm: Option<&str>,
        hash_value: Option<&str>,
    ) -> StorageDriverClaim {
        StorageDriverClaim::new(
            [
                (CLAIM_BUCKET, bucket),
                (CLAIM_KEY, key),
                (CLAIM_HASH_ALGORITHM, hash_algorithm),
                (CLAIM_HASH_VALUE, hash_value),
            ]
            .into_iter()
            .filter_map(|(name, value)| Some((name.to_owned(), value?.to_owned())))
            .collect(),
        )
    }

    fn valid_claim() -> StorageDriverClaim {
        claim(
            Some("my-bucket"),
            Some("my-key"),
            Some("sha256"),
            Some("abc"),
        )
    }

    async fn store_key(target: Option<StorageDriverTargetInfo>, data: &str) -> String {
        let driver = S3StorageDriver::new(Arc::new(FakeS3Client::default()), "my-bucket");
        let claims = driver
            .store(&store_context(target), vec![payload(data)])
            .await
            .unwrap();
        claims[0].claim_data[CLAIM_KEY].clone()
    }

    fn driver_error(err: ExternalStorageError) -> S3StorageDriverError {
        match err {
            ExternalStorageError::Driver(err) => *err.downcast().unwrap(),
            err => panic!("expected a driver error, got {err}"),
        }
    }

    #[test]
    fn default_name_is_aws_s3driver() {
        // Retrieval is routed by the name recorded in history, so this must stay identical across
        // SDKs. The literal is deliberate: a test against the constant would pass if it changed.
        let driver = S3StorageDriver::new(Arc::new(FakeS3Client::default()), "b");
        assert_eq!(driver.name(), "aws.s3driver");
    }

    #[test]
    fn max_payload_size_of_zero_is_rejected() {
        let result =
            S3StorageDriver::new(Arc::new(FakeS3Client::default()), "b").with_max_payload_size(0);
        assert!(matches!(
            result,
            Err(ExternalStorageError::InvalidConfiguration(_))
        ));
    }

    #[tokio::test]
    async fn retrieve_of_stored_claims_returns_the_original_payloads() {
        let driver = S3StorageDriver::new(Arc::new(FakeS3Client::default()), "my-bucket");
        let payloads = vec![payload("one"), payload("two")];

        let claims = driver
            .store(&store_context(Some(my_workflow())), payloads.clone())
            .await
            .unwrap();

        assert_eq!(
            driver.retrieve(&retrieve_context(), claims).await.unwrap(),
            payloads
        );
    }

    #[tokio::test]
    async fn store_claim_holds_bucket_key_and_sha256_hash() {
        let driver = S3StorageDriver::new(Arc::new(FakeS3Client::default()), "my-bucket");

        let claims = driver
            .store(&store_context(Some(my_workflow())), vec![payload("one")])
            .await
            .unwrap();

        let hash_value = hash_of("one");
        assert_eq!(
            claims[0].claim_data,
            HashMap::from([
                ("bucket".to_owned(), "my-bucket".to_owned()),
                (
                    "key".to_owned(),
                    format!(
                        "v0/ns/my-ns/wt/MyWorkflow/wi/my-workflow-id/ri/my-run-id/d/sha256/{hash_value}"
                    )
                ),
                ("hash_algorithm".to_owned(), "sha256".to_owned()),
                ("hash_value".to_owned(), hash_value),
            ])
        );
    }

    #[tokio::test]
    async fn store_workflow_target_builds_the_spec_example_key() {
        let target = workflow_target(
            "payments prod",
            Some("order+123=abc"),
            Some("3f1d6c7a-8b2e-4f7a-9d0a-87a6f95e4d31"),
            Some("ChargeWorkflow"),
        );

        assert_eq!(
            store_key(Some(target), "one").await,
            format!(
                "v0/ns/payments%20prod/wt/ChargeWorkflow/wi/order%2B123%3Dabc/ri/\
                 3f1d6c7a-8b2e-4f7a-9d0a-87a6f95e4d31/d/sha256/{}",
                hash_of("one")
            )
        );
    }

    #[tokio::test]
    async fn store_activity_target_builds_the_spec_example_key() {
        let mut info = StorageDriverActivityInfo::default();
        info.namespace = "payments prod".to_owned();
        info.id = Some("activity id+42".to_owned());
        info.run_id = Some("9e1d1fd9-2f8a-4c40-93e2-731f31b9268b".to_owned());
        info.activity_type = Some("Capture/Charge".to_owned());

        assert_eq!(
            store_key(Some(StorageDriverTargetInfo::Activity(info)), "one").await,
            format!(
                "v0/ns/payments%20prod/at/Capture%2FCharge/ai/activity%20id%2B42/ri/\
                 9e1d1fd9-2f8a-4c40-93e2-731f31b9268b/d/sha256/{}",
                hash_of("one")
            )
        );
    }

    #[tokio::test]
    async fn store_key_segment_escapes_every_byte_outside_s3_safe_characters() {
        let target = workflow_target(
            "AZaz09!-_.*'()",
            Some("~é"),
            Some("run-1"),
            Some("MyWorkflow"),
        );

        assert_eq!(
            store_key(Some(target), "one").await,
            format!(
                "v0/ns/AZaz09!-_.*'()/wt/MyWorkflow/wi/%7E%C3%A9/ri/run-1/d/sha256/{}",
                hash_of("one")
            )
        );
    }

    #[tokio::test]
    async fn store_empty_or_missing_target_values_write_null_segments() {
        let target = workflow_target("", None, Some(""), None);

        assert_eq!(
            store_key(Some(target), "one").await,
            format!(
                "v0/ns/null/wt/null/wi/null/ri/null/d/sha256/{}",
                hash_of("one")
            )
        );
    }

    #[tokio::test]
    async fn store_without_target_key_has_only_the_digest() {
        assert_eq!(
            store_key(None, "one").await,
            format!("v0/d/sha256/{}", hash_of("one"))
        );
    }

    #[tokio::test]
    async fn store_same_payload_twice_in_one_call_uploads_once() {
        let client = Arc::new(FakeS3Client::default());
        let driver = S3StorageDriver::new(client.clone(), "my-bucket");

        let claims = driver
            .store(
                &store_context(Some(my_workflow())),
                vec![payload("same"), payload("same")],
            )
            .await
            .unwrap();

        assert_eq!(client.put_count.load(Ordering::SeqCst), 1);
        assert_eq!(claims[0], claims[1]);
    }

    #[tokio::test]
    async fn store_object_already_in_s3_skips_the_upload() {
        let client = Arc::new(FakeS3Client::default());
        let driver = S3StorageDriver::new(client.clone(), "my-bucket");

        for _ in 0..2 {
            driver
                .store(&store_context(Some(my_workflow())), vec![payload("same")])
                .await
                .unwrap();
        }

        assert_eq!(client.put_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn store_bucket_selector_chooses_the_bucket_for_each_payload() {
        let client = Arc::new(FakeS3Client::default());
        let driver = S3StorageDriver::with_bucket_selector(client.clone(), |_, payload| {
            format!("bucket-{}", String::from_utf8_lossy(&payload.data))
        });

        let claims = driver
            .store(
                &store_context(Some(my_workflow())),
                vec![payload("a"), payload("b")],
            )
            .await
            .unwrap();

        let claimed_buckets: Vec<_> = claims
            .iter()
            .map(|claim| claim.claim_data[CLAIM_BUCKET].as_str())
            .collect();
        assert_eq!(claimed_buckets, ["bucket-a", "bucket-b"]);
        let mut stored_buckets: Vec<_> = client
            .objects
            .lock()
            .unwrap()
            .keys()
            .map(|(bucket, _)| bucket.clone())
            .collect();
        stored_buckets.sort();
        assert_eq!(stored_buckets, ["bucket-a", "bucket-b"]);
    }

    #[tokio::test]
    async fn each_s3_request_takes_one_limiter_permit() {
        let driver = S3StorageDriver::new(Arc::new(FakeS3Client::default()), "my-bucket");
        let store_limiter = Arc::new(RecordingLimiter::new());
        let retrieve_limiter = Arc::new(RecordingLimiter::new());

        let claims = driver
            .store(
                &store_context_with_limiter(Some(my_workflow()), store_limiter.clone()),
                vec![payload("one"), payload("two"), payload("one")],
            )
            .await
            .unwrap();
        driver
            .retrieve(
                &StorageDriverRetrieveContext::builder()
                    .limiter(retrieve_limiter.clone())
                    .build(),
                claims.clone(),
            )
            .await
            .unwrap();

        let mut stored = store_limiter.items.lock().unwrap().clone();
        stored.sort_by(|a, b| a.data.cmp(&b.data));
        assert_eq!(stored, [payload("one"), payload("two")]);
        assert_eq!(retrieve_limiter.items.lock().unwrap().len(), claims.len());
    }

    #[tokio::test]
    async fn every_s3_request_runs_while_holding_a_permit() {
        let live_permits = Arc::new(AtomicUsize::new(0));
        let client = Arc::new(PermitCheckingS3Client {
            inner: FakeS3Client::default(),
            live_permits: live_permits.clone(),
            request_count: AtomicUsize::new(0),
            requests_without_permit_count: AtomicUsize::new(0),
        });
        let driver = S3StorageDriver::new(client.clone(), "my-bucket");
        let limiter = Arc::new(LivePermitCountingLimiter(live_permits));

        // One payload at a time, so another request's permit cannot hide a request made without one
        let claims = driver
            .store(
                &store_context_with_limiter(None, limiter.clone()),
                vec![payload("one")],
            )
            .await
            .unwrap();
        driver
            .retrieve(
                &StorageDriverRetrieveContext::builder()
                    .limiter(limiter)
                    .build(),
                claims,
            )
            .await
            .unwrap();

        assert_eq!(client.request_count.load(Ordering::SeqCst), 3);
        assert_eq!(
            client.requests_without_permit_count.load(Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn store_runs_as_many_requests_at_once_as_the_limiter_allows() {
        let client = Arc::new(ConcurrencyTrackingS3Client {
            expected_concurrency: 2,
            expected_concurrency_reached: watch::Sender::new(false),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            request_count: AtomicUsize::new(0),
        });
        let driver = S3StorageDriver::new(client.clone(), "my-bucket");
        let context = store_context_with_limiter(
            None,
            Arc::new(SemaphoreLimiter(Arc::new(Semaphore::new(2)))),
        );
        let payloads = ["a", "b", "c", "d"].map(payload).to_vec();

        // The client holds requests until two are in flight, so a driver that did not fan out
        // would never finish
        timeout(Duration::from_secs(10), driver.store(&context, payloads))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(client.max_in_flight.load(Ordering::SeqCst), 2);
        assert_eq!(client.request_count.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn store_payload_over_max_payload_size_fails_and_at_the_limit_succeeds() {
        let size = payload("one").encode_to_vec().len();
        let at_limit = S3StorageDriver::new(Arc::new(FakeS3Client::default()), "b")
            .with_max_payload_size(size)
            .unwrap();
        let below_limit = S3StorageDriver::new(Arc::new(FakeS3Client::default()), "b")
            .with_max_payload_size(size - 1)
            .unwrap();

        at_limit
            .store(&store_context(None), vec![payload("one")])
            .await
            .unwrap();
        let err = below_limit
            .store(&store_context(None), vec![payload("one")])
            .await
            .unwrap_err();
        assert!(matches!(
            driver_error(err),
            S3StorageDriverError::PayloadTooLarge { .. }
        ));
    }

    #[tokio::test]
    async fn store_s3_request_failure_reports_bucket_key_and_region() {
        let driver = S3StorageDriver::new(Arc::new(FailingS3Client::default()), "my-bucket");

        let err = driver
            .store(&store_context(None), vec![payload("one")])
            .await
            .unwrap_err();

        let err = driver_error(err);
        assert_eq!(
            err.to_string(),
            format!(
                "S3StorageDriver store failed [bucket=my-bucket, key=v0/d/sha256/{}, \
                 client_region=us-west-2]",
                hash_of("one")
            )
        );
        assert_eq!(
            std::error::Error::source(&err).unwrap().to_string(),
            "access denied"
        );
    }

    #[tokio::test]
    async fn retrieve_s3_request_failure_reports_bucket_key_and_region() {
        let driver = S3StorageDriver::new(Arc::new(FailingS3Client::default()), "my-bucket");

        let err = driver
            .retrieve(&retrieve_context(), vec![valid_claim()])
            .await
            .unwrap_err();

        let err = driver_error(err);
        assert_eq!(
            err.to_string(),
            "S3StorageDriver retrieve failed [bucket=my-bucket, key=my-key, client_region=us-west-2]"
        );
        assert_eq!(
            std::error::Error::source(&err).unwrap().to_string(),
            "access denied"
        );
    }

    #[tokio::test]
    async fn store_one_upload_failing_cancels_the_others_and_returns_the_cause() {
        let canceled_count = Arc::new(AtomicUsize::new(0));
        let client = FailOneS3Client {
            failing_key_suffix: hash_of("fail"),
            other_requests_started: Semaphore::new(0),
            canceled_count: canceled_count.clone(),
        };
        let driver = S3StorageDriver::new(Arc::new(client), "my-bucket");

        let err = driver
            .store(
                &store_context(None),
                vec![payload("one"), payload("fail"), payload("two")],
            )
            .await
            .unwrap_err();

        let err = driver_error(err);
        assert_eq!(std::error::Error::source(&err).unwrap().to_string(), "fail");
        assert_eq!(canceled_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn retrieve_data_not_matching_claim_hash_fails() {
        let client = Arc::new(FakeS3Client::default());
        let driver = S3StorageDriver::new(client.clone(), "my-bucket");
        let claims = driver
            .store(&store_context(None), vec![payload("one")])
            .await
            .unwrap();
        client.objects.lock().unwrap().insert(
            (
                "my-bucket".to_owned(),
                claims[0].claim_data[CLAIM_KEY].clone(),
            ),
            payload("tampered").encode_to_vec(),
        );

        let err = driver
            .retrieve(&retrieve_context(), claims)
            .await
            .unwrap_err();

        assert!(matches!(
            driver_error(err),
            S3StorageDriverError::IntegrityCheck { .. }
        ));
    }

    #[tokio::test]
    async fn retrieve_incomplete_or_unsupported_claim_fails_before_any_s3_request() {
        let client = Arc::new(FailingS3Client::default());
        let driver = S3StorageDriver::new(client.clone(), "my-bucket");
        let invalid_claims = [
            claim(None, Some("my-key"), Some("sha256"), Some("abc")),
            claim(Some("my-bucket"), Some(""), Some("sha256"), Some("abc")),
            claim(Some("my-bucket"), Some("my-key"), None, Some("abc")),
            claim(Some("my-bucket"), Some("my-key"), Some("sha256"), None),
            claim(Some("my-bucket"), Some("my-key"), Some("md5"), Some("abc")),
        ];

        for invalid_claim in invalid_claims {
            driver
                .retrieve(&retrieve_context(), vec![valid_claim(), invalid_claim])
                .await
                .unwrap_err();
        }
        assert_eq!(client.request_count.load(Ordering::SeqCst), 0);
    }
}
