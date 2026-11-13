#![allow(unreachable_pub)]
use crate::codec::ZlibCodec;
use std::sync::Arc;
use temporalio_aws_s3driver::{
    AwsSdkS3StorageDriverClient, S3StorageDriver, S3StorageDriverOptions,
};
use temporalio_common::data_converters::{
    DataConverter, DefaultFailureConverter, ExternalStorage, ExternalStorageConfigError,
    PayloadConverter,
};

/// Set to a bucket name to use real S3 instead of the local mock.
pub const BUCKET_ENV_VAR: &str = "EXTERNAL_STORAGE_S3_BUCKET";

pub const MOCK_ENDPOINT: &str = "http://localhost:9100";
pub const MOCK_BUCKET: &str = "temporal-payloads";
pub const MOCK_ACCESS_KEY: &str = "test";
pub const MOCK_SECRET_KEY: &str = "test";
pub const MOCK_REGION: &str = "us-east-1";

/// Every process in this sample must agree on the codec and storage configuration, so each can
/// read what the others wrote.
pub async fn data_converter() -> Result<DataConverter, ExternalStorageConfigError> {
    let (client, bucket) = match std::env::var(BUCKET_ENV_VAR) {
        Ok(bucket) if !bucket.is_empty() => {
            // Region and credentials come from the usual AWS configuration, such as environment
            // variables or `~/.aws`.
            let config = aws_config::load_from_env().await;
            (aws_sdk_s3::Client::new(&config), bucket)
        }
        _ => (mock_client(), MOCK_BUCKET.to_owned()),
    };
    let driver = S3StorageDriver::new(
        S3StorageDriverOptions::builder()
            .client(Arc::new(AwsSdkS3StorageDriverClient::new(client)))
            .bucket(bucket)
            .build(),
    );
    let storage = ExternalStorage::new(Arc::new(driver))?;

    Ok(DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter::default(),
        ZlibCodec,
    )
    .with_external_storage(storage))
}

/// A client for the local mock S3 server in `s3_mock.rs`.
fn mock_client() -> aws_sdk_s3::Client {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new(MOCK_REGION))
        .endpoint_url(MOCK_ENDPOINT)
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            MOCK_ACCESS_KEY,
            MOCK_SECRET_KEY,
            None,
            None,
            "external-storage-sample",
        ))
        // Virtual-hosted addressing would need `temporal-payloads.localhost` to resolve, which
        // depends on how the OS handles `*.localhost`.
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(config)
}
