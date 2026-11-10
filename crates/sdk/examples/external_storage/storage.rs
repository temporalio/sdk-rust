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

pub const S3_ENDPOINT: &str = "http://localhost:5000";
pub const S3_BUCKET: &str = "temporal-payloads";
pub const S3_ACCESS_KEY: &str = "test";
pub const S3_SECRET_KEY: &str = "test";
pub const S3_REGION: &str = "us-east-1";

/// Every process in this sample must agree on the codec and storage configuration, so each can
/// read what the others wrote.
pub fn data_converter() -> Result<DataConverter, ExternalStorageConfigError> {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .region(aws_sdk_s3::config::Region::new(S3_REGION))
        .endpoint_url(S3_ENDPOINT)
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            S3_ACCESS_KEY,
            S3_SECRET_KEY,
            None,
            None,
            "external-storage-sample",
        ))
        // Virtual-hosted addressing would need `temporal-payloads.localhost` to resolve, which
        // depends on how the OS handles `*.localhost`.
        .force_path_style(true)
        .build();
    let driver = S3StorageDriver::new(
        S3StorageDriverOptions::builder()
            .client(Arc::new(AwsSdkS3StorageDriverClient::new(
                aws_sdk_s3::Client::from_conf(config),
            )))
            .bucket(S3_BUCKET)
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
