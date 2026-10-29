#![warn(missing_docs)] // error if there are missing docs
#![cfg_attr(docsrs, feature(doc_cfg))]

//! Amazon S3 storage driver for Temporal external storage.
//!
//! [`S3StorageDriver`] has no AWS dependency. It performs requests through an
//! [`S3StorageDriverClient`], which you can implement for any S3 library. The `aws-sdk` feature,
//! enabled by default, adds [`AwsSdkS3StorageDriverClient`] for the official AWS SDK. Disable
//! default features to bring your own client without depending on the AWS SDK.
//!
//! This crate is experimental and may change in the future.

#[cfg(feature = "aws-sdk")]
mod aws_sdk_client;
mod client;
mod driver;

#[cfg(feature = "aws-sdk")]
pub use aws_sdk_client::AwsSdkS3StorageDriverClient;
pub use client::{BoxError, S3StorageDriverClient};
pub use driver::{S3StorageDriver, S3StorageDriverError};
