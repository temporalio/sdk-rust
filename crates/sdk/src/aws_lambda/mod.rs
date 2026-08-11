//! Optional integrations for running Temporal workers in AWS Lambda.

#[cfg(feature = "aws-lambda-otel")]
pub mod otel;

#[cfg(feature = "aws-lambda")]
mod worker;

#[cfg(feature = "aws-lambda")]
pub use worker::*;
