//! Runtime configuration and low-level Core worker building blocks.
//!
//! These types are grouped here to keep Core-specific configuration separate from the SDK's
//! primary workflow and activity APIs. Create a [`crate::Runtime`] before connecting a client,
//! then pass it to [`crate::Worker::new`].

use std::time::Duration;

use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{
    CoreRuntime, PollerBehavior as CorePollerBehavior, RuntimeOptions as CoreRuntimeOptions,
    TokioRuntimeBuilder as CoreTokioRuntimeBuilder, WorkflowErrorType as CoreWorkflowErrorType,
};

use crate::error::RuntimeError;

/// Worker concurrency tuning.
pub mod worker_tuner;

// These remain public only for the raw-worker APIs that are being migrated separately.
pub use temporalio_sdk_core::{Worker as CoreWorker, WorkerConfig};

/// Wraps a Tokio runtime builder so the SDK can install its per-thread telemetry state.
#[derive(bon::Builder)]
#[builder(state_mod(vis = "pub"))]
#[non_exhaustive]
pub struct TokioRuntimeBuilder {
    /// The Tokio runtime builder used to create the runtime.
    pub inner: tokio::runtime::Builder,
}

impl Default for TokioRuntimeBuilder {
    fn default() -> Self {
        Self {
            inner: tokio::runtime::Builder::new_multi_thread(),
        }
    }
}

impl TokioRuntimeBuilder {
    fn into_core(self) -> CoreTokioRuntimeBuilder<Box<dyn Fn() + Send + Sync>> {
        CoreTokioRuntimeBuilder {
            inner: self.inner,
            lang_on_thread_start: None,
        }
    }
}

/// Options for automatically scaling the number of concurrent task polls.
#[derive(bon::Builder, Clone, Copy, Debug, PartialEq)]
#[builder(state_mod(vis = "pub"))]
#[non_exhaustive]
pub struct AutoscalingOptions {
    /// Minimum number of concurrent polls. Cannot be zero.
    pub minimum: usize,
    /// Maximum number of concurrent polls. Must be at least `minimum`.
    pub maximum: usize,
    /// Initial number of concurrent polls. Must be between `minimum` and `maximum`.
    pub initial: usize,
}

/// Controls how many concurrent task polls a worker issues.
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]
pub enum PollerBehavior {
    /// Poll whenever a slot is available, up to the supplied maximum.
    SimpleMaximum(usize),
    /// Adjust concurrent polls using feedback from the server.
    Autoscaling(AutoscalingOptions),
}

impl PollerBehavior {
    pub(crate) fn into_core(self) -> CorePollerBehavior {
        match self {
            PollerBehavior::SimpleMaximum(maximum) => CorePollerBehavior::SimpleMaximum(maximum),
            PollerBehavior::Autoscaling(AutoscalingOptions {
                minimum,
                maximum,
                initial,
            }) => CorePollerBehavior::Autoscaling {
                minimum,
                maximum,
                initial,
            },
        }
    }
}

/// Workflow-processing errors that may be configured to fail the workflow execution.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum WorkflowErrorType {
    /// A workflow produced commands that do not match its recorded history.
    Nondeterminism,
}

impl WorkflowErrorType {
    pub(crate) fn into_core(self) -> CoreWorkflowErrorType {
        match self {
            WorkflowErrorType::Nondeterminism => CoreWorkflowErrorType::Nondeterminism,
        }
    }
}

/// Configuration for the Rust SDK runtime. Construct with [`RuntimeOptions::builder`].
#[derive(bon::Builder)]
#[builder(finish_fn(vis = "", name = build_internal))]
#[non_exhaustive]
pub struct RuntimeOptions {
    /// Telemetry configuration options.
    #[builder(default)]
    telemetry_options: TelemetryOptions,
    /// Optional worker heartbeat interval for all workers created with this runtime.
    ///
    /// The interval must be between 1 and 60 seconds, inclusive.
    #[builder(required, default = Some(Duration::from_secs(60)))]
    heartbeat_interval: Option<Duration>,
    /// Disable including runtime, hosting, and platform information in worker heartbeats.
    #[builder(default)]
    disable_environment_info: bool,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self::builder().build().expect("builder defaults are valid")
    }
}

impl<S: runtime_options_builder::State> RuntimeOptionsBuilder<S> {
    /// Builds the runtime options.
    ///
    /// # Errors
    /// Returns an error if `heartbeat_interval` is set but is not between 1 and 60 seconds,
    /// inclusive.
    pub fn build(self) -> Result<RuntimeOptions, String> {
        let options = self.build_internal();
        if let Some(interval) = options.heartbeat_interval
            && (interval < Duration::from_secs(1) || interval > Duration::from_secs(60))
        {
            return Err(format!(
                "heartbeat_interval ({interval:?}) must be between 1s and 60s",
            ));
        }
        Ok(options)
    }
}

impl RuntimeOptions {
    fn into_core(self) -> CoreRuntimeOptions {
        CoreRuntimeOptions::builder()
            .telemetry_options(self.telemetry_options)
            .heartbeat_interval(self.heartbeat_interval)
            .disable_environment_info(self.disable_environment_info)
            .build()
            .expect("SDK runtime options have already been validated")
    }
}

/// Holds shared state and components used by Rust SDK workers.
pub struct Runtime(CoreRuntime);

impl Runtime {
    /// Creates a runtime with a newly constructed Tokio runtime.
    ///
    /// # Errors
    /// Returns an error if telemetry or the Tokio runtime cannot be initialized.
    pub fn new(
        options: RuntimeOptions,
        tokio_builder: TokioRuntimeBuilder,
    ) -> Result<Self, RuntimeError> {
        CoreRuntime::new(options.into_core(), tokio_builder.into_core())
            .map(Self)
            .map_err(RuntimeError::from_core)
    }

    /// Creates a runtime using the currently active Tokio runtime.
    ///
    /// # Errors
    /// Returns [`RuntimeError::NoCurrentTokioRuntime`] if there is no currently active Tokio
    /// runtime, or [`RuntimeError::Initialization`] if telemetry cannot be initialized.
    pub fn from_current_tokio(options: RuntimeOptions) -> Result<Self, RuntimeError> {
        tokio::runtime::Handle::try_current().map_err(|_| RuntimeError::NoCurrentTokioRuntime)?;
        CoreRuntime::new_assume_tokio(options.into_core())
            .map(Self)
            .map_err(RuntimeError::from_core)
    }

    /// Creates a runtime using the currently active Tokio runtime.
    ///
    /// # Errors
    /// Returns [`RuntimeError::NoCurrentTokioRuntime`] if there is no currently active Tokio
    /// runtime, or [`RuntimeError::Initialization`] if telemetry cannot be initialized.
    #[deprecated(note = "use `Runtime::from_current_tokio` instead")]
    pub fn new_assume_tokio(options: RuntimeOptions) -> Result<Self, RuntimeError> {
        Self::from_current_tokio(options)
    }

    pub(crate) fn core(&self) -> &CoreRuntime {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::{Runtime, TokioRuntimeBuilder};
    use crate::error::RuntimeError;

    #[test]
    fn from_current_tokio_without_runtime_returns_error() {
        assert!(matches!(
            Runtime::from_current_tokio(Default::default()),
            Err(RuntimeError::NoCurrentTokioRuntime)
        ));
    }

    #[test]
    fn tokio_runtime_builder_constructs_with_an_inner_builder() {
        let _builder = TokioRuntimeBuilder::builder()
            .inner(tokio::runtime::Builder::new_current_thread())
            .build();
    }
}
