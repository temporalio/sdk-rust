//! Runtime configuration and low-level Core worker building blocks.
//!
//! These types are grouped here to keep Core-specific configuration separate from the SDK's
//! primary workflow and activity APIs. Create a [`crate::Runtime`] before connecting a client,
//! then pass it to [`crate::Worker::new`].

use std::time::Duration;

use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions as CoreRuntimeOptions};

use crate::error::RuntimeError;

pub use temporalio_sdk_core::{
    ActivitySlotKind, FixedSizeSlotSupplier, LocalActivitySlotKind, LocalFirstOptions,
    LocalFirstOptionsBuilder, NexusSlotKind, PollerBehavior, ResourceBasedSlotsOptions,
    ResourceBasedSlotsOptionsBuilder, ResourceBasedTuner, ResourceBasedTunerConfig,
    ResourceController, ResourceSlotOptions, SlotInfo, SlotInfoTrait, SlotKind, SlotKindType,
    SlotMarkUsedContext, SlotReleaseContext, SlotReservationContext, SlotSupplier,
    SlotSupplierOptions, SlotSupplierPermit, TokioRuntimeBuilder, TunerBuilder, TunerHolder,
    TunerHolderOptions, TunerHolderOptionsBuilder, Worker as CoreWorker, WorkerConfig,
    WorkerConfigBuilder, WorkerTuner, WorkerVersioningStrategy, WorkflowErrorType,
    WorkflowSlotKind, init_replay_worker, replay,
};

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
