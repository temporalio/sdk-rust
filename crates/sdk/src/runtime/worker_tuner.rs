//! Worker concurrency tuning for SDK workers.

use std::{fmt::Debug, sync::Arc, time::Duration};

use temporalio_sdk_core::{
    ResourceBasedSlotsOptions as CoreResourceBasedSlotsOptions,
    ResourceBasedTunerConfig as CoreResourceBasedTunerConfig,
    ResourceController as CoreResourceController, ResourceSlotOptions as CoreResourceSlotOptions,
    SlotKind as CoreSlotKind, SlotSupplierOptions as CoreSlotSupplierOptions,
    TunerHolderOptions as CoreTunerHolderOptions, WorkerTuner as CoreWorkerTuner,
};

const DEFAULT_FIXED_SIZE_SLOTS: usize = 100;

/// A worker tuner configuration.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum WorkerTuner {
    /// A tuner that creates a resource controller scoped to this worker.
    ResourceBased(ResourceBasedTuner),
    /// A resource-based tuner using a controller that may be shared by multiple workers.
    ResourceBasedWithController(ResourceBasedTunerWithController),
    /// A tuner composed from independently selected slot suppliers.
    TunerHolder(TunerHolder),
}

impl WorkerTuner {
    pub(crate) fn to_core(&self) -> Result<Arc<dyn CoreWorkerTuner + Send + Sync>, String> {
        match self {
            Self::ResourceBased(tuner) => tuner.to_tuner_holder().to_core(),
            Self::ResourceBasedWithController(tuner) => tuner.to_tuner_holder().to_core(),
            Self::TunerHolder(tuner) => tuner.to_core(),
        }
    }
}

impl Default for WorkerTuner {
    fn default() -> Self {
        TunerHolder::builder()
            .workflow_task_slot_supplier(FixedSizeSlotSupplier::new(DEFAULT_FIXED_SIZE_SLOTS))
            .activity_task_slot_supplier(FixedSizeSlotSupplier::new(DEFAULT_FIXED_SIZE_SLOTS))
            .local_activity_task_slot_supplier(FixedSizeSlotSupplier::new(DEFAULT_FIXED_SIZE_SLOTS))
            .nexus_task_slot_supplier(FixedSizeSlotSupplier::new(DEFAULT_FIXED_SIZE_SLOTS))
            .build()
            .into()
    }
}

impl From<ResourceBasedTuner> for WorkerTuner {
    fn from(value: ResourceBasedTuner) -> Self {
        Self::ResourceBased(value)
    }
}

impl From<ResourceBasedTunerWithController> for WorkerTuner {
    fn from(value: ResourceBasedTunerWithController) -> Self {
        Self::ResourceBasedWithController(value)
    }
}

impl From<TunerHolder> for WorkerTuner {
    fn from(value: TunerHolder) -> Self {
        Self::TunerHolder(value)
    }
}

/// A tuner composed from independently selected slot suppliers.
#[derive(Clone, Debug, bon::Builder)]
#[builder(state_mod(vis = "pub"))]
#[non_exhaustive]
pub struct TunerHolder {
    /// Supplies workflow-task slots.
    #[builder(into)]
    pub workflow_task_slot_supplier: SlotSupplier,
    /// Supplies activity-task slots.
    #[builder(into)]
    pub activity_task_slot_supplier: SlotSupplier,
    /// Supplies local-activity slots.
    #[builder(into)]
    pub local_activity_task_slot_supplier: SlotSupplier,
    /// Supplies Nexus-task slots.
    #[builder(into)]
    pub nexus_task_slot_supplier: SlotSupplier,
}

impl TunerHolder {
    fn to_core(&self) -> Result<Arc<dyn CoreWorkerTuner + Send + Sync>, String> {
        let configurations = [
            self.workflow_task_slot_supplier.resource_configuration(),
            self.activity_task_slot_supplier.resource_configuration(),
            self.local_activity_task_slot_supplier
                .resource_configuration(),
            self.nexus_task_slot_supplier.resource_configuration(),
        ];
        let mut configurations = configurations.into_iter().flatten();
        let resource_based_config = configurations.next();
        if let Some(first) = resource_based_config
            && configurations.any(|other| !first.is_compatible_with(other))
        {
            return Err(
                "cannot construct worker tuner with multiple different resource-based tuner configurations"
                    .to_owned(),
            );
        }

        CoreTunerHolderOptions::builder()
            .workflow_slot_options(
                self.workflow_task_slot_supplier
                    .to_core(ResourceKind::Workflow),
            )
            .activity_slot_options(
                self.activity_task_slot_supplier
                    .to_core(ResourceKind::Activity),
            )
            .local_activity_slot_options(
                self.local_activity_task_slot_supplier
                    .to_core(ResourceKind::Activity),
            )
            .nexus_slot_options(self.nexus_task_slot_supplier.to_core(ResourceKind::Nexus))
            .maybe_resource_based_config(resource_based_config.map(ResourceBasedConfig::to_core))
            .build()
            .map_err(|error| error.to_string())?
            .build_tuner_holder()
            .map(|tuner| Arc::new(tuner) as Arc<dyn CoreWorkerTuner + Send + Sync>)
            .map_err(|error| error.to_string())
    }
}

/// A resource-based tuner that creates a controller scoped to its worker.
#[derive(Clone, Debug, bon::Builder)]
#[builder(state_mod(vis = "pub"))]
#[non_exhaustive]
pub struct ResourceBasedTuner {
    /// Target memory and CPU usage.
    pub tuner_options: ResourceBasedTunerOptions,
    /// Workflow-task slot options, or `None` to use defaults.
    pub workflow_task_slot_options: Option<ResourceBasedSlotOptions>,
    /// Activity-task slot options, or `None` to use defaults.
    pub activity_task_slot_options: Option<ResourceBasedSlotOptions>,
    /// Local-activity slot options, or `None` to use defaults.
    pub local_activity_task_slot_options: Option<ResourceBasedSlotOptions>,
    /// Nexus-task slot options, or `None` to use defaults.
    pub nexus_task_slot_options: Option<ResourceBasedSlotOptions>,
}

impl ResourceBasedTuner {
    fn to_tuner_holder(&self) -> TunerHolder {
        TunerHolder::builder()
            .workflow_task_slot_supplier(ResourceBasedSlotsForType::new(
                self.tuner_options,
                self.workflow_task_slot_options.unwrap_or_default(),
            ))
            .activity_task_slot_supplier(ResourceBasedSlotsForType::new(
                self.tuner_options,
                self.activity_task_slot_options.unwrap_or_default(),
            ))
            .local_activity_task_slot_supplier(ResourceBasedSlotsForType::new(
                self.tuner_options,
                self.local_activity_task_slot_options.unwrap_or_default(),
            ))
            .nexus_task_slot_supplier(ResourceBasedSlotsForType::new(
                self.tuner_options,
                self.nexus_task_slot_options.unwrap_or_default(),
            ))
            .build()
    }
}

/// A resource-based tuner governed by a controller shared across workers.
#[derive(Clone, Debug, bon::Builder)]
#[builder(state_mod(vis = "pub"))]
#[non_exhaustive]
pub struct ResourceBasedTunerWithController {
    /// The shared resource controller.
    pub controller: ResourceBasedController,
    /// Workflow-task slot options, or `None` to use defaults.
    pub workflow_task_slot_options: Option<ResourceBasedSlotOptions>,
    /// Activity-task slot options, or `None` to use defaults.
    pub activity_task_slot_options: Option<ResourceBasedSlotOptions>,
    /// Local-activity slot options, or `None` to use defaults.
    pub local_activity_task_slot_options: Option<ResourceBasedSlotOptions>,
    /// Nexus-task slot options, or `None` to use defaults.
    pub nexus_task_slot_options: Option<ResourceBasedSlotOptions>,
}

impl ResourceBasedTunerWithController {
    fn to_tuner_holder(&self) -> TunerHolder {
        TunerHolder::builder()
            .workflow_task_slot_supplier(ResourceBasedSlotsForType::with_controller(
                self.controller.clone(),
                self.workflow_task_slot_options.unwrap_or_default(),
            ))
            .activity_task_slot_supplier(ResourceBasedSlotsForType::with_controller(
                self.controller.clone(),
                self.activity_task_slot_options.unwrap_or_default(),
            ))
            .local_activity_task_slot_supplier(ResourceBasedSlotsForType::with_controller(
                self.controller.clone(),
                self.local_activity_task_slot_options.unwrap_or_default(),
            ))
            .nexus_task_slot_supplier(ResourceBasedSlotsForType::with_controller(
                self.controller.clone(),
                self.nexus_task_slot_options.unwrap_or_default(),
            ))
            .build()
    }
}

/// Target resource usage for resource-based tuning.
#[derive(Clone, Copy, Debug, PartialEq, bon::Builder)]
#[builder(state_mod(vis = "pub"))]
#[non_exhaustive]
pub struct ResourceBasedTunerOptions {
    /// Target system memory usage as a fraction from zero to one.
    pub target_memory_usage: f64,
    /// Target system CPU usage as a fraction from zero to one.
    pub target_cpu_usage: f64,
}

impl ResourceBasedTunerOptions {
    fn to_core(self) -> CoreResourceBasedSlotsOptions {
        CoreResourceBasedSlotsOptions::builder()
            .target_mem_usage(self.target_memory_usage)
            .target_cpu_usage(self.target_cpu_usage)
            .build()
    }
}

/// Coordinates resource-based slot allocation across multiple workers.
#[derive(Clone, derive_more::Debug)]
pub struct ResourceBasedController(#[debug(skip)] Arc<CoreResourceController>);

impl ResourceBasedController {
    /// Creates a controller using system-wide memory and CPU measurements.
    pub fn new(options: ResourceBasedTunerOptions) -> Self {
        Self(Arc::new(CoreResourceController::new(options.to_core())))
    }
}

/// Per-task-type options for resource-based slots.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, bon::Builder)]
#[builder(state_mod(vis = "pub"))]
#[non_exhaustive]
pub struct ResourceBasedSlotOptions {
    /// Slots issued without consulting resource usage, or `None` to use the task-type default.
    pub minimum_slots: Option<usize>,
    /// Maximum slots that may be issued, or `None` to use the task-type default.
    pub maximum_slots: Option<usize>,
    /// Minimum delay between slots above the minimum, or `None` to use the task-type default.
    pub ramp_throttle: Option<Duration>,
}

impl ResourceBasedSlotOptions {
    fn to_core(self, kind: ResourceKind) -> CoreResourceSlotOptions {
        let defaults = kind.slot_defaults();
        CoreResourceSlotOptions::new(
            self.minimum_slots.unwrap_or(defaults.minimum_slots),
            self.maximum_slots.unwrap_or(defaults.maximum_slots),
            self.ramp_throttle.unwrap_or(defaults.ramp_throttle),
        )
    }
}

/// Resource-based slot settings for one task type.
#[derive(Clone, Debug)]
pub struct ResourceBasedSlotsForType {
    configuration: ResourceBasedConfig,
    /// Per-task-type slot settings.
    pub slot_options: ResourceBasedSlotOptions,
}

impl ResourceBasedSlotsForType {
    /// Creates settings that construct a resource controller with the worker.
    pub fn new(
        tuner_options: ResourceBasedTunerOptions,
        slot_options: ResourceBasedSlotOptions,
    ) -> Self {
        Self {
            configuration: ResourceBasedConfig::Options(tuner_options),
            slot_options,
        }
    }

    /// Creates settings governed by a shared resource controller.
    pub fn with_controller(
        controller: ResourceBasedController,
        slot_options: ResourceBasedSlotOptions,
    ) -> Self {
        Self {
            configuration: ResourceBasedConfig::Controller(controller),
            slot_options,
        }
    }

    /// Returns target options when the controller is scoped to the worker.
    pub fn tuner_options(&self) -> Option<ResourceBasedTunerOptions> {
        match self.configuration {
            ResourceBasedConfig::Options(options) => Some(options),
            ResourceBasedConfig::Controller(_) => None,
        }
    }

    /// Returns the shared controller, when configured.
    pub fn controller(&self) -> Option<&ResourceBasedController> {
        match &self.configuration {
            ResourceBasedConfig::Options(_) => None,
            ResourceBasedConfig::Controller(controller) => Some(controller),
        }
    }
}

#[derive(Clone, Debug)]
enum ResourceBasedConfig {
    Options(ResourceBasedTunerOptions),
    Controller(ResourceBasedController),
}

impl ResourceBasedConfig {
    fn is_compatible_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Options(left), Self::Options(right)) => left == right,
            (Self::Controller(left), Self::Controller(right)) => Arc::ptr_eq(&left.0, &right.0),
            (Self::Options(_), Self::Controller(_)) | (Self::Controller(_), Self::Options(_)) => {
                false
            }
        }
    }

    fn to_core(&self) -> CoreResourceBasedTunerConfig {
        match self {
            Self::Options(options) => CoreResourceBasedTunerConfig::Options(options.to_core()),
            Self::Controller(controller) => {
                CoreResourceBasedTunerConfig::Controller(controller.0.clone())
            }
        }
    }
}

/// A fixed-size slot supplier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct FixedSizeSlotSupplier {
    /// Maximum number of slots that may be issued.
    pub num_slots: usize,
}

impl FixedSizeSlotSupplier {
    /// Creates a fixed-size supplier.
    pub fn new(num_slots: usize) -> Self {
        Self { num_slots }
    }
}

/// A fixed-size or resource-based slot supplier.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum SlotSupplier {
    /// A supplier with a fixed concurrency limit.
    FixedSize(FixedSizeSlotSupplier),
    /// A supplier governed by resource usage.
    ResourceBased(ResourceBasedSlotsForType),
}

impl From<FixedSizeSlotSupplier> for SlotSupplier {
    fn from(value: FixedSizeSlotSupplier) -> Self {
        Self::FixedSize(value)
    }
}

impl From<ResourceBasedSlotsForType> for SlotSupplier {
    fn from(value: ResourceBasedSlotsForType) -> Self {
        Self::ResourceBased(value)
    }
}

impl SlotSupplier {
    fn resource_configuration(&self) -> Option<&ResourceBasedConfig> {
        match self {
            Self::ResourceBased(options) => Some(&options.configuration),
            Self::FixedSize(_) => None,
        }
    }

    fn to_core<SK: CoreSlotKind>(&self, kind: ResourceKind) -> CoreSlotSupplierOptions<SK> {
        match self {
            Self::FixedSize(supplier) => CoreSlotSupplierOptions::FixedSize {
                slots: supplier.num_slots,
            },
            Self::ResourceBased(options) => {
                CoreSlotSupplierOptions::ResourceBased(options.slot_options.to_core(kind))
            }
        }
    }
}

#[derive(Clone, Copy)]
enum ResourceKind {
    Workflow,
    Activity,
    Nexus,
}

impl ResourceKind {
    fn slot_defaults(self) -> ResourceSlotDefaults {
        match self {
            Self::Workflow => ResourceSlotDefaults {
                minimum_slots: 2,
                maximum_slots: 1_000,
                ramp_throttle: Duration::from_millis(10),
            },
            Self::Activity | Self::Nexus => ResourceSlotDefaults {
                minimum_slots: 1,
                maximum_slots: 2_000,
                ramp_throttle: Duration::from_millis(50),
            },
        }
    }
}

struct ResourceSlotDefaults {
    minimum_slots: usize,
    maximum_slots: usize,
    ramp_throttle: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composite_tuner_rejects_different_resource_options() {
        let first_options = ResourceBasedTunerOptions::builder()
            .target_memory_usage(0.5)
            .target_cpu_usage(0.5)
            .build();
        let second_options = ResourceBasedTunerOptions::builder()
            .target_memory_usage(0.6)
            .target_cpu_usage(0.5)
            .build();
        let result = WorkerTuner::from(
            TunerHolder::builder()
                .workflow_task_slot_supplier(ResourceBasedSlotsForType::new(
                    first_options,
                    Default::default(),
                ))
                .activity_task_slot_supplier(ResourceBasedSlotsForType::new(
                    second_options,
                    Default::default(),
                ))
                .local_activity_task_slot_supplier(FixedSizeSlotSupplier::new(1))
                .nexus_task_slot_supplier(FixedSizeSlotSupplier::new(1))
                .build(),
        )
        .to_core();
        assert!(
            result
                .err()
                .is_some_and(|error| error.contains("different resource-based tuner"))
        );
    }
}
