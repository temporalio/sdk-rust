use temporalio_common_wasm::protos::temporal::api::enums::v1::ContinueAsNewVersioningBehavior as ProtoContinueAsNewVersioningBehavior;

/// Versioning behavior to use for the first workflow task of a new continue-as-new run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum ContinueAsNewVersioningBehavior {
    /// No initial versioning behavior was specified.
    #[default]
    Unspecified,
    /// Start the new run with AutoUpgrade behavior.
    AutoUpgrade,
    /// Start the new run on the task queue's ramping deployment version.
    UseRampingVersion,
}

impl From<ContinueAsNewVersioningBehavior> for ProtoContinueAsNewVersioningBehavior {
    fn from(value: ContinueAsNewVersioningBehavior) -> Self {
        match value {
            ContinueAsNewVersioningBehavior::Unspecified => {
                ProtoContinueAsNewVersioningBehavior::Unspecified
            }
            ContinueAsNewVersioningBehavior::AutoUpgrade => {
                ProtoContinueAsNewVersioningBehavior::AutoUpgrade
            }
            ContinueAsNewVersioningBehavior::UseRampingVersion => {
                ProtoContinueAsNewVersioningBehavior::UseRampingVersion
            }
        }
    }
}

impl From<ProtoContinueAsNewVersioningBehavior> for ContinueAsNewVersioningBehavior {
    fn from(value: ProtoContinueAsNewVersioningBehavior) -> Self {
        match value {
            ProtoContinueAsNewVersioningBehavior::Unspecified => {
                ContinueAsNewVersioningBehavior::Unspecified
            }
            ProtoContinueAsNewVersioningBehavior::AutoUpgrade => {
                ContinueAsNewVersioningBehavior::AutoUpgrade
            }
            ProtoContinueAsNewVersioningBehavior::UseRampingVersion => {
                ContinueAsNewVersioningBehavior::UseRampingVersion
            }
        }
    }
}
