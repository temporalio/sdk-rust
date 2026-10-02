use crate::{
    WorkerDeploymentVersion,
    protos::temporal::api::{
        deployment::v1::WorkerDeploymentVersion as ProtoWorkerDeploymentVersion,
        workflow::v1::{VersioningOverride as ProtoVersioningOverride, versioning_override},
    },
};

/// Overrides a workflow's worker deployment routing.
///
/// **Experimental:** Pinned and auto-upgrade overrides for client-started workflows require
/// Temporal Server 1.28.0 or later. Child workflow overrides require Temporal Server 1.32.0 or later.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum VersioningOverride {
    /// Keep routing the workflow to this deployment version until the override is removed.
    Pinned(WorkerDeploymentVersion),
    /// Route the workflow using the task queue's current deployment version.
    AutoUpgrade,
    /// Route to this version until one workflow task completes there, then use the
    /// versioning behavior and deployment version reported by that worker.
    ///
    /// Requires Temporal Server 1.32.0 or later.
    OneTime(WorkerDeploymentVersion),
}

impl From<VersioningOverride> for ProtoVersioningOverride {
    fn from(value: VersioningOverride) -> Self {
        let version = |value: WorkerDeploymentVersion| ProtoWorkerDeploymentVersion {
            deployment_name: value.deployment_name,
            build_id: value.build_id,
        };
        match value {
            VersioningOverride::Pinned(value) => Self {
                r#override: Some(versioning_override::Override::Pinned(
                    versioning_override::PinnedOverride {
                        behavior: versioning_override::PinnedOverrideBehavior::Pinned as i32,
                        version: Some(version(value)),
                    },
                )),
                ..Default::default()
            },
            VersioningOverride::AutoUpgrade => Self {
                r#override: Some(versioning_override::Override::AutoUpgrade(true)),
                ..Default::default()
            },
            VersioningOverride::OneTime(value) => Self {
                r#override: Some(versioning_override::Override::OneTime(
                    versioning_override::OneTimeOverride {
                        target_deployment_version: Some(version(value)),
                    },
                )),
                ..Default::default()
            },
        }
    }
}
