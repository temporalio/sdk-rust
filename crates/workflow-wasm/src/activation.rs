//! Hosts a guest that produces a complete Core Workflow Activation Completion.

use anyhow::anyhow;
use wasmtime::{
    Store, StoreLimits,
    component::{Linker, ResourceAny},
    error::Context,
};

use crate::{CompiledComponent, FUEL_PER_CALL};

wasmtime::component::bindgen!({
    path: "wit",
    world: "activation-workflow-module",
});
pub use exports::temporal::workflow_activation::guest::LocalActivities;

/// Compiled activation component and its startup-validated workflow names.
pub struct ActivationComponent {
    component: CompiledComponent,
    pre: ActivationWorkflowModulePre<StoreLimits>,
    workflow_types: Vec<String>,
}

impl ActivationComponent {
    pub fn new(bytes: &[u8]) -> anyhow::Result<Self> {
        let component = CompiledComponent::new(bytes, true)?;
        let linker = Linker::new(&component.engine);
        let pre = ActivationWorkflowModulePre::new(
            linker
                .instantiate_pre(&component.component)
                .context("linking activation workflow component without host imports")?,
        )?;
        let mut store = component.bounded_store()?;
        let guest = pre.instantiate(&mut store)?;
        store.set_fuel(FUEL_PER_CALL)?;
        let workflow_types = guest
            .temporal_workflow_activation_guest()
            .call_list_workflows(&mut store)
            .context("discovering WASM workflows")?
            .map_err(|err| anyhow!(err))?;
        anyhow::ensure!(
            !workflow_types.is_empty(),
            "component exports no workflow types"
        );
        Ok(Self {
            component,
            pre,
            workflow_types,
        })
    }

    pub fn workflow_types(&self) -> &[String] {
        &self.workflow_types
    }

    pub fn instantiate(
        &self,
        namespace: &str,
        task_queue: &str,
        enabled_logic_flags: &[u32],
        local_activity_registry: Option<&LocalActivities>,
        disable_eager_activity_execution: bool,
    ) -> anyhow::Result<ActivationInstance> {
        let mut store = self.component.bounded_store()?;
        let guest = self.pre.instantiate(&mut store)?;
        store.set_fuel(FUEL_PER_CALL)?;
        let resource = guest
            .temporal_workflow_activation_guest()
            .call_instantiate_workflow(
                &mut store,
                namespace,
                task_queue,
                enabled_logic_flags,
                local_activity_registry,
                disable_eager_activity_execution,
            )
            .context("creating WASM workflow instance")?
            .map_err(|err| anyhow!(err))?;
        Ok(ActivationInstance {
            store,
            guest,
            resource,
        })
    }
}

/// One guest store and workflow resource, owned by a single workflow run.
pub struct ActivationInstance {
    store: Store<StoreLimits>,
    guest: ActivationWorkflowModule,
    resource: ResourceAny,
}

impl ActivationInstance {
    pub fn activate(&mut self, activation: &[u8]) -> anyhow::Result<Vec<u8>> {
        self.store.set_fuel(FUEL_PER_CALL)?;
        self.guest
            .temporal_workflow_activation_guest()
            .workflow_instance()
            .call_activate(&mut self.store, self.resource, activation)
            .context("activating WASM workflow")?
            .map_err(|err| anyhow!(err))
    }
}
