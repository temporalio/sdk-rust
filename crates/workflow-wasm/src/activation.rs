//! Hosts a guest that produces a complete Core Workflow Activation Completion.

use anyhow::anyhow;
use std::{path::Path, sync::Arc};
use wasmtime::{
    Store, StoreContextMut,
    component::{Linker, ResourceAny},
    error::Context,
};

use crate::{
    BoundedStore, CompiledComponent, FUEL_PER_CALL,
    addressable::{ActivationBindings, AddressableState, Bindings, ContentStore},
};

wasmtime::component::bindgen!({
    path: "wit",
    world: "activation-workflow-module",
});
pub use exports::temporal::workflow_activation::guest::LocalActivities;

/// Compiled activation component and its startup-validated workflow names.
pub struct ActivationComponent {
    component: CompiledComponent,
    pre: ActivationWorkflowModulePre<BoundedStore<AddressableState>>,
    workflow_types: Vec<String>,
    addressable_store: Option<Arc<ContentStore>>,
}

impl ActivationComponent {
    pub fn new(bytes: &[u8]) -> anyhow::Result<Self> {
        Self::with_addressable_store(bytes, None)
    }

    pub fn with_addressable_store(bytes: &[u8], path: Option<&Path>) -> anyhow::Result<Self> {
        let addressable_store = path.map(ContentStore::open).transpose()?.map(Arc::new);
        let component = CompiledComponent::new(bytes, true)?;
        let mut linker = Linker::new(&component.engine);
        if addressable_store.is_some() {
            linker.root().func_wrap(
                "resolve-addressable",
                |mut caller: StoreContextMut<'_, BoundedStore<AddressableState>>,
                 (slot,): (String,)| {
                    caller
                        .data_mut()
                        .data
                        .resolve(slot)
                        .map(|body| (body,))
                        .map_err(wasmtime::Error::from_anyhow)
                },
            )?;
        }
        let pre = ActivationWorkflowModulePre::new(
            linker
                .instantiate_pre(&component.component)
                .context("linking activation workflow component")?,
        )?;
        let mut store = component.bounded_store(AddressableState::default())?;
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
            addressable_store,
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
        let mut store = self.component.bounded_store(AddressableState {
            store: self.addressable_store.clone(),
            activation: None,
        })?;
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
    store: Store<BoundedStore<AddressableState>>,
    guest: ActivationWorkflowModule,
    resource: ResourceAny,
}

impl ActivationInstance {
    pub fn activate(&mut self, activation: &[u8]) -> anyhow::Result<Vec<u8>> {
        self.activate_with_bindings(activation, false, Bindings::new())
            .map(|(output, _)| output)
    }

    pub fn activate_with_bindings(
        &mut self,
        activation: &[u8],
        replaying: bool,
        recorded: Bindings,
    ) -> anyhow::Result<(Vec<u8>, Bindings)> {
        self.store.set_fuel(FUEL_PER_CALL)?;
        self.store.data_mut().data.activation = Some(ActivationBindings {
            replaying,
            recorded,
            used: Bindings::new(),
        });
        let output = self
            .guest
            .temporal_workflow_activation_guest()
            .workflow_instance()
            .call_activate(&mut self.store, self.resource, activation)
            .context("activating WASM workflow");
        let used = self
            .store
            .data_mut()
            .data
            .activation
            .take()
            .expect("activation set before guest call")
            .used;
        Ok((output?.map_err(|err| anyhow!(err))?, used))
    }
}
