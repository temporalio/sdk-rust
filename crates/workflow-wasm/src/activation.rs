//! Hosts a guest that produces a complete Core Workflow Activation Completion.

use anyhow::{anyhow, ensure};
use std::{collections::HashMap, path::Path, sync::Arc};
use wasmtime::{
    Store, StoreContextMut,
    component::{Linker, ResourceAny},
    error::Context,
};

use wasmparser::{Parser, Payload};

use crate::{
    BoundedStore, CompiledComponent, FUEL_PER_CALL, MAX_COMPONENT_BYTES,
    addressable::{ActivationBindings, AddressableState, Bindings, ContentStore},
};

wasmtime::component::bindgen!({
    path: "wit",
    world: "activation-workflow-module",
});
pub use exports::temporal::workflow_activation::guest::LocalActivities;

const MAX_PYTHON_SOURCE_BYTES: usize = 64 * 1024 * 1024;

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
        ensure!(
            bytes.len() <= MAX_COMPONENT_BYTES + MAX_PYTHON_SOURCE_BYTES,
            "WASM workflow bundle exceeds the combined component and Python source limit"
        );
        let (sources, source_bytes) = python_sources(bytes)?;
        let addressable_store = path.map(ContentStore::open).transpose()?.map(Arc::new);
        let component =
            CompiledComponent::with_max_bytes(bytes, true, MAX_COMPONENT_BYTES + source_bytes)?;
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
        if let Some(sources) = sources {
            linker.root().func_wrap(
                "read-python-source",
                move |_caller: StoreContextMut<'_, BoundedStore<AddressableState>>,
                      (path,): (String,)| { Ok((sources.get(&path).cloned(),)) },
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

type PythonSources = HashMap<String, Vec<u8>>;

fn python_sources(bytes: &[u8]) -> anyhow::Result<(Option<PythonSources>, usize)> {
    let mut depth = 0;
    let mut sources = None;
    let mut source_bytes = 0;
    for payload in Parser::new(0).parse_all(bytes) {
        match payload? {
            Payload::Version { .. } => depth += 1,
            Payload::End(_) => depth -= 1,
            Payload::CustomSection(section)
                if depth == 1 && section.name() == "temporal-python-sources-v1" =>
            {
                ensure!(sources.is_none(), "duplicate WASM Python sources section");
                let mut data = section.data();
                source_bytes = data.len();
                ensure!(
                    source_bytes <= MAX_PYTHON_SOURCE_BYTES,
                    "WASM Python sources exceed {MAX_PYTHON_SOURCE_BYTES} bytes"
                );
                let mut entries = HashMap::new();
                while !data.is_empty() {
                    let name = std::str::from_utf8(source_field(&mut data)?)?;
                    let source = source_field(&mut data)?;
                    entries.insert(name.to_owned(), source.to_vec());
                }
                sources = Some(entries);
            }
            _ => {}
        }
    }
    Ok((sources, source_bytes))
}

fn source_field<'a>(data: &mut &'a [u8]) -> anyhow::Result<&'a [u8]> {
    let (length, remaining) = data
        .split_first_chunk::<4>()
        .ok_or_else(|| anyhow!("truncated WASM Python source field length"))?;
    let (field, remaining) = remaining
        .split_at_checked(u32::from_le_bytes(*length) as usize)
        .ok_or_else(|| anyhow!("truncated WASM Python source field"))?;
    *data = remaining;
    Ok(field)
}
