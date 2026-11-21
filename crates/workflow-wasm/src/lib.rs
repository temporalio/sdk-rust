//! Wasmtime hosting shared by Temporal's workflow SDKs.

use wasmtime::{
    Config, Engine, OptLevel, Store, StoreLimits, StoreLimitsBuilder, component::Component,
    error::Context,
};

pub mod activation;
mod addressable;
pub use wasmtime;

const FUEL_PER_CALL: u64 = 250_000_000;
const MEMORY_BYTES: usize = 128 * 1024 * 1024;
const MAX_COMPONENT_BYTES: usize = 64 * 1024 * 1024;

/// A portable component compiled once for use by multiple workflow runs.
pub struct CompiledComponent {
    pub engine: Engine,
    pub component: Component,
}

impl CompiledComponent {
    pub fn new(bytes: &[u8], bounded: bool) -> anyhow::Result<Self> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        if bounded {
            anyhow::ensure!(
                bytes.len() <= MAX_COMPONENT_BYTES,
                "WASM workflow component exceeds {MAX_COMPONENT_BYTES} bytes"
            );
            // Python bundles carry the whole interpreter. Keep compilation affordable per worker.
            config.cranelift_opt_level(OptLevel::None);
            config.consume_fuel(true);
            config.cranelift_nan_canonicalization(true);
        }
        let engine = Engine::new(&config)?;
        let component =
            Component::new(&engine, bytes).context("compiling WASM workflow component")?;
        Ok(Self { engine, component })
    }

    pub fn store<T>(&self, state: T) -> Store<T> {
        Store::new(&self.engine, state)
    }

    fn bounded_store<T>(&self, data: T) -> anyhow::Result<Store<BoundedStore<T>>> {
        let mut store = self.store(BoundedStore {
            limits: StoreLimitsBuilder::new()
                .memory_size(MEMORY_BYTES)
                .memories(4)
                .table_elements(100_000)
                .trap_on_grow_failure(true)
                .build(),
            data,
        });
        store.limiter(|state| &mut state.limits);
        store.set_fuel(FUEL_PER_CALL)?;
        Ok(store)
    }
}

struct BoundedStore<T> {
    limits: StoreLimits,
    data: T,
}
