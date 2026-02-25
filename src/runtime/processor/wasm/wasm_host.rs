// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::runtime::buffer_and_event::BufferOrEvent;
use crate::runtime::output::Output;
use crate::runtime::processor::wasm::wasm_cache;
use crate::storage::state_backend::{StateStore, StateStoreFactory};
use std::sync::{Arc, OnceLock};
use wasmtime::component::{Component, HasData, Linker, Resource, bindgen};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

static GLOBAL_ENGINE: OnceLock<Arc<Engine>> = OnceLock::new();

fn enable_incremental_compilation(config: &mut Config) -> bool {
    if !wasm_cache::is_cache_enabled() {
        return false;
    }

    #[cfg(feature = "incremental-cache")]
    {
        if let Some(cache_store) = wasm_cache::create_cache_store() {
            let _ = config.enable_incremental_compilation(cache_store);
            return true;
        }
    }

    false
}

fn get_global_engine(_wasm_size: usize) -> anyhow::Result<Arc<Engine>> {
    if let Some(engine) = GLOBAL_ENGINE.get() {
        return Ok(Arc::clone(engine));
    }

    let engine = GLOBAL_ENGINE.get_or_init(|| {
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.async_support(false);
        config.cranelift_opt_level(wasmtime::OptLevel::Speed);
        config.debug_info(false);
        config.parallel_compilation(true);
        config.cranelift_debug_verifier(false);
        config.consume_fuel(false);
        config.epoch_interruption(false);
        config.compiler_inlining(true);
        enable_incremental_compilation(&mut config);

        let engine = Engine::new(&config).unwrap_or_else(|e| {
            panic!("Failed to create global wasm engine: {}", e);
        });

        Arc::new(engine)
    });

    Ok(Arc::clone(engine))
}

bindgen!({
    world: "processor",
    path: "wit",
    with: {
        "functionstream:core/kv.store": FunctionStreamStoreHandle,
        "functionstream:core/kv.iterator": FunctionStreamIteratorHandle,
    }
});

use functionstream::core::kv::{self, ComplexKey, Error, HostIterator, HostStore};

pub struct FunctionStreamStoreHandle {
    pub name: String,
    pub state_store: Box<dyn StateStore>,
}

impl Drop for FunctionStreamStoreHandle {
    fn drop(&mut self) {}
}

pub struct FunctionStreamIteratorHandle {
    pub state_iterator: Box<dyn crate::storage::state_backend::StateIterator>,
}

pub struct HostState {
    pub wasi: WasiCtx,
    pub table: ResourceTable,
    pub factory: Arc<dyn StateStoreFactory>,
    pub outputs: Vec<Box<dyn Output>>,
}

pub struct HostStateData;

impl HasData for HostStateData {
    type Data<'a> = &'a mut HostState;
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl kv::Host for HostState {}

impl HostStore for HostState {
    fn new(&mut self, name: String) -> Resource<FunctionStreamStoreHandle> {
        let state_store = self
            .factory
            .new_state_store(Some(name.clone()))
            .unwrap_or_else(|e| {
                panic!("Failed to create state store: {}", e);
            });

        let handle = FunctionStreamStoreHandle { name, state_store };
        self.table.push(handle).unwrap_or_else(|e| {
            panic!("Failed to push resource to table: {}", e);
        })
    }

    fn put_state(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        store
            .state_store
            .put_state(key, value)
            .map_err(|e| Error::Other(format!("Failed to put state: {}", e)))
    }

    fn get_state(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        store
            .state_store
            .get_state(key)
            .map_err(|e| Error::Other(format!("Failed to get state: {}", e)))
    }

    fn delete_state(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: Vec<u8>,
    ) -> Result<(), Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        store
            .state_store
            .delete_state(key)
            .map_err(|e| Error::Other(format!("Failed to delete state: {}", e)))
    }

    fn list_states(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        store
            .state_store
            .list_states(start, end)
            .map_err(|e| Error::Other(format!("Failed to list states: {}", e)))
    }

    fn put(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: ComplexKey,
        value: Vec<u8>,
    ) -> Result<(), Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        let real_key = crate::storage::state_backend::key_builder::build_key(
            &key.key_group,
            &key.key,
            &key.namespace,
            &key.user_key,
        );

        store
            .state_store
            .put_state(real_key, value)
            .map_err(|e| Error::Other(format!("Failed to put: {}", e)))
    }

    fn get(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: ComplexKey,
    ) -> Result<Option<Vec<u8>>, Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        let real_key = crate::storage::state_backend::key_builder::build_key(
            &key.key_group,
            &key.key,
            &key.namespace,
            &key.user_key,
        );

        store
            .state_store
            .get_state(real_key)
            .map_err(|e| Error::Other(format!("Failed to get: {}", e)))
    }

    fn delete(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: ComplexKey,
    ) -> Result<(), Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        let real_key = crate::storage::state_backend::key_builder::build_key(
            &key.key_group,
            &key.key,
            &key.namespace,
            &key.user_key,
        );

        store
            .state_store
            .delete_state(real_key)
            .map_err(|e| Error::Other(format!("Failed to delete: {}", e)))?;
        Ok(())
    }

    fn merge(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: ComplexKey,
        value: Vec<u8>,
    ) -> Result<(), Error> {
        self.put(self_, key, value)
    }

    fn delete_prefix(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key: ComplexKey,
    ) -> Result<(), Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        let prefix_key = crate::storage::state_backend::key_builder::build_key(
            &key.key_group,
            &key.key,
            &key.namespace,
            &[],
        );

        let keys_to_delete = store
            .state_store
            .list_states(prefix_key.clone(), {
                let mut end_prefix = prefix_key.clone();
                end_prefix.extend_from_slice(&vec![0xFF; 256]);
                end_prefix
            })
            .map_err(|e| Error::Other(format!("Failed to list keys for delete_prefix: {}", e)))?;

        for key_to_delete in keys_to_delete {
            store.state_store.delete_state(key_to_delete).map_err(|e| {
                Error::Other(format!("Failed to delete key in delete_prefix: {}", e))
            })?;
        }

        Ok(())
    }

    fn list_complex(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
        start_inclusive: Vec<u8>,
        end_exclusive: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        let start_key = crate::storage::state_backend::key_builder::build_key(
            &key_group,
            &key,
            &namespace,
            &start_inclusive,
        );
        let end_key = crate::storage::state_backend::key_builder::build_key(
            &key_group,
            &key,
            &namespace,
            &end_exclusive,
        );

        store
            .state_store
            .list_states(start_key, end_key)
            .map_err(|e| Error::Other(format!("Failed to list_complex: {}", e)))
    }

    fn scan_complex(
        &mut self,
        self_: Resource<FunctionStreamStoreHandle>,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
    ) -> Result<Resource<FunctionStreamIteratorHandle>, Error> {
        let store = self
            .table
            .get(&self_)
            .map_err(|e| Error::Other(format!("Failed to get store resource: {}", e)))?;

        let state_iterator = store
            .state_store
            .scan_complex(key_group, key, namespace)
            .map_err(|e| Error::Other(format!("Failed to scan_complex: {}", e)))?;

        let iter = FunctionStreamIteratorHandle { state_iterator };
        self.table
            .push(iter)
            .map_err(|e| Error::Other(format!("Failed to push iterator resource: {}", e)))
    }

    fn drop(&mut self, rep: Resource<FunctionStreamStoreHandle>) -> Result<(), anyhow::Error> {
        self.table
            .delete(rep)
            .map_err(|e| anyhow::anyhow!("Failed to delete store resource: {}", e))?;
        Ok(())
    }
}

impl HostIterator for HostState {
    fn has_next(&mut self, self_: Resource<FunctionStreamIteratorHandle>) -> Result<bool, Error> {
        let iter = self
            .table
            .get_mut(&self_)
            .map_err(|e| Error::Other(format!("Failed to get iterator resource: {}", e)))?;

        iter.state_iterator
            .has_next()
            .map_err(|e| Error::Other(format!("Failed to check has_next: {}", e)))
    }

    fn next(
        &mut self,
        self_: Resource<FunctionStreamIteratorHandle>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
        let iter = self
            .table
            .get_mut(&self_)
            .map_err(|e| Error::Other(format!("Failed to get iterator resource: {}", e)))?;

        iter.state_iterator
            .next()
            .map_err(|e| Error::Other(format!("Failed to get next: {}", e)))
    }

    fn drop(&mut self, rep: Resource<FunctionStreamIteratorHandle>) -> Result<(), anyhow::Error> {
        self.table
            .delete(rep)
            .map_err(|e| anyhow::anyhow!("Failed to delete iterator resource: {}", e))?;
        Ok(())
    }
}

impl functionstream::core::collector::Host for HostState {
    fn emit(&mut self, target_id: u32, data: Vec<u8>) {
        let output_count = self.outputs.len();
        let out = self.outputs.get_mut(target_id as usize).unwrap_or_else(|| {
            panic!("Invalid target_id: {target_id}, available outputs: {output_count}");
        });

        let buffer_or_event =
            BufferOrEvent::new_buffer(data, Some(format!("target_{}", target_id)), false, false);

        out.collect(buffer_or_event).unwrap_or_else(|e| {
            panic!("failed to collect output: {e}");
        });
    }

    fn emit_watermark(&mut self, target_id: u32, ts: u64) {
        let output_count = self.outputs.len();
        let out = self.outputs.get_mut(target_id as usize).unwrap_or_else(|| {
            panic!("Invalid target_id: {target_id}, available outputs: {output_count}");
        });

        let mut watermark_data = Vec::with_capacity(12);
        watermark_data.extend_from_slice(&target_id.to_le_bytes());
        watermark_data.extend_from_slice(&ts.to_le_bytes());

        let buffer_or_event = BufferOrEvent::new_buffer(
            watermark_data,
            Some(format!("watermark_target_{}", target_id)),
            false,
            false,
        );

        out.collect(buffer_or_event).unwrap_or_else(|e| {
            panic!("failed to collect watermark: {e}");
        });
    }
}

pub fn create_wasm_host_with_component(
    engine: &Engine,
    component: &Component,
    outputs: Vec<Box<dyn Output>>,
    init_context: &crate::runtime::taskexecutor::InitContext,
    task_name: String,
    create_time: u64,
) -> anyhow::Result<(Processor, Store<HostState>)> {
    let mut linker = Linker::new(engine);

    wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
        .map_err(|e| anyhow::anyhow!("Failed to add WASI to linker: {}", e))?;

    Processor::add_to_linker::<HostState, HostStateData>(&mut linker, |s| s)
        .map_err(|e| anyhow::anyhow!("Failed to add interfaces to linker: {}", e))?;

    let factory = init_context
        .state_storage_server
        .create_factory(task_name.clone(), create_time)
        .map_err(|e| anyhow::anyhow!("Failed to create state store factory: {}", e))?;

    let mut store = Store::new(
        engine,
        HostState {
            wasi: WasiCtxBuilder::new()
                .inherit_stdio()
                .inherit_env()
                .inherit_args()
                .build(),
            table: ResourceTable::new(),
            factory,
            outputs,
        },
    );

    let processor = Processor::instantiate(&mut store, component, &linker).map_err(|e| {
        let error_msg = format!("Failed to instantiate wasm component: {}", e);
        let mut detailed_msg = error_msg.clone();
        if let Some(source) = e.source() {
            detailed_msg.push_str(&format!(". Source: {}", source));
        }
        anyhow::anyhow!("{}", detailed_msg)
    })?;

    Ok((processor, store))
}

pub fn create_wasm_host(
    wasm_bytes: &[u8],
    outputs: Vec<Box<dyn Output>>,
    init_context: &crate::runtime::taskexecutor::InitContext,
    task_name: String,
    create_time: u64,
) -> anyhow::Result<(Processor, Store<HostState>)> {
    let engine = get_global_engine(wasm_bytes.len())?;

    let component = Component::from_binary(&engine, wasm_bytes)
        .map_err(|e| anyhow::anyhow!("Failed to parse WebAssembly component: {}", e))?;

    create_wasm_host_with_component(
        &engine,
        &component,
        outputs,
        init_context,
        task_name,
        create_time,
    )
}
