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


use crate::storage::state_backend::error::BackendError;
use crate::storage::state_backend::factory::StateStoreFactory;
use super::store::MemoryStateStore;
use std::sync::{Arc, Mutex};

pub struct MemoryStateStoreFactory {

}

impl MemoryStateStoreFactory {
    pub fn new() -> Self {
        Self {}
    }

    pub fn default_factory() -> Arc<dyn StateStoreFactory> {
        static FACTORY: Mutex<Option<Arc<MemoryStateStoreFactory>>> = Mutex::new(None);

        let mut factory = FACTORY.lock().unwrap();
        if factory.is_none() {
            *factory = Some(Arc::new(MemoryStateStoreFactory::new()));
        }
        factory.as_ref().unwrap().clone()
    }
}

impl Default for MemoryStateStoreFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl StateStoreFactory for MemoryStateStoreFactory {
    fn new_state_store(
        &self,
        _column_family: Option<String>,
    ) -> Result<Box<dyn crate::storage::state_backend::store::StateStore>, BackendError> {
        Ok(Box::new(MemoryStateStore::new()))
    }
}
