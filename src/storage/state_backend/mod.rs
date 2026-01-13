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

// State Backend module
//
// 状态存储后端模块，提供状态存储后端的抽象接口和实现

/// 状态存储目录名称
pub const STATE_DIR_NAME: &str = "state";

pub mod error;
pub mod factory;
pub mod key_builder;
pub mod memory_factory;
pub mod memory_store;
pub mod rocksdb_factory;
pub mod rocksdb_store;
pub mod server;
pub mod store;

pub use factory::StateStoreFactory;
pub use server::StateStorageServer;
pub use store::{StateIterator, StateStore};
