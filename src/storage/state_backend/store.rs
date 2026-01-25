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

/// State store iterator
pub trait StateIterator: Send + Sync {
    /// Check if there is a next element
    ///
    /// # Returns
    /// - `Ok(true)`: has next element
    /// - `Ok(false)`: no more data
    /// - `Err(BackendError)`: check failed
    fn has_next(&mut self) -> Result<bool, BackendError>;

    /// Get the next key-value pair
    ///
    /// # Returns
    /// - `Ok(Some((key, value)))`: next key-value pair
    /// - `Ok(None)`: no more data
    /// - `Err(BackendError)`: iteration failed
    fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>, BackendError>;
}

/// State store interface
///
/// Provides complete state storage functionality, including:
/// - Simple KV operations (using byte arrays directly as keys)
/// - Complex KV operations (using complex keys)
/// - Iterator support
pub trait StateStore: Send + Sync {
    /// Store a key-value pair (simple key)
    ///
    /// # Arguments
    /// - `key`: key (byte array)
    /// - `value`: value (byte array)
    ///
    /// # Returns
    /// - `Ok(())`: store succeeded
    /// - `Err(BackendError)`: store failed
    fn put_state(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), BackendError>;

    /// Get value (simple key)
    ///
    /// # Arguments
    /// - `key`: key (byte array)
    ///
    /// # Returns
    /// - `Ok(Some(value))`: value found
    /// - `Ok(None)`: key does not exist
    /// - `Err(BackendError)`: get failed
    fn get_state(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, BackendError>;

    /// Delete a key-value pair (simple key)
    ///
    /// # Arguments
    /// - `key`: key (byte array)
    ///
    /// # Returns
    /// - `Ok(())`: delete succeeded
    /// - `Err(BackendError)`: delete failed
    fn delete_state(&self, key: Vec<u8>) -> Result<(), BackendError>;

    /// List all keys in the specified range
    ///
    /// # Arguments
    /// - `start_inclusive`: start key (inclusive)
    /// - `end_exclusive`: end key (exclusive)
    ///
    /// # Returns
    /// - `Ok(keys)`: list of keys
    /// - `Err(BackendError)`: list failed
    fn list_states(
        &self,
        start_inclusive: Vec<u8>,
        end_exclusive: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, BackendError>;

    /// Store a key-value pair (complex key)
    ///
    /// # Arguments
    /// - `key_group`: key group (byte array)
    /// - `key`: key (byte array)
    /// - `namespace`: namespace (byte array)
    /// - `user_key`: user key (byte array)
    /// - `value`: value (byte array)
    ///
    /// # Returns
    /// - `Ok(())`: store succeeded
    /// - `Err(BackendError)`: store failed
    fn put(
        &self,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
        user_key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), BackendError> {
        let key_bytes = crate::storage::state_backend::key_builder::build_key(
            &key_group, &key, &namespace, &user_key,
        );
        self.put_state(key_bytes, value)
    }

    /// Get value (complex key)
    ///
    /// # Arguments
    /// - `key_group`: key group (byte array)
    /// - `key`: key (byte array)
    /// - `namespace`: namespace (byte array)
    /// - `user_key`: user key (byte array)
    ///
    /// # Returns
    /// - `Ok(Some(value))`: value found
    /// - `Ok(None)`: key does not exist
    /// - `Err(BackendError)`: get failed
    fn get(
        &self,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
        user_key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let key_bytes = crate::storage::state_backend::key_builder::build_key(
            &key_group, &key, &namespace, &user_key,
        );
        self.get_state(key_bytes)
    }

    /// Delete a key-value pair (complex key)
    ///
    /// # Arguments
    /// - `key_group`: key group (byte array)
    /// - `key`: key (byte array)
    /// - `namespace`: namespace (byte array)
    /// - `user_key`: user key (byte array)
    ///
    /// # Returns
    /// - `Ok(())`: delete succeeded
    /// - `Err(BackendError)`: delete failed
    fn delete(
        &self,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
        user_key: Vec<u8>,
    ) -> Result<(), BackendError> {
        let key_bytes = crate::storage::state_backend::key_builder::build_key(
            &key_group, &key, &namespace, &user_key,
        );
        self.delete_state(key_bytes)
    }

    /// Merge value (complex key, using merge operation)
    ///
    /// # Arguments
    /// - `key_group`: key group (byte array)
    /// - `key`: key (byte array)
    /// - `namespace`: namespace (byte array)
    /// - `user_key`: user key (byte array)
    /// - `value`: value to merge (byte array)
    ///
    /// # Returns
    /// - `Ok(())`: merge succeeded
    /// - `Err(BackendError)`: merge failed
    fn merge(
        &self,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
        user_key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), BackendError>;

    /// Delete all keys with the specified prefix (complex key)
    ///
    /// # Arguments
    /// - `key_group`: key group (byte array)
    /// - `key`: key (byte array)
    /// - `namespace`: namespace (byte array)
    ///
    /// # Returns
    /// - `Ok(count)`: number of keys deleted
    /// - `Err(BackendError)`: delete failed
    fn delete_prefix(
        &self,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
    ) -> Result<usize, BackendError> {
        let prefix_bytes = crate::storage::state_backend::key_builder::build_key(
            &key_group,
            &key,
            &namespace,
            &[],
        );
        self.delete_prefix_bytes(prefix_bytes)
    }

    /// List all keys in the specified range (complex key)
    ///
    /// # Arguments
    /// - `key_group`: key group (byte array)
    /// - `key`: key (byte array)
    /// - `namespace`: namespace (byte array)
    /// - `start_inclusive`: start user_key (inclusive)
    /// - `end_exclusive`: end user_key (exclusive)
    ///
    /// # Returns
    /// - `Ok(keys)`: list of keys (returns complete complex key byte arrays)
    /// - `Err(BackendError)`: list failed
    fn list_complex(
        &self,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
        start_inclusive: Vec<u8>,
        end_exclusive: Vec<u8>,
    ) -> Result<Vec<Vec<u8>>, BackendError> {
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
        self.list_states(start_key, end_key)
    }

    /// Delete all keys with the specified prefix (byte array)
    ///
    /// # Arguments
    /// - `prefix`: key prefix (byte array)
    ///
    /// # Returns
    /// - `Ok(count)`: number of keys deleted
    /// - `Err(BackendError)`: delete failed
    fn delete_prefix_bytes(&self, prefix: Vec<u8>) -> Result<usize, BackendError>;

    /// Scan all key-value pairs with the specified prefix (simple key)
    ///
    /// # Arguments
    /// - `prefix`: key prefix (byte array)
    ///
    /// # Returns
    /// - `Ok(Box<dyn StateIterator>)`: iterator
    /// - `Err(BackendError)`: failed to create iterator
    fn scan(&self, prefix: Vec<u8>) -> Result<Box<dyn StateIterator>, BackendError>;

    /// Scan all key-value pairs with the specified prefix (complex key)
    ///
    /// # Arguments
    /// - `key_group`: key group (byte array)
    /// - `key`: key (byte array)
    /// - `namespace`: namespace (byte array)
    ///
    /// # Returns
    /// - `Ok(Box<dyn StateIterator>)`: iterator
    /// - `Err(BackendError)`: failed to create iterator
    fn scan_complex(
        &self,
        key_group: Vec<u8>,
        key: Vec<u8>,
        namespace: Vec<u8>,
    ) -> Result<Box<dyn StateIterator>, BackendError> {
        let prefix = crate::storage::state_backend::key_builder::build_key(
            &key_group,
            &key,
            &namespace,
            &[],
        );
        self.scan(prefix)
    }
}