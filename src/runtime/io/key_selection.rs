// KeySelection - Key selection interface
//
// Defines Key selection interfaces for multi-input and single-input

use std::hash::Hash;

/// KeySelection - Key selection interface
/// 
/// Used for selecting and processing Keys in stream processing
pub trait KeySelection<K>: Send + Sync
where
    K: Hash + Eq + Send + Sync + Clone,
{
    /// Get currently selected Key
    fn get_selected_key(&self) -> Option<K>;

    /// Select next Key
    fn select_next_key(&mut self) -> Result<Option<K>, Box<dyn std::error::Error + Send>>;

    /// Check if Key is available
    fn is_key_available(&self, key: &K) -> bool;

    /// Get all available Keys
    fn get_available_keys(&self) -> Vec<K>;
}

/// MultipleInputKeySelection - Multiple input Key selection
/// 
/// Used for Key selection in multi-input scenarios
pub trait MultipleInputKeySelection<K>: KeySelection<K>
where
    K: Hash + Eq + Send + Sync + Clone,
{
    /// Get Key for each input
    fn get_key_for_input(&self, input_index: usize) -> Option<K>;

    /// Select input index by Key
    fn select_input_by_key(&self, key: &K) -> Option<usize>;

    /// Get Key mapping for all inputs
    fn get_input_key_mapping(&self) -> Vec<(usize, K)>;
}

/// SingleInputKeySelection - Single input Key selection
/// 
/// Used for Key selection in single-input scenarios
pub trait SingleInputKeySelection<K>: KeySelection<K>
where
    K: Hash + Eq + Send + Sync + Clone,
{
    /// Get all Keys for current input
    fn get_all_keys(&self) -> Vec<K>;

    /// Get data by Key
    fn get_data_by_key(&self, key: &K) -> Option<Vec<u8>>;
}

/// Simple multiple input Key selection implementation
pub struct SimpleMultipleInputKeySelection<K>
where
    K: Hash + Eq + Send + Sync + Clone,
{
    /// Mapping from input index to Key
    input_key_mapping: Vec<(usize, K)>,
    /// Currently selected Key
    current_key: Option<K>,
    /// Currently selected input index
    current_input_index: Option<usize>,
}

impl<K> SimpleMultipleInputKeySelection<K>
where
    K: Hash + Eq + Send + Sync + Clone,
{
    /// Create new multiple input Key selector
    pub fn new(input_key_mapping: Vec<(usize, K)>) -> Self {
        Self {
            input_key_mapping,
            current_key: None,
            current_input_index: None,
        }
    }
}

impl<K> KeySelection<K> for SimpleMultipleInputKeySelection<K>
where
    K: Hash + Eq + Send + Sync + Clone,
{
    fn get_selected_key(&self) -> Option<K> {
        self.current_key.clone()
    }

    fn select_next_key(&mut self) -> Result<Option<K>, Box<dyn std::error::Error + Send>> {
        // Simple round-robin selection
        if self.input_key_mapping.is_empty() {
            return Ok(None);
        }

        let next_index = if let Some(ref current) = self.current_input_index {
            (*current + 1) % self.input_key_mapping.len()
        } else {
            0
        };

        let (_input_index, key) = &self.input_key_mapping[next_index];
        self.current_input_index = Some(next_index);
        self.current_key = Some(key.clone());

        Ok(self.current_key.clone())
    }

    fn is_key_available(&self, key: &K) -> bool {
        self.input_key_mapping.iter().any(|(_, k)| k == key)
    }

    fn get_available_keys(&self) -> Vec<K> {
        self.input_key_mapping.iter().map(|(_, k)| k.clone()).collect()
    }
}

impl<K> MultipleInputKeySelection<K> for SimpleMultipleInputKeySelection<K>
where
    K: Hash + Eq + Send + Sync + Clone,
{
    fn get_key_for_input(&self, input_index: usize) -> Option<K> {
        self.input_key_mapping
            .iter()
            .find(|(idx, _)| *idx == input_index)
            .map(|(_, k)| k.clone())
    }

    fn select_input_by_key(&self, key: &K) -> Option<usize> {
        self.input_key_mapping
            .iter()
            .find(|(_, k)| k == key)
            .map(|(idx, _)| *idx)
    }

    fn get_input_key_mapping(&self) -> Vec<(usize, K)> {
        self.input_key_mapping.clone()
    }
}

/// Simple single input Key selection implementation
pub struct SimpleSingleInputKeySelection<K>
where
    K: Hash + Eq + Send + Sync + Clone,
{
    /// All available Keys
    available_keys: Vec<K>,
    /// Currently selected Key
    current_key: Option<K>,
    /// Currently selected Key index
    current_key_index: Option<usize>,
}

impl<K> SimpleSingleInputKeySelection<K>
where
    K: Hash + Eq + Send + Sync + Clone,
{
    /// Create new single input Key selector
    pub fn new(available_keys: Vec<K>) -> Self {
        Self {
            available_keys,
            current_key: None,
            current_key_index: None,
        }
    }
}

impl<K> KeySelection<K> for SimpleSingleInputKeySelection<K>
where
    K: Hash + Eq + Send + Sync + Clone,
{
    fn get_selected_key(&self) -> Option<K> {
        self.current_key.clone()
    }

    fn select_next_key(&mut self) -> Result<Option<K>, Box<dyn std::error::Error + Send>> {
        if self.available_keys.is_empty() {
            return Ok(None);
        }

        let next_index = if let Some(ref current) = self.current_key_index {
            (*current + 1) % self.available_keys.len()
        } else {
            0
        };

        self.current_key_index = Some(next_index);
        self.current_key = Some(self.available_keys[next_index].clone());

        Ok(self.current_key.clone())
    }

    fn is_key_available(&self, key: &K) -> bool {
        self.available_keys.contains(key)
    }

    fn get_available_keys(&self) -> Vec<K> {
        self.available_keys.clone()
    }
}

impl<K> SingleInputKeySelection<K> for SimpleSingleInputKeySelection<K>
where
    K: Hash + Eq + Send + Sync + Clone,
{
    fn get_all_keys(&self) -> Vec<K> {
        self.available_keys.clone()
    }

    fn get_data_by_key(&self, _key: &K) -> Option<Vec<u8>> {
        // Placeholder implementation, should fetch from data source in practice
        None
    }
}

