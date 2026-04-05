// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataField {
    pub field_name: String,
    pub key: String,
    #[serde(default)]
    pub data_type: Option<String>,
}
