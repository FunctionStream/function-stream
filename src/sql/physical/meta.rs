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

use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::{DataType, Field, Fields};

use crate::sql::common::UPDATING_META_FIELD;
use crate::sql::common::constants::updating_state_field;

pub fn updating_meta_fields() -> Fields {
    static FIELDS: OnceLock<Fields> = OnceLock::new();
    FIELDS
        .get_or_init(|| {
            Fields::from(vec![
                Field::new(updating_state_field::IS_RETRACT, DataType::Boolean, true),
                Field::new(
                    updating_state_field::ID,
                    DataType::FixedSizeBinary(16),
                    true,
                ),
            ])
        })
        .clone()
}

pub fn updating_meta_field() -> Arc<Field> {
    static FIELD: OnceLock<Arc<Field>> = OnceLock::new();
    FIELD
        .get_or_init(|| {
            Arc::new(Field::new(
                UPDATING_META_FIELD,
                DataType::Struct(updating_meta_fields()),
                false,
            ))
        })
        .clone()
}
