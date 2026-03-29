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

use std::time::{SystemTime, UNIX_EPOCH};

const ID_LENGTH: usize = 10;

const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

pub enum IdTypes {
    ApiKey,
    ConnectionProfile,
    Schema,
    Pipeline,
    JobConfig,
    Checkpoint,
    JobStatus,
    ClusterInfo,
    JobLogMessage,
    ConnectionTable,
    ConnectionTablePipeline,
    Udf,
}

/// Generates a unique identifier with a type-specific prefix.
///
/// Uses a simple time + random approach instead of nanoid to avoid an extra dependency.
pub fn generate_id(id_type: IdTypes) -> String {
    let prefix = match id_type {
        IdTypes::ApiKey => "ak",
        IdTypes::ConnectionProfile => "cp",
        IdTypes::Schema => "sch",
        IdTypes::Pipeline => "pl",
        IdTypes::JobConfig => "job",
        IdTypes::Checkpoint => "chk",
        IdTypes::JobStatus => "js",
        IdTypes::ClusterInfo => "ci",
        IdTypes::JobLogMessage => "jlm",
        IdTypes::ConnectionTable => "ct",
        IdTypes::ConnectionTablePipeline => "ctp",
        IdTypes::Udf => "udf",
    };

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();

    let mut id = String::with_capacity(ID_LENGTH);
    let mut seed = nanos;
    for _ in 0..ID_LENGTH {
        seed ^= seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let idx = (seed % ALPHABET.len() as u128) as usize;
        id.push(ALPHABET[idx] as char);
    }

    format!("{prefix}_{id}")
}
