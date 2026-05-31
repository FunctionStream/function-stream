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

//! Shared source batching defaults (Kafka, MQTT, HTTP, ROS, …).

use std::time::Duration;

/// Rows buffered before emitting a `RecordBatch`.
pub const DEFAULT_SOURCE_BATCH_SIZE: usize = 4096;

/// Max wait when the buffer is non-empty but below `DEFAULT_SOURCE_BATCH_SIZE`.
pub const MAX_BATCH_LINGER_TIME: Duration = Duration::from_millis(20);

/// Poll/read timeout in the hot loop; keep small to avoid idle sleeps between messages.
pub const SOURCE_POLL_TIMEOUT: Duration = Duration::from_millis(10);
