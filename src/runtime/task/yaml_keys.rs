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

// YAML Configuration Keys - YAML configuration key name constants
//
// Defines all key name constants used in YAML configuration files

/// Configuration type key name
///
/// Used to specify configuration type, supported values:
/// - "processor": Processor type configuration
/// - "python": python runtime configuration
/// - "source": Source type configuration (future support)
/// - "sink": Sink type configuration (future support)
pub const TYPE: &str = "type";

/// Task name key name
///
/// Used to specify task name
pub const NAME: &str = "name";

/// Input groups key name
///
/// Used to specify input group configuration list
pub const INPUT_GROUPS: &str = "input-groups";

/// Input source key name (backward compatible)
///
/// Used to specify input source configuration list (backward compatible, equivalent to input-groups)
pub const INPUTS: &str = "inputs";

/// Output key name
///
/// Used to specify output sink configuration list
pub const OUTPUTS: &str = "outputs";

/// Input selector key name
///
/// Used to specify the strategy for selecting which input to read next when multiple inputs are available.
/// Supported values: "round-robin", "sequential", "priority", "group-parallel"
pub const INPUT_SELECTOR: &str = "input-selector";

/// Configuration type value constants
pub mod type_values {
    /// Processor configuration type value
    pub const PROCESSOR: &str = "processor";

    /// python runtime configuration type value
    pub const PYTHON: &str = "python";

    /// Source configuration type value (future support)
    pub const SOURCE: &str = "source";

    /// Sink configuration type value (future support)
    pub const SINK: &str = "sink";
}
