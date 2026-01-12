// YAML Configuration Keys - YAML configuration key name constants
//
// Defines all key name constants used in YAML configuration files

/// Configuration type key name
///
/// Used to specify configuration type, supported values:
/// - "processor": Processor type configuration
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

/// Configuration type value constants
pub mod type_values {
    /// Processor configuration type value
    pub const PROCESSOR: &str = "processor";

    /// Source configuration type value (future support)
    pub const SOURCE: &str = "source";

    /// Sink configuration type value (future support)
    pub const SINK: &str = "sink";
}
