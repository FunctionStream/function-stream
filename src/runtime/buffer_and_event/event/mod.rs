// Event module - Event module
//
// Provides event system implementation, including:
// - Event base classes and interfaces
// - Runtime events (RuntimeEvent)
// - Event serialization and deserialization

mod event;
mod event_serializer;

mod end_of_data;

pub use event::*;
pub use event_serializer::*;

// Export RuntimeEvent concrete types
pub use end_of_data::{EndOfData, StopMode};

