// BufferAndEvent module - Buffer and event module
//
// Provides BufferOrEvent implementation, unified representation of data received from network or message queue
// Can be a buffer containing data records, or an event

mod buffer_or_event;
pub mod event;
pub mod stream_element;

pub use buffer_or_event::BufferOrEvent;
// StreamRecord is now in the stream_element submodule, exported through stream_element
pub use stream_element::StreamRecord;

