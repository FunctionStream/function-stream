// Codec module - Codec utility module
//
// Provides basic serialization and deserialization utilities, including:
// - Variable-length integer encoding/decoding (VarInt)
// - ZigZag encoding/decoding
// - String and byte array encoding/decoding
// - Basic type encoding/decoding
// - Protocol Buffers codec

mod primitive;
mod protobuf;
mod string_codec;
mod varint;
mod zigzag;

pub use protobuf::*;
pub use string_codec::*;
pub use varint::*;
