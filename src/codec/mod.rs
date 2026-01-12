// Codec module - Codec utility module
//
// Provides basic serialization and deserialization utilities, including:
// - Variable-length integer encoding/decoding (VarInt)
// - ZigZag encoding/decoding
// - String and byte array encoding/decoding
// - Basic type encoding/decoding
// - Protocol Buffers codec

mod varint;
mod zigzag;
mod primitive;
mod string_codec;
mod protobuf;

pub use varint::*;
pub use zigzag::*;
pub use primitive::*;
pub use string_codec::*;
pub use protobuf::*;
