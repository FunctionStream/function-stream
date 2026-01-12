// Protocol Buffers - Protocol Buffers codec
//
// Implements Protocol Buffers codec, including:
// - Tag encoding/decoding (field number + wire type)
// - Encoding/decoding of various field types

use super::primitive::*;
use super::string_codec::*;
use super::varint::*;
use super::zigzag::*;

/// Wire Type - Protocol Buffers wire type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WireType {
    /// Varint encoding (int32, int64, uint32, uint64, sint32, sint64, bool, enum)
    Varint = 0,
    /// 64-bit fixed length (fixed64, sfixed64, double)
    Fixed64 = 1,
    /// Length-delimited (string, bytes, embedded messages, packed repeated fields)
    LengthDelimited = 2,
    /// Start group (deprecated)
    StartGroup = 3,
    /// End group (deprecated)
    EndGroup = 4,
    /// 32-bit fixed length (fixed32, sfixed32, float)
    Fixed32 = 5,
}

impl WireType {
    /// Create WireType from u8
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(WireType::Varint),
            1 => Some(WireType::Fixed64),
            2 => Some(WireType::LengthDelimited),
            3 => Some(WireType::StartGroup),
            4 => Some(WireType::EndGroup),
            5 => Some(WireType::Fixed32),
            _ => None,
        }
    }

    /// Convert to u8
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Encode Tag (field number + wire type)
///
/// Tag = (field_number << 3) | wire_type
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_tag(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    wire_type: WireType,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag = (field_number << 3) | (wire_type.to_u8() as u32);
    encode_var_uint32(buffer, offset, tag)
}

/// Decode Tag
///
/// Decodes from the specified position in the byte array, returns (field_number, wire_type, bytes consumed)
pub fn decode_tag(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, WireType, usize), Box<dyn std::error::Error + Send>> {
    let (tag, consumed) = decode_var_uint32(bytes, offset)?;
    let field_number = tag >> 3;
    let wire_type_value = (tag & 0x07) as u8;
    let wire_type = WireType::from_u8(wire_type_value).ok_or_else(|| {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid wire type: {}", wire_type_value),
        )) as Box<dyn std::error::Error + Send>
    })?;
    Ok((field_number, wire_type, consumed))
}

/// Compute the size after encoding Tag
pub fn compute_tag_size(field_number: u32) -> usize {
    let tag = field_number << 3; // wire_type is 0 (Varint)
    compute_var_uint32_size(tag)
}

/// Compute the size after encoding uint64 field (including tag)
pub fn compute_uint64_field_size(field_number: u32, value: u64) -> usize {
    compute_tag_size(field_number) + compute_var_uint64_size(value)
}

/// Compute the size after encoding uint32 field (including tag)
pub fn compute_uint32_field_size(field_number: u32, value: u32) -> usize {
    compute_tag_size(field_number) + compute_var_uint32_size(value)
}

/// Compute the size after encoding sint32 field (including tag)
pub fn compute_sint32_field_size(field_number: u32, value: i32) -> usize {
    compute_tag_size(field_number) + compute_var_uint32_size(encode_zigzag32(value))
}

/// Compute the size after encoding bool field (including tag)
pub fn compute_bool_field_size(field_number: u32) -> usize {
    compute_tag_size(field_number) + 1
}

/// Compute the size after encoding bytes field (including tag)
pub fn compute_bytes_field_size(field_number: u32, length: usize) -> usize {
    compute_tag_size(field_number) + compute_var_int32_size(length as i32) + length
}

// ========== Varint type fields ==========

/// Encode int32 field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_int32_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    value: i32,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::Varint)?;
    let value_written = encode_var_int32(buffer, offset + tag_written, value)?;
    Ok(tag_written + value_written)
}

/// Decode int32 field
pub fn decode_int32_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, i32, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::Varint {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected Varint wire type, got {:?}", wire_type),
        )));
    }
    let (value, value_consumed) = decode_var_int32(bytes, offset + tag_consumed)?;
    Ok((field_number, value, tag_consumed + value_consumed))
}

/// Encode int64 field
/// Encode int64 field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_int64_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    value: i64,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::Varint)?;
    let value_written = encode_var_int64(buffer, offset + tag_written, value)?;
    Ok(tag_written + value_written)
}

/// Decode int64 field
pub fn decode_int64_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, i64, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::Varint {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected Varint wire type, got {:?}", wire_type),
        )));
    }
    let (value, value_consumed) = decode_var_int64(bytes, offset + tag_consumed)?;
    Ok((field_number, value, tag_consumed + value_consumed))
}

/// Encode uint32 field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_uint32_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    value: u32,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::Varint)?;
    let value_written = encode_var_uint32(buffer, offset + tag_written, value)?;
    Ok(tag_written + value_written)
}

/// Decode uint32 field
pub fn decode_uint32_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, u32, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::Varint {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected Varint wire type, got {:?}", wire_type),
        )));
    }
    let (value, value_consumed) = decode_var_uint32(bytes, offset + tag_consumed)?;
    Ok((field_number, value, tag_consumed + value_consumed))
}

/// Encode uint64 field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_uint64_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    value: u64,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::Varint)?;
    // uint64 uses VarInt encoding, need to convert u64 to i64 for encoding
    let value_written = encode_var_int64(buffer, offset + tag_written, value as i64)?;
    Ok(tag_written + value_written)
}

/// Decode uint64 field
pub fn decode_uint64_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, u64, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::Varint {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected Varint wire type, got {:?}", wire_type),
        )));
    }
    let (value, value_consumed) = decode_var_int64(bytes, offset + tag_consumed)?;
    Ok((field_number, value as u64, tag_consumed + value_consumed))
}

/// Encode sint32 field (using ZigZag encoding)
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_sint32_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    value: i32,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::Varint)?;
    let zigzag = encode_zigzag32(value);
    let value_written = encode_var_uint32(buffer, offset + tag_written, zigzag)?;
    Ok(tag_written + value_written)
}

/// Decode sint32 field (using ZigZag decoding)
pub fn decode_sint32_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, i32, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::Varint {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected Varint wire type, got {:?}", wire_type),
        )));
    }
    let (zigzag, value_consumed) = decode_var_uint32(bytes, offset + tag_consumed)?;
    let value = decode_zigzag32(zigzag);
    Ok((field_number, value, tag_consumed + value_consumed))
}

/// Encode sint64 field (using ZigZag encoding)
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_sint64_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    value: i64,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::Varint)?;
    let zigzag = encode_zigzag64(value);
    // Convert u64 to VarInt encoding (using VarInt64)
    let value_written = encode_var_int64(buffer, offset + tag_written, zigzag as i64)?;
    Ok(tag_written + value_written)
}

/// Decode sint64 field (using ZigZag decoding)
pub fn decode_sint64_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, i64, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::Varint {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected Varint wire type, got {:?}", wire_type),
        )));
    }
    let (zigzag, value_consumed) = decode_var_int64(bytes, offset + tag_consumed)?;
    let value = decode_zigzag64(zigzag as u64);
    Ok((field_number, value, tag_consumed + value_consumed))
}

/// Encode bool field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_bool_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    value: bool,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::Varint)?;
    let value_written = encode_boolean(buffer, offset + tag_written, value)?;
    Ok(tag_written + value_written)
}

/// Decode bool field
pub fn decode_bool_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, bool, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::Varint {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected Varint wire type, got {:?}", wire_type),
        )));
    }
    let (value, value_consumed) = decode_boolean(bytes, offset + tag_consumed)?;
    Ok((field_number, value, tag_consumed + value_consumed))
}

// ========== Fixed type fields ==========

/// Encode fixed32 field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_fixed32_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    value: u32,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::Fixed32)?;
    let value_written = encode_u32(buffer, offset + tag_written, value)?;
    Ok(tag_written + value_written)
}

/// Decode fixed32 field
pub fn decode_fixed32_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, u32, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::Fixed32 {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected Fixed32 wire type, got {:?}", wire_type),
        )));
    }
    let (value, value_consumed) = decode_u32(bytes, offset + tag_consumed)?;
    Ok((field_number, value, tag_consumed + value_consumed))
}

/// Encode fixed64 field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_fixed64_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    value: u64,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::Fixed64)?;
    let value_written = encode_u64(buffer, offset + tag_written, value)?;
    Ok(tag_written + value_written)
}

/// Decode fixed64 field
pub fn decode_fixed64_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, u64, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::Fixed64 {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected Fixed64 wire type, got {:?}", wire_type),
        )));
    }
    let (value, value_consumed) = decode_u64(bytes, offset + tag_consumed)?;
    Ok((field_number, value, tag_consumed + value_consumed))
}

/// Encode sfixed32 field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_sfixed32_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    value: i32,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::Fixed32)?;
    let value_written = encode_i32(buffer, offset + tag_written, value)?;
    Ok(tag_written + value_written)
}

/// Decode sfixed32 field
pub fn decode_sfixed32_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, i32, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::Fixed32 {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected Fixed32 wire type, got {:?}", wire_type),
        )));
    }
    let (value, value_consumed) = decode_i32(bytes, offset + tag_consumed)?;
    Ok((field_number, value, tag_consumed + value_consumed))
}

/// Encode sfixed64 field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_sfixed64_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    value: i64,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::Fixed64)?;
    let value_written = encode_i64(buffer, offset + tag_written, value)?;
    Ok(tag_written + value_written)
}

/// Decode sfixed64 field
pub fn decode_sfixed64_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, i64, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::Fixed64 {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected Fixed64 wire type, got {:?}", wire_type),
        )));
    }
    let (value, value_consumed) = decode_i64(bytes, offset + tag_consumed)?;
    Ok((field_number, value, tag_consumed + value_consumed))
}

/// Encode float field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_float_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    value: f32,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::Fixed32)?;
    let value_written = encode_f32(buffer, offset + tag_written, value)?;
    Ok(tag_written + value_written)
}

/// Decode float field
pub fn decode_float_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, f32, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::Fixed32 {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected Fixed32 wire type, got {:?}", wire_type),
        )));
    }
    let (value, value_consumed) = decode_f32(bytes, offset + tag_consumed)?;
    Ok((field_number, value, tag_consumed + value_consumed))
}

/// Encode double field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_double_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    value: f64,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::Fixed64)?;
    let value_written = encode_f64(buffer, offset + tag_written, value)?;
    Ok(tag_written + value_written)
}

/// Decode double field
pub fn decode_double_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, f64, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::Fixed64 {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected Fixed64 wire type, got {:?}", wire_type),
        )));
    }
    let (value, value_consumed) = decode_f64(bytes, offset + tag_consumed)?;
    Ok((field_number, value, tag_consumed + value_consumed))
}

// ========== Length-Delimited type fields ==========

/// Encode string field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_string_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    value: &str,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::LengthDelimited)?;
    let string_written = encode_string(buffer, offset + tag_written, value)?;
    Ok(tag_written + string_written)
}

/// Decode string field
pub fn decode_string_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, String, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::LengthDelimited {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected LengthDelimited wire type, got {:?}", wire_type),
        )));
    }
    let (value, value_consumed) = decode_string(bytes, offset + tag_consumed)?;
    Ok((field_number, value, tag_consumed + value_consumed))
}

/// Encode bytes field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_bytes_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    value: &[u8],
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::LengthDelimited)?;
    let bytes_written = encode_byte_string(buffer, offset + tag_written, value)?;
    Ok(tag_written + bytes_written)
}

/// Decode bytes field
pub fn decode_bytes_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, Vec<u8>, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::LengthDelimited {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected LengthDelimited wire type, got {:?}", wire_type),
        )));
    }
    let (value, value_consumed) = decode_byte_string(bytes, offset + tag_consumed)?;
    Ok((field_number, value, tag_consumed + value_consumed))
}

// ========== Repeated/Packed type fields ==========

/// Encode packed repeated int32 field
/// Encode packed repeated int32 field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_packed_int32_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    values: &[i32],
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    // Encode tag first
    let tag_written = encode_tag(buffer, offset, field_number, WireType::LengthDelimited)?;
    let mut pos = offset + tag_written;

    // Encode all values to temporary buffer first, compute total length
    // Estimate capacity: at most 5 bytes per value
    let mut temp_buffer = vec![0u8; values.len() * 5];
    let mut packed_size = 0;
    for value in values {
        let written = encode_var_int32(&mut temp_buffer[packed_size..], 0, *value)?;
        packed_size += written;
    }

    // Encode length
    let length_written = encode_var_int32(buffer, pos, packed_size as i32)?;
    pos += length_written;

    // Check buffer space
    if pos + packed_size > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for packed int32 field encoding",
        )));
    }

    // Copy encoded values
    buffer[pos..pos + packed_size].copy_from_slice(&temp_buffer[..packed_size]);
    pos += packed_size;

    Ok(pos - offset)
}

/// Decode packed repeated int32 field
pub fn decode_packed_int32_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, Vec<i32>, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::LengthDelimited {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected LengthDelimited wire type, got {:?}", wire_type),
        )));
    }
    let (length, length_consumed) = decode_var_int32(bytes, offset + tag_consumed)?;
    let length = length as usize;
    let data_start = offset + tag_consumed + length_consumed;
    let data_end = data_start + length;

    if data_end > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete packed int32 field",
        )));
    }

    let mut values = Vec::new();
    let mut pos = data_start;
    while pos < data_end {
        let (value, consumed) = decode_var_int32(bytes, pos)?;
        values.push(value);
        pos += consumed;
    }

    Ok((
        field_number,
        values,
        tag_consumed + length_consumed + length,
    ))
}

/// Encode packed repeated int64 field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_packed_int64_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    values: &[i64],
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::LengthDelimited)?;
    let mut pos = offset + tag_written;

    let mut temp_buffer = vec![0u8; values.len() * 10];
    let mut packed_size = 0;
    for value in values {
        let written = encode_var_int64(&mut temp_buffer[packed_size..], 0, *value)?;
        packed_size += written;
    }

    let length_written = encode_var_int32(buffer, pos, packed_size as i32)?;
    pos += length_written;

    if pos + packed_size > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for packed int64 field encoding",
        )));
    }

    buffer[pos..pos + packed_size].copy_from_slice(&temp_buffer[..packed_size]);
    pos += packed_size;

    Ok(pos - offset)
}

/// Decode packed repeated int64 field
pub fn decode_packed_int64_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, Vec<i64>, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::LengthDelimited {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected LengthDelimited wire type, got {:?}", wire_type),
        )));
    }
    let (length, length_consumed) = decode_var_int32(bytes, offset + tag_consumed)?;
    let length = length as usize;
    let data_start = offset + tag_consumed + length_consumed;
    let data_end = data_start + length;

    if data_end > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete packed int64 field",
        )));
    }

    let mut values = Vec::new();
    let mut pos = data_start;
    while pos < data_end {
        let (value, consumed) = decode_var_int64(bytes, pos)?;
        values.push(value);
        pos += consumed;
    }

    Ok((
        field_number,
        values,
        tag_consumed + length_consumed + length,
    ))
}

/// Encode packed repeated uint32 field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_packed_uint32_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    values: &[u32],
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::LengthDelimited)?;
    let mut pos = offset + tag_written;

    let mut temp_buffer = vec![0u8; values.len() * 5];
    let mut packed_size = 0;
    for value in values {
        let written = encode_var_uint32(&mut temp_buffer[packed_size..], 0, *value)?;
        packed_size += written;
    }

    let length_written = encode_var_int32(buffer, pos, packed_size as i32)?;
    pos += length_written;

    if pos + packed_size > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for packed uint32 field encoding",
        )));
    }

    buffer[pos..pos + packed_size].copy_from_slice(&temp_buffer[..packed_size]);
    pos += packed_size;

    Ok(pos - offset)
}

/// Decode packed repeated uint32 field
pub fn decode_packed_uint32_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, Vec<u32>, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::LengthDelimited {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected LengthDelimited wire type, got {:?}", wire_type),
        )));
    }
    let (length, length_consumed) = decode_var_int32(bytes, offset + tag_consumed)?;
    let length = length as usize;
    let data_start = offset + tag_consumed + length_consumed;
    let data_end = data_start + length;

    if data_end > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete packed uint32 field",
        )));
    }

    let mut values = Vec::new();
    let mut pos = data_start;
    while pos < data_end {
        let (value, consumed) = decode_var_uint32(bytes, pos)?;
        values.push(value);
        pos += consumed;
    }

    Ok((
        field_number,
        values,
        tag_consumed + length_consumed + length,
    ))
}

/// Encode packed repeated sint32 field (using ZigZag)
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_packed_sint32_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    values: &[i32],
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::LengthDelimited)?;
    let mut pos = offset + tag_written;

    let mut temp_buffer = vec![0u8; values.len() * 5];
    let mut packed_size = 0;
    for value in values {
        let zigzag = encode_zigzag32(*value);
        let written = encode_var_uint32(&mut temp_buffer[packed_size..], 0, zigzag)?;
        packed_size += written;
    }

    let length_written = encode_var_int32(buffer, pos, packed_size as i32)?;
    pos += length_written;

    if pos + packed_size > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for packed sint32 field encoding",
        )));
    }

    buffer[pos..pos + packed_size].copy_from_slice(&temp_buffer[..packed_size]);
    pos += packed_size;

    Ok(pos - offset)
}

/// Decode packed repeated sint32 field (using ZigZag)
pub fn decode_packed_sint32_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, Vec<i32>, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::LengthDelimited {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected LengthDelimited wire type, got {:?}", wire_type),
        )));
    }
    let (length, length_consumed) = decode_var_int32(bytes, offset + tag_consumed)?;
    let length = length as usize;
    let data_start = offset + tag_consumed + length_consumed;
    let data_end = data_start + length;

    if data_end > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete packed sint32 field",
        )));
    }

    let mut values = Vec::new();
    let mut pos = data_start;
    while pos < data_end {
        let (zigzag, consumed) = decode_var_uint32(bytes, pos)?;
        values.push(decode_zigzag32(zigzag));
        pos += consumed;
    }

    Ok((
        field_number,
        values,
        tag_consumed + length_consumed + length,
    ))
}

/// Encode packed repeated fixed32 field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_packed_fixed32_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    values: &[u32],
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::LengthDelimited)?;
    let mut pos = offset + tag_written;

    let packed_size = values.len() * 4;
    let length_written = encode_var_int32(buffer, pos, packed_size as i32)?;
    pos += length_written;

    if pos + packed_size > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for packed fixed32 field encoding",
        )));
    }

    for value in values {
        encode_u32(buffer, pos, *value)?;
        pos += 4;
    }

    Ok(pos - offset)
}

/// Decode packed repeated fixed32 field
pub fn decode_packed_fixed32_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, Vec<u32>, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::LengthDelimited {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected LengthDelimited wire type, got {:?}", wire_type),
        )));
    }
    let (length, length_consumed) = decode_var_int32(bytes, offset + tag_consumed)?;
    let length = length as usize;
    let data_start = offset + tag_consumed + length_consumed;
    let data_end = data_start + length;

    if data_end > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete packed fixed32 field",
        )));
    }

    let mut values = Vec::new();
    let mut pos = data_start;
    while pos < data_end {
        let (value, consumed) = decode_u32(bytes, pos)?;
        values.push(value);
        pos += consumed;
    }

    Ok((
        field_number,
        values,
        tag_consumed + length_consumed + length,
    ))
}

/// Encode packed repeated fixed64 field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_packed_fixed64_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    values: &[u64],
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::LengthDelimited)?;
    let mut pos = offset + tag_written;

    let packed_size = values.len() * 8;
    let length_written = encode_var_int32(buffer, pos, packed_size as i32)?;
    pos += length_written;

    if pos + packed_size > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for packed fixed64 field encoding",
        )));
    }

    for value in values {
        encode_u64(buffer, pos, *value)?;
        pos += 8;
    }

    Ok(pos - offset)
}

/// Decode packed repeated fixed64 field
pub fn decode_packed_fixed64_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, Vec<u64>, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::LengthDelimited {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected LengthDelimited wire type, got {:?}", wire_type),
        )));
    }
    let (length, length_consumed) = decode_var_int32(bytes, offset + tag_consumed)?;
    let length = length as usize;
    let data_start = offset + tag_consumed + length_consumed;
    let data_end = data_start + length;

    if data_end > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete packed fixed64 field",
        )));
    }

    let mut values = Vec::new();
    let mut pos = data_start;
    while pos < data_end {
        let (value, consumed) = decode_u64(bytes, pos)?;
        values.push(value);
        pos += consumed;
    }

    Ok((
        field_number,
        values,
        tag_consumed + length_consumed + length,
    ))
}

/// Encode packed repeated float field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_packed_float_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    values: &[f32],
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::LengthDelimited)?;
    let mut pos = offset + tag_written;

    let packed_size = values.len() * 4;
    let length_written = encode_var_int32(buffer, pos, packed_size as i32)?;
    pos += length_written;

    if pos + packed_size > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for packed float field encoding",
        )));
    }

    for value in values {
        encode_f32(buffer, pos, *value)?;
        pos += 4;
    }

    Ok(pos - offset)
}

/// Decode packed repeated float field
pub fn decode_packed_float_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, Vec<f32>, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::LengthDelimited {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected LengthDelimited wire type, got {:?}", wire_type),
        )));
    }
    let (length, length_consumed) = decode_var_int32(bytes, offset + tag_consumed)?;
    let length = length as usize;
    let data_start = offset + tag_consumed + length_consumed;
    let data_end = data_start + length;

    if data_end > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete packed float field",
        )));
    }

    let mut values = Vec::new();
    let mut pos = data_start;
    while pos < data_end {
        let (value, consumed) = decode_f32(bytes, pos)?;
        values.push(value);
        pos += consumed;
    }

    Ok((
        field_number,
        values,
        tag_consumed + length_consumed + length,
    ))
}

/// Encode packed repeated double field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_packed_double_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    values: &[f64],
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::LengthDelimited)?;
    let mut pos = offset + tag_written;

    let packed_size = values.len() * 8;
    let length_written = encode_var_int32(buffer, pos, packed_size as i32)?;
    pos += length_written;

    if pos + packed_size > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for packed double field encoding",
        )));
    }

    for value in values {
        encode_f64(buffer, pos, *value)?;
        pos += 8;
    }

    Ok(pos - offset)
}

/// Decode packed repeated double field
pub fn decode_packed_double_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, Vec<f64>, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::LengthDelimited {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected LengthDelimited wire type, got {:?}", wire_type),
        )));
    }
    let (length, length_consumed) = decode_var_int32(bytes, offset + tag_consumed)?;
    let length = length as usize;
    let data_start = offset + tag_consumed + length_consumed;
    let data_end = data_start + length;

    if data_end > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete packed double field",
        )));
    }

    let mut values = Vec::new();
    let mut pos = data_start;
    while pos < data_end {
        let (value, consumed) = decode_f64(bytes, pos)?;
        values.push(value);
        pos += consumed;
    }

    Ok((
        field_number,
        values,
        tag_consumed + length_consumed + length,
    ))
}

/// Encode packed repeated bool field
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_packed_bool_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    values: &[bool],
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let tag_written = encode_tag(buffer, offset, field_number, WireType::LengthDelimited)?;
    let mut pos = offset + tag_written;

    let mut temp_buffer = vec![0u8; values.len() * 10];
    let mut packed_size = 0;
    for value in values {
        let written = encode_boolean(&mut temp_buffer[packed_size..], 0, *value)?;
        packed_size += written;
    }

    let length_written = encode_var_int32(buffer, pos, packed_size as i32)?;
    pos += length_written;

    if pos + packed_size > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for packed bool field encoding",
        )));
    }

    buffer[pos..pos + packed_size].copy_from_slice(&temp_buffer[..packed_size]);
    pos += packed_size;

    Ok(pos - offset)
}

/// Decode packed repeated bool field
pub fn decode_packed_bool_field(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, Vec<bool>, usize), Box<dyn std::error::Error + Send>> {
    let (field_number, wire_type, tag_consumed) = decode_tag(bytes, offset)?;
    if wire_type != WireType::LengthDelimited {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected LengthDelimited wire type, got {:?}", wire_type),
        )));
    }
    let (length, length_consumed) = decode_var_int32(bytes, offset + tag_consumed)?;
    let length = length as usize;
    let data_start = offset + tag_consumed + length_consumed;
    let data_end = data_start + length;

    if data_end > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete packed bool field",
        )));
    }

    let mut values = Vec::new();
    let mut pos = data_start;
    while pos < data_end {
        let (value, consumed) = decode_boolean(bytes, pos)?;
        values.push(value);
        pos += consumed;
    }

    Ok((
        field_number,
        values,
        tag_consumed + length_consumed + length,
    ))
}

// ========== Unpacked Repeated type fields ==========

/// Encode unpacked repeated int32 field (each value encoded separately)
/// Encode unpacked repeated int32 field (each value encoded separately)
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_unpacked_int32_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    values: &[i32],
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let mut pos = offset;
    for value in values {
        let written = encode_int32_field(buffer, pos, field_number, *value)?;
        pos += written;
    }
    Ok(pos - offset)
}

/// Encode unpacked repeated string field (each value encoded separately)
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_unpacked_string_field(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    values: &[String],
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let mut pos = offset;
    for value in values {
        let written = encode_string_field(buffer, pos, field_number, value)?;
        pos += written;
    }
    Ok(pos - offset)
}

// ========== Map type fields ==========

/// Map entry key type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapKeyType {
    Int32,
    Int64,
    Uint32,
    Uint64,
    Sint32,
    Sint64,
    Fixed32,
    Fixed64,
    Sfixed32,
    Sfixed64,
    Bool,
    String,
}

/// Map entry value type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapValueType {
    Int32,
    Int64,
    Uint32,
    Uint64,
    Sint32,
    Sint64,
    Fixed32,
    Fixed64,
    Sfixed32,
    Sfixed64,
    Float,
    Double,
    Bool,
    String,
    Bytes,
}

/// Encode Map field (key-value pairs)
///
/// In Protocol Buffers, Map is encoded as repeated entry message
/// Each entry is a nested message containing key (field 1) and value (field 2)
/// The entire entry uses LengthDelimited wire type
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_map_field<K, V>(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    _key_type: MapKeyType,
    _value_type: MapValueType,
    map: &[(K, V)],
    key_encoder: fn(&mut [u8], usize, u32, &K) -> Result<usize, Box<dyn std::error::Error + Send>>,
    value_encoder: fn(
        &mut [u8],
        usize,
        u32,
        &V,
    ) -> Result<usize, Box<dyn std::error::Error + Send>>,
) -> Result<usize, Box<dyn std::error::Error + Send>>
where
    K: Clone,
    V: Clone,
{
    let mut pos = offset;
    // Map field itself is a repeated field
    // Each entry is a nested message
    for (key, value) in map {
        // Encode entire entry as LengthDelimited field
        let tag_written = encode_tag(buffer, pos, field_number, WireType::LengthDelimited)?;
        pos += tag_written;

        // Encode key and value to temporary buffer first to calculate entry size
        // Use sufficiently large temporary buffer
        let mut temp_buffer = vec![0u8; 2048];
        let mut entry_pos = 0;

        // Encode key (field 1)
        let key_written = key_encoder(&mut temp_buffer, entry_pos, 1, key)?;
        entry_pos += key_written;

        // Encode value (field 2)
        let value_written = value_encoder(&mut temp_buffer, entry_pos, 2, value)?;
        entry_pos += value_written;

        let entry_size = entry_pos;

        // Encode entry length
        let length_written = encode_var_int32(buffer, pos, entry_size as i32)?;
        pos += length_written;

        // Check buffer space
        if pos + entry_size > buffer.len() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Buffer too small for map field encoding",
            )));
        }

        // Copy encoded entry content
        buffer[pos..pos + entry_size].copy_from_slice(&temp_buffer[..entry_size]);
        pos += entry_size;
    }
    Ok(pos - offset)
}

/// Decode Map field
///
/// Returns (field_number, map, bytes consumed)
pub fn decode_map_field<K, V>(
    bytes: &[u8],
    offset: usize,
    _key_type: MapKeyType,
    _value_type: MapValueType,
    key_decoder: fn(&[u8], usize) -> Result<(u32, K, usize), Box<dyn std::error::Error + Send>>,
    value_decoder: fn(&[u8], usize) -> Result<(u32, V, usize), Box<dyn std::error::Error + Send>>,
) -> Result<(u32, Vec<(K, V)>, usize), Box<dyn std::error::Error + Send>>
where
    K: Clone,
    V: Clone,
{
    let mut map = Vec::new();
    let mut pos = offset;
    let mut field_number = 0;

    while pos < bytes.len() {
        let (fn_num, wire_type, tag_consumed) = decode_tag(bytes, pos)?;
        field_number = fn_num;

        if wire_type == WireType::LengthDelimited {
            // Decode length of entry message
            let (entry_length, length_consumed) = decode_var_int32(bytes, pos + tag_consumed)?;
            let entry_length = entry_length as usize;
            let entry_start = pos + tag_consumed + length_consumed;
            let entry_end = entry_start + entry_length;

            if entry_end > bytes.len() {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Incomplete map entry",
                )));
            }

            // Decode key and value within entry
            let mut entry_pos = entry_start;
            let mut key: Option<K> = None;
            let mut value: Option<V> = None;

            while entry_pos < entry_end {
                let (fn_num, wire_type, tag_consumed) = decode_tag(bytes, entry_pos)?;

                match fn_num {
                    1 => {
                        // key field
                        let (_, k, consumed) = key_decoder(bytes, entry_pos)?;
                        key = Some(k);
                        entry_pos += consumed;
                    }
                    2 => {
                        // value field
                        let (_, v, consumed) = value_decoder(bytes, entry_pos)?;
                        value = Some(v);
                        entry_pos += consumed;
                    }
                    _ => {
                        // Skip unknown field
                        match wire_type {
                            WireType::Varint => {
                                let (_, consumed) =
                                    decode_var_int64(bytes, entry_pos + tag_consumed)?;
                                entry_pos += tag_consumed + consumed;
                            }
                            WireType::Fixed64 => {
                                entry_pos += tag_consumed + 8;
                            }
                            WireType::LengthDelimited => {
                                let (length, length_consumed) =
                                    decode_var_int32(bytes, entry_pos + tag_consumed)?;
                                entry_pos += tag_consumed + length_consumed + length as usize;
                            }
                            WireType::Fixed32 => {
                                entry_pos += tag_consumed + 4;
                            }
                            _ => {
                                return Err(Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    format!("Unsupported wire type in map entry: {:?}", wire_type),
                                )));
                            }
                        }
                    }
                }
            }

            if let (Some(k), Some(v)) = (key, value) {
                map.push((k, v));
            }

            pos = entry_end;
        } else {
            // Not LengthDelimited, skip
            match wire_type {
                WireType::Varint => {
                    let (_, consumed) = decode_var_int64(bytes, pos + tag_consumed)?;
                    pos += tag_consumed + consumed;
                }
                WireType::Fixed64 => {
                    pos += tag_consumed + 8;
                }
                WireType::Fixed32 => {
                    pos += tag_consumed + 4;
                }
                _ => {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Unexpected wire type in map: {:?}", wire_type),
                    )));
                }
            }
        }
    }

    Ok((field_number, map, pos - offset))
}

// ========== Map helper functions ==========

/// Encode Map of int32 -> string
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_map_int32_string(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    map: &[(i32, String)],
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    encode_map_field(
        buffer,
        offset,
        field_number,
        MapKeyType::Int32,
        MapValueType::String,
        map,
        |buf, off, fn_num, k| encode_int32_field(buf, off, fn_num, *k),
        |buf, off, fn_num, v| encode_string_field(buf, off, fn_num, v),
    )
}

/// Decode Map of int32 -> string
pub fn decode_map_int32_string(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, Vec<(i32, String)>, usize), Box<dyn std::error::Error + Send>> {
    decode_map_field(
        bytes,
        offset,
        MapKeyType::Int32,
        MapValueType::String,
        decode_int32_field,
        decode_string_field,
    )
}

/// Encode Map of string -> int32
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_map_string_int32(
    buffer: &mut [u8],
    offset: usize,
    field_number: u32,
    map: &[(String, i32)],
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    encode_map_field(
        buffer,
        offset,
        field_number,
        MapKeyType::String,
        MapValueType::Int32,
        map,
        |buf, off, fn_num, k| encode_string_field(buf, off, fn_num, k),
        |buf, off, fn_num, v| encode_int32_field(buf, off, fn_num, *v),
    )
}

/// Decode Map of string -> int32
pub fn decode_map_string_int32(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, Vec<(String, i32)>, usize), Box<dyn std::error::Error + Send>> {
    decode_map_field(
        bytes,
        offset,
        MapKeyType::String,
        MapValueType::Int32,
        decode_string_field,
        decode_int32_field,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tag() {
        let mut buffer = vec![0u8; 5];
        let written = encode_tag(&mut buffer, 0, 1, WireType::Varint).unwrap();
        let (field_number, wire_type, consumed) = decode_tag(&buffer, 0).unwrap();
        assert_eq!(field_number, 1);
        assert_eq!(wire_type, WireType::Varint);
        assert_eq!(written, consumed);
    }

    #[test]
    fn test_int32_field() {
        let mut buffer = vec![0u8; 20];
        let written = encode_int32_field(&mut buffer, 0, 1, 42).unwrap();
        let (field_number, value, consumed) = decode_int32_field(&buffer, 0).unwrap();
        assert_eq!(field_number, 1);
        assert_eq!(value, 42);
        assert_eq!(written, consumed);
    }

    #[test]
    fn test_string_field() {
        let mut buffer = vec![0u8; 50];
        let written = encode_string_field(&mut buffer, 0, 1, "hello").unwrap();
        let (field_number, value, consumed) = decode_string_field(&buffer, 0).unwrap();
        assert_eq!(field_number, 1);
        assert_eq!(value, "hello");
        assert_eq!(written, consumed);
    }

    #[test]
    fn test_bool_field() {
        let mut buffer = vec![0u8; 20];
        let written = encode_bool_field(&mut buffer, 0, 1, true).unwrap();
        let (field_number, value, consumed) = decode_bool_field(&buffer, 0).unwrap();
        assert_eq!(field_number, 1);
        assert_eq!(value, true);
        assert_eq!(written, consumed);
    }

    #[test]
    fn test_sint32_field() {
        let mut buffer = vec![0u8; 20];
        let written = encode_sint32_field(&mut buffer, 0, 1, -42).unwrap();
        let (field_number, value, consumed) = decode_sint32_field(&buffer, 0).unwrap();
        assert_eq!(field_number, 1);
        assert_eq!(value, -42);
        assert_eq!(written, consumed);
    }

    #[test]
    fn test_float_field() {
        let mut buffer = vec![0u8; 20];
        // Use a test value that's not a constant approximation
        #[allow(clippy::approx_constant)]
        let test_value = 3.14159;
        let written = encode_float_field(&mut buffer, 0, 1, test_value).unwrap();
        let (field_number, value, consumed) = decode_float_field(&buffer, 0).unwrap();
        assert_eq!(field_number, 1);
        assert!((value - test_value).abs() < f32::EPSILON);
        assert_eq!(written, consumed);
    }

    #[test]
    fn test_packed_int32_field() {
        let values = vec![1, 2, 3, 4, 5];
        let mut buffer = vec![0u8; 100];
        let written = encode_packed_int32_field(&mut buffer, 0, 1, &values).unwrap();
        let (field_number, decoded, consumed) = decode_packed_int32_field(&buffer, 0).unwrap();
        assert_eq!(field_number, 1);
        assert_eq!(decoded, values);
        assert_eq!(written, consumed);
    }

    #[test]
    fn test_packed_float_field() {
        let values = vec![1.0f32, 2.0, 3.0];
        let mut buffer = vec![0u8; 100];
        let written = encode_packed_float_field(&mut buffer, 0, 1, &values).unwrap();
        let (field_number, decoded, consumed) = decode_packed_float_field(&buffer, 0).unwrap();
        assert_eq!(field_number, 1);
        assert_eq!(decoded.len(), values.len());
        for (a, b) in decoded.iter().zip(values.iter()) {
            assert!((a - b).abs() < f32::EPSILON);
        }
        assert_eq!(written, consumed);
    }

    #[test]
    fn test_map_int32_string() {
        let map = vec![
            (1, "one".to_string()),
            (2, "two".to_string()),
            (3, "three".to_string()),
        ];
        // Use larger buffer: each entry may need tag(5) + length(5) + key(10) + value(20+) = 40+ bytes
        // 3 entries need at least 120 bytes, plus some margin
        let mut buffer = vec![0u8; 5000];
        let written = encode_map_int32_string(&mut buffer, 0, 1, &map).unwrap();
        let (field_number, decoded, consumed) = decode_map_int32_string(&buffer, 0).unwrap();
        assert_eq!(field_number, 1);
        assert_eq!(decoded.len(), map.len());
        for (i, (k, v)) in decoded.iter().enumerate() {
            assert_eq!(*k, map[i].0);
            assert_eq!(*v, map[i].1);
        }
        assert_eq!(written, consumed);
    }
}
