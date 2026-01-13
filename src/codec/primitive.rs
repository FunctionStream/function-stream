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

// Primitive - Primitive type encoding/decoding
//
// Provides encoding and decoding functionality for basic data types

use super::varint::{decode_var_int64, encode_var_int64};

/// Encode boolean value
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_boolean(
    buffer: &mut [u8],
    offset: usize,
    value: bool,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let val = if value { 1i64 } else { 0i64 };
    encode_var_int64(buffer, offset, val)
}

/// Decode boolean value
///
/// Decodes from the specified position in the byte array, returns (decoded value, bytes consumed)
pub fn decode_boolean(
    bytes: &[u8],
    offset: usize,
) -> Result<(bool, usize), Box<dyn std::error::Error + Send>> {
    let (value, consumed) = decode_var_int64(bytes, offset)?;
    Ok((value != 0, consumed))
}

/// Encode 32-bit integer (little-endian)
/// Writes to the specified position in the buffer, returns the number of bytes written (fixed at 4)
pub fn encode_i32(
    buffer: &mut [u8],
    offset: usize,
    value: i32,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    if offset + 4 > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for i32 encoding",
        )));
    }
    let bytes = value.to_le_bytes();
    buffer[offset..offset + 4].copy_from_slice(&bytes);
    Ok(4)
}

/// Decode 32-bit integer (little-endian)
///
/// Decodes from the specified position in the byte array, returns (decoded value, bytes consumed)
pub fn decode_i32(
    bytes: &[u8],
    offset: usize,
) -> Result<(i32, usize), Box<dyn std::error::Error + Send>> {
    if offset + 4 > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete i32",
        )));
    }
    let mut arr = [0u8; 4];
    arr.copy_from_slice(&bytes[offset..offset + 4]);
    Ok((i32::from_le_bytes(arr), 4))
}

/// Encode 64-bit integer (little-endian)
/// Writes to the specified position in the buffer, returns the number of bytes written (fixed at 8)
pub fn encode_i64(
    buffer: &mut [u8],
    offset: usize,
    value: i64,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    if offset + 8 > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for i64 encoding",
        )));
    }
    let bytes = value.to_le_bytes();
    buffer[offset..offset + 8].copy_from_slice(&bytes);
    Ok(8)
}

/// Decode 64-bit integer (little-endian)
///
/// Decodes from the specified position in the byte array, returns (decoded value, bytes consumed)
pub fn decode_i64(
    bytes: &[u8],
    offset: usize,
) -> Result<(i64, usize), Box<dyn std::error::Error + Send>> {
    if offset + 8 > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete i64",
        )));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[offset..offset + 8]);
    Ok((i64::from_le_bytes(arr), 8))
}

/// Encode 32-bit unsigned integer (little-endian)
/// Writes to the specified position in the buffer, returns the number of bytes written (fixed at 4)
pub fn encode_u32(
    buffer: &mut [u8],
    offset: usize,
    value: u32,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    if offset + 4 > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for u32 encoding",
        )));
    }
    let bytes = value.to_le_bytes();
    buffer[offset..offset + 4].copy_from_slice(&bytes);
    Ok(4)
}

/// Decode 32-bit unsigned integer (little-endian)
///
/// Decodes from the specified position in the byte array, returns (decoded value, bytes consumed)
pub fn decode_u32(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, usize), Box<dyn std::error::Error + Send>> {
    if offset + 4 > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete u32",
        )));
    }
    let mut arr = [0u8; 4];
    arr.copy_from_slice(&bytes[offset..offset + 4]);
    Ok((u32::from_le_bytes(arr), 4))
}

/// Encode 64-bit unsigned integer (little-endian)
/// Writes to the specified position in the buffer, returns the number of bytes written (fixed at 8)
pub fn encode_u64(
    buffer: &mut [u8],
    offset: usize,
    value: u64,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    if offset + 8 > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for u64 encoding",
        )));
    }
    let bytes = value.to_le_bytes();
    buffer[offset..offset + 8].copy_from_slice(&bytes);
    Ok(8)
}

/// Decode 64-bit unsigned integer (little-endian)
///
/// Decodes from the specified position in the byte array, returns (decoded value, bytes consumed)
pub fn decode_u64(
    bytes: &[u8],
    offset: usize,
) -> Result<(u64, usize), Box<dyn std::error::Error + Send>> {
    if offset + 8 > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete u64",
        )));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[offset..offset + 8]);
    Ok((u64::from_le_bytes(arr), 8))
}

/// Encode single-precision floating point number (little-endian)
/// Writes to the specified position in the buffer, returns the number of bytes written (fixed at 4)
pub fn encode_f32(
    buffer: &mut [u8],
    offset: usize,
    value: f32,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    if offset + 4 > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for f32 encoding",
        )));
    }
    let bytes = value.to_le_bytes();
    buffer[offset..offset + 4].copy_from_slice(&bytes);
    Ok(4)
}

/// Decode single-precision floating point number (little-endian)
///
/// Decodes from the specified position in the byte array, returns (decoded value, bytes consumed)
pub fn decode_f32(
    bytes: &[u8],
    offset: usize,
) -> Result<(f32, usize), Box<dyn std::error::Error + Send>> {
    if offset + 4 > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete f32",
        )));
    }
    let mut arr = [0u8; 4];
    arr.copy_from_slice(&bytes[offset..offset + 4]);
    Ok((f32::from_le_bytes(arr), 4))
}

/// Encode double-precision floating point number (little-endian)
/// Writes to the specified position in the buffer, returns the number of bytes written (fixed at 8)
pub fn encode_f64(
    buffer: &mut [u8],
    offset: usize,
    value: f64,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    if offset + 8 > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for f64 encoding",
        )));
    }
    let bytes = value.to_le_bytes();
    buffer[offset..offset + 8].copy_from_slice(&bytes);
    Ok(8)
}

/// Decode double-precision floating point number (little-endian)
///
/// Decodes from the specified position in the byte array, returns (decoded value, bytes consumed)
pub fn decode_f64(
    bytes: &[u8],
    offset: usize,
) -> Result<(f64, usize), Box<dyn std::error::Error + Send>> {
    if offset + 8 > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete f64",
        )));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[offset..offset + 8]);
    Ok((f64::from_le_bytes(arr), 8))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean() {
        let mut buffer_true = vec![0u8; 10];
        let mut buffer_false = vec![0u8; 10];
        let written_true = encode_boolean(&mut buffer_true, 0, true).unwrap();
        let written_false = encode_boolean(&mut buffer_false, 0, false).unwrap();

        let (decoded_true, _) = decode_boolean(&buffer_true, 0).unwrap();
        let (decoded_false, _) = decode_boolean(&buffer_false, 0).unwrap();

        assert_eq!(decoded_true, true);
        assert_eq!(decoded_false, false);
    }

    #[test]
    fn test_i32() {
        let test_cases = vec![0, 1, -1, i32::MAX, i32::MIN];

        for value in test_cases {
            let mut buffer = vec![0u8; 4];
            let written = encode_i32(&mut buffer, 0, value).unwrap();
            let (decoded, consumed) = decode_i32(&buffer, 0).unwrap();

            assert_eq!(value, decoded);
            assert_eq!(written, consumed);
            assert_eq!(written, 4);
        }
    }

    #[test]
    fn test_i64() {
        let test_cases = vec![0i64, 1, -1, i64::MAX, i64::MIN];

        for value in test_cases {
            let mut buffer = vec![0u8; 8];
            let written = encode_i64(&mut buffer, 0, value).unwrap();
            let (decoded, consumed) = decode_i64(&buffer, 0).unwrap();

            assert_eq!(value, decoded);
            assert_eq!(written, consumed);
            assert_eq!(written, 8);
        }
    }

    #[test]
    fn test_f32() {
        let test_cases = vec![0.0f32, 1.0, -1.0, f32::MAX, f32::MIN];

        for value in test_cases {
            let mut buffer = vec![0u8; 4];
            let written = encode_f32(&mut buffer, 0, value).unwrap();
            let (decoded, consumed) = decode_f32(&buffer, 0).unwrap();

            assert!((decoded - value).abs() < f32::EPSILON);
            assert_eq!(written, consumed);
            assert_eq!(written, 4);
        }
    }

    #[test]
    fn test_f64() {
        let test_cases = vec![0.0f64, 1.0, -1.0, f64::MAX, f64::MIN];

        for value in test_cases {
            let mut buffer = vec![0u8; 8];
            let written = encode_f64(&mut buffer, 0, value).unwrap();
            let (decoded, consumed) = decode_f64(&buffer, 0).unwrap();

            assert!((decoded - value).abs() < f64::EPSILON);
            assert_eq!(written, consumed);
            assert_eq!(written, 8);
        }
    }
}
