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

// StringCodec - String and byte array encoding/decoding
//
// Provides encoding and decoding functionality for strings and byte arrays

use super::varint::{compute_var_int32_size, decode_var_int32, encode_var_int32};

/// Encode string (UTF-8)
///
/// First encode length (VarInt32), then encode string content
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_string(
    buffer: &mut [u8],
    offset: usize,
    s: &str,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let bytes = s.as_bytes();
    let length = bytes.len() as i32;
    let length_size = compute_var_int32_size(length);

    if offset + length_size + bytes.len() > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for string encoding",
        )));
    }

    // Encode length
    let length_written = encode_var_int32(buffer, offset, length)?;

    // Encode string content
    buffer[offset + length_written..offset + length_written + bytes.len()].copy_from_slice(bytes);

    Ok(length_written + bytes.len())
}

/// Decode string (UTF-8)
///
/// First decode length, then decode string content
/// Decodes from the specified position in the byte array, returns (decoded value, bytes consumed)
pub fn decode_string(
    bytes: &[u8],
    offset: usize,
) -> Result<(String, usize), Box<dyn std::error::Error + Send>> {
    let (length, length_consumed) = decode_var_int32(bytes, offset)?;
    let length = length as usize;
    let start = offset + length_consumed;

    if start + length > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete string",
        )));
    }

    let string_bytes = &bytes[start..start + length];
    // Directly validate UTF-8 to avoid unnecessary copying
    let s = std::str::from_utf8(string_bytes)
        .map_err(|e| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid UTF-8 string: {}", e),
            )) as Box<dyn std::error::Error + Send>
        })?
        .to_string();

    Ok((s, length_consumed + length))
}

/// Encode byte array
///
/// First encode length (VarInt32), then encode byte content
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_byte_string(
    buffer: &mut [u8],
    offset: usize,
    bytes: &[u8],
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let length = bytes.len() as i32;
    let length_size = compute_var_int32_size(length);

    if offset + length_size + bytes.len() > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for byte string encoding",
        )));
    }

    // Encode length
    let length_written = encode_var_int32(buffer, offset, length)?;

    // Encode byte content
    buffer[offset + length_written..offset + length_written + bytes.len()].copy_from_slice(bytes);

    Ok(length_written + bytes.len())
}

/// Decode byte array
///
/// First decode length, then decode byte content
/// Decodes from the specified position in the byte array, returns (decoded value, bytes consumed)
pub fn decode_byte_string(
    bytes: &[u8],
    offset: usize,
) -> Result<(Vec<u8>, usize), Box<dyn std::error::Error + Send>> {
    let (length, length_consumed) = decode_var_int32(bytes, offset)?;
    let length = length as usize;
    let start = offset + length_consumed;

    if start + length > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete byte string",
        )));
    }

    let result = bytes[start..start + length].to_vec();
    Ok((result, length_consumed + length))
}

/// Decode string (with known length)
///
/// Directly decode string content based on known length
/// Decodes from the specified position in the byte array, returns (decoded value, bytes consumed)
pub fn decode_string_with_length(
    bytes: &[u8],
    offset: usize,
    length: usize,
) -> Result<(String, usize), Box<dyn std::error::Error + Send>> {
    if offset + length > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete string",
        )));
    }

    let string_bytes = &bytes[offset..offset + length];
    let s = String::from_utf8(string_bytes.to_vec()).map_err(|e| {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid UTF-8 string: {}", e),
        )) as Box<dyn std::error::Error + Send>
    })?;

    Ok((s, length))
}

/// Decode byte array (with known length)
///
/// Directly decode byte content based on known length
/// Decodes from the specified position in the byte array, returns (decoded value, bytes consumed)
pub fn decode_byte_string_with_length(
    bytes: &[u8],
    offset: usize,
    length: usize,
) -> Result<(Vec<u8>, usize), Box<dyn std::error::Error + Send>> {
    if offset + length > bytes.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Incomplete byte string",
        )));
    }

    let result = bytes[offset..offset + length].to_vec();
    Ok((result, length))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string() {
        let s = "a".repeat(1000);
        let test_cases = vec!["", "hello", "world", &s[0..]];

        for s in test_cases {
            // Estimate buffer size: length encoding (at most 5) + string content
            let mut buffer = vec![0u8; 5 + s.as_bytes().len()];
            let written = encode_string(&mut buffer, 0, s).unwrap();
            let (decoded, consumed) = decode_string(&buffer, 0).unwrap();

            assert_eq!(s, decoded);
            assert_eq!(written, consumed);
        }
    }

    #[test]
    fn test_byte_string() {
        let test_cases = vec![
            vec![],
            vec![0, 1, 2, 3],
            vec![255, 254, 253],
            (0..100).collect::<Vec<u8>>(),
        ];

        for bytes in test_cases {
            // Estimate buffer size: length encoding (at most 5) + byte content
            let mut buffer = vec![0u8; 5 + bytes.len()];
            let written = encode_byte_string(&mut buffer, 0, &bytes).unwrap();
            let (decoded, consumed) = decode_byte_string(&buffer, 0).unwrap();

            assert_eq!(bytes, decoded);
            assert_eq!(written, consumed);
        }
    }
}
