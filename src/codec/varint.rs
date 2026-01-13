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

// VarInt - Variable-length integer encoding/decoding
//
// Implements encoding and decoding of VarInt32, VarUInt32, VarInt64
// Reference: Protocol Buffers variable-length integer encoding

/// Encode VarInt32 (signed 32-bit variable-length integer)
///
/// If the value is negative, VarInt64 encoding will be used
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_var_int32(
    buffer: &mut [u8],
    offset: usize,
    value: i32,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    if value < 0 {
        encode_var_int64(buffer, offset, value as i64)
    } else {
        encode_var_uint32(buffer, offset, value as u32)
    }
}

/// Decode VarInt32 (signed 32-bit variable-length integer)
///
/// Decodes from the specified position in the byte array, returns (decoded value, bytes consumed)
pub fn decode_var_int32(
    bytes: &[u8],
    offset: usize,
) -> Result<(i32, usize), Box<dyn std::error::Error + Send>> {
    let mut value: i32 = 0;
    let mut shift = 0;
    let mut pos = offset;

    loop {
        if pos >= bytes.len() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Incomplete VarInt32",
            )));
        }

        let b = bytes[pos] as i8;
        pos += 1;

        if b >= 0 {
            value |= (b as i32) << shift;
            return Ok((value, pos - offset));
        }

        value |= ((b as i32) ^ (0xffffff80u32 as i32)) << shift;
        shift += 7;

        if shift >= 35 {
            // Handle negative numbers, need to read additional bytes
            if pos >= bytes.len() || bytes[pos] != 0xff {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid VarInt32 encoding",
                )));
            }
            pos += 1;

            for _ in 0..4 {
                if pos >= bytes.len() || bytes[pos] != 0xff {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid VarInt32 encoding",
                    )));
                }
                pos += 1;
            }

            if pos >= bytes.len() || bytes[pos] != 0x01 {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid VarInt32 encoding",
                )));
            }
            pos += 1;

            return Ok((value, pos - offset));
        }
    }
}

/// Encode VarUInt32 (unsigned 32-bit variable-length integer)
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_var_uint32(
    buffer: &mut [u8],
    offset: usize,
    value: u32,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    let size = compute_var_uint32_size(value);
    if offset + size > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for VarUInt32 encoding",
        )));
    }

    let mut val = value;
    let mut pos = offset;
    loop {
        if pos >= buffer.len() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Buffer too small for VarUInt32 encoding",
            )));
        }
        let byte = (val & 0x7f) as u8;
        val >>= 7;
        if val == 0 {
            buffer[pos] = byte;
            pos += 1;
            return Ok(pos - offset);
        } else {
            buffer[pos] = byte | 0x80;
            pos += 1;
        }
    }
}

/// Decode VarUInt32 (unsigned 32-bit variable-length integer)
///
/// Decodes from the specified position in the byte array, returns (decoded value, bytes consumed)
pub fn decode_var_uint32(
    bytes: &[u8],
    offset: usize,
) -> Result<(u32, usize), Box<dyn std::error::Error + Send>> {
    let mut value: u32 = 0;
    let mut shift = 0;
    let mut pos = offset;

    loop {
        if pos >= bytes.len() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Incomplete VarUInt32",
            )));
        }

        let b = bytes[pos];
        pos += 1;

        if (b & 0x80) == 0 {
            value |= (b as u32) << shift;
            return Ok((value, pos - offset));
        }

        value |= ((b & 0x7f) as u32) << shift;
        shift += 7;

        if shift >= 35 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "VarUInt32 too large",
            )));
        }
    }
}

/// Encode VarInt64 (signed 64-bit variable-length integer)
/// Writes to the specified position in the buffer, returns the number of bytes written
pub fn encode_var_int64(
    buffer: &mut [u8],
    offset: usize,
    value: i64,
) -> Result<usize, Box<dyn std::error::Error + Send>> {
    // VarInt64 requires at most 10 bytes
    if offset + 10 > buffer.len() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Buffer too small for VarInt64 encoding",
        )));
    }

    let mut val = value;
    let mut pos = offset;
    loop {
        let byte = (val & 0x7f) as u8;
        val >>= 7;
        if val == 0 && (byte & 0x80) == 0 {
            buffer[pos] = byte;
            pos += 1;
            return Ok(pos - offset);
        } else if val == -1 {
            // Handle negative numbers: all high bits are 1
            buffer[pos] = byte | 0x80;
            pos += 1;
            // Continue encoding until 0x01 is encountered (need 9 bytes of 0xff + 1 byte of 0x01)
            let remaining = 10 - (pos - offset);
            for _ in 0..remaining - 1 {
                if pos >= buffer.len() {
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Buffer too small for VarInt64 encoding",
                    )));
                }
                buffer[pos] = 0xff;
                pos += 1;
            }
            if pos >= buffer.len() {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Buffer too small for VarInt64 encoding",
                )));
            }
            buffer[pos] = 0x01;
            pos += 1;
            return Ok(pos - offset);
        } else {
            buffer[pos] = byte | 0x80;
            pos += 1;
        }
    }
}

/// Decode VarInt64 (signed 64-bit variable-length integer)
///
/// Decodes from the specified position in the byte array, returns (decoded value, bytes consumed)
pub fn decode_var_int64(
    bytes: &[u8],
    offset: usize,
) -> Result<(i64, usize), Box<dyn std::error::Error + Send>> {
    let mut value: i64 = 0;
    let mut shift = 0;
    let mut pos = offset;

    loop {
        if pos >= bytes.len() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Incomplete VarInt64",
            )));
        }

        let b = bytes[pos] as i8;
        pos += 1;

        if b >= 0 {
            value |= (b as i64) << shift;
            return Ok((value, pos - offset));
        }

        value |= ((b as i64) ^ (0xffffffffffffff80u64 as i64)) << shift;
        shift += 7;

        if shift >= 70 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "VarInt64 too large",
            )));
        }
    }
}

/// Compute the size after encoding VarInt32
pub fn compute_var_int32_size(value: i32) -> usize {
    if value < 0 {
        return 10;
    }

    let val = value as u32;
    if (val & 0xffffff80) == 0 {
        1
    } else if (val & 0xffffc000) == 0 {
        2
    } else if (val & 0xffe00000) == 0 {
        3
    } else if (val & 0xf0000000) == 0 {
        4
    } else {
        5
    }
}

/// Compute the size after encoding VarUInt32
pub fn compute_var_uint32_size(value: u32) -> usize {
    if (value & 0xffffff80) == 0 {
        1
    } else if (value & 0xffffc000) == 0 {
        2
    } else if (value & 0xffe00000) == 0 {
        3
    } else if (value & 0xf0000000) == 0 {
        4
    } else {
        5
    }
}

/// Compute the size after encoding VarInt64
pub fn compute_var_int64_size(value: i64) -> usize {
    let v = value as u64;
    if (v & 0xffffffffffffff80u64) == 0 {
        1
    } else if (v & 0xffffffffffffc000u64) == 0 {
        2
    } else if (v & 0xffffffffffe00000u64) == 0 {
        3
    } else if (v & 0xfffffffff0000000u64) == 0 {
        4
    } else if (v & 0xfffffff800000000u64) == 0 {
        5
    } else if (v & 0xfffffc0000000000u64) == 0 {
        6
    } else if (v & 0xfffe000000000000u64) == 0 {
        7
    } else if (v & 0xff00000000000000u64) == 0 {
        8
    } else if (v & 0x8000000000000000u64) == 0 {
        9
    } else {
        10
    }
}

/// Compute the size after encoding VarUint64
pub fn compute_var_uint64_size(value: u64) -> usize {
    if value == 0 {
        1
    } else {
        (64 - value.leading_zeros() as usize).div_ceil(7)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_var_uint32() {
        let test_cases = vec![0, 1, 127, 128, 16383, 16384, 2097151, 2097152, u32::MAX];

        for value in test_cases {
            let size = compute_var_uint32_size(value);
            let mut buffer = vec![0u8; size];
            let written = encode_var_uint32(&mut buffer, 0, value).unwrap();
            let (decoded, consumed) = decode_var_uint32(&buffer, 0).unwrap();

            assert_eq!(value, decoded);
            assert_eq!(written, consumed);
            assert_eq!(written, size);
        }
    }

    #[test]
    fn test_var_int32() {
        let test_cases = vec![0, 1, -1, 127, -128, 16383, -16384, i32::MAX, i32::MIN];

        for value in test_cases {
            // Use sufficiently large buffer (VarInt32 negative numbers require at most 10 bytes)
            let mut buffer = vec![0u8; 15];
            let written = encode_var_int32(&mut buffer, 0, value).unwrap();
            let (decoded, consumed) = decode_var_int32(&buffer, 0).unwrap();

            assert_eq!(value, decoded);
            assert_eq!(written, consumed);
        }
    }

    #[test]
    fn test_var_int64() {
        let test_cases = vec![0i64, 1, -1, 127, -128, 16383, -16384, i64::MAX, i64::MIN];

        for value in test_cases {
            // Use sufficiently large buffer (VarInt64 requires at most 10 bytes)
            let mut buffer = vec![0u8; 15];
            let written = encode_var_int64(&mut buffer, 0, value).unwrap();
            let (decoded, consumed) = decode_var_int64(&buffer, 0).unwrap();

            assert_eq!(value, decoded);
            assert_eq!(written, consumed);
        }
    }
}
