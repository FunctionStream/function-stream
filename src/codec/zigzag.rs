// ZigZag - ZigZag encoding/decoding
//
// Used to encode signed integers as unsigned integers, allowing small absolute value negative numbers
// to be represented with fewer bytes
// Reference: Protocol Buffers ZigZag encoding

/// Encode ZigZag32 (encode signed 32-bit integer as unsigned integer)
pub fn encode_zigzag32(value: i32) -> u32 {
    ((value << 1) ^ (value >> 31)) as u32
}

/// Decode ZigZag32 (decode unsigned integer to signed 32-bit integer)
pub fn decode_zigzag32(value: u32) -> i32 {
    ((value >> 1) as i32) ^ (-((value & 1) as i32))
}

/// Encode ZigZag64 (encode signed 64-bit integer as unsigned integer)
pub fn encode_zigzag64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

/// Decode ZigZag64 (decode unsigned integer to signed 64-bit integer)
pub fn decode_zigzag64(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zigzag32() {
        let test_cases = vec![
            (0, 0),
            (-1, 1),
            (1, 2),
            (-2, 3),
            (2, 4),
            (i32::MAX, u32::MAX - 1),
            (i32::MIN, u32::MAX),
        ];

        for (original, encoded) in test_cases {
            assert_eq!(encode_zigzag32(original), encoded);
            assert_eq!(decode_zigzag32(encoded), original);
        }
    }

    #[test]
    fn test_zigzag64() {
        let test_cases = vec![
            (0i64, 0u64),
            (-1, 1),
            (1, 2),
            (-2, 3),
            (2, 4),
            (i64::MAX, u64::MAX - 1),
            (i64::MIN, u64::MAX),
        ];

        for (original, encoded) in test_cases {
            assert_eq!(encode_zigzag64(original), encoded);
            assert_eq!(decode_zigzag64(encoded), original);
        }
    }
}
