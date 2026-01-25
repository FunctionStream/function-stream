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

/// Build complex key
///
/// Format: keyGroup | key | namespace | userKey
pub fn build_key(key_group: &[u8], key: &[u8], namespace: &[u8], user_key: &[u8]) -> Vec<u8> {
    let total_len = key_group.len() + key.len() + namespace.len() + user_key.len();
    let mut result = Vec::with_capacity(total_len);

    result.extend_from_slice(key_group);
    result.extend_from_slice(key);
    result.extend_from_slice(namespace);
    result.extend_from_slice(user_key);

    result
}

/// Increment key (for range deletion)
///
/// Increment the last byte of the key by 1, carrying forward if overflow
/// Used to create the upper bound for range deletion
pub fn increment_key(key: &[u8]) -> Vec<u8> {
    if key.is_empty() {
        return vec![0];
    }

    let mut result = key.to_vec();

    for i in (0..result.len()).rev() {
        if result[i] < 0xFF {
            result[i] += 1;
            result.truncate(i + 1);
            return result;
        } else {
            result[i] = 0;
        }
    }

    result.push(0);
    result
}

/// Check if key is all 0xFF
pub fn is_all_0xff(key: &[u8]) -> bool {
    key.iter().all(|&b| b == 0xFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_key() {
        let key = build_key(b"group", b"key", b"namespace", b"user");
        assert!(key.len() > 0);
    }

    #[test]
    fn test_increment_key() {
        let key1 = vec![0x01, 0x02, 0x03];
        let inc1 = increment_key(&key1);
        assert_eq!(inc1, vec![0x01, 0x02, 0x04]);

        let key2 = vec![0x01, 0x02, 0xFF];
        let inc2 = increment_key(&key2);
        assert_eq!(inc2, vec![0x01, 0x03, 0x00]);

        let key3 = vec![0xFF, 0xFF];
        let inc3 = increment_key(&key3);
        assert_eq!(inc3, vec![0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_is_all_0xff() {
        assert!(is_all_0xff(&[0xFF, 0xFF]));
        assert!(!is_all_0xff(&[0xFF, 0xFE]));
        assert!(!is_all_0xff(&[]));
    }
}
