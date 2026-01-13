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

// Key Builder - 键构建器
//
// 用于构建复杂键，支持 keyGroup, key, namespace, userKey 的组合

/// 构建复杂键
///
/// 格式：keyGroup | key | namespace | userKey
/// 每个组件前都有长度前缀（4 字节，大端序）
pub fn build_key(key_group: &[u8], key: &[u8], namespace: &[u8], user_key: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();

    // 写入 keyGroup 长度和内容
    result.extend_from_slice(&(key_group.len() as u32).to_be_bytes());
    result.extend_from_slice(key_group);

    // 写入 key 长度和内容
    result.extend_from_slice(&(key.len() as u32).to_be_bytes());
    result.extend_from_slice(key);

    // 写入 namespace 长度和内容
    result.extend_from_slice(&(namespace.len() as u32).to_be_bytes());
    result.extend_from_slice(namespace);

    // 写入 userKey 长度和内容
    result.extend_from_slice(&(user_key.len() as u32).to_be_bytes());
    result.extend_from_slice(user_key);

    result
}

/// 递增键（用于范围删除）
///
/// 将键的最后一个字节加 1，如果溢出则向前进位
/// 用于创建范围删除的上界
pub fn increment_key(key: &[u8]) -> Vec<u8> {
    if key.is_empty() {
        return vec![0];
    }

    let mut result = key.to_vec();

    // 从最后一个字节开始递增
    for i in (0..result.len()).rev() {
        if result[i] < 0xFF {
            result[i] += 1;
            // 截断到当前位置（移除后面的字节）
            result.truncate(i + 1);
            return result;
        } else {
            // 当前字节溢出，继续向前
            result[i] = 0;
        }
    }

    // 如果所有字节都是 0xFF，添加一个新的 0 字节
    result.push(0);
    result
}

/// 检查键是否全为 0xFF
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
