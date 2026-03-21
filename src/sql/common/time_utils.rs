use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn to_millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

pub fn to_micros(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
}

pub fn from_millis(ts: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ts)
}

pub fn from_micros(ts: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(ts)
}

pub fn to_nanos(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH).unwrap().as_nanos()
}

pub fn from_nanos(ts: u128) -> SystemTime {
    UNIX_EPOCH
        + Duration::from_secs((ts / 1_000_000_000) as u64)
        + Duration::from_nanos((ts % 1_000_000_000) as u64)
}

pub fn print_time(time: SystemTime) -> String {
    chrono::DateTime::<chrono::Utc>::from(time)
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}

/// Returns the number of days since the UNIX epoch (for Avro serialization).
pub fn days_since_epoch(time: SystemTime) -> i32 {
    time.duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .div_euclid(86400) as i32
}

pub fn single_item_hash_map<I: Into<K>, K: Hash + Eq, V>(key: I, value: V) -> HashMap<K, V> {
    let mut map = HashMap::new();
    map.insert(key.into(), value);
    map
}

pub fn string_to_map(s: &str, pair_delimiter: char) -> Option<HashMap<String, String>> {
    if s.trim().is_empty() {
        return Some(HashMap::new());
    }

    s.split(',')
        .map(|s| {
            let mut kv = s.trim().split(pair_delimiter);
            Some((kv.next()?.trim().to_string(), kv.next()?.trim().to_string()))
        })
        .collect()
}
