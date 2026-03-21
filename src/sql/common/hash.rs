use std::ops::RangeInclusive;

/// Randomly generated seeds for consistent hashing. Changing these breaks existing state.
pub const HASH_SEEDS: [u64; 4] = [
    5093852630788334730,
    1843948808084437226,
    8049205638242432149,
    17942305062735447798,
];

/// Returns the server index (0-based) responsible for the given hash value
/// when distributing across `n` servers.
pub fn server_for_hash(x: u64, n: usize) -> usize {
    if n == 1 {
        0
    } else {
        let range_size = (u64::MAX / (n as u64)) + 1;
        (x / range_size) as usize
    }
}

/// Returns the key range assigned to server `i` out of `n` total servers.
pub fn range_for_server(i: usize, n: usize) -> RangeInclusive<u64> {
    if n == 1 {
        return 0..=u64::MAX;
    }
    let range_size = (u64::MAX / (n as u64)) + 1;
    let start = range_size * (i as u64);
    let end = if i + 1 == n {
        u64::MAX
    } else {
        start + range_size - 1
    };
    start..=end
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_for_server() {
        let n = 6;

        for i in 0..(n - 1) {
            let range1 = range_for_server(i, n);
            let range2 = range_for_server(i + 1, n);

            assert_eq!(*range1.end() + 1, *range2.start(), "Ranges not adjacent");
            assert_eq!(
                i,
                server_for_hash(*range1.start(), n),
                "start not assigned to range"
            );
            assert_eq!(
                i,
                server_for_hash(*range1.end(), n),
                "end not assigned to range"
            );
        }

        let last_range = range_for_server(n - 1, n);
        assert_eq!(
            *last_range.end(),
            u64::MAX,
            "Last range does not contain u64::MAX"
        );
        assert_eq!(
            n - 1,
            server_for_hash(u64::MAX, n),
            "u64::MAX not in last range"
        );
    }

    #[test]
    fn test_server_for_hash() {
        let n = 2;
        let x = u64::MAX;

        let server_index = server_for_hash(x, n);
        let server_range = range_for_server(server_index, n);

        assert!(
            server_range.contains(&x),
            "u64::MAX is not in the correct range"
        );
    }
}
