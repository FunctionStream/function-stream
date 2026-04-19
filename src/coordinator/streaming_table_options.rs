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

use std::collections::HashMap;

fn parse_positive_u64(raw: &str) -> Option<u64> {
    let t = raw.trim().trim_matches('\'');
    t.parse::<u64>().ok().filter(|&v| v > 0)
}

fn parse_positive_u32(raw: &str) -> Option<u32> {
    let t = raw.trim().trim_matches('\'');
    t.parse::<u32>().ok().filter(|&v| v > 0)
}

pub fn parse_checkpoint_interval_ms(opts: Option<&HashMap<String, String>>) -> Option<u64> {
    opts.and_then(|m| m.get("checkpoint.interval"))
        .and_then(|s| parse_positive_u64(s))
}

pub fn parse_pipeline_parallelism(opts: Option<&HashMap<String, String>>) -> Option<u32> {
    opts.and_then(|m| m.get("parallelism"))
        .and_then(|s| parse_positive_u32(s))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_checkpoint_and_parallelism() {
        let mut m = HashMap::new();
        m.insert("checkpoint.interval".to_string(), "30000".to_string());
        m.insert("parallelism".to_string(), "2".to_string());
        assert_eq!(parse_checkpoint_interval_ms(Some(&m)), Some(30_000));
        assert_eq!(parse_pipeline_parallelism(Some(&m)), Some(2));
    }
}
