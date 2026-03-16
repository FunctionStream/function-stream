use serde::{Deserialize, Serialize};
use std::env;

/// A string that may contain `{{ VAR }}` placeholders for environment variable substitution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct VarStr {
    raw_val: String,
}

impl VarStr {
    pub fn new(raw_val: String) -> Self {
        VarStr { raw_val }
    }

    pub fn raw(&self) -> &str {
        &self.raw_val
    }

    /// Substitute `{{ VAR_NAME }}` patterns with the corresponding environment variable values.
    pub fn sub_env_vars(&self) -> anyhow::Result<String> {
        let mut result = self.raw_val.clone();
        let mut start = 0;

        while let Some(open) = result[start..].find("{{") {
            let open_abs = start + open;
            let Some(close) = result[open_abs..].find("}}") else {
                break;
            };
            let close_abs = open_abs + close;

            let var_name = result[open_abs + 2..close_abs].trim();
            if var_name.is_empty() {
                start = close_abs + 2;
                continue;
            }

            match env::var(var_name) {
                Ok(value) => {
                    let full_match = &result[open_abs..close_abs + 2];
                    let full_match_owned = full_match.to_string();
                    result = result.replacen(&full_match_owned, &value, 1);
                    start = open_abs + value.len();
                }
                Err(_) => {
                    anyhow::bail!("Environment variable {} not found", var_name);
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_placeholders() {
        let input = "This is a test string with no placeholders";
        assert_eq!(
            VarStr::new(input.to_string()).sub_env_vars().unwrap(),
            input
        );
    }

    #[test]
    fn test_with_placeholders() {
        unsafe { env::set_var("FS_TEST_VAR", "environment variable") };
        let input = "This is a {{ FS_TEST_VAR }}";
        let expected = "This is a environment variable";
        assert_eq!(
            VarStr::new(input.to_string()).sub_env_vars().unwrap(),
            expected
        );
        unsafe { env::remove_var("FS_TEST_VAR") };
    }
}
