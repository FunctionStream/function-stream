// Configuration file path resolution

use std::path::PathBuf;
use std::fs;

/// Find configuration file in multiple locations
/// Priority order:
/// 1. Current working directory
/// 2. Executable directory/../conf (for distribution/functionstream structure)
/// 3. Executable directory (for installed binaries)
/// 4. Executable directory/../conf (for installed binaries)
/// 5. Executable directory/.. (for installed binaries)
/// 6. Environment variable FUNCTION_STREAM_CONFIG
pub fn find_config_file(config_name: &str) -> Option<PathBuf> {
    // 1. Check current working directory
    let cwd_path = PathBuf::from(config_name);
    if cwd_path.exists() {
        return Some(cwd_path);
    }

    // 2. Check executable directory
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            // Check in executable directory/../conf (for distribution/functionstream/bin/../conf)
            if let Some(parent) = exe_dir.parent() {
                let conf_dir_path = parent.join("conf").join(config_name);
                if conf_dir_path.exists() {
                    return Some(conf_dir_path);
                }
            }

            // Check in executable directory
            let exe_dir_path = exe_dir.join(config_name);
            if exe_dir_path.exists() {
                return Some(exe_dir_path);
            }

            // Check in executable directory/../conf
            let conf_dir_path = exe_dir.parent()
                .map(|p| p.join("conf").join(config_name));
            if let Some(ref path) = conf_dir_path {
                if path.exists() {
                    return Some(path.clone());
                }
            }

            // Check in executable directory/..
            let parent_path = exe_dir.parent()
                .map(|p| p.join(config_name));
            if let Some(ref path) = parent_path {
                if path.exists() {
                    return Some(path.clone());
                }
            }
        }
    }

    // 3. Check environment variable
    if let Ok(env_path) = std::env::var("FUNCTION_STREAM_CONFIG") {
        let env_path_buf = PathBuf::from(&env_path);
        if env_path_buf.exists() {
            return Some(env_path_buf);
        }
    }

    None
}

/// Find and ensure data directory exists
/// Priority order:
/// 1. Current working directory/data
/// 2. Executable directory/../data (for distribution/functionstream structure)
/// 3. Executable directory/data
/// 4. Executable directory/../data
/// Creates the directory if it doesn't exist
pub fn find_or_create_data_dir() -> std::io::Result<PathBuf> {
    // 1. Check current working directory
    let cwd_data = PathBuf::from("data");
    if cwd_data.exists() {
        return Ok(cwd_data);
    }

    // 2. Check executable directory
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            // Check in executable directory/../data (for distribution/functionstream/bin/../data)
            if let Some(parent) = exe_dir.parent() {
                let data_dir = parent.join("data");
                if data_dir.exists() {
                    return Ok(data_dir);
                }
                // Create if doesn't exist
                fs::create_dir_all(&data_dir)?;
                return Ok(data_dir);
            }

            // Check in executable directory/data
            let exe_data = exe_dir.join("data");
            if exe_data.exists() {
                return Ok(exe_data);
            }

            // Check in executable directory/../data
            if let Some(parent) = exe_dir.parent() {
                let parent_data = parent.join("data");
                if parent_data.exists() {
                    return Ok(parent_data);
                }
                // Create if doesn't exist
                fs::create_dir_all(&parent_data)?;
                return Ok(parent_data);
            }
        }
    }

    // Default: create in current working directory
    fs::create_dir_all(&cwd_data)?;
    Ok(cwd_data)
}

/// Find and ensure conf directory exists
/// Priority order:
/// 1. Current working directory/conf
/// 2. Executable directory/../conf (for distribution/functionstream structure)
/// 3. Executable directory/conf
/// 4. Executable directory/../conf
/// Creates the directory if it doesn't exist
pub fn find_or_create_conf_dir() -> std::io::Result<PathBuf> {
    // 1. Check current working directory
    let cwd_conf = PathBuf::from("conf");
    if cwd_conf.exists() {
        return Ok(cwd_conf);
    }

    // 2. Check executable directory
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            // Check in executable directory/../conf (for distribution/functionstream/bin/../conf)
            if let Some(parent) = exe_dir.parent() {
                let conf_dir = parent.join("conf");
                if conf_dir.exists() {
                    return Ok(conf_dir);
                }
                // Create if doesn't exist
                fs::create_dir_all(&conf_dir)?;
                return Ok(conf_dir);
            }

            // Check in executable directory/conf
            let exe_conf = exe_dir.join("conf");
            if exe_conf.exists() {
                return Ok(exe_conf);
            }

            // Check in executable directory/../conf
            if let Some(parent) = exe_dir.parent() {
                let parent_conf = parent.join("conf");
                if parent_conf.exists() {
                    return Ok(parent_conf);
                }
                // Create if doesn't exist
                fs::create_dir_all(&parent_conf)?;
                return Ok(parent_conf);
            }
        }
    }

    // Default: create in current working directory
    fs::create_dir_all(&cwd_conf)?;
    Ok(cwd_conf)
}

/// Find and ensure logs directory exists
/// Priority order:
/// 1. Current working directory/logs
/// 2. Executable directory/../logs (for distribution/functionstream structure)
/// 3. Executable directory/logs
/// 4. Executable directory/../logs
/// Creates the directory if it doesn't exist
pub fn find_or_create_logs_dir() -> std::io::Result<PathBuf> {
    // 1. Check current working directory
    let cwd_logs = PathBuf::from("logs");
    if cwd_logs.exists() {
        return Ok(cwd_logs);
    }

    // 2. Check executable directory
    if let Ok(exe_path) = std::env::current_exe() {
        if let Some(exe_dir) = exe_path.parent() {
            // Check in executable directory/../logs (for distribution/functionstream/bin/../logs)
            if let Some(parent) = exe_dir.parent() {
                let logs_dir = parent.join("logs");
                if logs_dir.exists() {
                    return Ok(logs_dir);
                }
                // Create if doesn't exist
                fs::create_dir_all(&logs_dir)?;
                return Ok(logs_dir);
            }

            // Check in executable directory/logs
            let exe_logs = exe_dir.join("logs");
            if exe_logs.exists() {
                return Ok(exe_logs);
            }

            // Check in executable directory/../logs
            if let Some(parent) = exe_dir.parent() {
                let parent_logs = parent.join("logs");
                if parent_logs.exists() {
                    return Ok(parent_logs);
                }
                // Create if doesn't exist
                fs::create_dir_all(&parent_logs)?;
                return Ok(parent_logs);
            }
        }
    }

    // Default: create in current working directory
    fs::create_dir_all(&cwd_logs)?;
    Ok(cwd_logs)
}

