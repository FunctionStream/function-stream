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

use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;

pub const ENV_HOME: &str = "FUNCTION_STREAM_HOME";
pub const ENV_CONF: &str = "FUNCTION_STREAM_CONF";

static PROJECT_ROOT: OnceLock<PathBuf> = OnceLock::new();

pub fn get_project_root() -> &'static PathBuf {
    PROJECT_ROOT
        .get_or_init(|| resolve_project_root().expect("CRITICAL: Failed to resolve project root"))
}

fn resolve_project_root() -> std::io::Result<PathBuf> {
    if let Ok(home) = env::var(ENV_HOME) {
        let path = PathBuf::from(&home);
        return path.canonicalize().or(Ok(path));
    }

    if let Ok(manifest_dir) = env::var("CARGO_MANIFEST_DIR") {
        return Ok(PathBuf::from(manifest_dir));
    }

    if let Ok(exe_path) = env::current_exe() {
        let mut path = exe_path;
        path.pop();
        if path.file_name().map_or(false, |n| n == "bin") {
            path.pop();
        }
        return Ok(path);
    }

    env::current_dir()
}

pub fn resolve_path(input_path: &str) -> PathBuf {
    let path = PathBuf::from(input_path);
    if path.is_absolute() {
        path
    } else {
        get_project_root().join(path)
    }
}

fn to_absolute_path(input_path: &str) -> PathBuf {
    resolve_path(input_path)
}

pub fn find_config_file(config_name: &str) -> Option<PathBuf> {
    if let Ok(conf_env) = env::var(ENV_CONF) {
        let path = to_absolute_path(&conf_env);
        if path.is_file() {
            return Some(path);
        }
        if path.is_dir() {
            let full = path.join(config_name);
            if full.exists() {
                return Some(full);
            }
        }
    }

    let search_paths = vec![
        get_conf_dir().join(config_name),
        get_project_root().join(config_name),
    ];

    for path in search_paths {
        if path.exists() {
            return Some(path.canonicalize().unwrap_or(path));
        }
    }

    None
}

fn get_or_create_sub_dir(name: &str) -> PathBuf {
    let dir = get_project_root().join(name);
    if !dir.exists() {
        let _ = fs::create_dir_all(&dir);
    }
    dir
}

pub fn get_data_dir() -> PathBuf {
    get_or_create_sub_dir("data")
}

pub fn get_logs_dir() -> PathBuf {
    get_or_create_sub_dir("logs")
}

pub fn get_conf_dir() -> PathBuf {
    get_or_create_sub_dir("conf")
}

pub fn get_task_dir() -> PathBuf {
    get_or_create_sub_dir("data/task")
}

pub fn get_state_dir() -> PathBuf {
    get_or_create_sub_dir("data/state")
}

pub fn get_state_dir_for_base(base: &str) -> PathBuf {
    resolve_path(base).join("state")
}

pub fn get_app_log_path() -> PathBuf {
    get_logs_dir().join("app.log")
}

pub fn get_log_path(relative: &str) -> PathBuf {
    get_logs_dir().join(relative)
}

pub fn get_wasm_cache_dir() -> PathBuf {
    get_or_create_sub_dir("data/cache/wasm-incremental")
}

pub fn get_python_cache_dir() -> PathBuf {
    get_or_create_sub_dir("data/cache/python-runner")
}

pub fn get_python_wasm_path() -> PathBuf {
    get_python_cache_dir().join("functionstream-python-runtime.wasm")
}

pub fn get_python_cwasm_path() -> PathBuf {
    get_python_cache_dir().join("functionstream-python-runtime.cwasm")
}
