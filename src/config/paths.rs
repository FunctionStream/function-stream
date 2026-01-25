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

use std::fs;
use std::path::{Path, PathBuf};

fn get_exe_dir() -> Option<PathBuf> {
    std::env::current_exe().ok()?.parent().map(PathBuf::from)
}

fn search_paths<F>(mut check: F) -> Option<PathBuf>
where
    F: FnMut(&Path) -> bool,
{
    let cwd = PathBuf::from(".");
    if check(cwd.as_path()) {
        return Some(cwd);
    }

    if let Some(exe_dir) = get_exe_dir() {
        if check(exe_dir.as_path()) {
            return Some(exe_dir);
        }

        if let Some(parent) = exe_dir.parent() {
            if check(parent) {
                return Some(parent.to_path_buf());
            }
        }
    }

    None
}

pub fn find_config_file(config_name: &str) -> Option<PathBuf> {
    let config_path = PathBuf::from(config_name);
    if config_path.exists() {
        return Some(config_path);
    }

    search_paths(|base| base.join(config_name).exists())
        .or_else(|| {
            get_exe_dir().and_then(|exe_dir| {
                exe_dir.parent().and_then(|parent| {
                    let path = parent.join("conf").join(config_name);
                    path.exists().then_some(path)
                })
            })
        })
        .or_else(|| {
            std::env::var("FUNCTION_STREAM_CONFIG")
                .ok()
                .map(PathBuf::from)
                .filter(|p| p.exists())
        })
}

fn find_or_create_dir(dir_name: &str) -> std::io::Result<PathBuf> {
    let cwd_dir = PathBuf::from(dir_name);
    if cwd_dir.exists() {
        return Ok(cwd_dir);
    }

    if let Some(exe_dir) = get_exe_dir() {
        if let Some(parent) = exe_dir.parent() {
            let dir = parent.join(dir_name);
            if dir.exists() {
                return Ok(dir);
            }
            fs::create_dir_all(&dir)?;
            return Ok(dir);
        }

        let dir = exe_dir.join(dir_name);
        if dir.exists() {
            return Ok(dir);
        }
    }

    fs::create_dir_all(&cwd_dir)?;
    Ok(cwd_dir)
}

pub fn find_or_create_data_dir() -> std::io::Result<PathBuf> {
    find_or_create_dir("data")
}

pub fn find_or_create_conf_dir() -> std::io::Result<PathBuf> {
    find_or_create_dir("conf")
}

pub fn find_or_create_logs_dir() -> std::io::Result<PathBuf> {
    find_or_create_dir("logs")
}
