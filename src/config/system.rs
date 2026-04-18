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

use std::io;

pub struct SystemMemoryInfo {
    pub total_physical: u64,
    pub available_physical: u64,
    pub total_virtual: u64,
    pub available_virtual: u64,
}

pub fn system_memory_info() -> io::Result<SystemMemoryInfo> {
    sys::system_memory_info()
}

#[cfg(target_os = "linux")]
mod sys {
    use super::SystemMemoryInfo;
    use std::io;

    pub fn system_memory_info() -> io::Result<SystemMemoryInfo> {
        let content = std::fs::read_to_string("/proc/meminfo")?;

        let mut total_physical: Option<u64> = None;
        let mut available_physical: Option<u64> = None;
        let mut swap_total: u64 = 0;
        let mut swap_free: u64 = 0;

        for line in content.lines() {
            if let Some(v) = parse_meminfo_kb(line, "MemTotal:") {
                total_physical = Some(v);
            } else if let Some(v) = parse_meminfo_kb(line, "MemAvailable:") {
                available_physical = Some(v);
            } else if let Some(v) = parse_meminfo_kb(line, "SwapTotal:") {
                swap_total = v;
            } else if let Some(v) = parse_meminfo_kb(line, "SwapFree:") {
                swap_free = v;
            }
        }

        let total_phys = total_physical.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "MemTotal not found in /proc/meminfo",
            )
        })?;
        let avail_phys = available_physical.unwrap_or(0);

        Ok(SystemMemoryInfo {
            total_physical: total_phys,
            available_physical: avail_phys,
            total_virtual: total_phys + swap_total,
            available_virtual: avail_phys + swap_free,
        })
    }

    fn parse_meminfo_kb(line: &str, prefix: &str) -> Option<u64> {
        let rest = line.strip_prefix(prefix)?;
        let kb: u64 = rest.trim().trim_end_matches("kB").trim().parse().ok()?;
        Some(kb * 1024)
    }
}

#[cfg(target_os = "macos")]
mod sys {
    use super::SystemMemoryInfo;
    use std::io;

    pub fn system_memory_info() -> io::Result<SystemMemoryInfo> {
        let total_physical = sysctl_u64("hw.memsize")?;

        let page_size = sysctl_u64("hw.pagesize").unwrap_or(4096);
        let vm_stats = read_vm_stat()?;

        let free_pages = vm_stats.free + vm_stats.inactive + vm_stats.purgeable;
        let available_physical = free_pages * page_size;

        let swap = read_swap_usage();
        let swap_total = swap.0;
        let swap_free = swap_total.saturating_sub(swap.1);

        Ok(SystemMemoryInfo {
            total_physical,
            available_physical,
            total_virtual: total_physical + swap_total,
            available_virtual: available_physical + swap_free,
        })
    }

    fn sysctl_u64(name: &str) -> io::Result<u64> {
        let output = std::process::Command::new("sysctl")
            .arg("-n")
            .arg(name)
            .output()?;
        if !output.status.success() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("sysctl {name} failed"),
            ));
        }
        String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    struct VmPages {
        free: u64,
        inactive: u64,
        purgeable: u64,
    }

    fn read_vm_stat() -> io::Result<VmPages> {
        let output = std::process::Command::new("vm_stat").output()?;
        let text = String::from_utf8_lossy(&output.stdout);

        let mut free = 0u64;
        let mut inactive = 0u64;
        let mut purgeable = 0u64;

        for line in text.lines() {
            if let Some(v) = parse_vm_stat_line(line, "Pages free") {
                free = v;
            } else if let Some(v) = parse_vm_stat_line(line, "Pages inactive") {
                inactive = v;
            } else if let Some(v) = parse_vm_stat_line(line, "Pages purgeable") {
                purgeable = v;
            }
        }

        Ok(VmPages {
            free,
            inactive,
            purgeable,
        })
    }

    fn parse_vm_stat_line(line: &str, key: &str) -> Option<u64> {
        if !line.contains(key) {
            return None;
        }
        let val_str = line.rsplit(':').next()?.trim().trim_end_matches('.');
        val_str.parse().ok()
    }

    fn read_swap_usage() -> (u64, u64) {
        let output = match std::process::Command::new("sysctl")
            .arg("-n")
            .arg("vm.swapusage")
            .output()
        {
            Ok(o) => o,
            Err(_) => return (0, 0),
        };
        let text = String::from_utf8_lossy(&output.stdout);
        let mut total = 0u64;
        let mut used = 0u64;
        for part in text.split_whitespace() {
            if let Some(mb_str) = part.strip_suffix("M") {
                if let Ok(mb) = mb_str.parse::<f64>() {
                    if total == 0 {
                        total = (mb * 1024.0 * 1024.0) as u64;
                    } else if used == 0 {
                        used = (mb * 1024.0 * 1024.0) as u64;
                    }
                }
            }
        }
        (total, used)
    }
}

#[cfg(target_os = "windows")]
mod sys {
    use super::SystemMemoryInfo;
    use std::io;

    #[repr(C)]
    struct MemoryStatusEx {
        dw_length: u32,
        dw_memory_load: u32,
        ull_total_phys: u64,
        ull_avail_phys: u64,
        ull_total_page_file: u64,
        ull_avail_page_file: u64,
        ull_total_virtual: u64,
        ull_avail_virtual: u64,
        ull_avail_extended_virtual: u64,
    }

    extern "system" {
        fn GlobalMemoryStatusEx(lpBuffer: *mut MemoryStatusEx) -> i32;
    }

    pub fn system_memory_info() -> io::Result<SystemMemoryInfo> {
        unsafe {
            let mut status = std::mem::zeroed::<MemoryStatusEx>();
            status.dw_length = std::mem::size_of::<MemoryStatusEx>() as u32;
            if GlobalMemoryStatusEx(&mut status) == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(SystemMemoryInfo {
                total_physical: status.ull_total_phys,
                available_physical: status.ull_avail_phys,
                total_virtual: status.ull_total_virtual,
                available_virtual: status.ull_avail_virtual,
            })
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
mod sys {
    use super::SystemMemoryInfo;
    use std::io;

    pub fn system_memory_info() -> io::Result<SystemMemoryInfo> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "memory detection not supported on this platform",
        ))
    }
}
