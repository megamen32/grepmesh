//! Short native snapshots, shared by status and admission; no monitor process.
use anyhow::{anyhow, Context, Result};
use serde::Serialize;
use std::{
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};

#[derive(Clone, Debug, Serialize)]
pub(crate) struct HostLoad {
    pub cpu_utilization_percent: f64,
    pub available_memory_bytes: u64,
    pub memory_total_bytes: u64,
    pub cgroup_cpu_utilization_percent: Option<f64>,
    pub cgroup_memory_remaining_bytes: Option<u64>,
    pub cgroup_tasks_remaining: Option<u64>,
    pub sample_ms: u64,
    pub apple_gpu_utilization_percent: Option<f64>,
    pub apple_gpu_core_count: Option<u64>,
}

impl HostLoad {
    pub fn utilization(&self) -> f64 {
        self.cpu_utilization_percent
            .max(self.cgroup_cpu_utilization_percent.unwrap_or(0.0))
    }
    pub fn admission(&self) -> &'static str {
        if !self.utilization().is_finite() || self.utilization() >= 85.0 {
            "host_cpu_busy"
        } else if self.available_memory_bytes < self.memory_reserve_bytes()
            || self
                .cgroup_memory_remaining_bytes
                .is_some_and(|n| n < 512 * 1024 * 1024)
        {
            "host_memory_busy"
        } else if self.cgroup_tasks_remaining.is_some_and(|n| n < 16) {
            "host_tasks_busy"
        } else {
            "available"
        }
    }
    fn memory_reserve_bytes(&self) -> u64 {
        if self.memory_total_bytes >= 64 * 1024 * 1024 * 1024 {
            20 * 1024 * 1024 * 1024
        } else if self.memory_total_bytes >= 32 * 1024 * 1024 * 1024 {
            4 * 1024 * 1024 * 1024
        } else if self.memory_total_bytes >= 8 * 1024 * 1024 * 1024 {
            2 * 1024 * 1024 * 1024
        } else {
            1024 * 1024 * 1024
        }
    }
}

struct Raw {
    total: u64,
    idle: u64,
    available: u64,
    memory_total: u64,
    cgroup: Vec<Group>,
    apple_gpu: Option<(f64, u64)>,
}
struct Group {
    identity: (u64, u64),
    usage: u64,
    cpu_limit: Option<f64>,
    memory_remaining: Option<u64>,
    tasks_remaining: Option<u64>,
}

pub(crate) async fn observe() -> Result<HostLoad> {
    let started = Instant::now();
    let first = raw()?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let second = raw()?;
    let total = second
        .total
        .checked_sub(first.total)
        .filter(|n| *n > 0)
        .ok_or_else(|| anyhow!("native CPU sample has no valid delta"))?;
    let idle = second
        .idle
        .checked_sub(first.idle)
        .filter(|n| *n <= total)
        .ok_or_else(|| anyhow!("native CPU idle counter changed generation"))?;
    if first.cgroup.len() != second.cgroup.len() {
        return Err(anyhow!("native cgroup observation changed generation"));
    }
    let mut cpu_group: Option<f64> = None;
    for (a, b) in first.cgroup.iter().zip(&second.cgroup) {
        if a.identity != b.identity || a.cpu_limit != b.cpu_limit {
            return Err(anyhow!("native cgroup quota changed during sample"));
        }
        if let Some(limit) = b.cpu_limit {
            let delta = b
                .usage
                .checked_sub(a.usage)
                .ok_or_else(|| anyhow!("native cgroup usage changed generation"))?;
            let percent = (delta as f64 / 1_000_000.0 / started.elapsed().as_secs_f64() / limit
                * 100.0)
                .clamp(0.0, 100.0);
            cpu_group = Some(cpu_group.unwrap_or(0.0).max(percent));
        }
    }
    Ok(HostLoad {
        cpu_utilization_percent: (1.0 - idle as f64 / total as f64) * 100.0,
        available_memory_bytes: second.available,
        memory_total_bytes: second.memory_total,
        cgroup_cpu_utilization_percent: cpu_group,
        cgroup_memory_remaining_bytes: second
            .cgroup
            .iter()
            .filter_map(|g| g.memory_remaining)
            .min(),
        cgroup_tasks_remaining: second.cgroup.iter().filter_map(|g| g.tasks_remaining).min(),
        sample_ms: started.elapsed().as_millis() as u64,
        apple_gpu_utilization_percent: second.apple_gpu.map(|g| g.0),
        apple_gpu_core_count: second.apple_gpu.map(|g| g.1),
    })
}

#[cfg(target_os = "linux")]
fn raw() -> Result<Raw> {
    let stat = fs::read_to_string("/proc/stat").context("read native CPU statistics")?;
    let ticks: Vec<u64> = stat
        .lines()
        .next()
        .filter(|line| line.starts_with("cpu "))
        .ok_or_else(|| anyhow!("native CPU header missing"))?
        .split_whitespace()
        .skip(1)
        .take(8)
        .map(str::parse)
        .collect::<std::result::Result<_, _>>()?;
    if ticks.len() < 5 {
        return Err(anyhow!("native CPU counters missing"));
    }
    let mem = fs::read_to_string("/proc/meminfo")?;
    let memory = |key: &str| -> Result<u64> {
        let line = mem
            .lines()
            .find(|l| l.starts_with(key))
            .ok_or_else(|| anyhow!("native memory counter missing"))?;
        Ok(line
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| anyhow!("invalid memory counter"))?
            .parse::<u64>()?
            .checked_mul(1024)
            .ok_or_else(|| anyhow!("memory counter overflow"))?)
    };
    let available = memory("MemAvailable:")?;
    let memory_total = memory("MemTotal:")?;
    if available > memory_total {
        return Err(anyhow!("invalid native available memory"));
    }
    Ok(Raw {
        total: ticks.iter().sum(),
        idle: ticks[3],
        available,
        memory_total,
        cgroup: linux_group()?,
        apple_gpu: None,
    })
}

#[cfg(target_os = "linux")]
fn linux_group() -> Result<Vec<Group>> {
    let path = fs::read_to_string("/proc/self/cgroup")?;
    let Some(relative) = path.lines().find_map(|l| l.strip_prefix("0::")) else {
        return Err(anyhow!("native unified cgroup is unavailable"));
    };
    if !relative.starts_with('/') || relative.split('/').any(|p| p == "..") {
        return Err(anyhow!("invalid native cgroup path"));
    }
    let root = PathBuf::from("/sys/fs/cgroup");
    let mut directory = root.join(relative.trim_start_matches('/'));
    let mut groups = Vec::new();
    for _ in 0..32 {
        if directory == root {
            return Ok(groups);
        }
        groups.push(read_linux_group(&directory)?);
        if !directory.pop() {
            break;
        }
    }
    Err(anyhow!("native cgroup ancestry exceeds observation bound"))
}

#[cfg(target_os = "linux")]
fn read_linux_group(directory: &std::path::Path) -> Result<Group> {
    use std::os::unix::fs::MetadataExt;
    let before = fs::metadata(directory)?;
    let stat = fs::read_to_string(directory.join("cpu.stat"))?;
    let usage = stat
        .lines()
        .find_map(|l| l.strip_prefix("usage_usec "))
        .ok_or_else(|| anyhow!("native cgroup CPU usage missing"))?
        .parse()?;
    let cpu = fs::read_to_string(directory.join("cpu.max"))?;
    let fields: Vec<_> = cpu.split_whitespace().collect();
    if fields.len() != 2 {
        return Err(anyhow!("invalid cgroup CPU quota"));
    }
    let period: f64 = fields[1].parse()?;
    if !period.is_finite() || period <= 0.0 {
        return Err(anyhow!("invalid cgroup CPU period"));
    }
    let cpu_limit = if fields[0] == "max" {
        None
    } else {
        let quota: f64 = fields[0].parse()?;
        if !quota.is_finite() || quota <= 0.0 {
            return Err(anyhow!("invalid cgroup CPU quota"));
        }
        Some(quota / period)
    };
    let remaining = |current: &str, maximum: &str| -> Result<Option<u64>> {
        let max = fs::read_to_string(directory.join(maximum))?;
        if max.trim() == "max" {
            return Ok(None);
        }
        let max: u64 = max.trim().parse()?;
        let used: u64 = fs::read_to_string(directory.join(current))?
            .trim()
            .parse()?;
        Ok(Some(max.saturating_sub(used)))
    };
    let group = Group {
        identity: (before.dev(), before.ino()),
        usage,
        cpu_limit,
        memory_remaining: remaining("memory.current", "memory.max")?,
        tasks_remaining: remaining("pids.current", "pids.max")?,
    };
    let after = fs::metadata(directory)?;
    if group.identity != (after.dev(), after.ino()) {
        return Err(anyhow!("native cgroup directory changed generation"));
    }
    Ok(group)
}

#[cfg(target_os = "macos")]
fn raw() -> Result<Raw> {
    // Stable Mach counters; no shell/powermetrics/sudo process per status call.
    unsafe {
        let host = libc::mach_host_self();
        extern "C" {
            fn mach_port_deallocate(task: u32, name: u32) -> i32;
        }
        struct HostPort(libc::mach_port_t);
        impl Drop for HostPort {
            fn drop(&mut self) {
                unsafe {
                    mach_port_deallocate(libc::mach_task_self(), self.0);
                }
            }
        }
        let _port = HostPort(host);
        let mut ticks = [0i32; 4];
        let mut count = 4;
        if libc::host_statistics(
            host,
            libc::HOST_CPU_LOAD_INFO,
            ticks.as_mut_ptr(),
            &mut count,
        ) != 0
            || count != 4
        {
            return Err(anyhow!("Mach CPU observation failed"));
        }
        let mut vm: libc::vm_statistics64 = std::mem::zeroed();
        let mut count = libc::HOST_VM_INFO64_COUNT;
        if libc::host_statistics64(
            host,
            libc::HOST_VM_INFO64,
            &mut vm as *mut _ as *mut i32,
            &mut count,
        ) != 0
        {
            return Err(anyhow!("Mach memory observation failed"));
        }
        let sysctl = |name: &[u8]| -> Result<u64> {
            let mut value = 0u64;
            let mut len = std::mem::size_of_val(&value);
            if libc::sysctlbyname(
                name.as_ptr().cast(),
                &mut value as *mut _ as *mut libc::c_void,
                &mut len,
                std::ptr::null_mut(),
                0,
            ) != 0
                || len != 8
            {
                return Err(anyhow!("native memory size unavailable"));
            }
            Ok(value)
        };
        let total = sysctl(b"hw.memsize\0")?;
        // Count only reclaimable inactive plus free pages, not compressed memory.
        let page_size = libc::sysconf(libc::_SC_PAGESIZE);
        if page_size <= 0 {
            return Err(anyhow!("Mach page size unavailable"));
        }
        let available = (vm.free_count as u64 + vm.inactive_count as u64)
            .checked_mul(page_size as u64)
            .ok_or_else(|| anyhow!("Mach memory overflow"))?;
        if available > total {
            return Err(anyhow!("invalid Mach available memory"));
        }
        Ok(Raw {
            total: ticks.iter().map(|n| *n as u32 as u64).sum(),
            idle: ticks[2] as u32 as u64,
            available,
            memory_total: total,
            cgroup: Vec::new(),
            apple_gpu: apple_gpu().ok(),
        })
    }
}

#[cfg(target_os = "macos")]
fn apple_gpu() -> Result<(f64, u64)> {
    use std::ffi::{c_char, c_void};
    #[link(name = "IOKit", kind = "framework")]
    extern "C" {
        fn IOServiceMatching(name: *const c_char) -> *mut c_void;
        fn IOServiceGetMatchingServices(
            port: u32,
            matching: *mut c_void,
            iterator: *mut u32,
        ) -> i32;
        fn IOIteratorNext(iterator: u32) -> u32;
        fn IOObjectRelease(object: u32) -> i32;
        fn IORegistryEntryCreateCFProperty(
            entry: u32,
            key: *const c_void,
            allocator: *const c_void,
            options: u32,
        ) -> *const c_void;
    }
    #[link(name = "CoreFoundation", kind = "framework")]
    extern "C" {
        fn CFStringCreateWithCString(
            allocator: *const c_void,
            value: *const c_char,
            encoding: u32,
        ) -> *const c_void;
        fn CFDictionaryGetValue(dictionary: *const c_void, key: *const c_void) -> *const c_void;
        fn CFGetTypeID(value: *const c_void) -> usize;
        fn CFNumberGetTypeID() -> usize;
        fn CFDictionaryGetTypeID() -> usize;
        fn CFNumberGetValue(number: *const c_void, kind: i32, value: *mut c_void) -> u8;
        fn CFRelease(value: *const c_void);
    }
    struct Object(u32);
    impl Drop for Object {
        fn drop(&mut self) {
            unsafe {
                IOObjectRelease(self.0);
            }
        }
    }
    struct OwnedCF(*const c_void);
    impl Drop for OwnedCF {
        fn drop(&mut self) {
            unsafe {
                if !self.0.is_null() {
                    CFRelease(self.0);
                }
            }
        }
    }
    unsafe {
        let key = |text: &[u8]| {
            OwnedCF(CFStringCreateWithCString(
                std::ptr::null(),
                text.as_ptr().cast(),
                0x08000100,
            ))
        };
        let number = |pointer: *const c_void| -> Result<u64> {
            if pointer.is_null() || CFGetTypeID(pointer) != CFNumberGetTypeID() {
                return Err(anyhow!("GPU counter unavailable"));
            }
            let mut value = 0i64;
            if CFNumberGetValue(pointer, 4, (&mut value as *mut i64).cast()) == 0 || value < 0 {
                return Err(anyhow!("GPU counter invalid"));
            }
            Ok(value as u64)
        };
        let matching = IOServiceMatching(b"IOAccelerator\0".as_ptr().cast());
        if matching.is_null() {
            return Err(anyhow!("GPU registry class unavailable"));
        }
        let mut iterator = 0;
        // The matching dictionary is consumed by this API.
        if IOServiceGetMatchingServices(0, matching, &mut iterator) != 0 {
            return Err(anyhow!("GPU registry observation failed"));
        }
        let iterator = Object(iterator);
        let service = IOIteratorNext(iterator.0);
        if service == 0 {
            return Err(anyhow!("GPU registry device unavailable"));
        }
        let service = Object(service);
        let stats_key = key(b"PerformanceStatistics\0");
        let util_key = key(b"Device Utilization %\0");
        let cores_key = key(b"gpu-core-count\0");
        if stats_key.0.is_null() || util_key.0.is_null() || cores_key.0.is_null() {
            return Err(anyhow!("GPU counter keys unavailable"));
        }
        let stats = OwnedCF(IORegistryEntryCreateCFProperty(
            service.0,
            stats_key.0,
            std::ptr::null(),
            0,
        ));
        if stats.0.is_null() || CFGetTypeID(stats.0) != CFDictionaryGetTypeID() {
            return Err(anyhow!("GPU counters unavailable"));
        }
        let utilization = number(CFDictionaryGetValue(stats.0, util_key.0))?;
        let cores = OwnedCF(IORegistryEntryCreateCFProperty(
            service.0,
            cores_key.0,
            std::ptr::null(),
            0,
        ));
        let cores = number(cores.0)?;
        if utilization > 100 || cores == 0 || cores > 1024 {
            return Err(anyhow!("GPU counters out of range"));
        }
        Ok((utilization as f64, cores))
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn raw() -> Result<Raw> {
    Err(anyhow!("native load observation is unsupported"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn capacity_and_quota_load_reject_busy_hosts_without_using_load_average() {
        let mut load = HostLoad {
            cpu_utilization_percent: 10.0,
            available_memory_bytes: 4 << 30,
            memory_total_bytes: 8 << 30,
            cgroup_cpu_utilization_percent: Some(90.0),
            cgroup_memory_remaining_bytes: Some(1 << 30),
            cgroup_tasks_remaining: Some(64),
            sample_ms: 200,
            apple_gpu_utilization_percent: None,
            apple_gpu_core_count: None,
        };
        assert_eq!(load.admission(), "host_cpu_busy");
        load.cgroup_cpu_utilization_percent = Some(20.0);
        assert_eq!(load.admission(), "available");
        load.cgroup_tasks_remaining = Some(0);
        assert_eq!(load.admission(), "host_tasks_busy");
        load.cgroup_tasks_remaining = Some(64);
        load.available_memory_bytes = 512 << 20;
        assert_eq!(load.admission(), "host_memory_busy");
    }
    #[tokio::test]
    async fn actual_native_load_snapshot_has_valid_capacity_and_finite_counters() {
        let load = observe().await.unwrap();
        assert!(load.cpu_utilization_percent.is_finite());
        assert!(load.available_memory_bytes <= load.memory_total_bytes);
        assert!(load.sample_ms >= 190 && load.sample_ms < 2000);
    }
}
