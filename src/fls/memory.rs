#[cfg(target_os = "linux")]
use std::fs;

#[derive(Debug, Clone)]
pub struct MemoryStats {
    pub process_rss_mb: f64,
    pub process_swap_mb: f64,
    pub system_free_mb: f64,
    pub system_total_mb: f64,
    pub system_cached_mb: f64,
}

impl MemoryStats {
    /// Format memory stats in a compact format for display
    /// Example: "Mem: 1.2G/4G (proc: 250M+50Msw cache: 1.5G)"
    pub fn format_compact(&self) -> String {
        // MemAvailable already accounts for reclaimable cache, so don't subtract it again
        let system_used_mb = self.system_total_mb - self.system_free_mb;

        format!(
            "Mem: {}/{}  (proc: {}{}  cache: {})",
            format_mb(system_used_mb),
            format_mb(self.system_total_mb),
            format_mb(self.process_rss_mb),
            if self.process_swap_mb > 0.0 {
                format!("+{}sw", format_mb(self.process_swap_mb))
            } else {
                String::new()
            },
            format_mb(self.system_cached_mb),
        )
    }
}

/// Format MB value to human-readable format (M or G)
fn format_mb(mb: f64) -> String {
    if mb >= 1024.0 {
        format!("{:.1}G", mb / 1024.0)
    } else {
        format!("{:.0}M", mb)
    }
}

/// Get memory statistics from the system
/// Returns None if reading fails or not on Linux
pub fn get_memory_stats() -> Option<MemoryStats> {
    #[cfg(target_os = "linux")]
    {
        get_memory_stats_linux()
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(target_os = "linux")]
fn get_memory_stats_linux() -> Option<MemoryStats> {
    // Read process memory from /proc/self/status
    let (process_rss_mb, process_swap_mb) = read_process_memory()?;

    // Read system memory from /proc/meminfo
    let (system_total_mb, system_free_mb, system_cached_mb) = read_system_memory()?;

    Some(MemoryStats {
        process_rss_mb,
        process_swap_mb,
        system_free_mb,
        system_total_mb,
        system_cached_mb,
    })
}

#[cfg(target_os = "linux")]
fn read_process_memory() -> Option<(f64, f64)> {
    let status = fs::read_to_string("/proc/self/status").ok()?;

    let mut rss_kb = 0u64;
    let mut swap_kb = 0u64;

    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            // VmRSS: 123456 kB
            rss_kb = line.split_whitespace().nth(1)?.parse().ok()?;
        } else if line.starts_with("VmSwap:") {
            // VmSwap: 12345 kB
            swap_kb = line.split_whitespace().nth(1)?.parse().ok()?;
        }
    }

    let rss_mb = rss_kb as f64 / 1024.0;
    let swap_mb = swap_kb as f64 / 1024.0;

    Some((rss_mb, swap_mb))
}

#[cfg(target_os = "linux")]
fn read_system_memory() -> Option<(f64, f64, f64)> {
    let meminfo = fs::read_to_string("/proc/meminfo").ok()?;

    let mut total_kb = 0u64;
    let mut free_kb = 0u64;
    let mut available_kb = 0u64;
    let mut cached_kb = 0u64;
    let mut buffers_kb = 0u64;

    for line in meminfo.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 2 {
            continue;
        }

        let value: u64 = parts[1].parse().ok()?;

        match parts[0] {
            "MemTotal:" => total_kb = value,
            "MemFree:" => free_kb = value,
            "MemAvailable:" => available_kb = value,
            "Cached:" => cached_kb = value,
            "Buffers:" => buffers_kb = value,
            _ => {}
        }
    }

    // Use MemAvailable if present (more accurate), otherwise use MemFree
    let effective_free_kb = if available_kb > 0 {
        available_kb
    } else {
        free_kb
    };

    // Cache includes buffers and cached
    let effective_cached_kb = cached_kb + buffers_kb;

    let total_mb = total_kb as f64 / 1024.0;
    let free_mb = effective_free_kb as f64 / 1024.0;
    let cached_mb = effective_cached_kb as f64 / 1024.0;

    Some((total_mb, free_mb, cached_mb))
}
