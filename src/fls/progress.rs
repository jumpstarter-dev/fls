use std::io::{self, Write};
use std::time::{Duration, Instant};

use crate::fls::memory;

/// Format seconds into a human-readable time string (e.g., "1h30m45s", "5m30s", or "45s")
fn format_time(secs: f64) -> String {
    let total_secs = secs as u64;
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;

    if hours > 0 {
        format!("{}h{}m{}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m{}s", minutes, seconds)
    } else {
        // Show fractional seconds for times under 1 minute
        format!("{:.2}s", secs)
    }
}

pub struct FinalStats {
    pub mb_received: f64,
    pub mb_decompressed: f64,
    pub mb_written: f64,
    pub download_rate: f64,
    pub decompress_rate: f64,
    pub write_rate: f64,
    pub download_secs: f64,
    pub decompress_secs: f64,
    pub write_secs: f64,
    pub total_secs: f64,
}

impl FinalStats {
    /// Format download time as human-readable string
    pub fn download_time_formatted(&self) -> String {
        format_time(self.download_secs)
    }

    /// Format decompress time as human-readable string
    pub fn decompress_time_formatted(&self) -> String {
        format_time(self.decompress_secs)
    }

    /// Format write time as human-readable string
    pub fn write_time_formatted(&self) -> String {
        format_time(self.write_secs)
    }

    /// Format total runtime as human-readable string
    pub fn total_time_formatted(&self) -> String {
        format_time(self.total_secs)
    }
}

pub(crate) struct ProgressTracker {
    pub(crate) bytes_received: u64,
    pub(crate) bytes_decompressed: u64,
    pub(crate) bytes_written: u64,
    pub(crate) bytes_sent_to_decompressor: u64, // Track how much compressed data has been sent to decompressor
    pub(crate) start_time: Instant,
    last_update: Instant,
    // Store final rates when each phase completes
    pub(crate) final_download_rate: Option<f64>,
    pub(crate) final_decompress_rate: Option<f64>,
    pub(crate) final_write_rate: Option<f64>,
    // Store durations when each phase completes
    pub(crate) download_duration: Option<Duration>,
    pub(crate) decompress_duration: Option<Duration>,
    pub(crate) write_duration: Option<Duration>,
    // Store content length for percentage calculations
    pub(crate) content_length: Option<u64>,
    // Track if we're actually decompressing (not using cat)
    is_compressed: bool,
    // Whether to print on new lines instead of clearing and rewriting
    newline_progress: bool,
    // Whether to show memory statistics
    show_memory: bool,
}

impl ProgressTracker {
    pub(crate) fn new(newline_progress: bool, show_memory: bool) -> Self {
        let now = Instant::now();
        Self {
            bytes_received: 0,
            bytes_decompressed: 0,
            bytes_written: 0,
            bytes_sent_to_decompressor: 0,
            start_time: now,
            last_update: now,
            final_download_rate: None,
            final_decompress_rate: None,
            final_write_rate: None,
            download_duration: None,
            decompress_duration: None,
            write_duration: None,
            content_length: None,
            is_compressed: true,
            newline_progress,
            show_memory,
        }
    }

    pub(crate) fn set_content_length(&mut self, length: Option<u64>) {
        self.content_length = length;
    }

    pub(crate) fn set_is_compressed(&mut self, is_compressed: bool) {
        self.is_compressed = is_compressed;
    }

    pub(crate) fn update_progress(
        &mut self,
        content_length: Option<u64>,
        update_interval: Duration,
        force: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let now = Instant::now();
        if force || now.duration_since(self.last_update) >= update_interval {
            let elapsed = now.duration_since(self.start_time);
            let mb_received = self.bytes_received as f64 / (1024.0 * 1024.0);
            let mb_decompressed = self.bytes_decompressed as f64 / (1024.0 * 1024.0);
            let mb_written = self.bytes_written as f64 / (1024.0 * 1024.0);

            // Use stored final rates if available, otherwise calculate current rates
            let download_mb_per_sec = self.final_download_rate.unwrap_or_else(|| {
                if elapsed.as_secs_f64() > 0.0 {
                    mb_received / elapsed.as_secs_f64()
                } else {
                    0.0
                }
            });
            let written_mb_per_sec = self.final_write_rate.unwrap_or_else(|| {
                if elapsed.as_secs_f64() > 0.0 {
                    mb_written / elapsed.as_secs_f64()
                } else {
                    0.0
                }
            });

            // Format each phase - show "Done" if completed, otherwise show progress
            let download_status = if self.final_download_rate.is_some() {
                "Done".to_string()
            } else if let Some(total) = content_length {
                let progress = (self.bytes_received as f64 / total as f64) * 100.0;
                let total_mb = total as f64 / (1024.0 * 1024.0);
                format!(
                    "{:.2} MB / {:.2} MB ({:.1}%) | {:.2} MB/s",
                    mb_received, total_mb, progress, download_mb_per_sec
                )
            } else {
                format!("{:.2} MB | {:.2} MB/s", mb_received, download_mb_per_sec)
            };

            let decompress_status = if self.final_decompress_rate.is_some() {
                "Done".to_string()
            } else {
                // Show percentage based on how much compressed data has been sent to decompressor
                if let Some(total) = self.content_length {
                    let decompress_progress =
                        (self.bytes_sent_to_decompressor as f64 / total as f64) * 100.0;

                    // Create a simple inline progress bar
                    // Use 10 chars while downloading, 20 chars once download is complete
                    let bar_width = if self.final_download_rate.is_some() {
                        20
                    } else {
                        10
                    };
                    let filled = ((decompress_progress / 100.0) * bar_width as f64) as usize;
                    let filled = filled.min(bar_width);
                    let bar = format!("[{}{}]", "█".repeat(filled), "░".repeat(bar_width - filled));

                    format!("{} {:.1}%", bar, decompress_progress)
                } else {
                    format!("{:.2} MB", mb_decompressed)
                }
            };

            let write_status = if self.final_write_rate.is_some() {
                "Done".to_string()
            } else {
                format!("{:.2} MB | {:.2} MB/s", mb_written, written_mb_per_sec)
            };

            // Use ANSI escape code to clear to end of line to avoid leftover text
            let progress_label = if self.is_compressed {
                "Decompressed"
            } else {
                "Progress"
            };

            // Get memory stats if enabled
            let memory_suffix = if self.show_memory {
                memory::get_memory_stats()
                    .map(|stats| format!(" | {}", stats.format_compact()))
                    .unwrap_or_default()
            } else {
                String::new()
            };

            if self.newline_progress {
                // Print on a new line
                println!(
                    "Download: {} | {}: {} | Written: {}{}",
                    download_status, progress_label, decompress_status, write_status, memory_suffix
                );
            } else {
                // Use carriage return and clear line
                print!(
                    "\r\x1b[KDownload: {} | {}: {} | Written: {}{}",
                    download_status, progress_label, decompress_status, write_status, memory_suffix
                );
                io::stdout().flush()?;
            }
            self.last_update = now;
        }
        Ok(())
    }

    pub(crate) fn final_stats(&self) -> FinalStats {
        let final_mb_received = self.bytes_received as f64 / (1024.0 * 1024.0);
        let final_mb_decompressed = self.bytes_decompressed as f64 / (1024.0 * 1024.0);
        let final_mb_written = self.bytes_written as f64 / (1024.0 * 1024.0);

        // Calculate total runtime from start to now
        let total_secs = self.start_time.elapsed().as_secs_f64();

        // Use the stored durations for each phase
        let download_secs = self
            .download_duration
            .unwrap_or_else(|| Duration::from_secs(0))
            .as_secs_f64();
        let decompress_secs = self
            .decompress_duration
            .unwrap_or_else(|| Duration::from_secs(0))
            .as_secs_f64();
        let write_secs = self
            .write_duration
            .unwrap_or_else(|| Duration::from_secs(0))
            .as_secs_f64();

        let final_download_rate = if download_secs > 0.0 {
            final_mb_received / download_secs
        } else {
            0.0
        };
        let final_decompress_rate = if decompress_secs > 0.0 {
            final_mb_decompressed / decompress_secs
        } else {
            0.0
        };
        let final_written_rate = if write_secs > 0.0 {
            final_mb_written / write_secs
        } else {
            0.0
        };

        FinalStats {
            mb_received: final_mb_received,
            mb_decompressed: final_mb_decompressed,
            mb_written: final_mb_written,
            download_rate: final_download_rate,
            decompress_rate: final_decompress_rate,
            write_rate: final_written_rate,
            download_secs,
            decompress_secs,
            write_secs,
            total_secs,
        }
    }

    /// Print final statistics to stdout
    ///
    /// This consolidates the common stats printing pattern used by both
    /// URL and OCI flash operations.
    pub(crate) fn print_final_stats(&self) {
        let stats = self.final_stats();
        println!(
            "\nDownload complete: {:.2} MB in {} ({:.2} MB/s)",
            stats.mb_received,
            stats.download_time_formatted(),
            stats.download_rate
        );
        println!(
            "Decompression complete: {:.2} MB in {} ({:.2} MB/s)",
            stats.mb_decompressed,
            stats.decompress_time_formatted(),
            stats.decompress_rate
        );
        println!(
            "Write complete: {:.2} MB in {} ({:.2} MB/s)",
            stats.mb_written,
            stats.write_time_formatted(),
            stats.write_rate
        );
        println!("Total flash runtime: {}", stats.total_time_formatted());
    }

    /// Print final statistics including compression ratio (for URL flash)
    pub(crate) fn print_final_stats_with_ratio(&self) {
        let stats = self.final_stats();
        println!(
            "\nDownload complete: {:.2} MB in {} ({:.2} MB/s)",
            stats.mb_received,
            stats.download_time_formatted(),
            stats.download_rate
        );
        println!(
            "Decompression complete: {:.2} MB in {} ({:.2} MB/s)",
            stats.mb_decompressed,
            stats.decompress_time_formatted(),
            stats.decompress_rate
        );
        println!(
            "Write complete: {:.2} MB in {} ({:.2} MB/s)",
            stats.mb_written,
            stats.write_time_formatted(),
            stats.write_rate
        );
        if stats.mb_received > 0.0 {
            println!(
                "Compression ratio: {:.2}x",
                stats.mb_decompressed / stats.mb_received
            );
        }
        println!("Total flash runtime: {}", stats.total_time_formatted());
    }
}
