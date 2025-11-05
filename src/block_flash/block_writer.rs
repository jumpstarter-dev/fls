use std::fs::OpenOptions;
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use tokio::sync::mpsc;

const BLOCK_SIZE: usize = 1024 * 1024; // 1MB blocks for better throughput
const ALIGNMENT: usize = 4096; // 4KB alignment for direct I/O

// Platform-specific constants
#[cfg(target_os = "linux")]
const O_DIRECT: i32 = 0x4000; // O_DIRECT flag for Linux

#[cfg(target_os = "macos")]
#[allow(dead_code)]
const F_NOCACHE: i32 = 48; // F_NOCACHE for macOS (use with fcntl)

// Linux ioctl definitions using nix crate's ioctl macros for safety
#[cfg(target_os = "linux")]
mod linux_ioctl {
    use nix::ioctl_read;
    // BLKGETSIZE64 - get device size in bytes (u64)
    // This uses the nix crate to generate the correct ioctl number for the target architecture
    ioctl_read!(blkgetsize64, 0x12, 114, u64);
}

#[cfg(target_os = "macos")]
#[allow(dead_code)]
const DKIOCGETBLOCKCOUNT: libc::c_ulong = 0x40086419;

#[cfg(target_os = "macos")]
#[allow(dead_code)]
const DKIOCGETBLOCKSIZE: libc::c_ulong = 0x40046418;

/// BlockWriter handles writing data to a block device with direct I/O
pub(crate) struct BlockWriter {
    file: std::fs::File,
    buffer: Vec<u8>,
    buffer_pos: usize, // Current position in buffer
    bytes_written: u64,
    bytes_since_sync: u64, // Track bytes written since last sync
    written_tx: mpsc::UnboundedSender<u64>,
    use_direct_io: bool, // Track if O_DIRECT is active
}

// Sync every 64MB when not using direct I/O
const SYNC_INTERVAL: u64 = 64 * 1024 * 1024;

impl BlockWriter {
    /// Open a block device for writing with direct I/O
    pub(crate) fn new(device: &str, written_tx: mpsc::UnboundedSender<u64>) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        let (file, use_direct_io) = {
            // Try O_DIRECT without O_SYNC first (some devices have issues with both)
            match OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .custom_flags(O_DIRECT)
                .open(device)
            {
                Ok(f) => {
                    eprintln!("Opened {} with O_DIRECT (direct I/O enabled)", device);
                    (f, true)
                }
                Err(e1) => {
                    // Try O_DIRECT with O_SYNC
                    match OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .custom_flags(O_DIRECT | libc::O_SYNC)
                        .open(device)
                    {
                        Ok(f) => {
                            eprintln!(
                                "Opened {} with O_DIRECT|O_SYNC (direct I/O enabled)",
                                device
                            );
                            (f, true)
                        }
                        Err(e2) => {
                            // Fall back to regular buffered I/O without O_SYNC for better performance
                            // We'll sync explicitly during flush instead
                            eprintln!("Warning: O_DIRECT not supported on {} (tried without O_SYNC: {}, with O_SYNC: {})", 
                                     device, e1, e2);
                            eprintln!(
                                "Falling back to buffered I/O (writes will be synced on flush)"
                            );
                            let f = OpenOptions::new()
                                .write(true)
                                .create(true)
                                .truncate(true)
                                .open(device)?;
                            (f, false)
                        }
                    }
                }
            }
        };

        #[cfg(target_os = "macos")]
        let (file, use_direct_io) = {
            use std::os::unix::io::AsRawFd;
            // macOS uses F_NOCACHE instead of O_DIRECT
            let f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .custom_flags(libc::O_SYNC)
                .open(device)?;

            let fd = f.as_raw_fd();
            let direct_io = unsafe {
                // Enable F_NOCACHE to bypass filesystem cache
                if libc::fcntl(fd, libc::F_NOCACHE, 1) == -1 {
                    eprintln!("Warning: F_NOCACHE not supported, using buffered I/O");
                    false
                } else {
                    eprintln!("Opened {} with F_NOCACHE (direct I/O enabled)", device);
                    true
                }
            };
            (f, direct_io)
        };

        // Create aligned buffer for direct I/O
        // Direct I/O requires buffer to be aligned to sector size (typically 512 or 4096)
        let buffer = vec![0u8; BLOCK_SIZE];

        Ok(Self {
            file,
            buffer,
            buffer_pos: 0,
            bytes_written: 0,
            bytes_since_sync: 0,
            written_tx,
            use_direct_io,
        })
    }

    /// Write data to the block device
    /// Data is buffered internally and written in larger blocks for better performance
    pub(crate) fn write(&mut self, data: &[u8]) -> io::Result<()> {
        let mut offset = 0;

        while offset < data.len() {
            let remaining_in_buffer = BLOCK_SIZE - self.buffer_pos;
            let remaining_in_data = data.len() - offset;
            let to_copy = remaining_in_buffer.min(remaining_in_data);

            // Copy to internal buffer
            self.buffer[self.buffer_pos..self.buffer_pos + to_copy]
                .copy_from_slice(&data[offset..offset + to_copy]);

            self.buffer_pos += to_copy;
            offset += to_copy;
            self.bytes_written += to_copy as u64;

            // Flush buffer when full
            if self.buffer_pos == BLOCK_SIZE {
                self.flush_buffer()?;
            }

            // Send progress update periodically (every 256KB to reduce overhead)
            if self.bytes_written.is_multiple_of(256 * 1024) {
                let _ = self.written_tx.send(self.bytes_written);
            }
        }

        Ok(())
    }

    /// Flush the internal buffer to disk
    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.buffer_pos == 0 {
            return Ok(());
        }

        let write_size = if self.use_direct_io && self.buffer_pos < BLOCK_SIZE {
            // For direct I/O, align to sector size
            self.buffer_pos.div_ceil(ALIGNMENT) * ALIGNMENT
        } else {
            self.buffer_pos
        };

        let write_size = write_size.min(BLOCK_SIZE);

        self.file
            .write_all(&self.buffer[..write_size])
            .map_err(|e| {
                eprintln!("Write error at offset {}: {}", self.bytes_written, e);
                e
            })?;

        self.bytes_since_sync += write_size as u64;
        self.buffer_pos = 0;

        // For buffered I/O, sync periodically to avoid losing too much data on failure
        if !self.use_direct_io && self.bytes_since_sync >= SYNC_INTERVAL {
            self.file.sync_data()?;
            self.bytes_since_sync = 0;
        }

        Ok(())
    }

    /// Flush any remaining data and sync to disk
    pub(crate) fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        // Always do a final sync to ensure data is on disk
        self.file.flush()?;
        self.file.sync_all()?;
        self.bytes_since_sync = 0;
        // Send final progress update
        let _ = self.written_tx.send(self.bytes_written);
        Ok(())
    }

    /// Get total bytes written
    pub(crate) fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

/// Async wrapper for BlockWriter that runs in a blocking task
pub(crate) struct AsyncBlockWriter {
    writer_tx: mpsc::UnboundedSender<Vec<u8>>,
    writer_handle: tokio::task::JoinHandle<io::Result<u64>>,
}

impl AsyncBlockWriter {
    /// Create a new async block writer
    pub(crate) fn new(device: String, written_tx: mpsc::UnboundedSender<u64>) -> io::Result<Self> {
        let (writer_tx, mut writer_rx) = mpsc::unbounded_channel::<Vec<u8>>();

        // Spawn blocking task for I/O operations
        let writer_handle = tokio::task::spawn_blocking(move || {
            let mut writer = BlockWriter::new(&device, written_tx).map_err(|e| {
                eprintln!("Failed to open device '{}': {}", device, e);
                e
            })?;

            while let Some(data) = writer_rx.blocking_recv() {
                if let Err(e) = writer.write(&data) {
                    eprintln!("Failed to write to device '{}': {}", device, e);
                    return Err(e);
                }
            }

            writer.flush()?;
            Ok(writer.bytes_written())
        });

        Ok(Self {
            writer_tx,
            writer_handle,
        })
    }

    /// Write data asynchronously
    pub(crate) async fn write(&self, data: Vec<u8>) -> io::Result<()> {
        self.writer_tx
            .send(data)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Writer channel closed"))?;
        Ok(())
    }

    /// Close the writer and wait for completion
    pub(crate) async fn close(self) -> io::Result<u64> {
        drop(self.writer_tx);
        self.writer_handle.await.map_err(io::Error::other)?
    }
}

/// Check if a path is a block device
#[allow(dead_code)]
pub(crate) fn is_block_device(path: &str) -> io::Result<bool> {
    use std::os::unix::fs::FileTypeExt;
    let metadata = std::fs::metadata(path)?;
    Ok(metadata.file_type().is_block_device())
}

/// Get block device size in bytes
#[allow(dead_code)]
pub(crate) fn get_device_size(path: &str) -> io::Result<u64> {
    use std::os::unix::fs::OpenOptionsExt;
    use std::os::unix::io::AsRawFd;

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_RDONLY)
        .open(path)?;

    let fd = file.as_raw_fd();

    #[cfg(target_os = "linux")]
    {
        let mut size: u64 = 0;
        // Use nix crate's generated ioctl - this is architecture-safe
        unsafe {
            linux_ioctl::blkgetsize64(fd, &mut size)
                .map_err(|e| io::Error::from_raw_os_error(e as i32))?;
        }
        Ok(size)
    }

    #[cfg(target_os = "macos")]
    {
        unsafe {
            let mut size: u64 = 0;
            if libc::ioctl(fd, DKIOCGETBLOCKCOUNT, &mut size) == 0 {
                let mut block_size: u32 = 0;
                if libc::ioctl(fd, DKIOCGETBLOCKSIZE, &mut block_size) == 0 {
                    Ok(size * block_size as u64)
                } else {
                    Err(io::Error::last_os_error())
                }
            } else {
                Err(io::Error::last_os_error())
            }
        }
    }
}
