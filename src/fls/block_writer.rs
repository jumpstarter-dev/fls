use std::alloc::{alloc, dealloc, Layout};
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::ptr::NonNull;
use tokio::sync::mpsc;

const BLOCK_SIZE: usize = 1024 * 1024; // 1MB blocks for better throughput
const ALIGNMENT: usize = 4096; // 4KB alignment for direct I/O

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

/// Aligned buffer for direct I/O operations
/// This ensures the buffer memory is aligned to ALIGNMENT bytes as required by O_DIRECT
struct AlignedBuffer {
    ptr: NonNull<u8>,
    layout: Layout,
}

impl AlignedBuffer {
    fn new(size: usize, alignment: usize) -> Self {
        let layout = Layout::from_size_align(size, alignment).expect("Invalid layout");
        let ptr = unsafe {
            let raw_ptr = alloc(layout);
            if raw_ptr.is_null() {
                panic!("Failed to allocate aligned buffer");
            }
            // Zero the buffer
            std::ptr::write_bytes(raw_ptr, 0, size);
            NonNull::new_unchecked(raw_ptr)
        };
        Self { ptr, layout }
    }

    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.layout.size()) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.layout.size()) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

// Safety: AlignedBuffer owns its data and the pointer is valid for the lifetime of the struct
unsafe impl Send for AlignedBuffer {}

/// BlockWriter handles writing data to a block device with direct I/O
pub(crate) struct BlockWriter {
    file: std::fs::File,
    buffer: AlignedBuffer,
    buffer_pos: usize, // Current position in buffer
    bytes_written: u64,
    bytes_since_sync: u64, // Track bytes written since last sync
    written_progress_tx: mpsc::UnboundedSender<u64>,
    use_direct_io: bool, // Track if O_DIRECT is active
    #[allow(dead_code)]
    debug: bool, // Debug mode flag
}

// Sync every 64MB when not using direct I/O
const SYNC_INTERVAL: u64 = 64 * 1024 * 1024;

impl BlockWriter {
    /// Open a block device for writing with direct I/O
    pub(crate) fn new(
        device: &str,
        written_progress_tx: mpsc::UnboundedSender<u64>,
        debug: bool,
        o_direct: bool,
    ) -> io::Result<Self> {
        #[cfg(target_os = "linux")]
        let (file, use_direct_io) = {
            if o_direct {
                // Try O_DIRECT without O_SYNC first (some devices have issues with both)
                match OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .custom_flags(libc::O_DIRECT)
                    .open(device)
                {
                    Ok(f) => {
                        if debug {
                            eprintln!(
                                "[DEBUG] Opened {} with O_DIRECT (direct I/O enabled)",
                                device
                            );
                        }
                        (f, true)
                    }
                    Err(e1) => {
                        // Try O_DIRECT with O_SYNC
                        match OpenOptions::new()
                            .write(true)
                            .create(true)
                            .truncate(true)
                            .custom_flags(libc::O_DIRECT | libc::O_SYNC)
                            .open(device)
                        {
                            Ok(f) => {
                                if debug {
                                    eprintln!(
                                        "[DEBUG] Opened {} with O_DIRECT|O_SYNC (direct I/O enabled)",
                                        device
                                    );
                                }
                                (f, true)
                            }
                            Err(e2) => {
                                // Fail if O_DIRECT was explicitly requested but not available
                                return Err(io::Error::new(
                                    io::ErrorKind::Unsupported,
                                    format!(
                                        "O_DIRECT not supported on {} (tried without O_SYNC: {}, with O_SYNC: {})",
                                        device, e1, e2
                                    ),
                                ));
                            }
                        }
                    }
                }
            } else {
                // Use buffered I/O when O_DIRECT is not requested
                if debug {
                    eprintln!(
                        "[DEBUG] Opened {} with buffered I/O (O_DIRECT not requested)",
                        device
                    );
                }
                let f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(device)?;
                (f, false)
            }
        };

        #[cfg(target_os = "macos")]
        let (file, use_direct_io) = {
            use std::os::unix::io::AsRawFd;

            if o_direct {
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
                        return Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            "F_NOCACHE not supported on this device",
                        ));
                    } else {
                        if debug {
                            eprintln!(
                                "[DEBUG] Opened {} with F_NOCACHE (direct I/O enabled)",
                                device
                            );
                        }
                        true
                    }
                };
                (f, direct_io)
            } else {
                // Use buffered I/O when O_DIRECT is not requested
                if debug {
                    eprintln!(
                        "[DEBUG] Opened {} with buffered I/O (O_DIRECT not requested)",
                        device
                    );
                }
                let f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(device)?;
                (f, false)
            }
        };

        // Create aligned buffer for direct I/O
        // Direct I/O requires buffer to be aligned to sector size (typically 512 or 4096)
        let buffer = AlignedBuffer::new(BLOCK_SIZE, ALIGNMENT);

        Ok(Self {
            file,
            buffer,
            buffer_pos: 0,
            bytes_written: 0,
            bytes_since_sync: 0,
            written_progress_tx,
            use_direct_io,
            debug,
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
            let buffer_slice = self.buffer.as_mut_slice();
            buffer_slice[self.buffer_pos..self.buffer_pos + to_copy]
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
                let _ = self.written_progress_tx.send(self.bytes_written);
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

        let buffer_slice = self.buffer.as_slice();
        self.file
            .write_all(&buffer_slice[..write_size])
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
        let _ = self.written_progress_tx.send(self.bytes_written);
        Ok(())
    }

    /// Get total bytes written
    pub(crate) fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

/// Async wrapper for BlockWriter that runs in a blocking task
pub(crate) struct AsyncBlockWriter {
    writer_tx: mpsc::Sender<Vec<u8>>,
    writer_handle: tokio::task::JoinHandle<io::Result<u64>>,
}

impl AsyncBlockWriter {
    /// Create a new async block writer
    pub(crate) fn new(
        device: String,
        written_progress_tx: mpsc::UnboundedSender<u64>,
        debug: bool,
        o_direct: bool,
        write_buffer_size_mb: usize,
    ) -> io::Result<Self> {
        // Calculate channel capacity based on buffer size
        // Assuming 8MB chunks from decompressor reader (see from_url.rs line 47)
        const CHUNK_SIZE_MB: usize = 8;
        let channel_capacity = (write_buffer_size_mb / CHUNK_SIZE_MB).max(1);

        if debug {
            eprintln!(
                "[DEBUG] Write buffer: {} MB (capacity: {} chunks of {}MB each)",
                write_buffer_size_mb, channel_capacity, CHUNK_SIZE_MB
            );
        }

        // Create bounded channel to prevent OOM when decompression outpaces disk writes
        // This provides backpressure when writing is slower than decompression
        let (writer_tx, mut writer_rx) = mpsc::channel::<Vec<u8>>(channel_capacity);

        // Spawn blocking task for I/O operations
        let writer_handle = tokio::task::spawn_blocking(move || {
            let mut writer = BlockWriter::new(&device, written_progress_tx, debug, o_direct)
                .map_err(|e| {
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
            .await
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
