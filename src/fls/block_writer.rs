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

/// BmapBlockWriter handles writing data using BMAP
pub(crate) struct BmapBlockWriter {
    file: std::fs::File,
    bytes_written: u64,
    bytes_since_sync: u64,
    written_progress_tx: mpsc::UnboundedSender<u64>,
    use_direct_io: bool,
    debug: bool,
    bmap: crate::fls::bmap_parser::BmapFile,
    decompressed_offset: u64,
}

// Sync every 64MB when not using direct I/O
const SYNC_INTERVAL: u64 = 64 * 1024 * 1024;

/// Open a block device for writing with optional direct I/O
/// Returns (File, use_direct_io)
fn open_block_device(
    device: &str,
    is_block_dev: bool,
    o_direct: bool,
    debug: bool,
    debug_prefix: &str,
) -> io::Result<(std::fs::File, bool)> {
    #[cfg(target_os = "linux")]
    {
        if o_direct {
            // Try O_DIRECT without O_SYNC first (some devices have issues with both)
            let mut opts = OpenOptions::new();
            opts.write(true);
            // Only use create/truncate for regular files, not block devices
            if !is_block_dev {
                opts.create(true).truncate(true);
            }
            match opts.custom_flags(libc::O_DIRECT).open(device) {
                Ok(f) => {
                    if debug {
                        eprintln!(
                            "{} Opened {} with O_DIRECT (direct I/O enabled)",
                            debug_prefix, device
                        );
                    }
                    Ok((f, true))
                }
                Err(e1) => {
                    // Try O_DIRECT with O_SYNC
                    let mut opts = OpenOptions::new();
                    opts.write(true);
                    if !is_block_dev {
                        opts.create(true).truncate(true);
                    }
                    match opts
                        .custom_flags(libc::O_DIRECT | libc::O_SYNC)
                        .open(device)
                    {
                        Ok(f) => {
                            if debug {
                                eprintln!(
                                    "{} Opened {} with O_DIRECT|O_SYNC (direct I/O enabled)",
                                    debug_prefix, device
                                );
                            }
                            Ok((f, true))
                        }
                        Err(e2) => {
                            // Fail if O_DIRECT was explicitly requested but not available
                            Err(io::Error::new(
                                io::ErrorKind::Unsupported,
                                format!(
                                    "O_DIRECT not supported on {} (tried without O_SYNC: {}, with O_SYNC: {})",
                                    device, e1, e2
                                ),
                            ))
                        }
                    }
                }
            }
        } else {
            // Use buffered I/O when O_DIRECT is not requested
            if debug {
                eprintln!(
                    "{} Opened {} with buffered I/O (O_DIRECT not requested)",
                    debug_prefix, device
                );
            }
            let mut opts = OpenOptions::new();
            opts.write(true);
            if !is_block_dev {
                opts.create(true).truncate(true);
            }
            let f = opts.open(device)?;
            Ok((f, false))
        }
    }

    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;

        if o_direct {
            // macOS uses F_NOCACHE instead of O_DIRECT
            let mut opts = OpenOptions::new();
            opts.write(true);
            if !is_block_dev {
                opts.create(true).truncate(true);
            }
            let f = opts.custom_flags(libc::O_SYNC).open(device)?;

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
                            "{} Opened {} with F_NOCACHE (direct I/O enabled)",
                            debug_prefix, device
                        );
                    }
                    true
                }
            };
            Ok((f, direct_io))
        } else {
            // Use buffered I/O when O_DIRECT is not requested
            if debug {
                eprintln!(
                    "{} Opened {} with buffered I/O (O_DIRECT not requested)",
                    debug_prefix, device
                );
            }
            let mut opts = OpenOptions::new();
            opts.write(true);
            if !is_block_dev {
                opts.create(true).truncate(true);
            }
            let f = opts.open(device)?;
            Ok((f, false))
        }
    }
}

impl BlockWriter {
    /// Open a block device for writing with direct I/O
    pub(crate) fn new(
        device: &str,
        written_progress_tx: mpsc::UnboundedSender<u64>,
        debug: bool,
        o_direct: bool,
    ) -> io::Result<Self> {
        // Check if this is a block device
        let is_block_dev = is_block_device(device).unwrap_or(false);
        let (file, use_direct_io) =
            open_block_device(device, is_block_dev, o_direct, debug, "[DEBUG]")?;

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

    /// Write data to the block device sequentially
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
        let data_to_write = &buffer_slice[..write_size];

        self.file.write_all(data_to_write).map_err(|e| {
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

impl BmapBlockWriter {
    /// Open a block device for writing with BMAP (only writes mapped blocks)
    pub(crate) fn new(
        device: &str,
        written_progress_tx: mpsc::UnboundedSender<u64>,
        debug: bool,
        o_direct: bool,
        bmap: crate::fls::bmap_parser::BmapFile,
    ) -> io::Result<Self> {
        // Check if this is a block device
        let is_block_dev = is_block_device(device).unwrap_or(false);
        let (file, use_direct_io) =
            open_block_device(device, is_block_dev, o_direct, debug, "[BMAP]")?;

        Ok(Self {
            file,
            bytes_written: 0,
            bytes_since_sync: 0,
            written_progress_tx,
            use_direct_io,
            debug,
            bmap,
            decompressed_offset: 0,
        })
    }

    /// Write data using BMAP
    pub(crate) fn write(&mut self, data: &[u8]) -> io::Result<()> {
        use std::os::unix::io::AsRawFd;

        let fd = self.file.as_raw_fd();
        let mut data_offset = 0;

        while data_offset < data.len() {
            let chunk_size = data.len() - data_offset;
            let chunk = &data[data_offset..];
            let chunk_start = self.decompressed_offset;
            let chunk_end = chunk_start + chunk_size as u64;

            // Find all mapped ranges that overlap with this chunk
            for range in &self.bmap.ranges {
                let range_start_byte = range.start_block * self.bmap.block_size;
                let range_end_byte = (range.end_block + 1) * self.bmap.block_size;

                // Check if this chunk overlaps with this range
                if chunk_start < range_end_byte && chunk_end > range_start_byte {
                    // Calculate the overlapping portion
                    let overlap_start = chunk_start.max(range_start_byte);
                    let overlap_end = chunk_end.min(range_end_byte);

                    if overlap_start < overlap_end {
                        // Calculate offsets within the chunk
                        let chunk_overlap_start = (overlap_start - chunk_start) as usize;
                        let chunk_overlap_end = (overlap_end - chunk_start) as usize;
                        let overlap_data = &chunk[chunk_overlap_start..chunk_overlap_end];

                        // Calculate device offset for this range
                        let device_offset = overlap_start;

                        // Write using pwrite() at the specific device offset
                        let written = unsafe {
                            libc::pwrite(
                                fd,
                                overlap_data.as_ptr() as *const libc::c_void,
                                overlap_data.len(),
                                device_offset as i64,
                            )
                        };

                        if written < 0 {
                            return Err(io::Error::last_os_error());
                        }

                        let written = written as usize;
                        if written != overlap_data.len() {
                            return Err(io::Error::new(
                                io::ErrorKind::WriteZero,
                                format!(
                                    "pwrite() wrote {} bytes, expected {}",
                                    written,
                                    overlap_data.len()
                                ),
                            ));
                        }

                        self.bytes_written += written as u64;
                        self.bytes_since_sync += written as u64;

                        if self.debug && self.bytes_written.is_multiple_of(100 * 1024 * 1024) {
                            eprintln!(
                                "[BMAP] Wrote {} bytes at device offset {} (decompressed offset: {})",
                                written, device_offset, overlap_start
                            );
                        }
                    }
                }
            }

            // Advance decompressed stream position
            self.decompressed_offset += chunk_size as u64;
            data_offset += chunk_size;

            // Send progress update periodically
            if self.bytes_written.is_multiple_of(256 * 1024) {
                let _ = self.written_progress_tx.send(self.bytes_written);
            }

            // Sync periodically for buffered I/O
            if !self.use_direct_io && self.bytes_since_sync >= SYNC_INTERVAL {
                self.file.sync_data()?;
                self.bytes_since_sync = 0;
            }
        }

        Ok(())
    }

    /// Flush any remaining data and sync to disk
    pub(crate) fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.file.sync_all()?;
        self.bytes_since_sync = 0;
        let _ = self.written_progress_tx.send(self.bytes_written);
        Ok(())
    }

    /// Get total bytes written
    pub(crate) fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

/// Trait for block writers that can write data and flush to disk
trait BlockWriterTrait: Send {
    fn write(&mut self, data: &[u8]) -> io::Result<()>;
    fn flush(&mut self) -> io::Result<()>;
    fn bytes_written(&self) -> u64;
}

impl BlockWriterTrait for BlockWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<()> {
        BlockWriter::write(self, data)
    }

    fn flush(&mut self) -> io::Result<()> {
        BlockWriter::flush(self)
    }

    fn bytes_written(&self) -> u64 {
        BlockWriter::bytes_written(self)
    }
}

impl BlockWriterTrait for BmapBlockWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<()> {
        BmapBlockWriter::write(self, data)
    }

    fn flush(&mut self) -> io::Result<()> {
        BmapBlockWriter::flush(self)
    }

    fn bytes_written(&self) -> u64 {
        BmapBlockWriter::bytes_written(self)
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
        bmap: Option<crate::fls::bmap_parser::BmapFile>,
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
            let mut writer: Box<dyn BlockWriterTrait> = if let Some(bmap) = bmap {
                let writer =
                    BmapBlockWriter::new(&device, written_progress_tx, debug, o_direct, bmap)
                        .map_err(|e| {
                            eprintln!("Failed to open device '{}': {}", device, e);
                            e
                        })?;
                Box::new(writer)
            } else {
                let writer = BlockWriter::new(&device, written_progress_tx, debug, o_direct)
                    .map_err(|e| {
                        eprintln!("Failed to open device '{}': {}", device, e);
                        e
                    })?;
                Box::new(writer)
            };

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
    /// Returns bytes_written
    pub(crate) async fn close(self) -> io::Result<u64> {
        drop(self.writer_tx);
        self.writer_handle.await.map_err(io::Error::other)?
    }
}

/// Check if a path is a block device
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
