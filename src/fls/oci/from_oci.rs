/// Flash from OCI image
///
/// Implements the streaming pipeline:
/// Registry blob -> gzip decompress -> tar extract -> xzcat -> block device
use std::io::{Read, Write};
use std::path::Path;
use std::time::Duration;

use flate2::read::GzDecoder;
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use super::manifest::{LayerCompression, Manifest};
use super::reference::ImageReference;
use super::registry::RegistryClient;
use crate::fls::block_writer::AsyncBlockWriter;
use crate::fls::compression::Compression;
use crate::fls::decompress::{spawn_stderr_reader, start_decompressor_process};
use crate::fls::error_handling::process_error_messages;
use crate::fls::magic_bytes::{detect_content_and_compression, ContentType};
use crate::fls::options::OciOptions;
use crate::fls::progress::ProgressTracker;
use crate::fls::stream_utils::ChannelReader;

/// Flash a block device from an OCI image
pub async fn flash_from_oci(
    image: &str,
    options: OciOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse image reference
    let image_ref = ImageReference::parse(image)?;
    println!("Pulling image: {}", image_ref);

    // Create registry client and authenticate
    let mut client = RegistryClient::new(image_ref.clone(), &options).await?;
    println!("Connecting to registry: {}", image_ref.registry);
    client.authenticate().await?;

    // Fetch manifest
    println!("Fetching manifest...");
    let manifest = client.fetch_manifest().await?;

    // Handle manifest index (multi-platform images)
    let manifest = match manifest {
        Manifest::Index(idx) => {
            println!(
                "Found manifest index with {} platforms",
                idx.manifests.len()
            );
            let platform = idx
                .find_linux_manifest()
                .ok_or("No linux/arm64 or linux/amd64 manifest found")?;
            println!(
                "Selected platform: {:?}",
                platform
                    .platform
                    .as_ref()
                    .map(|p| format!("{}/{}", p.os, p.architecture))
            );
            client.fetch_manifest_by_digest(&platform.digest).await?
        }
        m => m,
    };

    // Get the layer to download
    let layer = manifest.get_single_layer()?;
    let layer_size = layer.size;
    let compression = layer.compression();

    println!("Layer digest: {}", layer.digest);
    println!(
        "Layer size: {} bytes ({:.2} MB)",
        layer_size,
        layer_size as f64 / (1024.0 * 1024.0)
    );
    println!("Layer media type: {}", layer.media_type);
    println!("Layer compression: {:?}", compression);

    // Validate compression - we only support gzip for now
    if compression == LayerCompression::Zstd {
        return Err("Zstd-compressed layers are not yet supported".into());
    }

    // Start blob download
    println!("\nStarting download...");
    let response = client.get_blob_stream(&layer.digest).await?;
    let content_length = response.content_length();

    // We'll detect actual compression from the data stream since
    // some registries don't set the media type correctly

    // First, do a small download to detect content type before setting up pipeline
    println!("\nDetecting content type...");
    let mut content_detection_buffer = Vec::new();
    let detection_size = 2 * 1024 * 1024; // 2MB should be enough for detection

    let mut stream = response.bytes_stream();
    while content_detection_buffer.len() < detection_size {
        match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(chunk_result)) => match chunk_result {
                Ok(chunk) => {
                    content_detection_buffer.extend_from_slice(&chunk);
                }
                Err(e) => {
                    return Err(format!("Download error during detection: {}", e).into());
                }
            },
            Ok(None) => break, // Stream ended early
            Err(_) => return Err("Detection timeout".into()),
        }
    }

    // Detect content and compression type
    let (content_type, compression_type) =
        detect_content_and_compression(&content_detection_buffer, options.common.debug)?;

    if options.common.debug {
        eprintln!(
            "[DEBUG] Detected content: {:?}, compression: {:?}",
            content_type, compression_type
        );
    }

    // Handle raw disk images with a separate, simpler pipeline
    if content_type == ContentType::RawDiskImage {
        return flash_raw_disk_image_directly(
            content_detection_buffer,
            stream,
            compression_type,
            options,
            layer_size,
        )
        .await;
    }

    // Handle XZ-compressed tar archives - bypass tar extraction and pass directly to main pipeline
    if compression_type == Compression::Xz {
        if options.common.debug {
            eprintln!("[DEBUG] XZ-compressed layer detected - bypassing tar extraction, passing directly to main pipeline");
        }
        return flash_raw_disk_image_directly(
            content_detection_buffer,
            stream,
            compression_type,
            options,
            layer_size,
        )
        .await;
    }

    // For tar archives, continue with the complex pipeline
    println!("Processing tar archive...");

    // Set up the streaming pipeline
    // Channel for HTTP chunks -> blocking tar extractor
    let buffer_size_mb = options.common.buffer_size_mb;
    let avg_chunk_size_kb = 16;
    let mut buffer_capacity = (buffer_size_mb * 1024) / avg_chunk_size_kb;
    buffer_capacity = buffer_capacity.max(1000);

    // For compressed layers, use much larger buffering to ensure gzip decoder gets enough data
    if compression != LayerCompression::None {
        buffer_capacity = buffer_capacity.max(10000); // Ensure substantial buffering for compression
        if options.common.debug {
            eprintln!(
                "[DEBUG] Using enhanced buffering for compressed layer: {} chunks",
                buffer_capacity
            );
        }
    }

    println!(
        "Using download buffer: {} MB (capacity: {} chunks)",
        buffer_size_mb, buffer_capacity
    );

    let (http_tx, http_rx) = mpsc::channel::<bytes::Bytes>(buffer_capacity);

    // Channel for tar entry data -> decompressor stdin
    let (tar_tx, mut tar_rx) = mpsc::channel::<Vec<u8>>(16); // 16 * 8MB = 128MB buffer

    // Channels for progress tracking
    let (decompressed_progress_tx, mut decompressed_progress_rx) = mpsc::unbounded_channel::<u64>();
    let (error_tx, error_rx) = mpsc::unbounded_channel::<String>();
    let (written_progress_tx, mut written_progress_rx) = mpsc::unbounded_channel::<u64>();
    // Channel for tracking bytes actually written to decompressor (for progress bar)
    let (decompressor_written_progress_tx, mut decompressor_written_progress_rx) =
        mpsc::unbounded_channel::<u64>();

    // Choose decompressor based on content type and compression detection
    let initial_decompressor_hint = get_decompressor_hint(
        content_type.clone(),
        compression,
        compression_type,
        options.file_pattern.as_deref(),
    );
    if options.common.debug {
        eprintln!(
            "[DEBUG] Selected decompressor hint: '{}' (content={:?}, layer_compression={:?}, content_compression={:?})",
            initial_decompressor_hint, content_type, compression, compression_type
        );
    }
    let (mut decompressor, decompressor_name) =
        start_decompressor_process(initial_decompressor_hint).await?;

    let mut decompressor_stdin = decompressor.stdin.take().unwrap();
    let decompressor_stdout = decompressor.stdout.take().unwrap();
    let decompressor_stderr = decompressor.stderr.take().unwrap();

    println!(
        "Opening block device for writing: {}",
        options.common.device
    );

    // Create block writer
    let block_writer = AsyncBlockWriter::new(
        options.common.device.clone(),
        written_progress_tx.clone(),
        options.common.debug,
        options.common.o_direct,
        options.common.write_buffer_size_mb,
    )?;

    // Spawn task: decompressor stdout -> block writer
    let error_tx_clone = error_tx.clone();
    let writer_handle = {
        let writer = block_writer;
        tokio::spawn(async move {
            let mut stdout = decompressor_stdout;
            let mut buffer = vec![0u8; 8 * 1024 * 1024]; // 8MB buffer

            loop {
                match tokio::io::AsyncReadExt::read(&mut stdout, &mut buffer).await {
                    Ok(0) => break, // EOF
                    Ok(n) => {
                        let data = buffer[..n].to_vec();
                        if decompressed_progress_tx.send(n as u64).is_err() {
                            break;
                        }
                        if let Err(e) = writer.write(data).await {
                            let _ = error_tx_clone.send(format!("Error writing to device: {}", e));
                            return Err(e);
                        }
                    }
                    Err(e) => {
                        let _ =
                            error_tx_clone.send(format!("Error reading from decompressor: {}", e));
                        return Err(e);
                    }
                }
            }
            writer.close().await
        })
    };

    // Spawn stderr reader for decompressor
    tokio::spawn(spawn_stderr_reader(
        decompressor_stderr,
        error_tx.clone(),
        decompressor_name,
    ));

    // Spawn error processor
    let error_processor = tokio::spawn(process_error_messages(error_rx));

    // Spawn task: tar channel -> decompressor stdin
    let decompressor_writer_handle = tokio::spawn(async move {
        while let Some(chunk) = tar_rx.recv().await {
            let chunk_len = chunk.len() as u64;
            if let Err(e) = decompressor_stdin.write_all(&chunk).await {
                return Err(format!("Error writing to decompressor: {}", e));
            }
            // Notify that bytes were written to decompressor (for progress bar)
            let _ = decompressor_written_progress_tx.send(chunk_len);
        }
        // Close stdin to signal EOF
        drop(decompressor_stdin);
        Ok::<(), String>(())
    });

    // Get the buffer size before moving it
    let detection_buffer_size = content_detection_buffer.len() as u64;

    // Send the already-downloaded detection buffer first
    if http_tx
        .send(bytes::Bytes::from(content_detection_buffer))
        .await
        .is_err()
    {
        return Err("Failed to send detection buffer to tar extractor".into());
    }

    // Spawn blocking task: HTTP rx -> gzip -> tar -> tar tx
    let file_pattern = options.file_pattern.clone();
    let debug = options.common.debug;
    let tar_extractor_handle = tokio::task::spawn_blocking(move || {
        extract_tar_archive_from_stream(
            http_rx,
            tar_tx,
            file_pattern.as_deref(),
            compression,
            compression_type,
            debug,
        )
    });

    // Main download loop
    let mut progress =
        ProgressTracker::new(options.common.newline_progress, options.common.show_memory);
    progress.set_content_length(content_length);
    progress.set_is_compressed(decompressor_name != "cat");
    progress.bytes_received = detection_buffer_size; // Account for detection buffer
    let update_interval = Duration::from_secs_f64(options.common.progress_interval_secs);

    // Download and send chunks (using stream from earlier)
    let mut chunk_count = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(chunk_result)) => {
                match chunk_result {
                    Ok(chunk) => {
                        let chunk_len = chunk.len() as u64;
                        progress.bytes_received += chunk_len;
                        chunk_count += 1;

                        if debug && chunk_count <= 5 {
                            eprintln!(
                                "[DEBUG] Received chunk {}: {} bytes (total: {} MB)",
                                chunk_count,
                                chunk_len,
                                progress.bytes_received / (1024 * 1024)
                            );
                        }

                        if http_tx.send(chunk).await.is_err() {
                            eprintln!("\nTar extractor channel closed");
                            if debug {
                                eprintln!(
                                    "[DEBUG] Channel closed after {} chunks, {} bytes",
                                    chunk_count, progress.bytes_received
                                );
                            }
                            break;
                        }

                        // Update progress
                        while let Ok(byte_count) = decompressed_progress_rx.try_recv() {
                            progress.bytes_decompressed += byte_count;
                        }
                        // Track bytes actually written to decompressor (for progress bar)
                        while let Ok(written_len) = decompressor_written_progress_rx.try_recv() {
                            progress.bytes_sent_to_decompressor += written_len;
                        }
                        while let Ok(written_bytes) = written_progress_rx.try_recv() {
                            progress.bytes_written = written_bytes;
                        }

                        if let Err(e) =
                            progress.update_progress(content_length, update_interval, false)
                        {
                            eprintln!();
                            return Err(e);
                        }
                    }
                    Err(e) => {
                        return Err(format!("Download error: {}", e).into());
                    }
                }
            }
            Ok(None) => {
                // Stream ended
                break;
            }
            Err(_) => {
                return Err("Download timeout".into());
            }
        }
    }

    // Close HTTP channel to signal download complete
    drop(http_tx);

    if debug {
        eprintln!(
            "[DEBUG] Download completed, {} bytes received",
            progress.bytes_received
        );
    }

    // Wait for tar extractor (now only handles tar archives)
    let tar_result = tar_extractor_handle.await?;
    if let Err(e) = tar_result {
        return Err(format!("Tar extraction failed: {}", e).into());
    }

    // Wait for decompressor writer
    if let Err(e) = decompressor_writer_handle.await? {
        return Err(format!("Decompressor write failed: {}", e).into());
    }

    // Wait for decompressor process
    loop {
        while let Ok(byte_count) = decompressed_progress_rx.try_recv() {
            progress.bytes_decompressed += byte_count;
        }
        while let Ok(written_bytes) = written_progress_rx.try_recv() {
            progress.bytes_written = written_bytes;
        }
        let _ = progress.update_progress(None, update_interval, false);

        match decompressor.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    return Err(format!(
                        "{} failed with status: {:?}",
                        decompressor_name,
                        status.code()
                    )
                    .into());
                }
                break;
            }
            Ok(None) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }

    // Wait for block writer
    loop {
        while let Ok(written_bytes) = written_progress_rx.try_recv() {
            progress.bytes_written = written_bytes;
        }
        let _ = progress.update_progress(None, update_interval, false);

        if writer_handle.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    match writer_handle.await {
        Ok(Ok(final_bytes)) => {
            progress.bytes_written = final_bytes;
        }
        Ok(Err(e)) => return Err(e.into()),
        Err(e) => return Err(e.into()),
    }

    // Final progress update
    let _ = progress.update_progress(None, update_interval, true);

    // Wait for error processor
    let _ = tokio::time::timeout(Duration::from_secs(2), error_processor).await;

    // Print final stats
    progress.print_final_stats();

    Ok(())
}

/// Common implementation for tar stream extraction
fn extract_tar_stream_impl<R: Read + Send>(
    reader: R,
    tar_tx: mpsc::Sender<Vec<u8>>,
    file_pattern: Option<&str>,
    debug: bool,
) -> Result<(), String> {
    if debug {
        eprintln!("[DEBUG] Tar extractor starting");
    }
    // Create a reader that auto-detects compression from magic bytes
    // Add substantial buffering for streaming gzip decompression
    let buffered_reader = std::io::BufReader::with_capacity(1024 * 1024, reader); // 1MB buffer
    let mut magic_reader = MagicDetectingReader::new(buffered_reader, debug);

    // Read first few bytes to detect compression
    let mut magic_buf = [0u8; 4];
    magic_reader
        .peek_bytes(&mut magic_buf)
        .map_err(|e| format!("Failed to peek magic bytes: {}", e))?;

    let is_gzipped = magic_buf[0] == 0x1f && magic_buf[1] == 0x8b;
    let is_xz = magic_buf[0] == 0xfd
        && magic_buf[1] == 0x37
        && magic_buf[2] == 0x7a
        && magic_buf[3] == 0x58;

    let reader: Box<dyn Read + Send> = if is_gzipped {
        if debug {
            eprintln!("[DEBUG] Auto-detected gzip compression from magic bytes");
            // Save first 1MB of raw gzip data for debugging
            let tee_reader = TeeReader::new(magic_reader, "/tmp/debug_gzip_data.gz", 1024 * 1024);
            let gz_decoder = GzDecoder::new(tee_reader);
            Box::new(DebugGzReader::new(gz_decoder, debug))
        } else {
            let gz_decoder = GzDecoder::new(magic_reader);
            Box::new(gz_decoder)
        }
    } else if is_xz {
        if debug {
            eprintln!("[DEBUG] Auto-detected XZ compression from magic bytes - passing raw XZ data to main pipeline");
        }
        // Don't decompress XZ internally - let the main pipeline's external xzcat handle it
        // Pass the raw XZ data through as-is
        Box::new(magic_reader)
    } else {
        if debug {
            eprintln!("[DEBUG] No compression detected, treating as raw tar");
        }
        Box::new(magic_reader)
    };

    // Wrap with debug reader if debug mode
    let reader: Box<dyn Read + Send> = if debug {
        Box::new(DebugReader::new(reader, debug))
    } else {
        reader
    };

    // Parse as tar archive (content detection already done by caller)
    let mut archive = tar::Archive::new(reader);

    if debug {
        eprintln!("[DEBUG] Starting tar archive parsing");
    }

    // Find and extract the disk image
    for entry_result in archive.entries().map_err(|e| format!("Tar error: {}", e))? {
        let mut entry = entry_result.map_err(|e| format!("Tar entry error: {}", e))?;

        let path = entry
            .path()
            .map_err(|e| format!("Invalid path: {}", e))?
            .to_path_buf();

        let size = entry.size();

        if debug {
            eprintln!("[DEBUG] Tar entry: {:?} ({} bytes)", path, size);
        }

        // Check if this is the disk image we're looking for
        if is_disk_image(&path, file_pattern) {
            println!("Found disk image: {:?} ({} bytes)", path, size);

            // Stream entry contents
            let mut buffer = vec![0u8; 8 * 1024 * 1024]; // 8MB chunks
            loop {
                let n = entry
                    .read(&mut buffer)
                    .map_err(|e| format!("Read error: {}", e))?;
                if n == 0 {
                    break;
                }
                tar_tx
                    .blocking_send(buffer[..n].to_vec())
                    .map_err(|_| "Channel closed")?;
            }

            // Done - we only extract one file
            return Ok(());
        }
    }

    Err("No disk image found in tar archive".to_string())
}

/// Determine the appropriate decompressor hint based on content and compression types.
///
/// Returns a file pattern string that `start_decompressor_process` uses to select
/// the appropriate decompressor command (e.g., "disk.img" → cat, "disk.img.xz" → xzcat).
fn get_decompressor_hint(
    content_type: ContentType,
    layer_compression: LayerCompression,
    content_compression: Compression,
    file_pattern: Option<&str>,
) -> &'static str {
    match (content_type, layer_compression, content_compression) {
        // Layer is compressed per manifest (gzip/zstd) - we decompress before tar extraction,
        // so external decompressor for extracted files should be "cat"
        (ContentType::TarArchive, LayerCompression::Gzip | LayerCompression::Zstd, _) => "disk.img",
        // Layer is uncompressed per manifest but content detection found gzip
        (ContentType::TarArchive, LayerCompression::None, Compression::Gzip) => "disk.img",
        // Layer is uncompressed per manifest but content detection found XZ
        (ContentType::TarArchive, LayerCompression::None, Compression::Xz) => "disk.img.xz",
        // Default: use file pattern or assume XZ compressed disk image
        _ => match file_pattern {
            Some(p) if p.ends_with(".xz") => "disk.img.xz",
            Some(p) if p.ends_with(".gz") => "disk.img.gz",
            Some(_) => "disk.img",
            None => "disk.img.xz",
        },
    }
}

/// Check if a path matches a disk image
fn is_disk_image(path: &Path, pattern: Option<&str>) -> bool {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();

    // Skip hidden files and directories
    if name.starts_with('.') {
        return false;
    }

    // If pattern specified, use it
    if let Some(pattern) = pattern {
        return matches_pattern(&name, pattern);
    }

    // Default: look for common disk image extensions
    let extensions = [
        ".img.xz", ".img.gz", ".img.bz2", ".raw.xz", ".raw.gz", ".raw.bz2", ".xz", ".gz", ".bz2",
        ".img", ".raw",
    ];

    extensions.iter().any(|ext| name.ends_with(ext))
}

/// Simple glob pattern matching
///
/// Supports limited glob patterns:
/// - `*.ext` - matches any file ending with `.ext`
/// - `prefix*` - matches any file starting with `prefix`
/// - `exact` - exact string match
///
/// **Limitations**: Does not support wildcards in the middle of patterns
/// (e.g., `disk*.img` or `*.img.*` won't work as expected)
fn matches_pattern(name: &str, pattern: &str) -> bool {
    if pattern.starts_with('*') && pattern.len() > 1 {
        // *.ext pattern
        name.ends_with(&pattern[1..])
    } else if pattern.ends_with('*') && pattern.len() > 1 {
        // prefix* pattern
        name.starts_with(&pattern[..pattern.len() - 1])
    } else {
        // Exact match
        name == pattern
    }
}

/// Reader that can peek at magic bytes without consuming them
struct MagicDetectingReader<R: Read> {
    inner: R,
    peeked: Vec<u8>,
    peek_pos: usize,
    debug: bool,
}

impl<R: Read> MagicDetectingReader<R> {
    fn new(inner: R, debug: bool) -> Self {
        Self {
            inner,
            peeked: Vec::new(),
            peek_pos: 0,
            debug,
        }
    }

    fn peek_bytes(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        if self.debug {
            eprintln!(
                "[DEBUG] MagicDetectingReader::peek_bytes called for {} bytes",
                buf.len()
            );
            eprintln!(
                "[DEBUG]   Current peeked.len(): {}, peek_pos: {}",
                self.peeked.len(),
                self.peek_pos
            );
        }

        // Read enough bytes to fill the peek buffer
        while self.peeked.len() < buf.len() {
            let mut temp_buf = [0u8; 1024];
            let n = self.inner.read(&mut temp_buf)?;
            if self.debug {
                eprintln!("[DEBUG]   Read {} bytes from inner reader", n);
            }
            if n == 0 {
                break; // EOF
            }
            if n > 0 && self.debug {
                eprintln!(
                    "[DEBUG]   First 8 bytes read: {:02x?}",
                    &temp_buf[..n.min(8)]
                );
            }
            self.peeked.extend_from_slice(&temp_buf[..n]);
        }

        // Copy the requested bytes
        let to_copy = buf.len().min(self.peeked.len());
        buf[..to_copy].copy_from_slice(&self.peeked[..to_copy]);
        if self.debug {
            eprintln!(
                "[DEBUG]   Copying {} bytes to peek buffer: {:02x?}",
                to_copy,
                &self.peeked[..to_copy]
            );
            eprintln!(
                "[DEBUG]   Total peeked buffer now: {} bytes",
                self.peeked.len()
            );
        }
        Ok(())
    }
}

impl<R: Read> Read for MagicDetectingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.debug {
            eprintln!(
                "[DEBUG] MagicDetectingReader::read called for {} bytes",
                buf.len()
            );
            eprintln!(
                "[DEBUG]   peek_pos: {}, peeked.len(): {}",
                self.peek_pos,
                self.peeked.len()
            );
        }

        // First, drain any peeked bytes
        if self.peek_pos < self.peeked.len() {
            let available = self.peeked.len() - self.peek_pos;
            let to_copy = buf.len().min(available);
            buf[..to_copy].copy_from_slice(&self.peeked[self.peek_pos..self.peek_pos + to_copy]);
            self.peek_pos += to_copy;
            if self.debug {
                eprintln!(
                    "[DEBUG]   Returning {} peeked bytes: {:02x?}",
                    to_copy,
                    &buf[..to_copy.min(8)]
                );
            }
            return Ok(to_copy);
        }

        // No more peeked data, read directly from inner
        let n = self.inner.read(buf)?;
        if self.debug {
            if n > 0 {
                eprintln!(
                    "[DEBUG]   Read {} bytes directly from inner: {:02x?}",
                    n,
                    &buf[..n.min(8)]
                );
            } else {
                eprintln!("[DEBUG]   EOF from inner reader");
            }
        }
        Ok(n)
    }
}

/// Reader that logs the first few bytes for debugging
struct DebugReader<R: Read> {
    inner: R,
    logged_first: bool,
    debug: bool,
}

/// Reader that saves data to file while passing it through (for debugging)
struct TeeReader<R: Read> {
    inner: R,
    file: Option<std::fs::File>,
    bytes_written: usize,
    max_bytes: usize,
}

impl<R: Read> TeeReader<R> {
    fn new(inner: R, path: &str, max_bytes: usize) -> Self {
        let file = match std::fs::File::create(path) {
            Ok(f) => {
                eprintln!("[DEBUG] TeeReader: Saving raw gzip data to {}", path);
                Some(f)
            }
            Err(e) => {
                eprintln!("[DEBUG] TeeReader: Failed to create {}: {}", path, e);
                None
            }
        };

        Self {
            inner,
            file,
            bytes_written: 0,
            max_bytes,
        }
    }
}

impl<R: Read> Read for TeeReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;

        // Save data to file if still under limit
        if n > 0 && self.bytes_written < self.max_bytes {
            if let Some(ref mut file) = self.file {
                let to_write = (self.max_bytes - self.bytes_written).min(n);
                if let Err(e) = file.write_all(&buf[..to_write]) {
                    eprintln!("[DEBUG] TeeReader: Failed to write to debug file: {}", e);
                    self.file = None;
                } else {
                    self.bytes_written += to_write;
                    if self.bytes_written >= self.max_bytes {
                        eprintln!(
                            "[DEBUG] TeeReader: Finished saving {} bytes to debug file",
                            self.bytes_written
                        );
                        self.file = None;
                    }
                }
            }
        }

        Ok(n)
    }
}

/// Reader that specifically debugs gzip decoder output
struct DebugGzReader<R: Read> {
    inner: R,
    bytes_read: u64,
    last_log: u64,
    debug: bool,
}

impl<R: Read> DebugGzReader<R> {
    fn new(inner: R, debug: bool) -> Self {
        if debug {
            eprintln!("[DEBUG] DebugGzReader created");
        }
        Self {
            inner,
            bytes_read: 0,
            last_log: 0,
            debug,
        }
    }
}

impl<R: Read> Read for DebugGzReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.inner.read(buf) {
            Ok(n) => {
                if n == 0 {
                    if self.debug {
                        eprintln!(
                            "[DEBUG] DebugGzReader: EOF reached after {} bytes",
                            self.bytes_read
                        );
                    }
                    return Ok(0);
                }

                self.bytes_read += n as u64;

                // Log every 10MB or first read
                if self.debug
                    && (self.bytes_read - self.last_log >= 10 * 1024 * 1024 || self.last_log == 0)
                {
                    eprintln!(
                        "[DEBUG] DebugGzReader: Read {} bytes (total: {} MB)",
                        n,
                        self.bytes_read / (1024 * 1024)
                    );
                    eprintln!(
                        "[DEBUG] DebugGzReader: First 8 bytes: {:02x?}",
                        &buf[..n.min(8)]
                    );

                    // Check if we're getting all zeros
                    let all_zeros = buf[..n].iter().all(|&b| b == 0);
                    if all_zeros {
                        eprintln!("[DEBUG] DebugGzReader: *** WARNING: All bytes are zero! ***");
                        eprintln!(
                            "[DEBUG] DebugGzReader: This suggests gzip decompression is failing!"
                        );
                    }

                    self.last_log = self.bytes_read;
                }

                Ok(n)
            }
            Err(e) => {
                if self.debug {
                    eprintln!(
                        "[DEBUG] DebugGzReader: ERROR during read: {} (after {} bytes)",
                        e, self.bytes_read
                    );
                }
                Err(e)
            }
        }
    }
}

impl<R: Read> DebugReader<R> {
    fn new(inner: R, debug: bool) -> Self {
        Self {
            inner,
            logged_first: false,
            debug,
        }
    }
}

impl<R: Read> Read for DebugReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.debug {
            eprintln!("[DEBUG] DebugReader::read called for {} bytes", buf.len());
        }
        let n = self.inner.read(buf)?;
        if self.debug {
            eprintln!("[DEBUG] DebugReader got {} bytes from inner", n);

            if !self.logged_first && n > 0 {
                eprintln!(
                    "[DEBUG] First 16 bytes after decompression: {:02x?}",
                    &buf[..n.min(16)]
                );
                self.logged_first = true;
            } else if n > 0 {
                eprintln!(
                    "[DEBUG] Subsequent read: first 8 bytes: {:02x?}",
                    &buf[..n.min(8)]
                );
            }
        }

        Ok(n)
    }
}

/// Flash raw disk image directly without tar extraction
async fn flash_raw_disk_image_directly(
    initial_buffer: Vec<u8>,
    mut stream: impl futures_util::Stream<Item = reqwest::Result<bytes::Bytes>> + std::marker::Unpin,
    compression_type: Compression,
    options: OciOptions,
    layer_size: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Raw disk image detected - streaming directly to device");
    println!("Compression: {:?}", compression_type);
    println!("Opening device: {}", options.common.device);

    // Create progress tracking
    let mut progress =
        ProgressTracker::new(options.common.newline_progress, options.common.show_memory);
    progress.set_content_length(Some(layer_size));
    progress.set_is_compressed(compression_type != Compression::None);
    progress.bytes_received = initial_buffer.len() as u64;
    let update_interval = Duration::from_secs_f64(options.common.progress_interval_secs);

    // Set up single-purpose block writer with its own progress channel
    let (raw_written_progress_tx, mut raw_written_progress_rx) = mpsc::unbounded_channel::<u64>();

    // Create block writer
    let block_writer = AsyncBlockWriter::new(
        options.common.device,
        raw_written_progress_tx,
        options.common.debug,
        options.common.o_direct,
        options.common.write_buffer_size_mb,
    )?;

    // Set up streaming pipeline using channels
    let buffer_size_mb = options.common.buffer_size_mb;
    let buffer_capacity = ((buffer_size_mb * 1024) / 16).max(1000); // 16KB average chunk size

    let (http_tx, http_rx) = mpsc::channel::<bytes::Bytes>(buffer_capacity);
    let (decompressed_progress_tx, mut decompressed_progress_rx) = mpsc::unbounded_channel::<u64>();

    // For gzip and none, we can decompress in-process and write directly to block writer
    // For XZ, we need the external xzcat process
    let needs_external_decompressor = compression_type == Compression::Xz;

    if options.common.debug {
        eprintln!(
            "[DEBUG] Compression type: {:?}, using external decompressor: {}",
            compression_type, needs_external_decompressor
        );
    }

    // Spawn the processing pipeline based on compression type
    let writer_handle = if needs_external_decompressor {
        // XZ: Use external xzcat process
        let (mut decompressor, decompressor_name) =
            start_decompressor_process("disk.img.xz").await?;

        let decompressor_stdin = decompressor.stdin.take().unwrap();
        let decompressor_stdout = decompressor.stdout.take().unwrap();
        let decompressor_stderr = decompressor.stderr.take().unwrap();

        let (error_tx, error_rx) = mpsc::unbounded_channel::<String>();

        // Spawn stderr reader
        tokio::spawn(spawn_stderr_reader(
            decompressor_stderr,
            error_tx.clone(),
            decompressor_name,
        ));

        // Spawn error processor
        tokio::spawn(process_error_messages(error_rx));

        // Spawn blocking task: read from channel and write to xzcat stdin
        // First, create a sync file handle from the async stdin
        #[cfg(unix)]
        let stdin_fd = {
            use std::os::unix::io::{AsRawFd, FromRawFd};
            let raw_fd = decompressor_stdin.as_raw_fd();
            // Duplicate the fd so we can use it in blocking context
            let dup_fd = unsafe { libc::dup(raw_fd) };
            if dup_fd == -1 {
                return Err(std::io::Error::last_os_error().into());
            }
            // SAFETY: dup_fd is a valid file descriptor (we checked above)
            unsafe { std::fs::File::from_raw_fd(dup_fd) }
        };
        #[cfg(not(unix))]
        let stdin_fd: std::fs::File = {
            return Err("XZ streaming decompression is not supported on non-unix platforms".into());
        };

        // Drop the original async stdin (the dup'd fd still points to the pipe)
        drop(decompressor_stdin);

        let stdin_writer_handle = {
            tokio::task::spawn_blocking(move || {
                use std::io::Write as _;
                let reader = ChannelReader::new(http_rx);
                let mut reader = reader;
                let mut stdin = stdin_fd;
                let mut buffer = vec![0u8; 1024 * 1024]; // 1MB chunks

                loop {
                    match reader.read(&mut buffer) {
                        Ok(0) => break, // EOF
                        Ok(n) => {
                            if let Err(e) = stdin.write_all(&buffer[..n]) {
                                return Err(format!("Error writing to xzcat: {}", e));
                            }
                        }
                        Err(e) => return Err(format!("Error reading stream: {}", e)),
                    }
                }

                drop(stdin);
                Ok::<(), String>(())
            })
        };

        // Spawn task: xzcat stdout -> block writer
        let writer = block_writer;
        let progress_tx = decompressed_progress_tx;
        tokio::spawn(async move {
            let mut stdout = decompressor_stdout;
            let mut buffer = vec![0u8; 8 * 1024 * 1024]; // 8MB buffer

            loop {
                match tokio::io::AsyncReadExt::read(&mut stdout, &mut buffer).await {
                    Ok(0) => break, // EOF
                    Ok(n) => {
                        let data = buffer[..n].to_vec();
                        let _ = progress_tx.send(n as u64);
                        writer.write(data).await?
                    }
                    Err(e) => return Err(e),
                }
            }

            // Wait for stdin writer to finish
            let _ = stdin_writer_handle.await;

            // Wait for decompressor to finish
            let _ = decompressor.wait().await;

            writer.close().await
        })
    } else {
        // Gzip or None: decompress in-process and write directly to block writer
        let writer = block_writer;
        let progress_tx = decompressed_progress_tx;
        let debug = options.common.debug;

        // Create an async channel for decompressed data
        let (data_tx, mut data_rx) = mpsc::channel::<Vec<u8>>(16);

        // Capture the runtime handle before entering the blocking task
        let rt = tokio::runtime::Handle::current();

        // Spawn blocking task: read, decompress, send to async channel
        let reader_handle = tokio::task::spawn_blocking(move || {
            let reader = ChannelReader::new(http_rx);

            // Apply in-process gzip decompression if needed
            let processed_reader: Box<dyn std::io::Read + Send> = match compression_type {
                Compression::Gzip => {
                    if debug {
                        eprintln!("[DEBUG] Applying in-process gzip decompression");
                    }
                    Box::new(flate2::read::GzDecoder::new(reader))
                }
                _ => Box::new(reader),
            };

            let mut reader = processed_reader;
            let mut buffer = vec![0u8; 1024 * 1024]; // 1MB chunks

            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => break, // EOF
                    Ok(n) => {
                        // Send to async channel using block_on
                        if rt.block_on(data_tx.send(buffer[..n].to_vec())).is_err() {
                            return Err("Data channel closed".to_string());
                        }
                    }
                    Err(e) => return Err(format!("Error reading/decompressing: {}", e)),
                }
            }

            Ok::<(), String>(())
        });

        // Spawn async task: receive from channel and write to block writer
        tokio::spawn(async move {
            while let Some(data) = data_rx.recv().await {
                let len = data.len() as u64;
                let _ = progress_tx.send(len);
                writer.write(data).await?
            }

            let _ = reader_handle.await;
            writer.close().await
        })
    };

    // Send the already-downloaded detection buffer first
    if http_tx
        .send(bytes::Bytes::from(initial_buffer))
        .await
        .is_err()
    {
        return Err("Failed to send detection buffer to streaming pipeline".into());
    }

    // Main download loop - stream chunks to processing pipeline
    let mut chunk_count = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(chunk_result)) => {
                match chunk_result {
                    Ok(chunk) => {
                        let chunk_len = chunk.len() as u64;
                        progress.bytes_received += chunk_len;
                        chunk_count += 1;

                        if options.common.debug && chunk_count <= 5 {
                            eprintln!(
                                "[DEBUG] Received chunk {}: {} bytes (total: {} MB)",
                                chunk_count,
                                chunk_len,
                                progress.bytes_received / (1024 * 1024)
                            );
                        }

                        if http_tx.send(chunk).await.is_err() {
                            eprintln!("\nStreaming pipeline closed");
                            break;
                        }

                        // Update progress
                        while let Ok(byte_count) = decompressed_progress_rx.try_recv() {
                            progress.bytes_decompressed += byte_count;
                        }
                        while let Ok(written_bytes) = raw_written_progress_rx.try_recv() {
                            progress.bytes_written = written_bytes;
                        }
                        // For raw disk images, bytes_sent_to_decompressor tracks bytes_received
                        // since data is immediately forwarded to the decompression pipeline
                        progress.bytes_sent_to_decompressor = progress.bytes_received;

                        if let Err(e) =
                            progress.update_progress(Some(layer_size), update_interval, false)
                        {
                            eprintln!();
                            return Err(e);
                        }
                    }
                    Err(e) => {
                        return Err(format!("Download error: {}", e).into());
                    }
                }
            }
            Ok(None) => {
                // Stream ended
                break;
            }
            Err(_) => {
                return Err("Download timeout".into());
            }
        }
    }

    // Close HTTP channel to signal download complete
    drop(http_tx);

    if options.common.debug {
        eprintln!(
            "[DEBUG] Download completed, {} bytes received",
            progress.bytes_received
        );
    }

    // Wait for the processing pipeline to complete
    loop {
        // Update progress from decompression and writes
        while let Ok(byte_count) = decompressed_progress_rx.try_recv() {
            progress.bytes_decompressed += byte_count;
        }
        while let Ok(written_bytes) = raw_written_progress_rx.try_recv() {
            progress.bytes_written = written_bytes;
        }
        let _ = progress.update_progress(Some(layer_size), update_interval, false);

        if writer_handle.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Get final result
    match writer_handle.await {
        Ok(Ok(final_bytes)) => {
            progress.bytes_written = final_bytes;
            progress.bytes_decompressed = final_bytes;
        }
        Ok(Err(e)) => return Err(e.into()),
        Err(e) => return Err(e.into()),
    }

    // Final progress update
    let _ = progress.update_progress(Some(layer_size), update_interval, true);

    // Print final stats
    progress.print_final_stats();

    Ok(())
}

/// Simple tar archive extraction without the complex buffering logic
fn extract_tar_archive_from_stream(
    http_rx: mpsc::Receiver<bytes::Bytes>,
    tar_tx: mpsc::Sender<Vec<u8>>,
    file_pattern: Option<&str>,
    compression: LayerCompression,
    compression_type: Compression,
    debug: bool,
) -> Result<(), String> {
    let reader = ChannelReader::new(http_rx);

    // Handle layer compression before tar extraction
    // Use both manifest compression and content-detected compression
    let decompressed_reader: Box<dyn Read + Send> = match compression {
        LayerCompression::Gzip => {
            if debug {
                eprintln!("[DEBUG] Layer is gzip compressed (manifest), decompressing before tar extraction");
            }
            Box::new(GzDecoder::new(reader))
        }
        LayerCompression::Zstd => {
            return Err("Zstd layer compression is not supported yet".to_string());
        }
        LayerCompression::None => {
            // When manifest says no compression, use content detection result
            match compression_type {
                Compression::Gzip => {
                    if debug {
                        eprintln!("[DEBUG] Content is gzip compressed (detected), decompressing before tar extraction");
                    }
                    Box::new(GzDecoder::new(reader))
                }
                Compression::Xz => {
                    if debug {
                        eprintln!("[DEBUG] Content is XZ compressed layer (detected), passing raw XZ data to main pipeline");
                    }
                    // Don't decompress XZ here - let the main pipeline's external xzcat handle it
                    // Just pass the raw XZ data through
                    Box::new(reader)
                }
                Compression::None => {
                    if debug {
                        eprintln!("[DEBUG] No compression detected, processing tar directly");
                    }
                    Box::new(reader)
                }
                Compression::Zstd => {
                    return Err("Zstd content compression is not supported yet".to_string());
                }
            }
        }
    };

    extract_tar_stream_impl(decompressed_reader, tar_tx, file_pattern, debug)
}
