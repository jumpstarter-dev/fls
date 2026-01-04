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
use super::registry::{OciOptions, RegistryClient};
use crate::fls::block_writer::AsyncBlockWriter;
use crate::fls::decompress::{spawn_stderr_reader, start_decompressor_process};
use crate::fls::error_handling::process_error_messages;
use crate::fls::progress::ProgressTracker;

/// Content and compression detection
#[derive(Debug, Clone, PartialEq)]
enum ContentType {
    RawDiskImage,
    TarArchive,
}

#[derive(Debug, Clone, PartialEq)]
enum CompressionType {
    None,
    Gzip,
    Xz,
}


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
        detect_content_and_compression(&content_detection_buffer, options.debug)?;

    if options.debug {
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

    // For tar archives, continue with the complex pipeline
    println!("Processing tar archive...");

    // Set up the streaming pipeline
    // Channel for HTTP chunks -> blocking tar extractor
    let buffer_size_mb = options.buffer_size_mb;
    let avg_chunk_size_kb = 16;
    let mut buffer_capacity = (buffer_size_mb * 1024) / avg_chunk_size_kb;
    buffer_capacity = buffer_capacity.max(1000);

    // For compressed layers, use much larger buffering to ensure gzip decoder gets enough data
    if compression != LayerCompression::None {
        buffer_capacity = buffer_capacity.max(10000); // Ensure substantial buffering for compression
        if options.debug {
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

    // We'll determine the right decompressor after content analysis
    // Start with a placeholder - we'll replace it once we know what we have
    let initial_decompressor_hint = options.file_pattern.as_deref().unwrap_or("disk.img.xz");
    let (mut decompressor, decompressor_name) =
        start_decompressor_process(initial_decompressor_hint).await?;

    let mut decompressor_stdin = decompressor.stdin.take().unwrap();
    let decompressor_stdout = decompressor.stdout.take().unwrap();
    let decompressor_stderr = decompressor.stderr.take().unwrap();

    println!("Opening block device for writing: {}", options.device);

    // Create block writer
    let block_writer = AsyncBlockWriter::new(
        options.device.clone(),
        written_progress_tx.clone(),
        options.debug,
        options.o_direct,
        options.write_buffer_size_mb,
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
            if let Err(e) = decompressor_stdin.write_all(&chunk).await {
                return Err(format!("Error writing to decompressor: {}", e));
            }
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
    let debug = options.debug;
    let tar_extractor_handle = tokio::task::spawn_blocking(move || {
        extract_tar_archive_from_stream(
            http_rx,
            tar_tx,
            file_pattern.as_deref(),
            compression,
            debug,
        )
    });

    // Main download loop
    let mut progress = ProgressTracker::new(options.newline_progress, options.show_memory);
    progress.set_content_length(content_length);
    progress.set_is_compressed(decompressor_name != "cat");
    progress.bytes_received = detection_buffer_size; // Account for detection buffer
    let update_interval = Duration::from_secs_f64(options.progress_interval_secs);

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
    let stats = progress.final_stats();
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
    let mut magic_reader = MagicDetectingReader::new(buffered_reader);

    // Read first few bytes to detect compression
    let mut magic_buf = [0u8; 3];
    magic_reader
        .peek_bytes(&mut magic_buf)
        .map_err(|e| format!("Failed to peek magic bytes: {}", e))?;

    let is_gzipped = magic_buf[0] == 0x1f && magic_buf[1] == 0x8b;

    let reader: Box<dyn Read + Send> = if is_gzipped {
        if debug {
            eprintln!("[DEBUG] Auto-detected gzip compression from magic bytes");
            // Save first 1MB of raw gzip data for debugging
            let tee_reader = TeeReader::new(magic_reader, "/tmp/debug_gzip_data.gz", 1024 * 1024);
            let gz_decoder = GzDecoder::new(tee_reader);
            Box::new(DebugGzReader::new(gz_decoder))
        } else {
            let gz_decoder = GzDecoder::new(magic_reader);
            Box::new(gz_decoder)
        }
    } else {
        if debug {
            eprintln!("[DEBUG] No compression detected, treating as raw tar");
        }
        Box::new(magic_reader)
    };

    // Wrap with debug reader if debug mode
    let reader: Box<dyn Read + Send> = if debug {
        Box::new(DebugReader::new(reader))
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

/// Detect content and compression type from pre-buffered data
fn detect_content_and_compression(
    data: &[u8],
    debug: bool,
) -> Result<(ContentType, CompressionType), String> {
    if data.len() < 16 {
        return Err("Insufficient data for detection".to_string());
    }

    // First, detect compression from magic bytes
    let compression_type = if data.len() >= 3 && data[0] == 0x1f && data[1] == 0x8b {
        CompressionType::Gzip
    } else if data.len() >= 6 && &data[0..6] == b"\xfd7zXZ\x00" {
        CompressionType::Xz
    } else {
        CompressionType::None
    };

    if debug {
        eprintln!("[DEBUG] Compression detection: {:?}", compression_type);
        eprintln!(
            "[DEBUG] First 16 bytes: {:02x?}",
            &data[..16.min(data.len())]
        );
    }

    // For compressed data, we need to peek at the decompressed content to determine type
    // For now, if it's compressed, we'll decompress a bit to check
    let content_to_analyze = match compression_type {
        CompressionType::Gzip => {
            // Try to decompress first few KB to analyze content
            match decompress_sample_gzip(data) {
                Ok(decompressed) => decompressed,
                Err(e) => {
                    if debug {
                        eprintln!("[DEBUG] Failed to decompress gzip sample: {}", e);
                    }
                    // Fall back to assuming it's a tar if we can't decompress
                    return Ok((ContentType::TarArchive, compression_type));
                }
            }
        }
        CompressionType::Xz => {
            // For XZ, we'll assume it's likely a tar archive (common pattern)
            // since XZ decompression is more complex to do partial
            if debug {
                eprintln!("[DEBUG] XZ detected, assuming tar archive");
            }
            return Ok((ContentType::TarArchive, compression_type));
        }
        CompressionType::None => data.to_vec(),
    };

    // Now analyze the (possibly decompressed) content
    let content_type = detect_content_type(&content_to_analyze, debug);

    Ok((content_type, compression_type))
}

/// Decompress a sample of gzip data to analyze content type
fn decompress_sample_gzip(data: &[u8]) -> Result<Vec<u8>, String> {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let mut decoder = GzDecoder::new(data);
    let mut buffer = vec![0u8; 8192]; // Decompress up to 8KB to analyze

    match decoder.read(&mut buffer) {
        Ok(n) => {
            buffer.truncate(n);
            Ok(buffer)
        }
        Err(e) => Err(format!("Gzip decompression failed: {}", e)),
    }
}

/// Detect content type from raw (uncompressed) data
fn detect_content_type(data: &[u8], debug: bool) -> ContentType {
    // Check for tar header magic ("ustar" at offset 257)
    if data.len() >= 262 {
        let tar_magic = &data[257..262];
        if tar_magic == b"ustar" || tar_magic == b"posix" {
            if debug {
                eprintln!("[DEBUG] Found tar magic signature, detected as tar archive");
            }
            return ContentType::TarArchive;
        }
    }

    // Everything else gets streamed directly
    if debug {
        eprintln!("[DEBUG] No tar signature found, streaming as raw data");
    }
    ContentType::RawDiskImage
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

/// Reader that pulls bytes from a tokio channel
struct ChannelReader {
    rx: mpsc::Receiver<bytes::Bytes>,
    current: Option<bytes::Bytes>,
    offset: usize,
}

/// Reader that can peek at magic bytes without consuming them
struct MagicDetectingReader<R: Read> {
    inner: R,
    peeked: Vec<u8>,
    peek_pos: usize,
}

impl<R: Read> MagicDetectingReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            peeked: Vec::new(),
            peek_pos: 0,
        }
    }

    fn peek_bytes(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        eprintln!(
            "[DEBUG] MagicDetectingReader::peek_bytes called for {} bytes",
            buf.len()
        );
        eprintln!(
            "[DEBUG]   Current peeked.len(): {}, peek_pos: {}",
            self.peeked.len(),
            self.peek_pos
        );

        // Read enough bytes to fill the peek buffer
        while self.peeked.len() < buf.len() {
            let mut temp_buf = [0u8; 1024];
            let n = self.inner.read(&mut temp_buf)?;
            eprintln!("[DEBUG]   Read {} bytes from inner reader", n);
            if n == 0 {
                break; // EOF
            }
            if n > 0 {
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
        eprintln!(
            "[DEBUG]   Copying {} bytes to peek buffer: {:02x?}",
            to_copy,
            &self.peeked[..to_copy]
        );
        eprintln!(
            "[DEBUG]   Total peeked buffer now: {} bytes",
            self.peeked.len()
        );
        Ok(())
    }
}

impl<R: Read> Read for MagicDetectingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        eprintln!(
            "[DEBUG] MagicDetectingReader::read called for {} bytes",
            buf.len()
        );
        eprintln!(
            "[DEBUG]   peek_pos: {}, peeked.len(): {}",
            self.peek_pos,
            self.peeked.len()
        );

        // First, drain any peeked bytes
        if self.peek_pos < self.peeked.len() {
            let available = self.peeked.len() - self.peek_pos;
            let to_copy = buf.len().min(available);
            buf[..to_copy].copy_from_slice(&self.peeked[self.peek_pos..self.peek_pos + to_copy]);
            self.peek_pos += to_copy;
            eprintln!(
                "[DEBUG]   Returning {} peeked bytes: {:02x?}",
                to_copy,
                &buf[..to_copy.min(8)]
            );
            return Ok(to_copy);
        }

        // No more peeked data, read directly from inner
        let n = self.inner.read(buf)?;
        if n > 0 {
            eprintln!(
                "[DEBUG]   Read {} bytes directly from inner: {:02x?}",
                n,
                &buf[..n.min(8)]
            );
        } else {
            eprintln!("[DEBUG]   EOF from inner reader");
        }
        Ok(n)
    }
}

/// Reader that logs the first few bytes for debugging
struct DebugReader<R: Read> {
    inner: R,
    logged_first: bool,
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
}

impl<R: Read> DebugGzReader<R> {
    fn new(inner: R) -> Self {
        eprintln!("[DEBUG] DebugGzReader created");
        Self {
            inner,
            bytes_read: 0,
            last_log: 0,
        }
    }
}

impl<R: Read> Read for DebugGzReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.inner.read(buf) {
            Ok(n) => {
                if n == 0 {
                    eprintln!(
                        "[DEBUG] DebugGzReader: EOF reached after {} bytes",
                        self.bytes_read
                    );
                    return Ok(0);
                }

                self.bytes_read += n as u64;

                // Log every 10MB or first read
                if self.bytes_read - self.last_log >= 10 * 1024 * 1024 || self.last_log == 0 {
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
                eprintln!(
                    "[DEBUG] DebugGzReader: ERROR during read: {} (after {} bytes)",
                    e, self.bytes_read
                );
                Err(e)
            }
        }
    }
}

impl<R: Read> DebugReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            logged_first: false,
        }
    }
}

impl<R: Read> Read for DebugReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        eprintln!("[DEBUG] DebugReader::read called for {} bytes", buf.len());
        let n = self.inner.read(buf)?;
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

        Ok(n)
    }
}

impl ChannelReader {
    fn new(rx: mpsc::Receiver<bytes::Bytes>) -> Self {
        Self {
            rx,
            current: None,
            offset: 0,
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            // If we have current data, use it
            if let Some(ref data) = self.current {
                let remaining = &data[self.offset..];
                if !remaining.is_empty() {
                    let to_copy = remaining.len().min(buf.len());
                    buf[..to_copy].copy_from_slice(&remaining[..to_copy]);
                    self.offset += to_copy;

                    // Debug: print first few bytes
                    static FIRST_READ: std::sync::Once = std::sync::Once::new();
                    FIRST_READ.call_once(|| {
                        eprintln!(
                            "[DEBUG] First 16 bytes from layer: {:02x?}",
                            &buf[..to_copy.min(16)]
                        );
                    });

                    return Ok(to_copy);
                }
            }

            // Need more data - blocking receive
            match self.rx.blocking_recv() {
                Some(data) => {
                    self.current = Some(data);
                    self.offset = 0;
                    // Loop to process the new data
                }
                None => {
                    // Channel closed - EOF
                    return Ok(0);
                }
            }
        }
    }
}

/// Flash raw disk image directly without tar extraction
async fn flash_raw_disk_image_directly(
    initial_buffer: Vec<u8>,
    mut stream: impl futures_util::Stream<Item = reqwest::Result<bytes::Bytes>> + std::marker::Unpin,
    compression_type: CompressionType,
    options: OciOptions,
    layer_size: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Raw disk image detected - streaming directly to device");
    println!("Compression: {:?}", compression_type);
    println!("Opening device: {}", options.device);

    // Create progress tracking
    let mut progress = ProgressTracker::new(options.newline_progress, options.show_memory);
    progress.set_content_length(Some(layer_size));
    progress.set_is_compressed(compression_type != CompressionType::None);
    progress.bytes_received = initial_buffer.len() as u64;
    let update_interval = Duration::from_secs_f64(options.progress_interval_secs);

    // Set up single-purpose block writer with its own progress channel
    let (raw_written_progress_tx, mut raw_written_progress_rx) = mpsc::unbounded_channel::<u64>();

    // Create block writer
    let block_writer = AsyncBlockWriter::new(
        options.device,
        raw_written_progress_tx,
        options.debug,
        options.o_direct,
        options.write_buffer_size_mb,
    )?;

    // Download all remaining data first (simpler than complex async-to-sync bridging)
    let mut all_data = initial_buffer;
    loop {
        match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                progress.bytes_received += chunk.len() as u64;
                all_data.extend_from_slice(&chunk);

                // Update progress periodically during download
                if let Err(e) = progress.update_progress(Some(layer_size), update_interval, false) {
                    eprintln!();
                    return Err(e);
                }
            }
            Ok(Some(Err(e))) => {
                return Err(format!("Download error: {}", e).into());
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

    if options.debug {
        eprintln!(
            "[DEBUG] Downloaded {} MB total",
            all_data.len() / (1024 * 1024)
        );
    }

    // Create appropriate reader based on compression
    let data_cursor = std::io::Cursor::new(all_data);

    // Apply decompression if needed
    let reader: Box<dyn std::io::Read + Send> = match compression_type {
        CompressionType::None => Box::new(data_cursor),
        CompressionType::Gzip => {
            if options.debug {
                eprintln!("[DEBUG] Applying gzip decompression");
            }
            Box::new(flate2::read::GzDecoder::new(data_cursor))
        }
        CompressionType::Xz => {
            return Err("XZ-compressed raw disk images are not yet supported. Use tar archives with XZ compression.".into());
        }
    };

    // Stream data directly to block writer in a background task
    let writer_handle = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Handle::current();
        let mut reader = reader;
        let mut buffer = vec![0u8; 8 * 1024 * 1024]; // 8MB chunks

        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break, // EOF
                Ok(n) => {
                    if let Err(e) = rt.block_on(block_writer.write(buffer[..n].to_vec())) {
                        return Err(format!("Write error: {}", e));
                    }
                }
                Err(e) => return Err(format!("Read error: {}", e)),
            }
        }

        // Close and return final byte count
        rt.block_on(block_writer.close())
            .map_err(|e| format!("Close error: {}", e))
    });

    // Wait for writer to complete
    loop {
        // Update written bytes
        while let Ok(written_bytes) = raw_written_progress_rx.try_recv() {
            progress.bytes_written = written_bytes;
            progress.bytes_decompressed = written_bytes;
        }

        // Update progress
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
    let stats = progress.final_stats();
    println!(
        "\nDownload complete: {:.2} MB in {} ({:.2} MB/s)",
        stats.mb_received,
        stats.download_time_formatted(),
        stats.download_rate
    );
    println!(
        "Write complete: {:.2} MB in {} ({:.2} MB/s)",
        stats.mb_written,
        stats.write_time_formatted(),
        stats.write_rate
    );
    println!("Total flash runtime: {}", stats.total_time_formatted());

    Ok(())
}

/// Simple tar archive extraction without the complex buffering logic
fn extract_tar_archive_from_stream(
    http_rx: mpsc::Receiver<bytes::Bytes>,
    tar_tx: mpsc::Sender<Vec<u8>>,
    file_pattern: Option<&str>,
    _compression: LayerCompression,
    debug: bool,
) -> Result<(), String> {
    let reader = ChannelReader::new(http_rx);
    extract_tar_stream_impl(
        reader,
        tar_tx,
        file_pattern,
        debug,
    )
}
