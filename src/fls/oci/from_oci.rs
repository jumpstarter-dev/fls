/// Flash from OCI image
///
/// Implements the streaming pipeline:
/// Registry blob -> gzip decompress -> tar extract -> xzcat -> block device
use std::io::Read;
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
    println!("Layer compression: {:?}", compression);

    // Validate compression - we only support gzip for now
    if compression == LayerCompression::Zstd {
        return Err("Zstd-compressed layers are not yet supported".into());
    }

    // Start blob download
    println!("\nStarting download...");
    let response = client.get_blob_stream(&layer.digest).await?;
    let content_length = response.content_length();

    // Set up the streaming pipeline
    // Channel for HTTP chunks -> blocking tar extractor
    let buffer_size_mb = options.buffer_size_mb;
    let avg_chunk_size_kb = 16;
    let buffer_capacity = (buffer_size_mb * 1024) / avg_chunk_size_kb;
    let buffer_capacity = buffer_capacity.max(1000);

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

    // Determine decompressor based on file pattern or default to xzcat
    let decompressor_hint = options.file_pattern.as_deref().unwrap_or("disk.img.xz");
    let (mut decompressor, decompressor_name) =
        start_decompressor_process(decompressor_hint).await?;

    let mut decompressor_stdin = decompressor.stdin.take().unwrap();
    let decompressor_stdout = decompressor.stdout.take().unwrap();
    let decompressor_stderr = decompressor.stderr.take().unwrap();

    println!("Opening block device for writing: {}", options.device);

    // Create block writer
    let block_writer = AsyncBlockWriter::new(
        options.device.clone(),
        written_progress_tx,
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

    // Spawn blocking task: HTTP rx -> gzip -> tar -> tar tx
    let file_pattern = options.file_pattern.clone();
    let debug = options.debug;
    let tar_extractor_handle = tokio::task::spawn_blocking(move || {
        extract_tar_stream(http_rx, tar_tx, file_pattern.as_deref(), compression, debug)
    });

    // Main download loop
    let mut progress = ProgressTracker::new(options.newline_progress, options.show_memory);
    progress.set_content_length(content_length);
    progress.set_is_compressed(decompressor_name != "cat");
    let update_interval = Duration::from_secs_f64(options.progress_interval_secs);

    let mut stream = response.bytes_stream();

    // Download and send chunks
    loop {
        match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(chunk_result)) => {
                match chunk_result {
                    Ok(chunk) => {
                        let chunk_len = chunk.len() as u64;
                        progress.bytes_received += chunk_len;

                        if http_tx.send(chunk).await.is_err() {
                            eprintln!("\nTar extractor channel closed");
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

    // Wait for tar extractor
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

/// Extract disk image from tar stream
///
/// This runs in a blocking task and reads from the HTTP channel,
/// decompresses gzip, parses tar, and sends the disk image contents
/// to the tar channel.
fn extract_tar_stream(
    http_rx: mpsc::Receiver<bytes::Bytes>,
    tar_tx: mpsc::Sender<Vec<u8>>,
    file_pattern: Option<&str>,
    compression: LayerCompression,
    debug: bool,
) -> Result<(), String> {
    // Create a reader that pulls from the channel
    let channel_reader = ChannelReader::new(http_rx);

    // Wrap with gzip decoder if needed
    let reader: Box<dyn Read + Send> = match compression {
        LayerCompression::Gzip => {
            if debug {
                eprintln!("[DEBUG] Using gzip decompression for layer");
            }
            Box::new(GzDecoder::new(channel_reader))
        }
        LayerCompression::None => {
            if debug {
                eprintln!("[DEBUG] Layer is uncompressed");
            }
            Box::new(channel_reader)
        }
        LayerCompression::Zstd => {
            return Err("Zstd compression not supported".to_string());
        }
    };

    // Parse as tar archive
    let mut archive = tar::Archive::new(reader);

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
