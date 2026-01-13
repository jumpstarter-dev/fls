use std::io;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::fls::block_writer::AsyncBlockWriter;
use crate::fls::decompress::{spawn_stderr_reader, start_decompressor_process};
use crate::fls::download_error::DownloadError;
use crate::fls::error_handling::process_error_messages;
use crate::fls::format_detector::{DetectionResult, FileFormat, FormatDetector};
use crate::fls::http::{setup_http_client, start_download};
use crate::fls::options::{BlockFlashOptions, HttpClientOptions};
use crate::fls::progress::ProgressTracker;
use crate::fls::simg::{SparseParser, WriteCommand};

/// Await a writer handle and return its result
///
/// Converts task panics into io::Error for uniform error handling.
async fn await_writer_result(handle: JoinHandle<io::Result<u64>>) -> io::Result<u64> {
    match handle.await {
        Ok(result) => result,
        Err(e) => Err(io::Error::other(format!("Writer task panicked: {}", e))),
    }
}

/// Get error from a prematurely finished writer handle
///
/// Called when writer_handle.is_finished() returns true unexpectedly during download.
/// Returns an appropriate error for the unexpected termination.
async fn get_writer_error(handle: JoinHandle<io::Result<u64>>) -> Box<dyn std::error::Error> {
    match await_writer_result(handle).await {
        Ok(_) => "Writer closed unexpectedly before download completed".into(),
        Err(e) => e.into(),
    }
}

/// Get error from a prematurely finished decompressor writer handle
///
/// Called when decompressor_writer_handle.is_finished() returns true unexpectedly.
async fn get_decompressor_error(
    handle: JoinHandle<Result<(), String>>,
) -> Box<dyn std::error::Error> {
    match handle.await {
        Ok(Ok(())) => "Decompressor stdin closed unexpectedly".into(),
        Ok(Err(e)) => e.into(),
        Err(e) => format!("Decompressor writer task panicked: {}", e).into(),
    }
}

/// Handles retry logic for download errors.
///
/// Returns `Some(Duration)` with the delay to wait before retrying, or `None` if
/// the error is non-retryable or max retries have been exceeded.
fn handle_retry_error(
    error: &DownloadError,
    retry_count: &mut usize,
    max_retries: usize,
    default_retry_delay_secs: u64,
) -> Option<Duration> {
    if !error.is_retryable() {
        eprintln!(
            "\nDownload failed with non-retryable error: {}",
            error.format_error()
        );
        return None;
    }

    if *retry_count >= max_retries {
        eprintln!("\nMax retries ({}) reached, giving up", max_retries);
        eprintln!("Last error: {}", error.format_error());
        return None;
    }

    let retry_delay = error
        .suggested_retry_delay()
        .unwrap_or_else(|| Duration::from_secs(default_retry_delay_secs));

    eprintln!("\nDownload failed: {}", error.format_error());
    eprintln!(
        "Retrying in {} seconds... (attempt {}/{})",
        retry_delay.as_secs(),
        *retry_count + 1,
        max_retries
    );
    *retry_count += 1;
    Some(retry_delay)
}

/// Execute a sequence of write commands on the block writer
async fn execute_write_commands(
    commands: Vec<WriteCommand>,
    writer: &AsyncBlockWriter,
    error_tx: &mpsc::UnboundedSender<String>,
    debug: bool,
) -> io::Result<()> {
    for cmd in commands {
        match cmd {
            WriteCommand::Write(data) => {
                writer.write(data).await.map_err(|e| {
                    let _ = error_tx.send(format!("Error writing to device: {}", e));
                    e
                })?;
            }
            WriteCommand::Seek(offset) => {
                if debug {
                    eprintln!("[DEBUG] Sparse: seeking to offset {}", offset);
                }
                writer.seek(offset).await.map_err(|e| {
                    let _ = error_tx.send(format!("Error seeking device: {}", e));
                    e
                })?;
            }
            WriteCommand::Fill { pattern, bytes } => {
                if debug {
                    eprintln!(
                        "[DEBUG] Sparse: fill pattern {:02x}{:02x}{:02x}{:02x} for {} bytes",
                        pattern[0], pattern[1], pattern[2], pattern[3], bytes
                    );
                }
                writer.fill(pattern, bytes).await.map_err(|e| {
                    let _ = error_tx.send(format!("Error filling device: {}", e));
                    e
                })?;
            }
            WriteCommand::Complete { expected_size } => {
                if debug {
                    eprintln!(
                        "[DEBUG] Sparse: complete, expected output size {} bytes",
                        expected_size
                    );
                }
            }
        }
    }
    Ok(())
}

/// Process data through sparse parser and execute resulting write commands
async fn process_sparse_data(
    parser: &mut SparseParser,
    data: &[u8],
    writer: &AsyncBlockWriter,
    error_tx: &mpsc::UnboundedSender<String>,
    debug: bool,
) -> io::Result<()> {
    let (commands, _consumed) = parser.process(data).map_err(|e| {
        let msg = format!("Sparse image parse error: {}", e);
        let _ = error_tx.send(msg.clone());
        io::Error::new(io::ErrorKind::InvalidData, msg)
    })?;
    execute_write_commands(commands, writer, error_tx, debug).await
}

/// Write data to the block writer with error reporting
async fn write_regular_data(
    data: Vec<u8>,
    writer: &AsyncBlockWriter,
    error_tx: &mpsc::UnboundedSender<String>,
) -> io::Result<()> {
    writer.write(data).await.map_err(|e| {
        let _ = error_tx.send(format!("Error writing to device: {}", e));
        e
    })
}

/// Handle detected format: process initial data and return parser if sparse
async fn handle_detected_format(
    format: FileFormat,
    consumed_bytes: Vec<u8>,
    remaining_data: &[u8],
    writer: &AsyncBlockWriter,
    error_tx: &mpsc::UnboundedSender<String>,
    debug: bool,
) -> io::Result<Option<SparseParser>> {
    match format {
        FileFormat::SparseImage => {
            println!("Sparse image (simg) format detected");
            if debug {
                eprintln!("[DEBUG] Auto-detect: Detected sparse image format");
            }
            let mut parser = SparseParser::new();

            // Process the accumulated detection data
            process_sparse_data(&mut parser, &consumed_bytes, writer, error_tx, debug).await?;

            // Process any remaining data in current buffer
            if !remaining_data.is_empty() {
                process_sparse_data(&mut parser, remaining_data, writer, error_tx, debug).await?;
            }

            Ok(Some(parser))
        }
        FileFormat::Regular => {
            if debug {
                eprintln!("[DEBUG] Auto-detect: Detected regular file format");
            }
            // Write the accumulated detection data
            write_regular_data(consumed_bytes, writer, error_tx).await?;

            // Write any remaining data
            if !remaining_data.is_empty() {
                write_regular_data(remaining_data.to_vec(), writer, error_tx).await?;
            }

            Ok(None)
        }
    }
}

pub async fn flash_from_url(
    url: &str,
    options: BlockFlashOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    let http_options: HttpClientOptions = (&options).into();
    let client = setup_http_client(&http_options).await?;

    let (mut decompressor, decompressor_name) = start_decompressor_process(url).await?;

    // Extract stdio handles
    let mut decompressor_stdin = decompressor.stdin.take().unwrap();
    let decompressor_stdout = decompressor.stdout.take().unwrap();
    let decompressor_stderr = decompressor.stderr.take().unwrap();

    // Create channels
    let (decompressed_progress_tx, mut decompressed_progress_rx) = mpsc::unbounded_channel::<u64>();
    let (error_tx, error_rx) = mpsc::unbounded_channel::<String>();
    let (written_progress_tx, mut written_progress_rx) = mpsc::unbounded_channel::<u64>();

    println!(
        "Opening block device for writing: {}",
        options.common.device
    );

    // Create block writer
    let block_writer = AsyncBlockWriter::new(
        options.common.device.clone(),
        written_progress_tx,
        options.common.debug,
        options.common.o_direct,
        options.common.write_buffer_size_mb,
    )?;

    // Spawn background task to read from decompressor and write to block device
    let error_tx_clone = error_tx.clone();
    let debug = options.common.debug;
    let writer_handle = {
        let writer = block_writer;
        tokio::spawn(async move {
            let mut stdout = decompressor_stdout;
            let mut buffer = vec![0u8; 8 * 1024 * 1024]; // 8MB buffer

            // Auto-detect sparse image format from initial data
            let mut detector = FormatDetector::new();
            let mut parser: Option<SparseParser> = None;
            let mut format_determined = false;

            loop {
                let n = match stdout.read(&mut buffer).await {
                    Ok(0) => {
                        // EOF - check if we have incomplete format detection
                        if !format_determined {
                            if let Some(buffered_data) = detector.finalize_at_eof() {
                                if debug {
                                    eprintln!(
                                        "[DEBUG] EOF before format detection complete, writing {} buffered bytes as regular data",
                                        buffered_data.len()
                                    );
                                }
                                write_regular_data(buffered_data, &writer, &error_tx_clone).await?;
                            }
                        }
                        break;
                    }
                    Ok(n) => n,
                    Err(e) => {
                        let _ = error_tx_clone
                            .send(format!("Error reading from decompressor stdout: {}", e));
                        return Err(e);
                    }
                };

                if decompressed_progress_tx.send(n as u64).is_err() {
                    break;
                }

                if !format_determined {
                    match detector.process(&buffer[..n]) {
                        DetectionResult::NeedMoreData => {
                            if debug {
                                eprintln!(
                                    "[DEBUG] Auto-detect: Need more data for format detection"
                                );
                            }
                            continue;
                        }
                        DetectionResult::Detected {
                            format,
                            consumed_bytes,
                            consumed_from_input,
                        } => {
                            let remaining = &buffer[consumed_from_input..n];
                            parser = handle_detected_format(
                                format,
                                consumed_bytes,
                                remaining,
                                &writer,
                                &error_tx_clone,
                                debug,
                            )
                            .await?;
                            format_determined = true;
                        }
                        DetectionResult::Error(msg) => {
                            let _ = error_tx_clone.send(msg.clone());
                            return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
                        }
                    }
                } else if let Some(ref mut p) = parser {
                    process_sparse_data(p, &buffer[..n], &writer, &error_tx_clone, debug).await?;
                } else {
                    write_regular_data(buffer[..n].to_vec(), &writer, &error_tx_clone).await?;
                }
            }

            // Close writer and get final bytes written
            writer.close().await
        })
    };

    tokio::spawn(spawn_stderr_reader(
        decompressor_stderr,
        error_tx.clone(),
        decompressor_name,
    ));

    // Spawn message processors
    let error_processor = tokio::spawn(process_error_messages(error_rx));

    // Main download loop with retry logic
    let mut progress =
        ProgressTracker::new(options.common.newline_progress, options.common.show_memory);
    // Set whether we're actually decompressing (not using cat for uncompressed files)
    progress.set_is_compressed(decompressor_name != "cat");
    let update_interval = Duration::from_secs_f64(options.common.progress_interval_secs);
    let mut bytes_sent_to_decompressor: u64 = 0;
    let mut retry_count = 0;
    let debug = options.common.debug;

    use futures_util::StreamExt;

    // Calculate buffer capacity (shared across all retry attempts)
    let buffer_size_mb = options.common.buffer_size_mb;
    // HTTP chunks from reqwest are typically 8-32 KB, not 64 KB
    // To ensure we get the full buffer size, use a conservative estimate
    let avg_chunk_size_kb = 16; // From common obvervation: 16kb
    let buffer_capacity = (buffer_size_mb * 1024) / avg_chunk_size_kb;
    let buffer_capacity = buffer_capacity.max(1000); // At least 1000 chunks

    println!(
        "Using download buffer: {} MB (capacity: {} chunks, ~{} KB per chunk)",
        buffer_size_mb, buffer_capacity, avg_chunk_size_kb
    );

    // Create persistent bounded channel for download buffering (lives across retries)
    let (buffer_tx, mut buffer_rx) = mpsc::channel::<bytes::Bytes>(buffer_capacity);

    // Channels for tracking bytes actually written to decompressor
    let (decompressor_written_progress_tx, mut decompressor_written_progress_rx) =
        mpsc::unbounded_channel::<u64>();

    // Spawn persistent task to write buffered chunks to decompressor
    let decompressor_writer_handle = tokio::spawn(async move {
        while let Some(chunk) = buffer_rx.recv().await {
            let chunk_len = chunk.len() as u64;
            if let Err(e) = decompressor_stdin.write_all(&chunk).await {
                return Err(format!("Error writing to decompressor stdin: {}", e));
            }
            // Notify that bytes were written to decompressor
            let _ = decompressor_written_progress_tx.send(chunk_len);
        }
        // Close decompressor stdin when channel is closed
        Ok::<(), String>(())
    });

    loop {
        // Check if writer or decompressor has failed before attempting download/retry
        if writer_handle.is_finished() {
            eprintln!();
            eprintln!("Writer task has terminated, stopping download");
            return Err(get_writer_error(writer_handle).await);
        }
        if decompressor_writer_handle.is_finished() {
            eprintln!();
            eprintln!("Decompressor writer task has terminated, stopping download");
            return Err(get_decompressor_error(decompressor_writer_handle).await);
        }

        // Resume from the HTTP download position, not the decompressor write position
        // The buffer may contain data that's been downloaded but not yet written to decompressor
        let resume_from = if progress.bytes_received > 0 {
            Some(progress.bytes_received)
        } else {
            None
        };

        // Start or resume download
        let response =
            match start_download(url, &client, resume_from, &options.headers, debug).await {
                Ok(r) => r,
                Err(e) => {
                    match handle_retry_error(
                        &e,
                        &mut retry_count,
                        options.max_retries,
                        options.retry_delay_secs,
                    ) {
                        Some(delay) => {
                            tokio::time::sleep(delay).await;
                            continue;
                        }
                        None => return Err(e.into()),
                    }
                }
            };

        let content_length = if let Some(offset) = resume_from {
            // For resumed downloads, we need to add the offset to partial content length
            response.content_length().map(|len| len + offset)
        } else {
            response.content_length()
        };

        // Set content length in progress tracker (only on first attempt)
        if progress.content_length.is_none() {
            progress.set_content_length(content_length);
        }

        let mut stream = response.bytes_stream();

        // Download and buffer chunks for this connection
        let mut connection_broken = false;
        let mut connection_error: Option<DownloadError> = None;

        loop {
            // Try to get next chunk with timeout
            match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
                Ok(Some(chunk_result)) => {
                    match chunk_result {
                        Ok(chunk) => {
                            let chunk_len = chunk.len() as u64;

                            // Send to buffer - detect if it's blocking
                            let send_start = std::time::Instant::now();
                            if buffer_tx.send(chunk).await.is_err() {
                                // Check if writer or decompressor has failed
                                if writer_handle.is_finished() {
                                    eprintln!();
                                    eprintln!("Writer task has terminated unexpectedly");
                                    return Err(get_writer_error(writer_handle).await);
                                }
                                if decompressor_writer_handle.is_finished() {
                                    eprintln!();
                                    eprintln!(
                                        "Decompressor writer task has terminated unexpectedly"
                                    );
                                    return Err(
                                        get_decompressor_error(decompressor_writer_handle).await
                                    );
                                }
                                connection_error =
                                    Some(DownloadError::Other("Buffer channel closed".to_string()));
                                connection_broken = true;
                                break;
                            }
                            let send_duration = send_start.elapsed();

                            // If send took a long time, buffer was probably full
                            if debug && send_duration > Duration::from_millis(100) {
                                eprintln!("\n[DEBUG] Buffer send blocked for {:.2}s (buffer full, decompressor bottleneck)", send_duration.as_secs_f64());
                            }

                            // Update download progress
                            progress.bytes_received += chunk_len;
                            retry_count = 0; // Reset retry count on successful download

                            // Track bytes actually written to decompressor
                            while let Ok(written_len) = decompressor_written_progress_rx.try_recv()
                            {
                                bytes_sent_to_decompressor += written_len;
                                progress.bytes_sent_to_decompressor += written_len;
                            }

                            // Debug: Show buffer lag (data downloaded but not yet written to decompressor)
                            if debug && (progress.bytes_received % (50 * 1024 * 1024)) < chunk_len {
                                let buffer_lag_mb =
                                    (progress.bytes_received - bytes_sent_to_decompressor) as f64
                                        / (1024.0 * 1024.0);
                                eprintln!("[DEBUG] Buffer lag: {:.2} MB (downloaded but not yet sent to decompressor)", buffer_lag_mb);
                            }

                            // Update progress from other channels
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
                            connection_error = Some(DownloadError::from_reqwest(e));
                            connection_broken = true;
                            break;
                        }
                    }
                }
                Ok(None) => {
                    // Stream ended successfully
                    break;
                }
                Err(_) => {
                    // Timeout
                    connection_error = Some(DownloadError::TimeoutError(
                        "Connection timeout (30s)".to_string(),
                    ));
                    connection_broken = true;
                    break;
                }
            }
        }

        // If connection broke, retry
        if connection_broken {
            // First check if writer or decompressor has failed - if so, don't retry
            if writer_handle.is_finished() {
                eprintln!();
                eprintln!("Connection interrupted and writer task has terminated");
                return Err(get_writer_error(writer_handle).await);
            }
            if decompressor_writer_handle.is_finished() {
                eprintln!();
                eprintln!("Connection interrupted and decompressor writer task has terminated");
                return Err(get_decompressor_error(decompressor_writer_handle).await);
            }

            if let Some(e) = connection_error {
                eprintln!("\nConnection interrupted: {}", e.format_error());
                match handle_retry_error(
                    &e,
                    &mut retry_count,
                    options.max_retries,
                    options.retry_delay_secs,
                ) {
                    Some(delay) => {
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    None => return Err(e.into()),
                }
            } else {
                // Unknown error
                eprintln!("\nConnection interrupted with unknown error");
                if retry_count >= options.max_retries {
                    eprintln!("\nMax retries ({}) reached, giving up", options.max_retries);
                    return Err("Download failed after max retries".into());
                }
                tokio::time::sleep(Duration::from_secs(options.retry_delay_secs)).await;
                retry_count += 1;
                continue;
            }
        }

        // Download completed successfully
        break;
    }

    // Capture the download rate and duration at completion
    let elapsed = progress.start_time.elapsed();
    progress.download_duration = Some(elapsed);
    if elapsed.as_secs_f64() > 0.0 {
        let mb_received = progress.bytes_received as f64 / (1024.0 * 1024.0);
        progress.final_download_rate = Some(mb_received / elapsed.as_secs_f64());
    }

    // Close buffer channel to signal end of download
    drop(buffer_tx);

    // Poll for decompressor writer completion while showing progress
    loop {
        // Update progress from all channels
        let mut updated = false;

        while let Ok(written_len) = decompressor_written_progress_rx.try_recv() {
            progress.bytes_sent_to_decompressor += written_len;
            updated = true;
        }

        while let Ok(byte_count) = decompressed_progress_rx.try_recv() {
            progress.bytes_decompressed += byte_count;
            updated = true;
        }

        while let Ok(written_bytes) = written_progress_rx.try_recv() {
            progress.bytes_written = written_bytes;
            updated = true;
        }

        if updated {
            let _ = progress.update_progress(None, update_interval, false);
        }

        // Check if decompressor writer task is done
        if decompressor_writer_handle.is_finished() {
            break;
        }

        // Small sleep to avoid busy waiting
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Get the result from the decompressor writer task
    if let Err(e) = decompressor_writer_handle
        .await
        .map_err(|e| e.to_string())
        .and_then(|r| r)
    {
        eprintln!();
        return Err(e.into());
    }

    // Update any remaining progress
    while let Ok(byte_count) = decompressed_progress_rx.try_recv() {
        progress.bytes_decompressed += byte_count;
    }

    // Check if decompressor has already finished
    let decompressor_already_done = match decompressor.try_wait() {
        Ok(Some(status)) => {
            if !status.success() {
                eprintln!();
                return Err(format!(
                    "{} process failed with status: {:?}",
                    decompressor_name,
                    status.code()
                )
                .into());
            }
            true
        }
        Ok(None) => false,
        Err(e) => {
            eprintln!();
            return Err(e.into());
        }
    };

    // Only wait if decompressor is not already done
    if !decompressor_already_done {
        // Poll for decompressor completion while showing progress
        loop {
            // Update progress from channels
            let mut updated = false;

            while let Ok(byte_count) = decompressed_progress_rx.try_recv() {
                progress.bytes_decompressed += byte_count;
                updated = true;
            }

            while let Ok(written_bytes) = written_progress_rx.try_recv() {
                progress.bytes_written = written_bytes;
                updated = true;
            }

            if updated {
                let _ = progress.update_progress(None, update_interval, false);
            }

            // Check if decompressor is done (non-blocking check)
            match decompressor.try_wait() {
                Ok(Some(status)) => {
                    if !status.success() {
                        eprintln!();
                        return Err(format!(
                            "{} process failed with status: {:?}",
                            decompressor_name,
                            status.code()
                        )
                        .into());
                    }
                    break;
                }
                Ok(None) => {
                    // Still running, sleep briefly
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    eprintln!();
                    return Err(e.into());
                }
            }
        }
    }

    // Capture the decompression rate and duration at completion
    let elapsed = progress.start_time.elapsed();
    progress.decompress_duration = Some(elapsed);
    if elapsed.as_secs_f64() > 0.0 {
        let mb_decompressed = progress.bytes_decompressed as f64 / (1024.0 * 1024.0);
        progress.final_decompress_rate = Some(mb_decompressed / elapsed.as_secs_f64());
    }

    // Wait for writer to complete
    loop {
        // Update progress from channels
        let mut updated = false;

        while let Ok(written_bytes) = written_progress_rx.try_recv() {
            progress.bytes_written = written_bytes;
            updated = true;
        }

        if updated {
            let _ = progress.update_progress(None, update_interval, false);
        }

        // Check if writer is done
        if writer_handle.is_finished() {
            break;
        }

        // Small sleep to avoid busy waiting
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Get final result from writer
    match writer_handle.await {
        Ok(Ok(final_bytes)) => progress.bytes_written = final_bytes,
        Ok(Err(e)) => {
            eprintln!();
            return Err(e.into());
        }
        Err(e) => {
            eprintln!();
            return Err(e.into());
        }
    }

    // Capture the write rate and duration at completion
    let elapsed = progress.start_time.elapsed();
    progress.write_duration = Some(elapsed);
    if elapsed.as_secs_f64() > 0.0 {
        let mb_written = progress.bytes_written as f64 / (1024.0 * 1024.0);
        progress.final_write_rate = Some(mb_written / elapsed.as_secs_f64());
    }

    // Read any remaining progress updates
    while let Ok(byte_count) = decompressed_progress_rx.try_recv() {
        progress.bytes_decompressed += byte_count;
    }

    while let Ok(written_bytes) = written_progress_rx.try_recv() {
        progress.bytes_written = written_bytes;
    }

    // Force a final progress update to show completion
    let _ = progress.update_progress(None, update_interval, true);

    // Wait for message processor to finish (with timeout)
    let timeout_duration = Duration::from_secs(2);
    let _ = tokio::time::timeout(timeout_duration, error_processor).await;

    progress.print_final_stats_with_ratio();

    Ok(())
}
