use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::fls::block_writer::AsyncBlockWriter;
use crate::fls::decompress::{spawn_stderr_reader, start_decompressor_process};
use crate::fls::error_handling::process_error_messages;
use crate::fls::http::{setup_http_client, start_download};
use crate::fls::options::BlockFlashOptions;
use crate::fls::progress::ProgressTracker;

pub async fn flash_from_url(
    url: &str,
    options: BlockFlashOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = setup_http_client(&options).await?;

    println!("Starting decompressor subprocess for streaming decompression...");
    let (mut decompressor, decompressor_name) = start_decompressor_process(url).await?;

    println!("Opening block device for writing: {}", options.device);

    // Extract stdio handles
    let mut decompressor_stdin = decompressor.stdin.take().unwrap();
    let decompressor_stdout = decompressor.stdout.take().unwrap();
    let decompressor_stderr = decompressor.stderr.take().unwrap();

    // Create channels
    let (decompressed_tx, mut decompressed_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (error_tx, error_rx) = mpsc::unbounded_channel::<String>();
    let (written_tx, mut written_rx) = mpsc::unbounded_channel::<u64>();

    // Create block writer
    let block_writer = AsyncBlockWriter::new(
        options.device.clone(),
        written_tx,
        options.debug,
        options.o_direct,
    )?;

    // Spawn background task to read from decompressor and write to block device
    let error_tx_clone = error_tx.clone();
    let writer_handle = {
        let writer = block_writer;
        tokio::spawn(async move {
            let mut stdout = decompressor_stdout;
            let mut buffer = vec![0u8; 8 * 1024 * 1024]; // 8MB buffer

            loop {
                match stdout.read(&mut buffer).await {
                    Ok(0) => break, // EOF
                    Ok(n) => {
                        let data = buffer[..n].to_vec();
                        // Send to progress tracking
                        if decompressed_tx.send(data.clone()).is_err() {
                            break;
                        }
                        // Write to block device
                        if let Err(e) = writer.write(data).await {
                            let _ = error_tx_clone.send(format!("Error writing to device: {}", e));
                            return Err(e);
                        }
                    }
                    Err(e) => {
                        let _ = error_tx_clone
                            .send(format!("Error reading from decompressor stdout: {}", e));
                        return Err(e);
                    }
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
    let mut progress = ProgressTracker::new(options.newline_progress);
    // Set whether we're actually decompressing (not using cat for uncompressed files)
    progress.set_is_compressed(decompressor_name != "cat");
    let update_interval = Duration::from_secs_f64(options.progress_interval_secs);
    let mut bytes_sent_to_decompressor: u64 = 0;
    let mut retry_count = 0;
    let debug = options.debug;

    use futures_util::StreamExt;

    // Calculate buffer capacity (shared across all retry attempts)
    let buffer_size_mb = options.buffer_size_mb;
    // HTTP chunks from reqwest are typically 8-32 KB, not 64 KB
    // To ensure we get the full buffer size, use a conservative estimate
    let avg_chunk_size_kb = 8; // Conservative estimate (8 KB)
    let buffer_capacity = (buffer_size_mb * 1024) / avg_chunk_size_kb;
    let buffer_capacity = buffer_capacity.max(1000); // At least 1000 chunks

    println!(
        "Using download buffer: {} MB (capacity: {} chunks, ~{} KB per chunk)",
        buffer_size_mb, buffer_capacity, avg_chunk_size_kb
    );

    // Create persistent bounded channel for download buffering (lives across retries)
    let (buffer_tx, mut buffer_rx) = mpsc::channel::<bytes::Bytes>(buffer_capacity);

    // Channels for tracking bytes actually written to decompressor
    let (decompressor_written_tx, mut decompressor_written_rx) = mpsc::unbounded_channel::<u64>();

    // Spawn persistent task to write buffered chunks to decompressor
    let decompressor_writer_handle = tokio::spawn(async move {
        while let Some(chunk) = buffer_rx.recv().await {
            let chunk_len = chunk.len() as u64;
            if let Err(e) = decompressor_stdin.write_all(&chunk).await {
                return Err(format!("Error writing to decompressor stdin: {}", e));
            }
            // Notify that bytes were written to decompressor
            let _ = decompressor_written_tx.send(chunk_len);
        }
        // Close decompressor stdin when channel is closed
        Ok::<(), String>(())
    });

    loop {
        // Resume from the HTTP download position, not the decompressor write position
        // The buffer may contain data that's been downloaded but not yet written to decompressor
        let resume_from = if progress.bytes_received > 0 {
            Some(progress.bytes_received)
        } else {
            None
        };

        // Start or resume download
        let response = match start_download(url, &client, resume_from, &options.headers).await {
            Ok(r) => r,
            Err(e) => {
                if retry_count >= options.max_retries {
                    eprintln!("\nMax retries ({}) reached, giving up", options.max_retries);
                    return Err(e);
                }
                eprintln!(
                    "\nDownload connection failed: {}, retrying in {} seconds... (attempt {}/{})",
                    e,
                    options.retry_delay_secs,
                    retry_count + 1,
                    options.max_retries
                );
                tokio::time::sleep(Duration::from_secs(options.retry_delay_secs)).await;
                retry_count += 1;
                continue;
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
        let mut connection_error: Option<Box<dyn std::error::Error>> = None;

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
                                connection_error = Some("Buffer channel closed".into());
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
                            while let Ok(written_len) = decompressor_written_rx.try_recv() {
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
                            while let Ok(decompressed_chunk) = decompressed_rx.try_recv() {
                                progress.bytes_decompressed += decompressed_chunk.len() as u64;
                            }

                            while let Ok(written_bytes) = written_rx.try_recv() {
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
                            connection_error = Some(Box::new(e));
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
                    connection_error = Some("Connection timeout (30s)".into());
                    connection_broken = true;
                    break;
                }
            }
        }

        // If connection broke, retry
        if connection_broken {
            if retry_count >= options.max_retries {
                eprintln!("\nMax retries ({}) reached, giving up", options.max_retries);
                if let Some(e) = connection_error {
                    return Err(e);
                } else {
                    return Err("Download failed after max retries".into());
                }
            }

            eprintln!(
                "\nConnection interrupted: {}, resuming in {} seconds... (attempt {}/{})",
                connection_error
                    .as_ref()
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "Unknown error".to_string()),
                options.retry_delay_secs,
                retry_count + 1,
                options.max_retries
            );
            tokio::time::sleep(Duration::from_secs(options.retry_delay_secs)).await;
            retry_count += 1;
            continue;
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

        while let Ok(written_len) = decompressor_written_rx.try_recv() {
            bytes_sent_to_decompressor += written_len;
            progress.bytes_sent_to_decompressor += written_len;
            updated = true;
        }

        while let Ok(decompressed_chunk) = decompressed_rx.try_recv() {
            progress.bytes_decompressed += decompressed_chunk.len() as u64;
            updated = true;
        }

        while let Ok(written_bytes) = written_rx.try_recv() {
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

    // Get the result from the writer task
    match decompressor_writer_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            eprintln!();
            return Err(e.into());
        }
        Err(e) => {
            eprintln!();
            return Err(e.into());
        }
    }

    // Update any remaining progress
    while let Ok(decompressed_chunk) = decompressed_rx.try_recv() {
        progress.bytes_decompressed += decompressed_chunk.len() as u64;
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

            while let Ok(decompressed_chunk) = decompressed_rx.try_recv() {
                progress.bytes_decompressed += decompressed_chunk.len() as u64;
                updated = true;
            }

            while let Ok(written_bytes) = written_rx.try_recv() {
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

        while let Ok(written_bytes) = written_rx.try_recv() {
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
        Ok(Ok(final_bytes)) => {
            progress.bytes_written = final_bytes;
        }
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
    while let Ok(decompressed_chunk) = decompressed_rx.try_recv() {
        progress.bytes_decompressed += decompressed_chunk.len() as u64;
    }

    while let Ok(written_bytes) = written_rx.try_recv() {
        progress.bytes_written = written_bytes;
    }

    // Force a final progress update to show completion
    let _ = progress.update_progress(None, update_interval, true);

    // Wait for message processor to finish (with timeout)
    let timeout_duration = Duration::from_secs(2);
    let _ = tokio::time::timeout(timeout_duration, error_processor).await;

    let stats = progress.final_stats();
    println!(
        "\nDownload complete: {:.2} MB in {:.2}s ({:.2} MB/s)",
        stats.mb_received, stats.download_secs, stats.download_rate
    );
    println!(
        "Decompression complete: {:.2} MB in {:.2}s ({:.2} MB/s)",
        stats.mb_decompressed, stats.decompress_secs, stats.decompress_rate
    );
    println!(
        "Write complete: {:.2} MB in {:.2}s ({:.2} MB/s)",
        stats.mb_written, stats.write_secs, stats.write_rate
    );
    println!(
        "Compression ratio: {:.2}x",
        stats.mb_decompressed / stats.mb_received
    );

    Ok(())
}
