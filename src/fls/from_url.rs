use futures_util::stream::BoxStream;
use futures_util::{stream, StreamExt};
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::fls::block_writer::DeviceWriter;
use crate::fls::byte_channel::byte_bounded_channel;
use crate::fls::compression::Compression;
use crate::fls::decompress::{
    get_compression_from_path, get_compression_from_url, start_inprocess_decompressor,
};
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

use crate::fls::download_error::handle_download_retry;

/// Execute a sequence of write commands on the block writer
async fn execute_write_commands(
    commands: Vec<WriteCommand>,
    writer: &DeviceWriter,
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
    writer: &DeviceWriter,
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
    writer: &DeviceWriter,
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
    writer: &DeviceWriter,
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

/// Classify a flash source: `None` for HTTP/HTTPS URLs (handled by the
/// download pipeline), `Some(path)` for local files (plain or `file://`-prefixed).
///
/// `file://` is a literal path prefix, not a URI: `file://./a.img` is the
/// relative path `./a.img`, and `file:///home/a.img` is `/home/a.img`.
/// Characters such as spaces, `%`, `?`, and `#` are treated literally.
fn local_source_path(source: &str) -> Result<Option<PathBuf>, io::Error> {
    if source.split_once("://").is_some_and(|(scheme, _)| {
        scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
    }) {
        return Ok(None);
    }
    let path = source.strip_prefix("file://").unwrap_or(source);
    if path.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Empty source path",
        ));
    }
    // Reject other explicit URL schemes (e.g. ftp://, oci://) when they lead
    // the source, so paths that merely contain "://" later are still paths.
    if let Some((scheme, _)) = path.split_once("://") {
        if !scheme.is_empty()
            && scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Unsupported source scheme '{scheme}://' (supported: http://, https://, file://, or a local path)"
                ),
            ));
        }
    }
    Ok(Some(PathBuf::from(path)))
}

/// Stream a local source file in bounded 64 KiB reads.
///
/// A read error terminates the stream with that error; it is not retried.
fn local_file_stream(
    file: tokio::fs::File,
    path: PathBuf,
) -> BoxStream<'static, Result<bytes::Bytes, DownloadError>> {
    stream::try_unfold((file, path), |(mut file, path)| async move {
        let mut buffer = vec![0u8; 64 * 1024];
        let n = file.read(&mut buffer).await.map_err(|error| {
            DownloadError::Other(format!(
                "Failed to read source file '{}': {error}",
                path.display()
            ))
        })?;
        if n == 0 {
            return Ok(None);
        }
        buffer.truncate(n);
        Ok(Some((bytes::Bytes::from(buffer), (file, path))))
    })
    .boxed()
}

/// The decompressor thread has already exited (its input channel closed), so
/// joining it directly is safe and short. Returns its originating error.
fn decompressor_exited_error(
    handle: std::thread::JoinHandle<Result<(), String>>,
) -> Box<dyn std::error::Error> {
    match handle.join() {
        Ok(Ok(_)) => "Decompressor closed unexpectedly before input completed".into(),
        Ok(Err(e)) => e.into(),
        Err(_) => "Decompressor thread panicked".into(),
    }
}

/// The buffer channel closed, so the pipeline stopped early. Report the real
/// cause: the writer's error if it has one, otherwise the decompressor's.
/// The writer always finishes (a dead decompressor closes its input channel,
/// so the writer sees EOF and exits), so joining it cannot hang.
async fn pipeline_stopped_error(
    writer_handle: JoinHandle<io::Result<u64>>,
    decompressor_handle: std::thread::JoinHandle<Result<(), String>>,
) -> Box<dyn std::error::Error> {
    match await_writer_result(writer_handle).await {
        // The writer finished cleanly, so the decompressor must have exited early.
        Ok(_) => decompressor_exited_error(decompressor_handle),
        // The writer had an error (or panicked); that is the real cause.
        Err(e) => e.into(),
    }
}

/// Reject a local source that is the same file as a regular-file destination,
/// to avoid truncating the source before it is read. Compares file identities
/// (device + inode) rather than path strings, so equivalent paths (symlinks,
/// relative vs absolute, hard links) are detected. A destination that does not
/// yet exist cannot conflict, so it is allowed.
fn reject_same_file(source_id: (u64, u64), dest: &str) -> Result<(), Box<dyn std::error::Error>> {
    let dest_meta = match std::fs::metadata(dest) {
        Ok(m) => m,
        Err(_) => return Ok(()), // Destination does not exist yet; no conflict.
    };
    let dest_id = (dest_meta.dev(), dest_meta.ino());
    if dest_id == source_id {
        return Err(format!(
            "Source and destination are the same file ('{dest}'); refusing to truncate the source"
        )
        .into());
    }
    Ok(())
}

/// A flash source: a local file stream or an HTTP/HTTPS download.
///
/// Both expose the same `next_chunk()` interface so the feed loop is a single
/// path. The HTTP variant owns the 30s read timeout, `Range` resume, and retry
/// logic, so retries are transparent to the caller. The local variant is a
/// single pass with no timeout and no retry.
enum SourceFeed {
    Local {
        stream: Option<InputStream>,
        content_length: Option<u64>,
    },
    Http {
        client: reqwest::Client,
        source: String,
        headers: Vec<(String, String)>,
        max_retries: usize,
        retry_delay_secs: u64,
        debug: bool,
        retry_count: usize,
        stream: Option<InputStream>,
        content_length: Option<u64>,
    },
}

type InputStream = BoxStream<'static, Result<bytes::Bytes, DownloadError>>;

impl SourceFeed {
    /// Return the next chunk, or `None` at end of input.
    ///
    /// `bytes_received` is the total bytes fed so far; the HTTP variant uses it
    /// as the `Range` resume offset when re-establishing a connection.
    async fn next_chunk(
        &mut self,
        bytes_received: u64,
    ) -> Result<Option<bytes::Bytes>, DownloadError> {
        match self {
            // Local: a single pass, no timeout, no retry.
            SourceFeed::Local { stream, .. } => match stream.as_mut().unwrap().next().await {
                Some(Ok(chunk)) => Ok(Some(chunk)),
                Some(Err(e)) => Err(e),
                None => Ok(None),
            },
            SourceFeed::Http {
                client,
                source,
                headers,
                max_retries,
                retry_delay_secs,
                debug,
                retry_count,
                stream,
                content_length,
            } => loop {
                // Ensure a live connection, retrying on start/resume failure.
                if stream.is_none() {
                    let resume_from = (bytes_received > 0).then_some(bytes_received);
                    match start_download(source, client, resume_from, headers, *debug).await {
                        Ok(response) => {
                            *content_length = if let Some(offset) = resume_from {
                                response.content_length().map(|len| len + offset)
                            } else {
                                response.content_length()
                            };
                            *stream = Some(
                                response
                                    .bytes_stream()
                                    .map(|result| result.map_err(DownloadError::from_reqwest))
                                    .boxed(),
                            );
                        }
                        Err(e) => {
                            if !Self::retry(
                                &e,
                                retry_count,
                                *max_retries,
                                *retry_delay_secs,
                                stream,
                            )
                            .await
                            {
                                return Err(e);
                            }
                            // The retry dropped the connection; re-establish it.
                            continue;
                        }
                    }
                }
                // HTTP chunks wait at most 30s.
                match tokio::time::timeout(Duration::from_secs(30), stream.as_mut().unwrap().next())
                    .await
                {
                    Ok(Some(Ok(chunk))) => {
                        *retry_count = 0; // Reset retry count on successful read
                        return Ok(Some(chunk));
                    }
                    Ok(None) => return Ok(None), // Stream ended successfully
                    Ok(Some(Err(e))) => {
                        if !Self::retry(&e, retry_count, *max_retries, *retry_delay_secs, stream)
                            .await
                        {
                            return Err(e);
                        }
                    }
                    Err(_) => {
                        let e = DownloadError::TimeoutError("Connection timeout (30s)".to_string());
                        if !Self::retry(&e, retry_count, *max_retries, *retry_delay_secs, stream)
                            .await
                        {
                            return Err(e);
                        }
                    }
                }
            },
        }
    }

    /// The content length for progress, if known.
    fn content_length(&self) -> Option<u64> {
        match self {
            SourceFeed::Local { content_length, .. } => *content_length,
            SourceFeed::Http { content_length, .. } => *content_length,
        }
    }

    /// Sleep for the retry delay and drop the connection so the next read
    /// re-establishes it (resuming from `bytes_received`). Returns `false` when
    /// the error is non-retryable or the retry budget is exhausted.
    async fn retry(
        error: &DownloadError,
        retry_count: &mut usize,
        max_retries: usize,
        retry_delay_secs: u64,
        stream: &mut Option<InputStream>,
    ) -> bool {
        match handle_download_retry(error, retry_count, max_retries, retry_delay_secs) {
            Some(delay) => {
                tokio::time::sleep(delay).await;
                *stream = None;
                true
            }
            None => false,
        }
    }
}

/// How the feed loop ended.
enum FeedEnd {
    /// The source reached end of input.
    Done,
    /// The source failed (a local read error, or a non-retryable/exhausted
    /// download error).
    SourceFailed(DownloadError),
    /// The writer (or the buffer channel) stopped early.
    PipelineStopped,
}

/// Flash a block device from a source.
///
/// The source is an HTTP/HTTPS URL, a `file://`-prefixed path, or a plain
/// local path (relative or absolute). Local files stream in bounded chunks
/// through the same decompression, sparse, and writer pipeline as downloads;
/// local failures are reported without download retries.
pub async fn flash_from(
    source: &str,
    options: BlockFlashOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    // Classify the source: local file or HTTP/HTTPS download.
    let local_path = local_source_path(source)?;
    let is_local = local_path.is_some();

    // Prepare the input before touching the destination: open and validate the
    // local source, or create the HTTP client for downloads.
    let (compression, mut source_feed, source_file_id) = if let Some(path) = local_path {
        let file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| format!("Failed to open source file '{}': {e}", path.display()))?;
        let metadata = file.metadata().await.map_err(|e| {
            format!(
                "Failed to read source file metadata '{}': {e}",
                path.display()
            )
        })?;
        if !metadata.is_file() {
            return Err(format!("Source '{}' is not a regular file", path.display()).into());
        }
        let file_size = metadata.len();
        // File identity (device + inode) for the same-file guard below.
        let source_file_id = (metadata.dev(), metadata.ino());
        eprintln!("Reading source file: {}", path.display());
        let compression = get_compression_from_path(&path);
        (
            compression,
            SourceFeed::Local {
                stream: Some(local_file_stream(file, path)),
                content_length: Some(file_size),
            },
            Some(source_file_id),
        )
    } else {
        let http_options: HttpClientOptions = (&options).into();
        let client = setup_http_client(&http_options).await?;
        (
            get_compression_from_url(source),
            SourceFeed::Http {
                client,
                source: source.to_string(),
                headers: options.headers.clone(),
                max_retries: options.max_retries,
                retry_delay_secs: options.retry_delay_secs,
                debug: options.common.debug,
                retry_count: 0,
                stream: None,
                content_length: None,
            },
            None,
        )
    };

    if compression == Compression::Zstd {
        return Err("Zstd in-process decompression is not supported".into());
    }
    let is_compressed = compression != Compression::None;
    if is_compressed {
        eprintln!("Using decompressor: {} (in-process)", compression);
    }

    // Create channels
    let (decompressed_progress_tx, mut decompressed_progress_rx) = mpsc::unbounded_channel::<u64>();
    let (error_tx, error_rx) = mpsc::unbounded_channel::<String>();
    let (written_progress_tx, mut written_progress_rx) = mpsc::unbounded_channel::<u64>();

    // Guard against truncating the source: if the local source is the same
    // file as the destination, refuse before the destination is opened.
    if let Some(source_file_id) = source_file_id {
        reject_same_file(source_file_id, &options.common.device)?;
    }

    println!(
        "Opening block device for writing: {}",
        options.common.device
    );

    // Create block writer
    let block_writer = DeviceWriter::new(&options.common, written_progress_tx)?;

    // Create byte-bounded input buffer
    let buffer_size_mb = options.common.buffer_size_mb;
    let max_buffer_bytes = buffer_size_mb * 1024 * 1024;

    if is_local {
        println!("Using input buffer: {} MB (byte-bounded)", buffer_size_mb);
    } else {
        println!(
            "Using download buffer: {} MB (byte-bounded)",
            buffer_size_mb
        );
    }

    let (buffer_tx, buffer_rx) = byte_bounded_channel::<bytes::Bytes>(max_buffer_bytes, 4096);

    // Channel for tracking bytes consumed from buffer by decompressor
    let (decompressor_written_progress_tx, mut decompressor_written_progress_rx) =
        mpsc::unbounded_channel::<u64>();

    // Start in-process decompressor thread
    let (mut decompressed_rx, decompressor_handle) = start_inprocess_decompressor(
        buffer_rx,
        compression,
        decompressor_written_progress_tx,
        options.common.xz_memlimit_mb,
    )?;

    // Signals the feed loop when the writer task finishes, so a full input
    // buffer cannot deadlock it (e.g. the SSH connection breaks mid-flash).
    let (writer_done_tx, mut writer_done_rx) = tokio::sync::watch::channel(false);

    // Spawn background task to read decompressed data and write to block device
    let error_tx_clone = error_tx.clone();
    let debug = options.common.debug;
    let writer_handle = {
        let writer = block_writer;
        let done_tx = writer_done_tx;
        tokio::spawn(async move {
            // Fire `writer_done` however this task ends (success, error, or
            // panic) so the feed loop's blocking buffer send can always unblock.
            struct WriterDone(Option<tokio::sync::watch::Sender<bool>>);
            impl Drop for WriterDone {
                fn drop(&mut self) {
                    if let Some(tx) = self.0.take() {
                        let _ = tx.send(true);
                    }
                }
            }
            let _done = WriterDone(Some(done_tx));

            let mut detector = FormatDetector::new();
            let mut parser: Option<SparseParser> = None;
            let mut format_determined = false;

            loop {
                let data = match decompressed_rx.recv().await {
                    Some(data) => data,
                    None => {
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
                };

                let n = data.len();
                if decompressed_progress_tx.send(n as u64).is_err() {
                    break;
                }

                if !format_determined {
                    match detector.process(&data) {
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
                            let remaining = &data[consumed_from_input..n];
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
                    process_sparse_data(p, &data, &writer, &error_tx_clone, debug).await?;
                } else {
                    write_regular_data(data, &writer, &error_tx_clone).await?;
                }
            }

            writer.close().await
        })
    };

    // Spawn message processors
    let error_processor = tokio::spawn(process_error_messages(error_rx));

    // Main input loop: a single path for both local and HTTP sources.
    let mut progress =
        ProgressTracker::new(options.common.newline_progress, options.common.show_memory);
    progress.set_is_compressed(is_compressed);
    if is_local {
        progress.set_input_label("Read");
    }
    let update_interval = Duration::from_secs_f64(options.common.progress_interval_secs);
    let mut bytes_sent_to_decompressor: u64 = 0;
    let debug = options.common.debug;

    let end = loop {
        // Gzip decoding and writing can finish before we observe source EOF.
        // Read the source first; a finished writer is only premature if more
        // input remains. The send below still watches for writer failure.
        let chunk = match source_feed.next_chunk(progress.bytes_received).await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break FeedEnd::Done,
            Err(e) => break FeedEnd::SourceFailed(e),
        };

        // Set content length in progress tracker (only on first attempt)
        if progress.content_length.is_none() {
            progress.set_content_length(source_feed.content_length());
        }

        let chunk_len = chunk.len() as u64;

        // Send to buffer - detect if it's blocking. Race the send against the
        // writer finishing: if the writer stops (e.g. SSH breaks) while the
        // buffer is full, the send would otherwise block forever.
        let send_start = std::time::Instant::now();
        let send_result = tokio::select! {
            result = buffer_tx.send(chunk) => result,
            _ = writer_done_rx.wait_for(|done| *done) => break FeedEnd::PipelineStopped,
        };
        if send_result.is_err() {
            break FeedEnd::PipelineStopped;
        }
        let send_duration = send_start.elapsed();

        // If send took a long time, buffer was probably full
        if debug && send_duration > Duration::from_millis(100) {
            eprintln!(
                "\n[DEBUG] Buffer send blocked for {:.2}s (buffer full, decompressor bottleneck)",
                send_duration.as_secs_f64()
            );
        }

        // Update input progress
        progress.bytes_received += chunk_len;

        // Track bytes actually written to decompressor
        while let Ok(written_len) = decompressor_written_progress_rx.try_recv() {
            bytes_sent_to_decompressor += written_len;
            progress.bytes_sent_to_decompressor += written_len;
        }

        // Debug: Show buffer lag (data read but not yet written to decompressor)
        if debug && (progress.bytes_received % (50 * 1024 * 1024)) < chunk_len {
            let buffer_lag_mb =
                (progress.bytes_received - bytes_sent_to_decompressor) as f64 / (1024.0 * 1024.0);
            eprintln!(
                "[DEBUG] Buffer lag: {:.2} MB (read but not yet sent to decompressor)",
                buffer_lag_mb
            );
        }

        // Update progress from other channels
        while let Ok(byte_count) = decompressed_progress_rx.try_recv() {
            progress.bytes_decompressed += byte_count;
        }

        while let Ok(written_bytes) = written_progress_rx.try_recv() {
            progress.bytes_written = written_bytes;
        }

        if let Err(e) =
            progress.update_progress(source_feed.content_length(), update_interval, false)
        {
            eprintln!();
            return Err(e);
        }
    };

    match end {
        FeedEnd::Done => {}
        FeedEnd::SourceFailed(e) if !writer_handle.is_finished() => return Err(e.into()),
        // A source error after the writer stopped is a symptom; report the cause.
        FeedEnd::SourceFailed(_) | FeedEnd::PipelineStopped => {
            return Err(pipeline_stopped_error(writer_handle, decompressor_handle).await)
        }
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

    let decompressor_result = tokio::task::spawn_blocking(move || decompressor_handle.join())
        .await
        .map_err(|_| "Decompressor task panicked")?
        .map_err(|_| "Decompressor thread panicked")?;

    if let Err(e) = decompressor_result {
        eprintln!();
        return Err(e.into());
    }

    while let Ok(byte_count) = decompressed_progress_rx.try_recv() {
        progress.bytes_decompressed += byte_count;
    }
    while let Ok(written_len) = decompressor_written_progress_rx.try_recv() {
        progress.bytes_sent_to_decompressor += written_len;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn file_id(path: &std::path::Path) -> (u64, u64) {
        let m = std::fs::metadata(path).unwrap();
        (m.dev(), m.ino())
    }

    #[test]
    fn reject_same_file_detects_identical_and_allows_distinct() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.img");
        let other = dir.path().join("other.img");
        std::fs::write(&source, b"data").unwrap();
        std::fs::write(&other, b"data").unwrap();
        let source_id = file_id(&source);

        // The same file is rejected.
        assert!(reject_same_file(source_id, source.to_str().unwrap()).is_err());
        // A different file is allowed.
        assert!(reject_same_file(source_id, other.to_str().unwrap()).is_ok());
        // A destination that does not exist yet is allowed (nothing to truncate).
        assert!(reject_same_file(
            source_id,
            dir.path().join("does-not-exist").to_str().unwrap()
        )
        .is_ok());
    }

    #[test]
    fn reject_same_file_detects_equivalent_paths_via_hard_link() {
        // A hard link is a different path but the same file identity; a path
        // string comparison would miss it, an identity comparison must not.
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source.img");
        let link = dir.path().join("link.img");
        std::fs::write(&source, b"data").unwrap();
        std::fs::hard_link(&source, &link).unwrap();
        let source_id = file_id(&source);

        assert!(reject_same_file(source_id, link.to_str().unwrap()).is_err());
    }
}
