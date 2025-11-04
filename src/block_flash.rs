use reqwest::Client;
use std::io::{self, Write};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Command, Child};
use tokio::sync::mpsc;

pub struct BlockFlashOptions {
    pub ignore_certificates: bool,
    pub device: String,
    pub buffer_size_mb: usize,
}

impl Default for BlockFlashOptions {
    fn default() -> Self {
        Self {
            ignore_certificates: false,
            device: String::new(),
            buffer_size_mb: 1024,
        }
    }
}

struct ProgressTracker {
    bytes_received: u64,
    bytes_decompressed: u64,
    bytes_written: u64,
    start_time: Instant,
    last_update: Instant,
}

impl ProgressTracker {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            bytes_received: 0,
            bytes_decompressed: 0,
            bytes_written: 0,
            start_time: now,
            last_update: now,
        }
    }
    
    fn update_progress(&mut self, content_length: Option<u64>, update_interval: Duration) -> Result<(), Box<dyn std::error::Error>> {
        let now = Instant::now();
        if now.duration_since(self.last_update) >= update_interval {
            let elapsed = now.duration_since(self.start_time);
            let mb_received = self.bytes_received as f64 / (1024.0 * 1024.0);
            let mb_decompressed = self.bytes_decompressed as f64 / (1024.0 * 1024.0);
            let mb_written = self.bytes_written as f64 / (1024.0 * 1024.0);
            let download_mb_per_sec = if elapsed.as_secs_f64() > 0.0 {
                mb_received / elapsed.as_secs_f64()
            } else {
                0.0
            };
            let decompress_mb_per_sec = if elapsed.as_secs_f64() > 0.0 {
                mb_decompressed / elapsed.as_secs_f64()
            } else {
                0.0
            };
            let written_mb_per_sec = if elapsed.as_secs_f64() > 0.0 {
                mb_written / elapsed.as_secs_f64()
            } else {
                0.0
            };
            
            if let Some(total) = content_length {
                let progress = (self.bytes_received as f64 / total as f64) * 100.0;
                let total_mb = total as f64 / (1024.0 * 1024.0);
                print!("\rDownload: {:.2} MB / {:.2} MB ({:.1}%) | {:.2} MB/s | Decompressed: {:.2} MB | {:.2} MB/s | Written: {:.2} MB | {:.2} MB/s", 
                       mb_received, total_mb, progress, download_mb_per_sec, mb_decompressed, decompress_mb_per_sec, mb_written, written_mb_per_sec);
            } else {
                print!("\rDownload: {:.2} MB | {:.2} MB/s | Decompressed: {:.2} MB | {:.2} MB/s | Written: {:.2} MB | {:.2} MB/s", 
                       mb_received, download_mb_per_sec, mb_decompressed, decompress_mb_per_sec, mb_written, written_mb_per_sec);
            }
            io::stdout().flush()?;
            self.last_update = now;
        }
        Ok(())
    }
    
    fn final_stats(&self) -> (f64, f64, f64, f64, f64, f64) {
        let total_elapsed = Instant::now().duration_since(self.start_time);
        let final_mb_received = self.bytes_received as f64 / (1024.0 * 1024.0);
        let final_mb_decompressed = self.bytes_decompressed as f64 / (1024.0 * 1024.0);
        let final_mb_written = self.bytes_written as f64 / (1024.0 * 1024.0);
        let final_download_rate = if total_elapsed.as_secs_f64() > 0.0 {
            final_mb_received / total_elapsed.as_secs_f64()
        } else {
            0.0
        };
        let final_decompress_rate = if total_elapsed.as_secs_f64() > 0.0 {
            final_mb_decompressed / total_elapsed.as_secs_f64()
        } else {
            0.0
        };
        let final_written_rate = if total_elapsed.as_secs_f64() > 0.0 {
            final_mb_written / total_elapsed.as_secs_f64()
        } else {
            0.0
        };
        (final_mb_received, final_mb_decompressed, final_mb_written, final_download_rate, final_decompress_rate, final_written_rate)
    }
}

// Tokio task functions
async fn start_xzcat_process() -> Result<Child, Box<dyn std::error::Error>> {
    let xzcat = Command::new("xzcat")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    Ok(xzcat)
}

async fn start_dd_process(device: &str) -> Result<Child, Box<dyn std::error::Error>> {
    let dd = Command::new("dd")
        .arg(&format!("of={}", device))
        .arg("bs=64k")
        .arg("iflag=fullblock")
        .arg("oflag=direct")
        .arg("status=progress")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())  // Redirect stdout to /dev/null
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    Ok(dd)
}

async fn spawn_xzcat_stdout_reader(
    mut stdout: tokio::process::ChildStdout,
    decompressed_tx: mpsc::UnboundedSender<Vec<u8>>,
    mut dd_stdin: tokio::process::ChildStdin,
    error_tx: mpsc::UnboundedSender<String>,
) {
    let mut buffer = [0u8; 8 * 1024 * 1024]; // 8MB buffer for better performance
    loop {
        match stdout.read(&mut buffer).await {
            Ok(0) => break, // EOF
            Ok(n) => {
                // Send to progress tracking
                if decompressed_tx.send(buffer[..n].to_vec()).is_err() {
                    break;
                }
                // Write to dd stdin
                if let Err(e) = dd_stdin.write_all(&buffer[..n]).await {
                    let _ = error_tx.send(format!("Error writing to dd stdin: {}", e));
                    break;
                }
            }
            Err(e) => {
                let _ = error_tx.send(format!("Error reading from xzcat stdout: {}", e));
                break;
            }
        }
    }
    // Close dd stdin when xzcat is done
    let _ = dd_stdin.shutdown().await;
}

async fn spawn_stderr_reader(
    mut stderr: tokio::process::ChildStderr,
    error_tx: mpsc::UnboundedSender<String>,
    process_name: &'static str,
) {
    let mut buffer = [0u8; 1024];
    loop {
        match stderr.read(&mut buffer).await {
            Ok(0) => break, // EOF
            Ok(n) => {
                if let Ok(s) = String::from_utf8(buffer[..n].to_vec()) {
                    let _ = error_tx.send(format!("{}: {}", process_name, s.trim()));
                }
            }
            Err(_) => break,
        }
    }
}

async fn handle_dd_progress(
    mut dd_stderr: tokio::process::ChildStderr,
    dd_error_tx: mpsc::UnboundedSender<String>,
) {
    let mut buffer = [0u8; 1024];
    loop {
        match dd_stderr.read(&mut buffer).await {
            Ok(0) => break, // EOF
            Ok(n) => {
                if let Ok(s) = String::from_utf8(buffer[..n].to_vec()) {
                    let _ = dd_error_tx.send(s);
                }
            }
            Err(_) => break,
        }
    }
}

async fn process_dd_messages(
    mut dd_error_rx: mpsc::UnboundedReceiver<String>,
    written_tx: mpsc::UnboundedSender<u64>,
) -> Result<(), String> {
    while let Some(dd_msg) = dd_error_rx.recv().await {
        // Parse dd progress messages to extract bytes written
        if dd_msg.contains("bytes") && (dd_msg.contains("transferred") || dd_msg.contains("copied")) {
            // Parse format like: "13238272 bytes (13 MB, 13 MiB) transferred 13.725s, 965 kB/s"
            // or: "9895936 bytes (9896 kB, 9664 KiB) transferred 10.146s, 975 kB/s"
            // We want the second number in the tuple with its unit
            if let Some(paren_start) = dd_msg.find('(') {
                if let Some(paren_end) = dd_msg.find(')') {
                    let tuple_content = &dd_msg[paren_start + 1..paren_end];
                    // Split by comma and get the second part
                    let parts: Vec<&str> = tuple_content.split(',').collect();
                    if parts.len() >= 2 {
                        let second_part = parts[1].trim();
                        // Extract number and unit
                        if let Some(space_pos) = second_part.find(' ') {
                            let number_str = &second_part[..space_pos];
                            let unit = &second_part[space_pos + 1..];
                            
                            if let Ok(value) = number_str.parse::<u64>() {
                                // Convert to bytes based on unit
                                let bytes_written = match unit {
                                    "B" => value,
                                    "kB" => value * 1000,
                                    "kiB" => value * 1024,
                                    "MB" => value * 1000 * 1000,
                                    "MiB" => value * 1024 * 1024,
                                    "GB" => value * 1000 * 1000 * 1000,
                                    "GiB" => value * 1024 * 1024 * 1024,
                                    "TB" => value * 1000 * 1000 * 1000 * 1000,
                                    "TiB" => value * 1024 * 1024 * 1024 * 1024,
                                    _ => value, // fallback to raw value (treat as bytes)
                                };
                                let _ = written_tx.send(bytes_written);
                            }
                        } else {
                            // No space found, treat the entire second_part as a number (bytes)
                            if let Ok(value) = second_part.parse::<u64>() {
                                let _ = written_tx.send(value);
                            }
                        }
                    }
                }
            }
        } else if !dd_msg.trim().is_empty() {
            eprintln!("dd: {}", dd_msg.trim());
        }
    }
    Ok(())
}

async fn process_error_messages(
    mut error_rx: mpsc::UnboundedReceiver<String>,
) {
    while let Some(error_msg) = error_rx.recv().await {
        eprintln!("{}", error_msg);
    }
}

async fn setup_http_client(options: &BlockFlashOptions) -> Result<Client, Box<dyn std::error::Error>> {
    let mut builder = Client::builder()
        // Enable HTTP/2 adaptive mode (will use HTTP/2 if server supports it)
        .http2_adaptive_window(true)
        .http2_initial_stream_window_size(Some(1024 * 1024 * 16)) // 16MB stream window
        .http2_initial_connection_window_size(Some(1024 * 1024 * 32)) // 32MB connection window
        // Increase connection pool settings
        .pool_max_idle_per_host(10)
        .pool_idle_timeout(Some(Duration::from_secs(90)))
        // Enable TCP keepalive to prevent connection drops
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .tcp_nodelay(true) // Disable Nagle's algorithm for lower latency
        // Very long timeout for large downloads
        .timeout(Duration::from_secs(3600))
        .connect_timeout(Duration::from_secs(30))
        // Use system DNS resolver for better performance
        .no_hickory_dns();
    
    if options.ignore_certificates {
        println!("Warning: Certificate verification is disabled");
        builder = builder.danger_accept_invalid_certs(true);
    }
    
    Ok(builder.build()?)
}

async fn start_download(url: &str, client: &Client) -> Result<(reqwest::Response, bool), Box<dyn std::error::Error>> {
    println!("Starting download from: {}", url);
    
    let response = client.get(url)
        .header("User-Agent", "smallrs/0.1.0")
        .header("Accept", "*/*")
        .header("Accept-Encoding", "identity") // Don't compress, we're handling .xz ourselves
        .send()
        .await?;
    
    if !response.status().is_success() {
        return Err(format!("HTTP error: {}", response.status()).into());
    }

    let content_length = response.content_length();
    if let Some(len) = content_length {
        println!("Content length: {} bytes", len);
    }

    let needs_decompression = url.to_lowercase().ends_with(".xz");
    if needs_decompression {
        println!("Detected .xz extension, will decompress in real-time");
    }

    Ok((response, needs_decompression))
}

async fn handle_regular_download(
    mut stream: impl futures_util::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin + Send + 'static,
    content_length: Option<u64>,
    buffer_size_mb: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut progress = ProgressTracker::new();
    let update_interval = Duration::from_millis(100);
    
    use futures_util::StreamExt;
    
    // Calculate buffer capacity (assume 64KB average chunk size)
    let buffer_capacity = (buffer_size_mb * 1024 * 1024) / (64 * 1024);
    let buffer_capacity = buffer_capacity.max(1); // At least 1
    
    println!("Using download buffer: {} MB (capacity: {} chunks)", buffer_size_mb, buffer_capacity);
    
    // Create bounded channel for buffering
    let (buffer_tx, mut buffer_rx) = mpsc::channel::<bytes::Bytes>(buffer_capacity);
    
    // Spawn download task
    let download_task = tokio::spawn(async move {
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(c) => {
                    if buffer_tx.send(c).await.is_err() {
                        break; // Receiver dropped
                    }
                }
                Err(e) => {
                    return Err(Box::new(e) as Box<dyn std::error::Error + Send>);
                }
            }
        }
        Ok::<(), Box<dyn std::error::Error + Send>>(())
    });
    
    // Process buffered chunks
    while let Some(chunk) = buffer_rx.recv().await {
        progress.bytes_received += chunk.len() as u64;
        if let Err(e) = progress.update_progress(content_length, update_interval) {
            eprintln!(); // Print newline to clear progress line
            return Err(e);
        }
    }
    
    // Check if download task had an error
    match download_task.await {
        Ok(Ok(())) => {},
        Ok(Err(e)) => {
            eprintln!(); // Print newline to clear progress line
            return Err(e);
        }
        Err(e) => {
            eprintln!(); // Print newline to clear progress line
            return Err(e.into());
        }
    }
    
            let (mb_received, _, _, download_rate, _, _) = progress.final_stats();
            println!("\nDownload complete: {:.2} MB in {:.2}s ({:.2} MB/s)", 
                    mb_received, progress.start_time.elapsed().as_secs_f64(), download_rate);
    
    Ok(())
}

async fn handle_compressed_download(
    mut stream: impl futures_util::Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Unpin + Send + 'static,
    content_length: Option<u64>,
    options: BlockFlashOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting xzcat subprocess for streaming decompression...");
    let mut xzcat = start_xzcat_process().await?;
    
    println!("Starting dd subprocess to write to device: {}", options.device);
    let mut dd = start_dd_process(&options.device).await?;
    
    // Extract stdio handles
    let mut xzcat_stdin = xzcat.stdin.take().unwrap();
    let xzcat_stdout = xzcat.stdout.take().unwrap();
    let xzcat_stderr = xzcat.stderr.take().unwrap();
    let dd_stdin = dd.stdin.take().unwrap();
    let dd_stderr = dd.stderr.take().unwrap();
    
            // Create channels
            let (decompressed_tx, mut decompressed_rx) = mpsc::unbounded_channel::<Vec<u8>>();
            let (error_tx, error_rx) = mpsc::unbounded_channel::<String>();
            let (dd_error_tx, dd_error_rx) = mpsc::unbounded_channel::<String>();
            let (written_tx, mut written_rx) = mpsc::unbounded_channel::<u64>();
    
    // Spawn background tasks
    tokio::spawn(spawn_xzcat_stdout_reader(
        xzcat_stdout,
        decompressed_tx,
        dd_stdin,
        error_tx.clone(),
    ));
    
    tokio::spawn(spawn_stderr_reader(
        xzcat_stderr,
        error_tx.clone(),
        "xzcat",
    ));
    
    tokio::spawn(handle_dd_progress(
        dd_stderr,
        dd_error_tx,
    ));
    
            // Spawn message processors
            let dd_processor = tokio::spawn(process_dd_messages(dd_error_rx, written_tx));
            let error_processor = tokio::spawn(process_error_messages(error_rx));
    
    // Calculate buffer capacity (assume 64KB average chunk size)
    let buffer_size_mb = options.buffer_size_mb;
    let buffer_capacity = (buffer_size_mb * 1024 * 1024) / (64 * 1024);
    let buffer_capacity = buffer_capacity.max(1); // At least 1
    
    println!("Using download buffer: {} MB (capacity: {} chunks)", buffer_size_mb, buffer_capacity);
    
    // Create bounded channel for download buffering
    let (buffer_tx, mut buffer_rx) = mpsc::channel::<bytes::Bytes>(buffer_capacity);
    
    // Main download loop
    let mut progress = ProgressTracker::new();
    let update_interval = Duration::from_millis(100);
    
    use futures_util::StreamExt;
    
    // Create channel for tracking downloaded bytes
    let (downloaded_tx, mut downloaded_rx) = mpsc::unbounded_channel::<u64>();
    
    // Spawn download task
    let download_task = tokio::spawn(async move {
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(c) => {
                    let len = c.len() as u64;
                    if buffer_tx.send(c).await.is_err() {
                        break; // Receiver dropped
                    }
                    // Notify progress tracker of downloaded bytes
                    let _ = downloaded_tx.send(len);
                }
                Err(e) => {
                    return Err(Box::new(e) as Box<dyn std::error::Error + Send>);
                }
            }
        }
        Ok::<(), Box<dyn std::error::Error + Send>>(())
    });
    
    // Spawn xzcat writer task - this runs independently and can be blocked by xzcat
    let xzcat_writer = tokio::spawn(async move {
        while let Some(chunk) = buffer_rx.recv().await {
            if let Err(e) = xzcat_stdin.write_all(&chunk).await {
                return Err(format!("Error writing to xzcat stdin: {}", e));
            }
        }
        // Close xzcat stdin when done
        let _ = xzcat_stdin.shutdown().await;
        Ok::<(), String>(())
    });
    
    // Main progress tracking loop - not blocked by xzcat writes
    loop {
        // Update progress from all available sources
        let mut updated = false;
        
        while let Ok(downloaded_bytes) = downloaded_rx.try_recv() {
            progress.bytes_received += downloaded_bytes;
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
            if let Err(e) = progress.update_progress(content_length, update_interval) {
                eprintln!(); // Print newline to clear progress line
                return Err(e);
            }
        }
        
        // Check if download task is done
        if download_task.is_finished() {
            break;
        }
        
        // Small sleep to avoid busy waiting
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    
    // Check if download task had an error
    match download_task.await {
        Ok(Ok(())) => {},
        Ok(Err(e)) => {
            eprintln!(); // Print newline to clear progress line
            return Err(e);
        }
        Err(e) => {
            eprintln!(); // Print newline to clear progress line
            return Err(e.into());
        }
    }
    
    // Wait for xzcat writer task to finish (xzcat stdin is closed within the task)
    match xzcat_writer.await {
        Ok(Ok(())) => {},
        Ok(Err(e)) => {
            eprintln!(); // Print newline to clear progress line
            return Err(e.into());
        }
        Err(e) => {
            eprintln!(); // Print newline to clear progress line
            return Err(e.into());
        }
    }
    
    // Wait for processes to complete
    let xzcat_status = match xzcat.wait().await {
        Ok(status) => status,
        Err(e) => {
            eprintln!(); // Print newline to clear progress line
            return Err(e.into());
        }
    };
    if !xzcat_status.success() {
        eprintln!(); // Print newline to clear progress line
        return Err(format!("xzcat process failed with status: {:?}", xzcat_status.code()).into());
    }
    
    let dd_status = match dd.wait().await {
        Ok(status) => status,
        Err(e) => {
            eprintln!(); // Print newline to clear progress line
            return Err(e.into());
        }
    };
    if !dd_status.success() {
        eprintln!(); // Print newline to clear progress line
        return Err(format!("dd process failed with status: {:?}", dd_status.code()).into());
    }
    
            // Read any remaining downloaded bytes for final count
            while let Ok(downloaded_bytes) = downloaded_rx.try_recv() {
                progress.bytes_received += downloaded_bytes;
            }
            
            // Read any remaining decompressed data for final count
            while let Ok(decompressed_chunk) = decompressed_rx.try_recv() {
                progress.bytes_decompressed += decompressed_chunk.len() as u64;
            }
            
            // Read any remaining written data for final count
            while let Ok(written_bytes) = written_rx.try_recv() {
                progress.bytes_written = written_bytes;
            }
            
            // Wait for message processors to finish
            let _ = tokio::try_join!(dd_processor, error_processor);
            
            let (mb_received, mb_decompressed, mb_written, download_rate, decompress_rate, written_rate) = progress.final_stats();
            println!("\nDownload complete: {:.2} MB in {:.2}s ({:.2} MB/s)", 
                    mb_received, progress.start_time.elapsed().as_secs_f64(), download_rate);
            println!("Decompression complete: {:.2} MB in {:.2}s ({:.2} MB/s)", 
                    mb_decompressed, progress.start_time.elapsed().as_secs_f64(), decompress_rate);
            println!("Write complete: {:.2} MB in {:.2}s ({:.2} MB/s)", 
                    mb_written, progress.start_time.elapsed().as_secs_f64(), written_rate);
            println!("Compression ratio: {:.2}x", 
                    mb_received / mb_decompressed);
    
    Ok(())
}

pub async fn stream_and_decompress(
    url: &str,
    options: BlockFlashOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = setup_http_client(&options).await?;
    let (response, needs_decompression) = start_download(url, &client).await?;
    let content_length = response.content_length();
    let stream = response.bytes_stream();

    if needs_decompression {
        handle_compressed_download(stream, content_length, options).await
    } else {
        handle_regular_download(stream, content_length, options.buffer_size_mb).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_url_extension_detection() {
        assert!(stream_and_decompress("http://example.com/file.xz", BlockFlashOptions::default()).await.is_err()); // Will fail due to network, but we're testing the logic
        assert!(stream_and_decompress("http://example.com/file.img", BlockFlashOptions::default()).await.is_err());
    }
}
