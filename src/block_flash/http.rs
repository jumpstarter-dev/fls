use reqwest::Client;
use std::time::Duration;
use crate::block_flash::options::BlockFlashOptions;

pub(crate) async fn setup_http_client(options: &BlockFlashOptions) -> Result<Client, Box<dyn std::error::Error>> {
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
    
    // Add custom CA certificate if provided
    if let Some(ca_cert_path) = &options.ca_cert {
        println!("Loading CA certificate from: {}", ca_cert_path.display());
        let cert_bytes = std::fs::read(ca_cert_path)
            .map_err(|e| format!("Failed to read CA certificate file: {}", e))?;
        
        let cert = reqwest::Certificate::from_pem(&cert_bytes)
            .map_err(|e| format!("Failed to parse CA certificate: {}", e))?;
        
        builder = builder.add_root_certificate(cert);
        println!("CA certificate loaded successfully");
    }
    
    if options.ignore_certificates {
        println!("Warning: Certificate verification is disabled");
        builder = builder.danger_accept_invalid_certs(true);
    }
    
    Ok(builder.build()?)
}

pub(crate) async fn start_download(url: &str, client: &Client, resume_from: Option<u64>) -> Result<(reqwest::Response, bool), Box<dyn std::error::Error>> {
    if let Some(offset) = resume_from {
        println!("Resuming download from: {} (byte offset: {})", url, offset);
    } else {
        println!("Starting download from: {}", url);
    }
    
    let mut request = client.get(url)
        .header("User-Agent", "smallrs/0.1.0")
        .header("Accept", "*/*")
        .header("Accept-Encoding", "identity"); // Don't compress, we're handling .xz ourselves
    
    // Add Range header if resuming
    if let Some(offset) = resume_from {
        request = request.header("Range", format!("bytes={}-", offset));
    }
    
    let response = request.send().await?;
    
    // Accept both 200 (full content) and 206 (partial content) as success
    if !response.status().is_success() && response.status().as_u16() != 206 {
        return Err(format!("HTTP error: {}", response.status()).into());
    }
    
    // Check if server supports resume
    if resume_from.is_some() && response.status().as_u16() != 206 {
        println!("Warning: Server does not support range requests, starting from beginning");
    }

    let content_length = response.content_length();
    if let Some(len) = content_length {
        if resume_from.is_some() {
            println!("Remaining content length: {} bytes", len);
        } else {
            println!("Content length: {} bytes", len);
        }
    }

    let needs_decompression = matches!(
        url.rsplit('.').next().map(|s| s.to_lowercase()).as_deref(),
        Some("xz") | Some("gz") | Some("bz") | Some("bz2")
    );
    if needs_decompression && resume_from.is_none() {
        let extension = url.rsplit('.').next().unwrap_or("unknown");
        println!("Detected .{} extension, will decompress in real-time", extension);
    }

    Ok((response, needs_decompression))
}

