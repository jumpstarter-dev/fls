use crate::fls::download_error::DownloadError;
use crate::fls::options::HttpClientOptions;
use reqwest::Client;
use std::time::Duration;

pub(crate) async fn setup_http_client(
    options: &HttpClientOptions,
) -> Result<Client, Box<dyn std::error::Error>> {
    if options.debug {
        eprintln!("\n[DEBUG] Initializing HTTP Client:");
        eprintln!("[DEBUG]   TLS Backend: rustls");
        eprintln!("[DEBUG]   HTTP/2: Adaptive mode enabled");
        eprintln!("[DEBUG]   HTTP/2 Stream Window: 16 MB");
        eprintln!("[DEBUG]   HTTP/2 Connection Window: 32 MB");
        eprintln!("[DEBUG]   Connection Pool: max 10 idle per host");
        eprintln!("[DEBUG]   Pool Idle Timeout: 90s");
        eprintln!("[DEBUG]   TCP Keepalive: 10s");
        eprintln!("[DEBUG]   TCP Nodelay: enabled");
        eprintln!("[DEBUG]   Request Timeout: 7200s (2 hours)");
        eprintln!("[DEBUG]   Connect Timeout: 30s");
        eprintln!("[DEBUG]   DNS Resolver: system resolver");
    }

    let mut builder = Client::builder()
        // Explicitly use rustls TLS backend for better custom CA support
        .use_rustls_tls()
        // Enable HTTP/2 adaptive mode (will use HTTP/2 if server supports it)
        .http2_adaptive_window(true)
        .http2_initial_stream_window_size(Some(1024 * 1024 * 16)) // 16MB stream window
        .http2_initial_connection_window_size(Some(1024 * 1024 * 32)) // 32MB connection window
        // Increase connection pool settings
        .pool_max_idle_per_host(10)
        .pool_idle_timeout(Some(Duration::from_secs(90)))
        // Enable TCP keepalive to prevent connection drops
        .tcp_keepalive(Some(Duration::from_secs(10)))
        .tcp_nodelay(true) // Disable Nagle's algorithm for lower latency
        // Very long timeout for large downloads
        .timeout(Duration::from_secs(2 * 3600))
        .connect_timeout(Duration::from_secs(30))
        // Use system DNS resolver for better performance
        .no_hickory_dns();

    // Add custom CA certificate if provided
    if let Some(ca_cert_path) = &options.cacert {
        println!("Loading CA certificate from: {}", ca_cert_path.display());
        if options.debug {
            eprintln!(
                "[DEBUG]   Custom CA Certificate: {}",
                ca_cert_path.display()
            );
        }
        let cert_bytes = std::fs::read(ca_cert_path)
            .map_err(|e| format!("Failed to read CA certificate file: {}", e))?;

        let cert = reqwest::Certificate::from_pem(&cert_bytes)
            .map_err(|e| format!("Failed to parse CA certificate: {}", e))?;

        builder = builder.add_root_certificate(cert);
        println!("CA certificate loaded successfully");
    }

    if options.insecure_tls {
        println!("Warning: Certificate verification is disabled");
        if options.debug {
            eprintln!("[DEBUG]   Certificate Verification: DISABLED (insecure)");
        }
        builder = builder.danger_accept_invalid_certs(true);
    }

    if options.debug {
        eprintln!("[DEBUG] HTTP Client initialized successfully\n");
    }

    Ok(builder.build()?)
}

pub(crate) async fn start_download(
    url: &str,
    client: &Client,
    resume_from: Option<u64>,
    custom_headers: &[(String, String)],
    debug: bool,
) -> Result<reqwest::Response, DownloadError> {
    if let Some(offset) = resume_from {
        println!("Resuming download from: {} (byte offset: {})", url, offset);
    } else {
        println!("Starting download from: {}", url);
    }

    let mut request = client
        .get(url)
        .header("User-Agent", "fls/0.1.0")
        .header("Accept", "*/*")
        .header("Accept-Encoding", "identity"); // Don't compress, we're handling .xz ourselves

    // Add custom headers
    for (name, value) in custom_headers {
        request = request.header(name, value);
    }

    // Add Range header if resuming
    if let Some(offset) = resume_from {
        request = request.header("Range", format!("bytes={}-", offset));
    }

    // Debug: Log request details
    if debug {
        eprintln!("\n[DEBUG] HTTP Request:");
        eprintln!("[DEBUG]   Method: GET");
        eprintln!("[DEBUG]   URL: {}", url);
        eprintln!("[DEBUG]   Headers:");
        eprintln!("[DEBUG]     User-Agent: fls/0.1.0");
        eprintln!("[DEBUG]     Accept: */*");
        eprintln!("[DEBUG]     Accept-Encoding: identity");
        for (name, value) in custom_headers {
            eprintln!("[DEBUG]     {}: {}", name, value);
        }
        if let Some(offset) = resume_from {
            eprintln!("[DEBUG]     Range: bytes={}-", offset);
        }
    }

    let response = request.send().await.map_err(DownloadError::from_reqwest)?;

    // Debug: Log response details
    if debug {
        eprintln!("\n[DEBUG] HTTP Response:");
        eprintln!(
            "[DEBUG]   Status: {} {}",
            response.status().as_u16(),
            response.status().canonical_reason().unwrap_or("")
        );
        eprintln!("[DEBUG]   Version: {:?}", response.version());
        eprintln!("[DEBUG]   Headers:");
        for (name, value) in response.headers() {
            if let Ok(val_str) = value.to_str() {
                eprintln!("[DEBUG]     {}: {}", name, val_str);
            } else {
                eprintln!("[DEBUG]     {}: <binary data>", name);
            }
        }
        if let Some(remote_addr) = response.remote_addr() {
            eprintln!("[DEBUG]   Remote Address: {}", remote_addr);
        }
        eprintln!();
    }

    // Accept both 200 (full content) and 206 (partial content) as success
    if !response.status().is_success() && response.status().as_u16() != 206 {
        return Err(DownloadError::from_http_response(&response));
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

    Ok(response)
}
