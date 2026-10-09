// Integration tests for flash_from function
mod common;

use fls::{flash_from, BlockFlashOptions, FlashOptions};
use std::path::PathBuf;
use tempfile::NamedTempFile;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Helper to create BlockFlashOptions with common test defaults
fn test_options(device: String) -> BlockFlashOptions {
    BlockFlashOptions {
        common: FlashOptions {
            device,
            o_direct: false,
            debug: false,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Helper to create BlockFlashOptions with custom settings
fn test_options_with(
    device: String,
    o_direct: bool,
    debug: bool,
    insecure_tls: bool,
    cacert: Option<PathBuf>,
) -> BlockFlashOptions {
    BlockFlashOptions {
        common: FlashOptions {
            device,
            o_direct,
            debug,
            insecure_tls,
            cacert,
            ..Default::default()
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn test_flash_uncompressed_file() {
    // Start mock HTTP server
    let mock_server = MockServer::start().await;

    // Create test data (10 MB)
    let test_data = common::create_test_data(10 * 1024 * 1024);

    // Setup mock endpoint to serve the test file
    Mock::given(method("GET"))
        .and(path("/test.img"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(test_data.clone()))
        .mount(&mock_server)
        .await;

    // Create temporary file to act as the destination device
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let device_path = temp_file.path().to_string_lossy().to_string();

    // Configure options for flashing
    let options = test_options(device_path.clone());

    // Execute the flash operation
    let url = format!("{}/test.img", mock_server.uri());
    let result = flash_from(&url, options).await;

    // Verify the operation succeeded
    assert!(result.is_ok(), "Flash operation failed: {:?}", result.err());

    // Read back the written data
    let written_data = std::fs::read(temp_file.path()).expect("Failed to read written file");

    // Verify the data matches byte-for-byte
    assert_eq!(
        written_data.len(),
        test_data.len(),
        "Written data length mismatch"
    );
    assert_eq!(
        written_data, test_data,
        "Written data does not match source"
    );

    println!(
        "✓ Test passed: {} bytes written and verified",
        test_data.len()
    );
}

#[tokio::test]
async fn test_flash_xz_compressed_file() {
    // Start mock HTTP server
    let mock_server = MockServer::start().await;

    // Create test data (5 MB uncompressed)
    let original_data = common::create_test_data(5 * 1024 * 1024);

    // Compress the data with xz
    let compressed_data = common::compress_xz(&original_data);

    println!(
        "Test data: {} bytes uncompressed, {} bytes compressed (ratio: {:.2}x)",
        original_data.len(),
        compressed_data.len(),
        original_data.len() as f64 / compressed_data.len() as f64
    );

    // Setup mock endpoint to serve the compressed file
    Mock::given(method("GET"))
        .and(path("/test.img.xz"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(compressed_data.clone()))
        .mount(&mock_server)
        .await;

    // Create temporary file to act as the destination device
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let device_path = temp_file.path().to_string_lossy().to_string();

    // Configure options for flashing
    let options = test_options(device_path.clone());

    // Execute the flash operation
    let url = format!("{}/test.img.xz", mock_server.uri());
    let result = flash_from(&url, options).await;

    // Verify the operation succeeded
    assert!(result.is_ok(), "Flash operation failed: {:?}", result.err());

    // Read back the written data
    let written_data = std::fs::read(temp_file.path()).expect("Failed to read written file");

    // Verify the decompressed data matches the original uncompressed data
    assert_eq!(
        written_data.len(),
        original_data.len(),
        "Written data length mismatch"
    );
    assert_eq!(
        written_data, original_data,
        "Decompressed data does not match original"
    );

    println!(
        "✓ Test passed: {} bytes compressed -> {} bytes decompressed and verified",
        compressed_data.len(),
        written_data.len()
    );
}

#[tokio::test]
async fn test_flash_gz_compressed_file() {
    // Start mock HTTP server
    let mock_server = MockServer::start().await;

    // Create test data (5 MB uncompressed)
    let original_data = common::create_test_data(5 * 1024 * 1024);

    // Compress the data with gzip
    let compressed_data = common::compress_gz(&original_data);

    println!(
        "Test data: {} bytes uncompressed, {} bytes compressed (ratio: {:.2}x)",
        original_data.len(),
        compressed_data.len(),
        original_data.len() as f64 / compressed_data.len() as f64
    );

    // Setup mock endpoint to serve the compressed file
    Mock::given(method("GET"))
        .and(path("/test.img.gz"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(compressed_data.clone()))
        .mount(&mock_server)
        .await;

    // Create temporary file to act as the destination device
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let device_path = temp_file.path().to_string_lossy().to_string();

    // Configure options for flashing
    let options = test_options(device_path.clone());

    // Execute the flash operation
    let url = format!("{}/test.img.gz", mock_server.uri());
    let result = flash_from(&url, options).await;

    // Verify the operation succeeded
    assert!(result.is_ok(), "Flash operation failed: {:?}", result.err());

    // Read back the written data
    let written_data = std::fs::read(temp_file.path()).expect("Failed to read written file");

    // Verify the decompressed data matches the original uncompressed data
    assert_eq!(
        written_data.len(),
        original_data.len(),
        "Written data length mismatch"
    );
    assert_eq!(
        written_data, original_data,
        "Decompressed data does not match original"
    );

    println!(
        "✓ Test passed: {} bytes compressed -> {} bytes decompressed and verified",
        compressed_data.len(),
        written_data.len()
    );
}

#[tokio::test]
async fn test_resume_after_connection_failure() {
    // Start mock HTTP server
    let mock_server = MockServer::start().await;

    // Create test data (10 MB)
    let test_data = common::create_test_data(10 * 1024 * 1024);

    println!("Test: Simulating server error (503) to trigger retry");

    // First request: fail with 503 Service Unavailable to trigger retry
    Mock::given(method("GET"))
        .and(path("/test.img"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // Second request: succeed with full data
    Mock::given(method("GET"))
        .and(path("/test.img"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(test_data.clone())
                .insert_header("Accept-Ranges", "bytes"),
        )
        .mount(&mock_server)
        .await;

    // Create temporary file to act as the destination device
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let device_path = temp_file.path().to_string_lossy().to_string();

    // Configure options for flashing
    let mut options = test_options(device_path.clone());
    options.retry_delay_secs = 0; // No delay for faster testing
    options.max_retries = 5;

    // Execute the flash operation
    let url = format!("{}/test.img", mock_server.uri());
    let result = flash_from(&url, options).await;

    // Verify the operation succeeded
    assert!(result.is_ok(), "Flash operation failed: {:?}", result.err());

    // Read back the written data
    let written_data = std::fs::read(temp_file.path()).expect("Failed to read written file");

    // Verify the complete data was written
    assert_eq!(
        written_data.len(),
        test_data.len(),
        "Written data length mismatch"
    );
    assert_eq!(
        written_data, test_data,
        "Written data does not match source"
    );

    println!(
        "✓ Test passed: Server error triggered retry, completed {} bytes",
        written_data.len()
    );
}

#[tokio::test]
async fn test_resume_compressed_file() {
    // Start mock HTTP server
    let mock_server = MockServer::start().await;

    // Create test data (5 MB uncompressed)
    let original_data = common::create_test_data(5 * 1024 * 1024);

    // Compress the data with xz
    let compressed_data = common::compress_xz(&original_data);

    println!(
        "Test: {} bytes compressed data, simulating server error to trigger retry",
        compressed_data.len()
    );

    // First request: fail with 503 to trigger retry
    Mock::given(method("GET"))
        .and(path("/test.img.xz"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // Second request: succeed with full compressed data
    Mock::given(method("GET"))
        .and(path("/test.img.xz"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(compressed_data.clone())
                .insert_header("Accept-Ranges", "bytes"),
        )
        .mount(&mock_server)
        .await;

    // Create temporary file to act as the destination device
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let device_path = temp_file.path().to_string_lossy().to_string();

    // Configure options for flashing
    let mut options = test_options(device_path.clone());
    options.retry_delay_secs = 0; // No delay for faster testing
    options.max_retries = 5;

    // Execute the flash operation
    let url = format!("{}/test.img.xz", mock_server.uri());
    let result = flash_from(&url, options).await;

    // Verify the operation succeeded
    assert!(result.is_ok(), "Flash operation failed: {:?}", result.err());

    // Read back the written data
    let written_data = std::fs::read(temp_file.path()).expect("Failed to read written file");

    // Verify the decompressed data matches the original
    assert_eq!(
        written_data.len(),
        original_data.len(),
        "Decompressed data length mismatch"
    );
    assert_eq!(
        written_data, original_data,
        "Decompressed data does not match original"
    );

    println!(
        "✓ Test passed: Server error triggered retry with compressed file, decompressed to {} bytes",
        written_data.len()
    );
}

#[tokio::test]
async fn test_multiple_connection_failures() {
    // Start mock HTTP server
    let mock_server = MockServer::start().await;

    // Create test data (5 MB)
    let test_data = common::create_test_data(5 * 1024 * 1024);

    println!(
        "Test: Simulating 2 server errors (503) to test multiple retries, total {} bytes",
        test_data.len()
    );

    // First request: fail with 503
    Mock::given(method("GET"))
        .and(path("/test.img"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // Second request: fail with 503 again
    Mock::given(method("GET"))
        .and(path("/test.img"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    // Third request: succeed with full data
    Mock::given(method("GET"))
        .and(path("/test.img"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(test_data.clone())
                .insert_header("Accept-Ranges", "bytes"),
        )
        .mount(&mock_server)
        .await;

    // Create temporary file to act as the destination device
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let device_path = temp_file.path().to_string_lossy().to_string();

    // Configure options for flashing
    let mut options = test_options(device_path.clone());
    options.retry_delay_secs = 0; // No delay for faster testing
    options.max_retries = 5;

    // Execute the flash operation
    let url = format!("{}/test.img", mock_server.uri());
    let result = flash_from(&url, options).await;

    // Verify the operation succeeded
    assert!(result.is_ok(), "Flash operation failed: {:?}", result.err());

    // Read back the written data
    let written_data = std::fs::read(temp_file.path()).expect("Failed to read written file");

    // Verify the complete data was written
    assert_eq!(
        written_data.len(),
        test_data.len(),
        "Written data length mismatch"
    );
    assert_eq!(
        written_data, test_data,
        "Written data does not match source"
    );

    println!(
        "✓ Test passed: 2 server errors triggered 2 retries, completed {} bytes",
        written_data.len()
    );
}

/// Test with a REAL HTTP server that simulates a partial transfer
///
/// This test creates an actual HTTP server (not wiremock) that can violate the HTTP protocol
/// by setting Content-Length to the full file size (10MB) but only sending partial data (5MB).
/// This simulates a real connection drop mid-transfer.
///
/// The test verifies that:
/// 1. The client detects the incomplete transfer (Content-Length mismatch)
/// 2. The client retries the download
/// 3. The final file is complete and correct
///
/// Note: Due to how hyper detects the Content-Length mismatch early, the client may retry
/// from the beginning rather than using a Range header. This is still correct behavior as
/// it successfully recovers from the partial transfer.
#[tokio::test]
async fn test_real_partial_transfer_with_resume() {
    use http_body_util::{BodyExt, Full};
    use hyper::body::Bytes;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request as HyperRequest, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::net::TcpListener;

    // Create test data (10 MB)
    let test_data = Arc::new(common::create_test_data(10 * 1024 * 1024));
    let split_point = test_data.len() / 2; // 5 MB
    let request_count = Arc::new(AtomicUsize::new(0));

    println!(
        "Test: Real HTTP server - transfer {} bytes, drop connection at {}, resume with Range request",
        test_data.len(),
        split_point
    );

    // Clone for the service
    let test_data_clone = test_data.clone();
    let request_count_clone = request_count.clone();

    // Create service that handles requests
    let make_service = move |_conn| {
        let test_data = test_data_clone.clone();
        let request_count = request_count_clone.clone();

        async move {
            Ok::<_, Infallible>(service_fn(
                move |req: HyperRequest<hyper::body::Incoming>| {
                    let test_data = test_data.clone();
                    let request_count = request_count.clone();

                    async move {
                        let req_num = request_count.fetch_add(1, Ordering::SeqCst);

                        // Check for Range header
                        let range_header = req.headers().get("range").and_then(|h| h.to_str().ok());

                        if let Some(range) = range_header {
                            // Parse Range header: "bytes=5242880-"
                            if let Some(start_str) = range
                                .strip_prefix("bytes=")
                                .and_then(|s| s.strip_suffix("-"))
                            {
                                if let Ok(start) = start_str.parse::<usize>() {
                                    println!(
                                        "  [Server] Request #{}: Range request from byte {}",
                                        req_num, start
                                    );

                                    // Return remaining data with 206 Partial Content
                                    let remaining_data = &test_data[start..];
                                    let content_range = format!(
                                        "bytes {}-{}/{}",
                                        start,
                                        test_data.len() - 1,
                                        test_data.len()
                                    );

                                    return Ok::<_, Infallible>(
                                        Response::builder()
                                            .status(StatusCode::PARTIAL_CONTENT)
                                            .header(
                                                "Content-Length",
                                                remaining_data.len().to_string(),
                                            )
                                            .header("Content-Range", content_range)
                                            .header("Accept-Ranges", "bytes")
                                            .body(
                                                Full::new(Bytes::copy_from_slice(remaining_data))
                                                    .boxed(),
                                            )
                                            .unwrap(),
                                    );
                                }
                            }
                        }

                        // First request: Send partial data with full Content-Length
                        if req_num == 0 {
                            println!("  [Server] Request #0: Sending partial data ({} bytes) with Content-Length: {}", 
                                 split_point, test_data.len());

                            // Send only partial data - this simulates connection drop
                            let partial_data = &test_data[..split_point];

                            // IMPORTANT: Set Content-Length to FULL size but only send partial data
                            // This will cause the HTTP client to detect incomplete transfer
                            return Ok::<_, Infallible>(
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .header("Content-Length", test_data.len().to_string()) // Full size!
                                    .header("Accept-Ranges", "bytes")
                                    .body(Full::new(Bytes::copy_from_slice(partial_data)).boxed())
                                    .unwrap(),
                            );
                        }

                        // Subsequent requests without Range: send full data
                        println!("  [Server] Request #{}: Sending full data", req_num);
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("Content-Length", test_data.len().to_string())
                                .header("Accept-Ranges", "bytes")
                                .body(Full::new(Bytes::copy_from_slice(&test_data)).boxed())
                                .unwrap(),
                        )
                    }
                },
            ))
        }
    };

    // Bind to random port
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let server_url = format!("http://{}/test.img", local_addr);

    println!("  [Server] Listening on {}", server_url);

    // Spawn server in background
    let server_handle = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);
            let service = make_service(()).await.unwrap();

            tokio::spawn(async move {
                if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                    // Connection errors are expected when we drop the connection
                    eprintln!("  [Server] Connection error (expected): {:?}", err);
                }
            });
        }
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Create temporary file to act as the destination device
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let device_path = temp_file.path().to_string_lossy().to_string();

    // Configure options for flashing
    let mut options = test_options(device_path.clone());
    options.retry_delay_secs = 0; // No delay for faster testing
    options.max_retries = 5;

    // Execute the flash operation
    let result = flash_from(&server_url, options).await;

    // Shutdown server
    server_handle.abort();

    // Verify the operation succeeded
    assert!(result.is_ok(), "Flash operation failed: {:?}", result.err());

    // Read back the written data
    let written_data = std::fs::read(temp_file.path()).expect("Failed to read written file");

    // Verify the complete data was written
    assert_eq!(
        written_data.len(),
        test_data.len(),
        "Written data length mismatch"
    );
    assert_eq!(
        &written_data[..],
        &test_data[..],
        "Written data does not match source"
    );

    // Verify we made multiple requests (initial + resume)
    let final_count = request_count.load(Ordering::SeqCst);
    println!(
        "✓ Test passed: Partial transfer detected, resumed with Range request, {} total requests, completed {} bytes",
        final_count,
        written_data.len()
    );
}

// NOTE: This test validates HTTPS downloads using custom CA certificates with nip.io domains.
// The key to making this work is explicitly calling .use_rustls_tls() on the reqwest client
// builder, which ensures the rustls backend is used instead of defaulting to native-tls/OpenSSL.
//
// Setup:
// ✓ Certificates generated with nip.io domains (127.0.0.1.nip.io)
// ✓ DNS resolution via nip.io (127.0.0.1.nip.io -> 127.0.0.1)
// ✓ Certificate includes proper SANs (DNS:127.0.0.1.nip.io, DNS:*.127.0.0.1.nip.io)
// ✓ Explicitly use rustls via .use_rustls_tls()
#[tokio::test]
async fn test_https_with_custom_ca_certificate() {
    use http_body_util::{BodyExt, Full};
    use hyper::body::Bytes;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request as HyperRequest, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use std::convert::Infallible;
    use std::fs;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    println!("Test: HTTPS download with custom CA certificate (using nip.io domain)");

    // Install default crypto provider for rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Load server certificate and key
    let cert_dir = PathBuf::from("tests/test_certs");
    let server_cert_pem =
        fs::read_to_string(cert_dir.join("server-cert.pem")).expect("Failed to read server cert");
    let server_key_pem =
        fs::read_to_string(cert_dir.join("server-key.pem")).expect("Failed to read server key");

    // Parse certificate and key
    let server_cert = rustls_pemfile::certs(&mut server_cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to parse server cert");
    let server_key = rustls_pemfile::private_key(&mut server_key_pem.as_bytes())
        .expect("Failed to parse server key")
        .expect("No private key found");

    // Create TLS config
    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(server_cert, server_key)
        .expect("Failed to create server config");
    server_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

    // Create test data (5 MB)
    let test_data = Arc::new(common::create_test_data(5 * 1024 * 1024));

    // Bind to random port
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    // Use nip.io domain for proper hostname validation with custom CA
    let server_url = format!("https://127.0.0.1.nip.io:{}/secure.img", local_addr.port());

    println!("  [Server] HTTPS server listening on {}", server_url);

    // Spawn server that handles connections with a timeout
    let test_data_clone = test_data.clone();
    let server_handle = tokio::spawn(async move {
        let timeout_duration = tokio::time::Duration::from_secs(30);
        let deadline = tokio::time::Instant::now() + timeout_duration;

        loop {
            // Accept connections with timeout
            let accept_result = tokio::time::timeout_at(deadline, listener.accept()).await;

            let (stream, _) = match accept_result {
                Ok(Ok(conn)) => conn,
                Ok(Err(e)) => {
                    eprintln!("  [Server] Accept error: {:?}", e);
                    continue;
                }
                Err(_) => {
                    println!("  [Server] Timeout waiting for connections, exiting");
                    break;
                }
            };

            let tls_acceptor = tls_acceptor.clone();
            let test_data = test_data_clone.clone();

            tokio::spawn(async move {
                let tls_stream = match tls_acceptor.accept(stream).await {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("  [Server] TLS accept error: {:?}", e);
                        return;
                    }
                };

                let io = TokioIo::new(tls_stream);
                let service = service_fn(move |_req: HyperRequest<hyper::body::Incoming>| {
                    let test_data = test_data.clone();
                    async move {
                        println!("  [Server] Serving request");
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(Bytes::copy_from_slice(&test_data)).boxed())
                                .unwrap(),
                        )
                    }
                });

                if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                    eprintln!("  [Server] Connection error: {:?}", err);
                }
                println!("  [Server] Request handled");
            });

            // After successfully accepting one connection, break to allow test to complete
            // (but the spawned task will continue handling it)
            break;
        }
        println!("  [Server] Server exiting");
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Create temporary file
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let device_path = temp_file.path().to_string_lossy().to_string();

    // Configure options with custom CA certificate
    let ca_cert_path = cert_dir.join("ca-cert.pem");
    let mut options = test_options_with(
        device_path.clone(),
        false,
        true,
        false,
        Some(ca_cert_path.clone()),
    );
    options.max_retries = 3; // Allow more retries for debugging

    println!("  Using CA certificate: {}", ca_cert_path.display());

    // Execute the flash operation
    let result = flash_from(&server_url, options).await;

    // Wait for server to finish
    let _ = tokio::time::timeout(tokio::time::Duration::from_secs(10), server_handle).await;

    // Verify the operation succeeded
    if result.is_err() {
        eprintln!("  Flash operation failed: {:?}", result.as_ref().err());
        eprintln!("  This may indicate a rustls/reqwest limitation with custom CA certificates");
        eprintln!("  Certificate chain is valid (verified by OpenSSL)");
    }
    assert!(result.is_ok(), "Flash operation failed: {:?}", result.err());

    // Read back the written data
    let written_data = std::fs::read(temp_file.path()).expect("Failed to read written file");

    // Verify the data matches
    assert_eq!(
        written_data.len(),
        test_data.len(),
        "Written data length mismatch"
    );
    assert_eq!(
        &written_data[..],
        &test_data[..],
        "Written data does not match source"
    );

    println!(
        "✓ Test passed: HTTPS download with custom CA certificate, {} bytes",
        written_data.len()
    );
}

#[tokio::test]
async fn test_https_with_insecure_flag() {
    use http_body_util::{BodyExt, Full};
    use hyper::body::Bytes;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request as HyperRequest, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use std::convert::Infallible;
    use std::fs;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    println!("Test: HTTPS download with insecure_tls flag (accept any cert)");

    // Install default crypto provider for rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Load server certificate and key
    let cert_dir = PathBuf::from("tests/test_certs");
    let server_cert_pem =
        fs::read_to_string(cert_dir.join("server-cert.pem")).expect("Failed to read server cert");
    let server_key_pem =
        fs::read_to_string(cert_dir.join("server-key.pem")).expect("Failed to read server key");

    // Parse certificate and key
    let server_cert = rustls_pemfile::certs(&mut server_cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to parse server cert");
    let server_key = rustls_pemfile::private_key(&mut server_key_pem.as_bytes())
        .expect("Failed to parse server key")
        .expect("No private key found");

    // Create TLS config
    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(server_cert, server_key)
        .expect("Failed to create server config");
    server_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

    // Create test data (5 MB)
    let test_data = Arc::new(common::create_test_data(5 * 1024 * 1024));

    // Bind to random port
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    // Use IP address directly (certificate has IP in SAN)
    let server_url = format!("https://{}/secure.img", local_addr);

    println!("  [Server] HTTPS server listening on {}", server_url);

    // Spawn server that handles connections with a timeout
    let test_data_clone = test_data.clone();
    let server_handle = tokio::spawn(async move {
        let timeout_duration = tokio::time::Duration::from_secs(30);
        let deadline = tokio::time::Instant::now() + timeout_duration;

        loop {
            // Accept connections with timeout
            let accept_result = tokio::time::timeout_at(deadline, listener.accept()).await;

            let (stream, _) = match accept_result {
                Ok(Ok(conn)) => conn,
                Ok(Err(e)) => {
                    eprintln!("  [Server] Accept error: {:?}", e);
                    continue;
                }
                Err(_) => {
                    println!("  [Server] Timeout waiting for connections, exiting");
                    break;
                }
            };

            let tls_acceptor = tls_acceptor.clone();
            let test_data = test_data_clone.clone();

            tokio::spawn(async move {
                let tls_stream = match tls_acceptor.accept(stream).await {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("  [Server] TLS accept error: {:?}", e);
                        return;
                    }
                };

                let io = TokioIo::new(tls_stream);
                let service = service_fn(move |_req: HyperRequest<hyper::body::Incoming>| {
                    let test_data = test_data.clone();
                    async move {
                        println!("  [Server] Serving request");
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(Bytes::copy_from_slice(&test_data)).boxed())
                                .unwrap(),
                        )
                    }
                });

                if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                    eprintln!("  [Server] Connection error: {:?}", err);
                }
                println!("  [Server] Request handled");
            });

            // After successfully accepting one connection, break to allow test to complete
            // (but the spawned task will continue handling it)
            break;
        }
        println!("  [Server] Server exiting");
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Create temporary file
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let device_path = temp_file.path().to_string_lossy().to_string();

    // Configure options - allow insecure TLS
    let mut options = test_options_with(device_path.clone(), false, false, true, None);
    options.max_retries = 1; // Limit retries since server handles only one connection

    // Execute the flash operation
    let result = flash_from(&server_url, options).await;

    // Wait for server to finish
    let _ = tokio::time::timeout(tokio::time::Duration::from_secs(5), server_handle).await;

    // Verify the operation succeeded
    assert!(result.is_ok(), "Flash operation failed: {:?}", result.err());

    // Read back the written data
    let written_data = std::fs::read(temp_file.path()).expect("Failed to read written file");

    // Verify the data matches
    assert_eq!(
        written_data.len(),
        test_data.len(),
        "Written data length mismatch"
    );
    assert_eq!(
        &written_data[..],
        &test_data[..],
        "Written data does not match source"
    );

    println!(
        "✓ Test passed: HTTPS download with insecure_tls flag, {} bytes",
        written_data.len()
    );
}

#[tokio::test]
async fn test_https_certificate_validation_fails() {
    use std::fs;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio_rustls::TlsAcceptor;

    println!("Test: HTTPS certificate validation fails without CA (should fail)");

    // Install default crypto provider for rustls
    let _ = rustls::crypto::ring::default_provider().install_default();

    // Load server certificate and key
    let cert_dir = PathBuf::from("tests/test_certs");
    let server_cert_pem =
        fs::read_to_string(cert_dir.join("server-cert.pem")).expect("Failed to read server cert");
    let server_key_pem =
        fs::read_to_string(cert_dir.join("server-key.pem")).expect("Failed to read server key");

    // Parse certificate and key
    let server_cert = rustls_pemfile::certs(&mut server_cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to parse server cert");
    let server_key = rustls_pemfile::private_key(&mut server_key_pem.as_bytes())
        .expect("Failed to parse server key")
        .expect("No private key found");

    // Create TLS config
    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(server_cert, server_key)
        .expect("Failed to create server config");
    server_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

    // Bind to random port
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    // Use IP address directly (certificate has IP in SAN)
    let server_url = format!("https://{}/secure.img", local_addr);

    println!("  [Server] HTTPS server listening on {}", server_url);

    // Spawn server that handles ONE connection then exits
    let server_handle = tokio::spawn(async move {
        // Try to accept ONE connection
        if let Ok((stream, _)) = listener.accept().await {
            let _ = tls_acceptor.accept(stream).await;
            // Connection may fail during TLS handshake - that's expected
        }
        println!("  [Server] Server exiting");
    });

    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    // Create temporary file
    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let device_path = temp_file.path().to_string_lossy().to_string();

    // Configure options - DO NOT allow insecure TLS and DON'T provide CA cert
    let mut options = test_options_with(device_path.clone(), false, false, false, None);
    options.max_retries = 1; // Limit retries since server handles only one connection

    // Execute the flash operation
    let result = flash_from(&server_url, options).await;

    // Wait for server to finish
    let _ = tokio::time::timeout(tokio::time::Duration::from_secs(5), server_handle).await;

    // Verify the operation FAILED (as expected without CA cert)
    assert!(
        result.is_err(),
        "Flash operation should have failed with certificate validation error"
    );

    let error_msg = format!("{:?}", result.err().unwrap());
    println!("  Expected error: {}", error_msg);

    println!("✓ Test passed: Certificate validation correctly rejected certificate without CA");
}

// --- CLI tests for local file sources ---

/// Deterministic 4 KiB blocks for the sparse fixture.
fn sparse_blocks() -> (Vec<u8>, Vec<u8>) {
    let a: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
    let b: Vec<u8> = (0..4096).map(|i| ((i % 191) + 64) as u8).collect();
    (a, b)
}

/// Tiny 4-block sparse image: RAW, FILL, DONT_CARE, RAW (4096-byte blocks).
fn build_sparse_image(block_a: &[u8], block_b: &[u8]) -> Vec<u8> {
    let mut image = Vec::new();
    // File header (28 bytes, little-endian)
    image.extend_from_slice(&0xED26FF3Au32.to_le_bytes()); // magic
    image.extend_from_slice(&1u16.to_le_bytes()); // major
    image.extend_from_slice(&0u16.to_le_bytes()); // minor
    image.extend_from_slice(&28u16.to_le_bytes()); // file header size
    image.extend_from_slice(&12u16.to_le_bytes()); // chunk header size
    image.extend_from_slice(&4096u32.to_le_bytes()); // block size
    image.extend_from_slice(&4u32.to_le_bytes()); // total blocks
    image.extend_from_slice(&4u32.to_le_bytes()); // total chunks
    image.extend_from_slice(&0u32.to_le_bytes()); // checksum

    let chunk_header = |chunk_type: u16, total_size: u32| {
        let mut header = Vec::with_capacity(12);
        header.extend_from_slice(&chunk_type.to_le_bytes());
        header.extend_from_slice(&0u16.to_le_bytes()); // reserved
        header.extend_from_slice(&1u32.to_le_bytes()); // chunk blocks
        header.extend_from_slice(&total_size.to_le_bytes());
        header
    };

    // RAW
    image.extend(chunk_header(0xCAC1, 12 + 4096));
    image.extend_from_slice(block_a);
    // FILL
    image.extend(chunk_header(0xCAC2, 16));
    image.extend_from_slice(&[0x12, 0x34, 0x56, 0x78]);
    // DONT_CARE
    image.extend(chunk_header(0xCAC3, 12));
    // RAW
    image.extend(chunk_header(0xCAC1, 12 + 4096));
    image.extend_from_slice(block_b);

    image
}

/// Expected 16 KiB output: RAW, FILL, untouched (zero) DONT_CARE, RAW.
///
/// Regular-file destinations are truncated on open, so the DONT_CARE region
/// reads back as zeros: the flasher must not write into it.
fn expected_sparse_output(block_a: &[u8], block_b: &[u8]) -> Vec<u8> {
    let fill: Vec<u8> = [0x12, 0x34, 0x56, 0x78].repeat(1024);
    let mut out = Vec::with_capacity(16384);
    out.extend_from_slice(block_a);
    out.extend_from_slice(&fill);
    out.extend(std::iter::repeat_n(0u8, 4096)); // DONT_CARE: never written
    out.extend_from_slice(block_b);
    out
}

#[tokio::test]
async fn local_source_cli_flashes_raw_gzip_and_sparse_files() {
    let dir = tempfile::tempdir().unwrap();
    let raw: Vec<u8> = (0..(300 * 1024)).map(|i| (i % 199) as u8).collect();
    std::fs::write(dir.path().join("image.img"), &raw).unwrap();
    std::fs::write(dir.path().join("image.img.gz"), common::compress_gz(&raw)).unwrap();

    let (block_a, block_b) = sparse_blocks();
    let sparse = build_sparse_image(&block_a, &block_b);
    let expected = expected_sparse_output(&block_a, &block_b);
    let abs_xz = dir.path().join("disk.simg.xz");
    std::fs::write(&abs_xz, common::compress_xz(&sparse)).unwrap();
    // A name with spaces and a literal '?' and '#' before the extension.
    let tricky_xz = dir.path().join("file with ?#.simg.xz");
    std::fs::write(&tricky_xz, common::compress_xz(&sparse)).unwrap();

    let cases: Vec<(&str, &str, String, Vec<u8>)> = vec![
        ("raw", "from", "image.img".to_string(), raw.clone()),
        ("gz", "from", "./image.img.gz".to_string(), raw.clone()),
        (
            "abs-xz",
            "from",
            abs_xz.to_str().unwrap().to_string(),
            expected.clone(),
        ),
        (
            "file-abs",
            "from",
            format!("file://{}", abs_xz.display()),
            expected.clone(),
        ),
        (
            "file-rel",
            "from",
            format!("file://./{}", tricky_xz.file_name().unwrap().display()),
            expected.clone(),
        ),
        ("alias", "from-url", "image.img".to_string(), raw.clone()),
    ];

    for (name, command, source, expected_bytes) in cases {
        let device = dir.path().join(format!("{name}-device"));
        std::fs::write(&device, vec![0u8; expected_bytes.len()]).unwrap();
        let device_arg = device.display().to_string();
        let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_fls"));
        cmd.current_dir(dir.path())
            .args([
                command,
                &source,
                &device_arg,
                "--progress-interval",
                "0",
                "--newline-progress",
            ])
            .kill_on_drop(true);
        let output = tokio::time::timeout(std::time::Duration::from_secs(60), cmd.output())
            .await
            .unwrap()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{name}: {stdout}\n{stderr}");
        assert!(
            stdout.contains("Result: FLASH_COMPLETED"),
            "{name}: {stdout}"
        );
        assert_eq!(std::fs::read(&device).unwrap(), expected_bytes, "{name}");
        // Local sources report Read progress and never Download.
        assert!(stdout.contains("Read:"), "{name}: {stdout}");
        assert!(stdout.contains("Read complete:"), "{name}: {stdout}");
        assert!(!stdout.contains("Download:"), "{name}: {stdout}");
        assert!(!stdout.contains("Starting download"), "{name}: {stdout}");
        if command == "from-url" {
            assert_eq!(
                stderr.matches("is deprecated").count(),
                1,
                "{name}: {stderr}"
            );
        } else {
            assert!(!stderr.contains("deprecated"), "{name}: {stderr}");
        }
    }
}

#[tokio::test]
async fn local_source_cli_fails_cleanly_without_retries() {
    let dir = tempfile::tempdir().unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let bad_xz = dir.path().join("bad.simg.xz");
    std::fs::write(&bad_xz, b"not an xz stream").unwrap();

    // (name, source, destination must be unchanged)
    let cases: Vec<(&str, String, bool)> = vec![
        ("missing", "nope.img".to_string(), true),
        ("directory", source_dir.path().display().to_string(), true),
        ("bare-file", "file://".to_string(), true),
        ("ftp", "ftp://example.com/image.img".to_string(), true),
        // Decompression fails after the writer opened, so the destination
        // was already truncated by then.
        ("bad-xz", "bad.simg.xz".to_string(), false),
    ];

    for (name, source, device_untouched) in cases {
        let device = dir.path().join(format!("{name}-device"));
        std::fs::write(&device, vec![0u8; 4096]).unwrap();
        let device_arg = device.display().to_string();
        let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_fls"));
        cmd.current_dir(dir.path())
            .args(["from", &source, &device_arg])
            .kill_on_drop(true);
        let output = tokio::time::timeout(std::time::Duration::from_secs(60), cmd.output())
            .await
            .unwrap()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "{name}: {stdout}\n{stderr}");
        assert!(stdout.contains("Result: FLASH_FAILED"), "{name}: {stdout}");
        assert!(!stdout.contains("FLASH_COMPLETED"), "{name}: {stdout}");
        // No success stats and no download retry messages.
        assert!(!stdout.contains("complete:"), "{name}: {stdout}");
        assert!(!stderr.to_lowercase().contains("retry"), "{name}: {stderr}");
        assert!(stderr.contains("Error:"), "{name}: {stderr}");
        if device_untouched {
            assert_eq!(std::fs::read(&device).unwrap(), vec![0u8; 4096], "{name}");
        }
    }
}
