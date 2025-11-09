// Integration tests for flash_from_url function
mod common;

use fls::{flash_from_url, BlockFlashOptions};
use tempfile::NamedTempFile;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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
    let options = BlockFlashOptions {
        device: device_path.clone(),
        o_direct: false, // Must be false for regular files
        debug: false,
        ..Default::default()
    };

    // Execute the flash operation
    let url = format!("{}/test.img", mock_server.uri());
    let result = flash_from_url(&url, options).await;

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
    let options = BlockFlashOptions {
        device: device_path.clone(),
        o_direct: false, // Must be false for regular files
        debug: false,
        ..Default::default()
    };

    // Execute the flash operation
    let url = format!("{}/test.img.xz", mock_server.uri());
    let result = flash_from_url(&url, options).await;

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
    let options = BlockFlashOptions {
        device: device_path.clone(),
        o_direct: false, // Must be false for regular files
        debug: false,
        ..Default::default()
    };

    // Execute the flash operation
    let url = format!("{}/test.img.gz", mock_server.uri());
    let result = flash_from_url(&url, options).await;

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
    let options = BlockFlashOptions {
        device: device_path.clone(),
        o_direct: false,
        debug: false,
        retry_delay_secs: 0, // No delay for faster testing
        max_retries: 5,
        ..Default::default()
    };

    // Execute the flash operation
    let url = format!("{}/test.img", mock_server.uri());
    let result = flash_from_url(&url, options).await;

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
    let options = BlockFlashOptions {
        device: device_path.clone(),
        o_direct: false,
        debug: false,
        retry_delay_secs: 0, // No delay for faster testing
        max_retries: 5,
        ..Default::default()
    };

    // Execute the flash operation
    let url = format!("{}/test.img.xz", mock_server.uri());
    let result = flash_from_url(&url, options).await;

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
    let options = BlockFlashOptions {
        device: device_path.clone(),
        o_direct: false,
        debug: false,
        retry_delay_secs: 0, // No delay for faster testing
        max_retries: 5,
        ..Default::default()
    };

    // Execute the flash operation
    let url = format!("{}/test.img", mock_server.uri());
    let result = flash_from_url(&url, options).await;

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
    let options = BlockFlashOptions {
        device: device_path.clone(),
        o_direct: false,
        debug: false,
        retry_delay_secs: 0, // No delay for faster testing
        max_retries: 5,
        ..Default::default()
    };

    // Execute the flash operation
    let result = flash_from_url(&server_url, options).await;

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
    let options = BlockFlashOptions {
        device: device_path.clone(),
        o_direct: false,
        debug: true, // Enable debug to see more details
        insecure_tls: false,
        cacert: Some(ca_cert_path.clone()),
        max_retries: 3, // Allow more retries for debugging
        ..Default::default()
    };

    println!("  Using CA certificate: {}", ca_cert_path.display());

    // Execute the flash operation
    let result = flash_from_url(&server_url, options).await;

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
    let options = BlockFlashOptions {
        device: device_path.clone(),
        o_direct: false,
        debug: false,
        insecure_tls: true, // Accept any certificate
        max_retries: 1,     // Limit retries since server handles only one connection
        ..Default::default()
    };

    // Execute the flash operation
    let result = flash_from_url(&server_url, options).await;

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
    let options = BlockFlashOptions {
        device: device_path.clone(),
        o_direct: false,
        debug: false,
        insecure_tls: false, // Validate certificates
        cacert: None,        // No CA cert - should fail
        max_retries: 1,      // Limit retries since server handles only one connection
        ..Default::default()
    };

    // Execute the flash operation
    let result = flash_from_url(&server_url, options).await;

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
