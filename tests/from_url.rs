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
    assert!(
        result.is_ok(),
        "Flash operation failed: {:?}",
        result.err()
    );

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
    assert!(
        result.is_ok(),
        "Flash operation failed: {:?}",
        result.err()
    );

    // Read back the written data
    let written_data = std::fs::read(temp_file.path()).expect("Failed to read written file");

    // Verify the complete data was written
    assert_eq!(
        written_data.len(),
        test_data.len(),
        "Written data length mismatch"
    );
    assert_eq!(written_data, test_data, "Written data does not match source");

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
    assert!(
        result.is_ok(),
        "Flash operation failed: {:?}",
        result.err()
    );

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
    assert!(
        result.is_ok(),
        "Flash operation failed: {:?}",
        result.err()
    );

    // Read back the written data
    let written_data = std::fs::read(temp_file.path()).expect("Failed to read written file");

    // Verify the complete data was written
    assert_eq!(
        written_data.len(),
        test_data.len(),
        "Written data length mismatch"
    );
    assert_eq!(written_data, test_data, "Written data does not match source");

    println!(
        "✓ Test passed: 2 server errors triggered 2 retries, completed {} bytes",
        written_data.len()
    );
}
