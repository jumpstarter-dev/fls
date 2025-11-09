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
