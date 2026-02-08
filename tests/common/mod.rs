// Shared test utilities
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;
use xz2::write::XzEncoder;

/// Generate deterministic test data of a given size
pub fn create_test_data(size: usize) -> Vec<u8> {
    // Create a repeating pattern for easier debugging
    let pattern = b"TESTDATA";
    let mut data = Vec::with_capacity(size);

    for i in 0..size {
        data.push(pattern[i % pattern.len()]);
    }

    data
}

/// Compress data using xz compression
#[allow(dead_code)]
pub fn compress_xz(data: &[u8]) -> Vec<u8> {
    let mut encoder = XzEncoder::new(Vec::new(), 6);
    encoder
        .write_all(data)
        .expect("Failed to write to xz encoder");
    encoder.finish().expect("Failed to finish xz compression")
}

/// Compress data using gzip compression
pub fn compress_gz(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(data)
        .expect("Failed to write to gz encoder");
    encoder.finish().expect("Failed to finish gz compression")
}
