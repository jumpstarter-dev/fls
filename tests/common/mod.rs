// Shared test utilities

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
