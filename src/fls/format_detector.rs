//! Automatic file format detection for streaming data
//!
//! This module provides format detection for incoming data streams, allowing
//! automatic identification of file formats without explicit user specification.

use crate::fls::simg;

/// Detected file formats supported by the format detector
#[derive(Debug, Clone, PartialEq)]
pub enum FileFormat {
    /// Android sparse image format
    SparseImage,
    /// Regular (non-sparse) file format
    Regular,
}

/// Result of format detection process
#[derive(Debug)]
pub enum DetectionResult {
    /// More data needed to make a format determination
    NeedMoreData,
    /// Format successfully detected with consumed buffer data
    Detected {
        format: FileFormat,
        consumed_bytes: Vec<u8>,
        consumed_from_input: usize,
    },
    /// Error during format detection (e.g., valid sparse magic but corrupted header)
    Error(String),
}

/// Streaming format detector that buffers initial bytes to identify file formats
pub struct FormatDetector {
    /// Buffer accumulating initial bytes for detection
    buffer: Vec<u8>,
    /// Whether format detection has completed
    detection_complete: bool,
    /// Detected format (if any)
    detected_format: Option<FileFormat>,
}

impl FormatDetector {
    /// Minimum bytes required to detect sparse image format (simg header size)
    const DETECTION_THRESHOLD: usize = 28;

    /// Maximum buffer size to prevent excessive memory usage during detection
    const MAX_BUFFER_SIZE: usize = 64;

    /// Create a new format detector
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            detection_complete: false,
            detected_format: None,
        }
    }

    /// Process incoming data for format detection
    ///
    /// # Arguments
    /// * `data` - Incoming data bytes to analyze
    ///
    /// # Returns
    /// * `DetectionResult::NeedMoreData` if more data is required
    /// * `DetectionResult::Detected` with format and buffered bytes when detection completes
    pub fn process(&mut self, data: &[u8]) -> DetectionResult {
        // If detection already completed, return the cached result
        if self.detection_complete {
            return DetectionResult::Detected {
                format: self
                    .detected_format
                    .clone()
                    .expect("format always set when complete"),
                consumed_bytes: Vec::new(), // No additional bytes to consume
                consumed_from_input: 0,     // No additional bytes consumed from current input
            };
        }

        // Determine how much data we can safely buffer
        let space_left = Self::MAX_BUFFER_SIZE - self.buffer.len();
        let take = data.len().min(space_left);

        // Add new data to buffer
        self.buffer.extend_from_slice(&data[..take]);

        // Attempt detection if we have enough data or reached buffer limit
        if self.buffer.len() >= Self::DETECTION_THRESHOLD
            || self.buffer.len() >= Self::MAX_BUFFER_SIZE
        {
            return self.attempt_detection_with_consumed(take);
        }

        DetectionResult::NeedMoreData
    }

    /// Attempt to detect the file format from buffered data, tracking consumption from current input
    fn attempt_detection_with_consumed(&mut self, consumed_from_input: usize) -> DetectionResult {
        // Try to parse as sparse image header
        match simg::parse_header(&self.buffer) {
            Ok(_) => {
                // Valid sparse image header
                self.detected_format = Some(FileFormat::SparseImage);
                self.detection_complete = true;
                DetectionResult::Detected {
                    format: FileFormat::SparseImage,
                    consumed_bytes: self.buffer.clone(),
                    consumed_from_input,
                }
            }
            Err(simg::SparseError::InvalidMagic(_)) => {
                // Not a sparse image - treat as regular file
                self.detected_format = Some(FileFormat::Regular);
                self.detection_complete = true;
                DetectionResult::Detected {
                    format: FileFormat::Regular,
                    consumed_bytes: self.buffer.clone(),
                    consumed_from_input,
                }
            }
            Err(e) => {
                // Valid sparse magic but corrupted header - don't flash as raw data
                self.detection_complete = true;
                DetectionResult::Error(format!(
                    "Corrupted sparse image header (valid magic but invalid fields): {}",
                    e
                ))
            }
        }
    }

    /// Check if format detection has completed
    #[allow(dead_code)]
    pub fn is_complete(&self) -> bool {
        self.detection_complete
    }

    /// Get the detected format (if detection is complete)
    #[allow(dead_code)]
    pub fn detected_format(&self) -> Option<&FileFormat> {
        self.detected_format.as_ref()
    }

    /// Finalize detection at EOF when we haven't accumulated enough data
    ///
    /// This handles the case where the input stream ends before we've received
    /// the 28 bytes needed for sparse image detection. Returns the buffered data
    /// to be written as a regular file.
    pub fn finalize_at_eof(&mut self) -> Option<Vec<u8>> {
        if self.detection_complete || self.buffer.is_empty() {
            return None;
        }

        // Not enough data to detect format - treat as regular file
        self.detected_format = Some(FileFormat::Regular);
        self.detection_complete = true;

        Some(std::mem::take(&mut self.buffer))
    }
}

impl Default for FormatDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a valid sparse image header for testing
    fn create_test_sparse_header() -> Vec<u8> {
        let mut header = Vec::with_capacity(28);
        // Magic number: 0xED26FF3A (little endian)
        header.extend_from_slice(&0xED26FF3Au32.to_le_bytes());
        // Major version: 1
        header.extend_from_slice(&1u16.to_le_bytes());
        // Minor version: 0
        header.extend_from_slice(&0u16.to_le_bytes());
        // File header size: 28
        header.extend_from_slice(&28u16.to_le_bytes());
        // Chunk header size: 12
        header.extend_from_slice(&12u16.to_le_bytes());
        // Block size: 4096
        header.extend_from_slice(&4096u32.to_le_bytes());
        // Total blocks: 1000
        header.extend_from_slice(&1000u32.to_le_bytes());
        // Total chunks: 1
        header.extend_from_slice(&1u32.to_le_bytes());
        // Checksum: 0
        header.extend_from_slice(&0u32.to_le_bytes());
        header
    }

    #[test]
    fn test_detect_sparse_image_format() {
        let mut detector = FormatDetector::new();

        // Create a complete valid sparse image header
        let header = create_test_sparse_header();

        match detector.process(&header) {
            DetectionResult::Detected {
                format,
                consumed_bytes,
                consumed_from_input,
            } => {
                assert_eq!(format, FileFormat::SparseImage);
                assert_eq!(consumed_bytes.len(), 28);
                assert_eq!(consumed_from_input, 28);
                assert!(detector.is_complete());
            }
            _ => panic!("Expected detection to complete with sparse image format"),
        }
    }

    #[test]
    fn test_detect_regular_file_format() {
        let mut detector = FormatDetector::new();

        // Use non-sparse data (random bytes)
        let data = vec![0x12, 0x34, 0x56, 0x78]; // Not a sparse image magic
        let mut full_data = data;
        full_data.extend(vec![0u8; 24]); // Pad to 28 bytes

        match detector.process(&full_data) {
            DetectionResult::Detected {
                format,
                consumed_bytes,
                consumed_from_input,
            } => {
                assert_eq!(format, FileFormat::Regular);
                assert_eq!(consumed_bytes.len(), 28);
                assert_eq!(consumed_from_input, 28);
                assert!(detector.is_complete());
            }
            _ => panic!("Expected detection to complete with regular format"),
        }
    }

    #[test]
    fn test_need_more_data() {
        let mut detector = FormatDetector::new();

        // Provide insufficient data (less than 28 bytes)
        let partial_data = vec![0x3A, 0xFF, 0x26]; // Only 3 bytes

        match detector.process(&partial_data) {
            DetectionResult::NeedMoreData => {
                assert!(!detector.is_complete());
                assert_eq!(detector.detected_format(), None);
            }
            _ => panic!("Expected to need more data"),
        }
    }

    #[test]
    fn test_incremental_detection() {
        let mut detector = FormatDetector::new();

        // Create sparse image header in parts
        let full_header = create_test_sparse_header();
        let part1 = full_header[0..4].to_vec(); // Magic number
        let part2 = full_header[4..].to_vec(); // Rest of header

        // First part - should need more data
        match detector.process(&part1) {
            DetectionResult::NeedMoreData => assert!(!detector.is_complete()),
            _ => panic!("Expected to need more data after first part"),
        }

        // Second part - should complete detection
        match detector.process(&part2) {
            DetectionResult::Detected {
                format,
                consumed_bytes,
                consumed_from_input,
            } => {
                assert_eq!(format, FileFormat::SparseImage);
                assert_eq!(consumed_bytes.len(), 28);
                assert_eq!(consumed_from_input, part2.len());
                assert!(detector.is_complete());
            }
            _ => panic!("Expected detection to complete after second part"),
        }
    }

    #[test]
    fn test_buffer_limit() {
        let mut detector = FormatDetector::new();

        // Provide more than MAX_BUFFER_SIZE bytes of non-sparse data
        let large_data = vec![0x12; FormatDetector::MAX_BUFFER_SIZE + 10];

        match detector.process(&large_data) {
            DetectionResult::Detected {
                format,
                consumed_bytes,
                consumed_from_input,
            } => {
                assert_eq!(format, FileFormat::Regular);
                assert_eq!(consumed_bytes.len(), FormatDetector::MAX_BUFFER_SIZE);
                assert_eq!(consumed_from_input, FormatDetector::MAX_BUFFER_SIZE);
                assert!(detector.is_complete());
            }
            _ => panic!("Expected detection to complete with buffer limit reached"),
        }
    }

    #[test]
    fn test_corrupted_sparse_header_returns_error() {
        let mut detector = FormatDetector::new();

        // Create a header with valid magic but invalid version (major version 2)
        let mut header = Vec::with_capacity(28);
        header.extend_from_slice(&0xED26FF3Au32.to_le_bytes()); // Valid magic
        header.extend_from_slice(&2u16.to_le_bytes()); // Invalid major version (2)
        header.extend_from_slice(&0u16.to_le_bytes()); // Minor version
        header.extend_from_slice(&28u16.to_le_bytes()); // File header size
        header.extend_from_slice(&12u16.to_le_bytes()); // Chunk header size
        header.extend_from_slice(&4096u32.to_le_bytes()); // Block size
        header.extend_from_slice(&1000u32.to_le_bytes()); // Total blocks
        header.extend_from_slice(&1u32.to_le_bytes()); // Total chunks
        header.extend_from_slice(&0u32.to_le_bytes()); // Checksum

        match detector.process(&header) {
            DetectionResult::Error(msg) => {
                assert!(msg.contains("Corrupted sparse image header"));
                assert!(detector.is_complete());
            }
            _ => panic!("Expected Error for corrupted sparse header"),
        }
    }

    #[test]
    fn test_finalize_at_eof() {
        let mut detector = FormatDetector::new();

        // Provide less than 28 bytes
        let partial_data = vec![0x12, 0x34, 0x56];
        let result = detector.process(&partial_data);
        assert!(matches!(result, DetectionResult::NeedMoreData));

        // Finalize at EOF - should return buffered data
        let buffered = detector.finalize_at_eof();
        assert!(buffered.is_some());
        assert_eq!(buffered.unwrap(), partial_data);
        assert!(detector.is_complete());
        assert_eq!(detector.detected_format(), Some(&FileFormat::Regular));

        // Calling finalize_at_eof again should return None
        assert!(detector.finalize_at_eof().is_none());
    }

    #[test]
    fn test_detection_idempotent() {
        let mut detector = FormatDetector::new();

        // Complete detection with sparse image
        let header = create_test_sparse_header();

        // First call completes detection
        let result1 = detector.process(&header);
        assert!(matches!(result1, DetectionResult::Detected { .. }));

        // Subsequent calls should return cached result without consuming more data
        let result2 = detector.process(&[0x99, 0x88, 0x77]);
        match result2 {
            DetectionResult::Detected {
                format,
                consumed_bytes,
                consumed_from_input,
            } => {
                assert_eq!(format, FileFormat::SparseImage);
                assert_eq!(consumed_bytes.len(), 0); // No new bytes consumed
                assert_eq!(consumed_from_input, 0); // No new bytes consumed from input
            }
            _ => panic!("Expected cached detection result"),
        }
    }

    /// Create a sparse header typical of android-image-builder (a-i-b) output
    /// These images often have larger sizes (8GB) and many chunks
    fn create_aib_sparse_header() -> Vec<u8> {
        let mut header = Vec::with_capacity(28);
        // Magic number: 0xED26FF3A (little endian)
        header.extend_from_slice(&0xED26FF3Au32.to_le_bytes());
        // Major version: 1
        header.extend_from_slice(&1u16.to_le_bytes());
        // Minor version: 0
        header.extend_from_slice(&0u16.to_le_bytes());
        // File header size: 28
        header.extend_from_slice(&28u16.to_le_bytes());
        // Chunk header size: 12
        header.extend_from_slice(&12u16.to_le_bytes());
        // Block size: 4096 (standard 4KB blocks)
        header.extend_from_slice(&4096u32.to_le_bytes());
        // Total blocks: 2097152 (8GB image = 8 * 1024 * 1024 * 1024 / 4096)
        header.extend_from_slice(&2097152u32.to_le_bytes());
        // Total chunks: 256 (typical for a real image with many partitions)
        header.extend_from_slice(&256u32.to_le_bytes());
        // Checksum: 0
        header.extend_from_slice(&0u32.to_le_bytes());
        header
    }

    #[test]
    fn test_detect_aib_sparse_image() {
        let mut detector = FormatDetector::new();

        // Create an AIB-style sparse header (8GB image, 256 chunks)
        let header = create_aib_sparse_header();

        match detector.process(&header) {
            DetectionResult::Detected {
                format,
                consumed_bytes,
                consumed_from_input,
            } => {
                assert_eq!(format, FileFormat::SparseImage);
                assert_eq!(consumed_bytes.len(), 28);
                assert_eq!(consumed_from_input, 28);
                assert!(detector.is_complete());
            }
            DetectionResult::Error(msg) => {
                panic!("AIB sparse header should be valid, got error: {}", msg);
            }
            DetectionResult::NeedMoreData => {
                panic!("28 bytes should be enough for detection");
            }
        }
    }

    #[test]
    fn test_detect_aib_sparse_image_with_trailing_data() {
        let mut detector = FormatDetector::new();

        // Create header followed by chunk data (simulating real file)
        let mut data = create_aib_sparse_header();
        // Append a RAW chunk header (12 bytes) + some data
        data.extend_from_slice(&0xCAC1u16.to_le_bytes()); // CHUNK_TYPE_RAW
        data.extend_from_slice(&0u16.to_le_bytes()); // reserved
        data.extend_from_slice(&1u32.to_le_bytes()); // chunk_sz (1 block)
        data.extend_from_slice(&4108u32.to_le_bytes()); // total_sz (header + 1 block)
        data.extend_from_slice(&[0u8; 100]); // Some raw data

        match detector.process(&data) {
            DetectionResult::Detected {
                format,
                consumed_bytes,
                consumed_from_input,
            } => {
                assert_eq!(format, FileFormat::SparseImage);
                // Detector buffers up to MAX_BUFFER_SIZE (64 bytes) for detection
                // Header (28) + partial chunk header (36 more = 64 total)
                assert_eq!(consumed_bytes.len(), FormatDetector::MAX_BUFFER_SIZE);
                assert_eq!(consumed_from_input, FormatDetector::MAX_BUFFER_SIZE);
            }
            _ => panic!("Expected sparse image detection"),
        }
    }
}
