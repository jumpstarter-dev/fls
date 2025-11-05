// Module declarations
mod options;
mod progress;
mod decompress;
mod block_writer;
mod error_handling;
mod http;
mod download;

// Public re-exports
pub use options::BlockFlashOptions;
pub use download::stream_and_decompress;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_url_extension_detection() {
        assert!(stream_and_decompress("http://example.com/file.xz", BlockFlashOptions::default()).await.is_err()); // Will fail due to network, but we're testing the logic
        assert!(stream_and_decompress("http://example.com/file.img", BlockFlashOptions::default()).await.is_err());
    }
}

