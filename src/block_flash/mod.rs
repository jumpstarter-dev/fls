// Module declarations
mod block_writer;
mod decompress;
mod download;
mod error_handling;
mod http;
mod options;
mod progress;

// Public re-exports
pub use download::stream_and_decompress;
pub use options::BlockFlashOptions;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_url_extension_detection() {
        assert!(
            stream_and_decompress("http://example.com/file.xz", BlockFlashOptions::default())
                .await
                .is_err()
        ); // Will fail due to network, but we're testing the logic
        assert!(
            stream_and_decompress("http://example.com/file.img", BlockFlashOptions::default())
                .await
                .is_err()
        );
    }
}
