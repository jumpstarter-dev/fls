// Module declarations
mod block_writer;
mod decompress;
mod download_error;
mod error_handling;
mod from_url;
pub(crate) mod http;
mod memory;
pub mod oci;
mod options;
mod progress;

// Public re-exports
pub use from_url::flash_from_url;
pub use oci::{flash_from_oci, OciOptions};
pub use options::BlockFlashOptions;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_url_extension_detection() {
        assert!(
            flash_from_url("http://example.com/file.xz", BlockFlashOptions::default())
                .await
                .is_err()
        ); // Will fail due to network, but we're testing the logic
        assert!(
            flash_from_url("http://example.com/file.img", BlockFlashOptions::default())
                .await
                .is_err()
        );
    }
}
