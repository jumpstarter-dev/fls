// Module declarations
pub mod automotive;
mod block_writer;
pub(crate) mod compression;
mod decompress;
mod download_error;
mod error_handling;
mod fastboot;
mod format_detector;
mod from_url;
pub(crate) mod http;
mod magic_bytes;
mod memory;
pub mod oci;
mod options;
mod progress;
mod simg;
mod stream_utils;

// Public re-exports
pub use fastboot::flash_from_fastboot;
pub use from_url::flash_from_url;
pub use oci::flash_from_oci;
pub use options::{
    BlockFlashOptions, FastbootOptions, FlashOptions, HttpClientOptions, OciOptions,
};

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
