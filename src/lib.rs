// Library module for fls
// This exposes the public API for integration tests and potential library usage

pub mod fls;

// Re-export the main public API
pub use fls::{
    flash_from, flash_from_oci, BlockFlashOptions, FlashOptions, OciOptions, DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_DELAY_SECS,
};
