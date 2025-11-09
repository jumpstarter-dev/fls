// Library module for fls
// This exposes the public API for integration tests and potential library usage

pub mod fls;

// Re-export the main public API
pub use fls::{flash_from_url, BlockFlashOptions};
