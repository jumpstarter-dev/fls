/// OCI registry support for fls
///
/// This module implements streaming OCI image pulls with a pipeline that
/// decompresses and extracts disk images directly to block devices.
mod auth;
mod from_oci;
mod manifest;
mod reference;
mod registry;

// Public re-exports
pub use from_oci::flash_from_oci;
pub use reference::ImageReference;
pub use registry::OciOptions;
