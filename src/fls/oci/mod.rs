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
pub use from_oci::{
    extract_files_by_annotations, extract_files_by_annotations_to_dir,
    extract_files_by_annotations_with_overrides_to_dir, extract_files_from_oci_image,
    extract_files_from_oci_image_to_dir, flash_from_oci,
};
pub use reference::ImageReference;
