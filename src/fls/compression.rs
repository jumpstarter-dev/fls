//! Unified compression type detection and handling
//!
//! This module provides a single source of truth for compression types
//! used across both OCI and URL-based flash operations.

/// Compression type for data streams
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// No compression - raw data
    None,
    /// Gzip compression (.gz)
    Gzip,
    /// XZ/LZMA compression (.xz)
    Xz,
    /// Zstandard compression (.zst)
    Zstd,
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Compression::None => write!(f, "none"),
            Compression::Gzip => write!(f, "gzip"),
            Compression::Xz => write!(f, "xz"),
            Compression::Zstd => write!(f, "zstd"),
        }
    }
}
