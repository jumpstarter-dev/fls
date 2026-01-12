/// Magic byte detection utilities
///
/// Provides centralized detection of file formats and compression
/// from magic bytes (file signatures).
use crate::fls::compression::Compression;

/// Magic byte signatures for compression formats
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];
const XZ_MAGIC: [u8; 6] = *b"\xfd7zXZ\x00";
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xb5, 0x2f, 0xfd];

/// Detect compression type from magic bytes
///
/// Examines the first few bytes of data to determine the compression format.
/// Returns `Compression::None` if no recognized compression signature is found.
pub fn detect_compression(data: &[u8]) -> Compression {
    if data.starts_with(&GZIP_MAGIC) {
        Compression::Gzip
    } else if data.starts_with(&XZ_MAGIC) {
        Compression::Xz
    } else if data.starts_with(&ZSTD_MAGIC) {
        Compression::Zstd
    } else {
        Compression::None
    }
}

/// Offset and magic bytes for tar archive detection
const TAR_MAGIC_OFFSET: usize = 257;
const TAR_MAGIC_USTAR: &[u8] = b"ustar\0";

/// Check if data appears to be a tar archive
///
/// Tar archives have "ustar\0" magic at offset 257 (POSIX ustar format).
pub fn is_tar_archive(data: &[u8]) -> bool {
    data.get(TAR_MAGIC_OFFSET..TAR_MAGIC_OFFSET + 6)
        .map(|magic| magic == TAR_MAGIC_USTAR)
        .unwrap_or(false)
}

/// Content type for OCI layer detection
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentType {
    /// Raw disk image (not a tar archive)
    RawDiskImage,
    /// Tar archive containing files
    TarArchive,
}

/// Detect both content type and compression from data
///
/// For compressed data, this will attempt to decompress a sample
/// to determine if the content is a tar archive.
pub fn detect_content_and_compression(
    data: &[u8],
    debug: bool,
) -> Result<(ContentType, Compression), String> {
    if data.len() < 16 {
        return Err("Insufficient data for detection".to_string());
    }

    let compression = detect_compression(data);

    if debug {
        eprintln!("[DEBUG] Compression detection: {:?}", compression);
        eprintln!(
            "[DEBUG] First 16 bytes: {:02x?}",
            &data[..16.min(data.len())]
        );
    }

    // For compressed data, we need to peek at the decompressed content
    let content_to_analyze = match compression {
        Compression::Gzip => {
            // Try to decompress first few KB to analyze content
            match decompress_gzip_sample(data) {
                Ok(decompressed) => decompressed,
                Err(e) => {
                    if debug {
                        eprintln!("[DEBUG] Failed to decompress gzip sample: {}", e);
                    }
                    // Fall back to assuming it's a tar if we can't decompress
                    return Ok((ContentType::TarArchive, compression));
                }
            }
        }
        Compression::Xz => {
            // Try to decompress first few KB to analyze content
            match decompress_xz_sample(data) {
                Ok(decompressed) => decompressed,
                Err(e) => {
                    if debug {
                        eprintln!("[DEBUG] Failed to decompress XZ sample: {}", e);
                    }
                    // Fall back to assuming it's a tar if we can't decompress
                    return Ok((ContentType::TarArchive, compression));
                }
            }
        }
        Compression::Zstd => {
            // Zstd sample decompression not implemented - assume tar archive
            // (Zstd layers are rejected by from_oci.rs anyway)
            if debug {
                eprintln!("[DEBUG] Zstd detected, assuming tar archive");
            }
            return Ok((ContentType::TarArchive, compression));
        }
        Compression::None => data.to_vec(),
    };

    // Analyze the (possibly decompressed) content
    let content_type = if is_tar_archive(&content_to_analyze) {
        if debug {
            eprintln!("[DEBUG] Found tar magic signature");
        }
        ContentType::TarArchive
    } else {
        if debug {
            eprintln!("[DEBUG] No tar signature found, treating as raw data");
        }
        ContentType::RawDiskImage
    };

    Ok((content_type, compression))
}

/// Decompress a sample of gzip data to analyze content type
fn decompress_gzip_sample(data: &[u8]) -> Result<Vec<u8>, String> {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let mut decoder = GzDecoder::new(data);
    let mut buffer = vec![0u8; 8192]; // Decompress up to 8KB

    match decoder.read(&mut buffer) {
        Ok(n) => {
            buffer.truncate(n);
            Ok(buffer)
        }
        Err(e) => Err(format!("Gzip decompression failed: {}", e)),
    }
}

/// Decompress a sample of XZ data to analyze content type
fn decompress_xz_sample(data: &[u8]) -> Result<Vec<u8>, String> {
    use std::io::Read;
    use xz2::read::XzDecoder;

    let mut decoder = XzDecoder::new(data);
    let mut buffer = vec![0u8; 8192]; // Decompress up to 8KB

    match decoder.read(&mut buffer) {
        Ok(n) => {
            buffer.truncate(n);
            Ok(buffer)
        }
        Err(e) => Err(format!("XZ decompression failed: {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_gzip() {
        let gzip_magic = [0x1f, 0x8b, 0x08, 0x00];
        assert_eq!(detect_compression(&gzip_magic), Compression::Gzip);
    }

    #[test]
    fn test_detect_xz() {
        let xz_magic = b"\xfd7zXZ\x00";
        assert_eq!(detect_compression(xz_magic), Compression::Xz);
    }

    #[test]
    fn test_detect_zstd() {
        let zstd_magic = [0x28, 0xb5, 0x2f, 0xfd];
        assert_eq!(detect_compression(&zstd_magic), Compression::Zstd);
    }

    #[test]
    fn test_detect_none() {
        let random_data = [0x00, 0x01, 0x02, 0x03];
        assert_eq!(detect_compression(&random_data), Compression::None);
    }

    #[test]
    fn test_is_tar() {
        let mut tar_data = vec![0u8; 512];
        tar_data[257..263].copy_from_slice(b"ustar\0");
        assert!(is_tar_archive(&tar_data));
    }
}
