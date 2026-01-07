/// Magic byte detection utilities
///
/// Provides centralized detection of file formats and compression
/// from magic bytes (file signatures).
use crate::fls::compression::Compression;

/// Detect compression type from magic bytes
///
/// Examines the first few bytes of data to determine the compression format.
/// Returns `Compression::None` if no recognized compression signature is found.
pub fn detect_compression(data: &[u8]) -> Compression {
    if data.len() < 2 {
        return Compression::None;
    }

    // Gzip: 1f 8b
    if data[0] == 0x1f && data[1] == 0x8b {
        return Compression::Gzip;
    }

    // XZ: fd 37 7a 58 5a 00 (0xFD + "7zXZ" + 0x00)
    if data.len() >= 6 && &data[0..6] == b"\xfd7zXZ\x00" {
        return Compression::Xz;
    }

    // Zstd: 28 b5 2f fd
    if data.len() >= 4 && data[0] == 0x28 && data[1] == 0xb5 && data[2] == 0x2f && data[3] == 0xfd {
        return Compression::Zstd;
    }

    Compression::None
}

/// Check if data appears to be a tar archive
///
/// Tar archives have "ustar" or "posix" magic at offset 257.
pub fn is_tar_archive(data: &[u8]) -> bool {
    if data.len() < 262 {
        return false;
    }

    let magic = &data[257..262];
    magic == b"ustar" || magic == b"posix"
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
            // For XZ, assume tar archive (common pattern)
            if debug {
                eprintln!("[DEBUG] XZ detected, assuming tar archive");
            }
            return Ok((ContentType::TarArchive, compression));
        }
        Compression::Zstd => {
            // For Zstd, assume tar archive
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
        tar_data[257..262].copy_from_slice(b"ustar");
        assert!(is_tar_archive(&tar_data));
    }
}
