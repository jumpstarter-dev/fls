/// OCI manifest types and parsing
///
/// Supports:
/// - OCI Image Manifest (application/vnd.oci.image.manifest.v1+json)
/// - Docker Manifest V2 (application/vnd.docker.distribution.manifest.v2+json)
use serde::Deserialize;

/// Media types for OCI/Docker manifests
pub mod media_types {
    pub const OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
    pub const DOCKER_MANIFEST_V2: &str = "application/vnd.docker.distribution.manifest.v2+json";
    pub const OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";
    pub const DOCKER_MANIFEST_LIST: &str =
        "application/vnd.docker.distribution.manifest.list.v2+json";

    // Layer media types
    pub const OCI_LAYER_GZIP: &str = "application/vnd.oci.image.layer.v1.tar+gzip";
    pub const OCI_LAYER_ZSTD: &str = "application/vnd.oci.image.layer.v1.tar+zstd";
    pub const DOCKER_LAYER: &str = "application/vnd.docker.image.rootfs.diff.tar.gzip";
}

/// OCI content descriptor
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Descriptor {
    /// Media type of the content
    pub media_type: String,

    /// Content digest (e.g., "sha256:abc123...")
    pub digest: String,

    /// Size in bytes
    pub size: u64,

    /// Optional annotations
    #[serde(default)]
    pub annotations: Option<std::collections::HashMap<String, String>>,

    /// Platform (for manifest lists/indexes)
    #[serde(default)]
    pub platform: Option<Platform>,
}

/// Platform specification
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct Platform {
    pub architecture: String,
    pub os: String,
    #[serde(default)]
    pub variant: Option<String>,
}

/// OCI Image Manifest
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct ImageManifest {
    /// Schema version (should be 2)
    pub schema_version: u32,

    /// Media type
    #[serde(default)]
    pub media_type: Option<String>,

    /// Config blob descriptor
    pub config: Descriptor,

    /// Layer descriptors
    pub layers: Vec<Descriptor>,

    /// Optional annotations
    #[serde(default)]
    pub annotations: Option<std::collections::HashMap<String, String>>,
}

/// OCI Image Index (manifest list)
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct ImageIndex {
    /// Schema version (should be 2)
    pub schema_version: u32,

    /// Media type
    #[serde(default)]
    pub media_type: Option<String>,

    /// Manifest descriptors
    pub manifests: Vec<Descriptor>,
}

/// Parsed manifest - can be either a single manifest or an index
#[derive(Debug)]
pub enum Manifest {
    Image(ImageManifest),
    Index(ImageIndex),
}

impl Manifest {
    /// Parse manifest from JSON bytes
    pub fn parse(data: &[u8], content_type: Option<&str>) -> Result<Self, String> {
        // Try to determine type from content-type header or media_type field
        let data_str =
            std::str::from_utf8(data).map_err(|e| format!("Invalid UTF-8 in manifest: {}", e))?;

        // First, try to detect based on content
        if let Ok(index) = serde_json::from_str::<ImageIndex>(data_str) {
            if !index.manifests.is_empty() {
                return Ok(Manifest::Index(index));
            }
        }

        // Try as image manifest
        if let Ok(manifest) = serde_json::from_str::<ImageManifest>(data_str) {
            return Ok(Manifest::Image(manifest));
        }

        // Try based on content-type
        if let Some(ct) = content_type {
            if ct.contains("index") || ct.contains("list") {
                let index: ImageIndex = serde_json::from_str(data_str)
                    .map_err(|e| format!("Failed to parse manifest index: {}", e))?;
                return Ok(Manifest::Index(index));
            }
        }

        Err("Unable to parse manifest as image or index".to_string())
    }

    /// Get the single layer from an image manifest
    /// Returns error if there are no layers or multiple layers
    pub fn get_single_layer(&self) -> Result<&Descriptor, String> {
        match self {
            Manifest::Image(m) => {
                if m.layers.is_empty() {
                    Err("Manifest has no layers".to_string())
                } else if m.layers.len() > 1 {
                    // For multi-layer images, we take the last layer
                    // which typically contains the actual content
                    Ok(m.layers.last().unwrap())
                } else {
                    Ok(&m.layers[0])
                }
            }
            Manifest::Index(_) => Err(
                "Cannot get layer from manifest index - need to resolve platform first".to_string(),
            ),
        }
    }

    /// Get all layers from an image manifest
    #[allow(dead_code)]
    pub fn get_layers(&self) -> Result<&[Descriptor], String> {
        match self {
            Manifest::Image(m) => Ok(&m.layers),
            Manifest::Index(_) => Err("Cannot get layers from manifest index".to_string()),
        }
    }
}

impl ImageIndex {
    /// Find a manifest for a specific platform
    pub fn find_platform(&self, os: &str, arch: &str) -> Option<&Descriptor> {
        self.manifests.iter().find(|m| {
            if let Some(platform) = &m.platform {
                platform.os == os && platform.architecture == arch
            } else {
                false
            }
        })
    }

    /// Find a manifest for linux/amd64 or linux/arm64
    pub fn find_linux_manifest(&self) -> Option<&Descriptor> {
        // Try arm64 first (common for embedded), then amd64
        self.find_platform("linux", "arm64")
            .or_else(|| self.find_platform("linux", "amd64"))
    }
}

impl Descriptor {
    /// Check if this is a gzip-compressed layer
    pub fn is_gzip_layer(&self) -> bool {
        self.media_type == media_types::OCI_LAYER_GZIP
            || self.media_type == media_types::DOCKER_LAYER
    }

    /// Check if this is a zstd-compressed layer
    pub fn is_zstd_layer(&self) -> bool {
        self.media_type == media_types::OCI_LAYER_ZSTD
    }

    /// Get compression type
    pub fn compression(&self) -> LayerCompression {
        if self.is_gzip_layer() {
            LayerCompression::Gzip
        } else if self.is_zstd_layer() {
            LayerCompression::Zstd
        } else {
            LayerCompression::None
        }
    }
}

/// Layer compression type
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LayerCompression {
    None,
    Gzip,
    Zstd,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_image_manifest() {
        let json = r#"{
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:config123",
                "size": 1234
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": "sha256:layer123",
                    "size": 5678
                }
            ]
        }"#;

        let manifest = Manifest::parse(json.as_bytes(), None).unwrap();
        match manifest {
            Manifest::Image(m) => {
                assert_eq!(m.schema_version, 2);
                assert_eq!(m.layers.len(), 1);
                assert_eq!(m.layers[0].digest, "sha256:layer123");
            }
            _ => panic!("Expected image manifest"),
        }
    }

    #[test]
    fn test_parse_index() {
        let json = r#"{
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:manifest123",
                    "size": 1000,
                    "platform": {
                        "architecture": "arm64",
                        "os": "linux"
                    }
                }
            ]
        }"#;

        let manifest = Manifest::parse(json.as_bytes(), None).unwrap();
        match manifest {
            Manifest::Index(idx) => {
                assert_eq!(idx.manifests.len(), 1);
                let linux = idx.find_linux_manifest().unwrap();
                assert_eq!(linux.digest, "sha256:manifest123");
            }
            _ => panic!("Expected manifest index"),
        }
    }
}
