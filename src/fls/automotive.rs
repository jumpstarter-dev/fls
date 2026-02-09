//! CentOS Automotive Suite OCI constant
//!

/// OCI annotation keys used by automotive images for partition mapping
pub mod annotations {
    pub const PARTITION_ANNOTATION: &str = "automotive.sdv.cloud.redhat.com/partition";

    pub const DECOMPRESSED_SIZE: &str = "automotive.sdv.cloud.redhat.com/decompressed-size";

    pub const TARGET: &str = "automotive.sdv.cloud.redhat.com/target";

    pub const ARCH: &str = "automotive.sdv.cloud.redhat.com/arch";

    pub const DISTRO: &str = "automotive.sdv.cloud.redhat.com/distro";

    pub const MULTI_LAYER: &str = "automotive.sdv.cloud.redhat.com/multi-layer";

    pub const PARTS: &str = "automotive.sdv.cloud.redhat.com/parts";

    /// Comma-separated list of default partitions to flash
    pub const DEFAULT_PARTITIONS: &str = "automotive.sdv.cloud.redhat.com/default-partitions";
}

pub mod targets {
    pub const RIDESX4: &str = "ridesx4";
    pub const AUTOSD: &str = "autosd";
}

/// Extract partition name from layer annotations
pub fn extract_partition_name(
    layer_annotations: &std::collections::HashMap<String, String>,
) -> Option<String> {
    layer_annotations
        .get(annotations::PARTITION_ANNOTATION)
        .cloned()
}

/// Extract decompressed size from layer annotations
pub fn extract_decompressed_size(
    layer_annotations: &std::collections::HashMap<String, String>,
) -> Option<u64> {
    layer_annotations
        .get(annotations::DECOMPRESSED_SIZE)
        .and_then(|s| s.parse().ok())
}

/// Extract target platform from OCI annotations
pub fn extract_target_from_annotations(
    manifest_annotations: &std::collections::HashMap<String, String>,
) -> Option<String> {
    manifest_annotations.get(annotations::TARGET).cloned()
}
