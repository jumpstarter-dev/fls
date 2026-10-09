use std::path::PathBuf;

use super::annotation_schema::AnnotationSchema;

pub const DEFAULT_MAX_RETRIES: usize = 10;
pub const DEFAULT_RETRY_DELAY_SECS: u64 = 2;

pub const DEFAULT_XZ_MEMLIMIT_MB: u64 = 256;

/// Common options shared between URL and OCI flash operations
#[derive(Debug, Clone)]
pub struct FlashOptions {
    pub insecure_tls: bool,
    pub cacert: Option<PathBuf>,
    pub device: String,
    pub buffer_size_mb: usize,
    pub write_buffer_size_mb: usize,
    pub debug: bool,
    pub o_direct: bool,
    pub progress_interval_secs: f64,
    pub newline_progress: bool,
    pub show_memory: bool,
    pub xz_memlimit_mb: u64,
    pub ssh_password_file: Option<String>,
    pub ssh_compress: bool,
    pub wh_bin: Option<String>,
}

impl Default for FlashOptions {
    fn default() -> Self {
        Self {
            insecure_tls: false,
            cacert: None,
            device: String::new(),
            buffer_size_mb: 128,
            write_buffer_size_mb: 128,
            debug: false,
            o_direct: false,
            progress_interval_secs: 0.5,
            newline_progress: false,
            show_memory: false,
            xz_memlimit_mb: DEFAULT_XZ_MEMLIMIT_MB,
            ssh_password_file: None,
            ssh_compress: false,
            wh_bin: None,
        }
    }
}

/// Options for HTTP/HTTPS URL flash operations
#[derive(Debug, Clone)]
pub struct BlockFlashOptions {
    pub common: FlashOptions,
    pub max_retries: usize,
    pub retry_delay_secs: u64,
    pub headers: Vec<(String, String)>,
}

impl Default for BlockFlashOptions {
    fn default() -> Self {
        Self {
            common: FlashOptions::default(),
            max_retries: DEFAULT_MAX_RETRIES,
            retry_delay_secs: DEFAULT_RETRY_DELAY_SECS,
            headers: Vec::new(),
        }
    }
}

/// Options for OCI image flash operations
#[derive(Debug, Clone)]
pub struct OciOptions {
    pub common: FlashOptions,
    pub username: Option<String>,
    pub password: Option<String>,
    pub file_pattern: Option<String>,
    pub max_retries: usize,
    pub retry_delay_secs: u64,
    /// Annotation schemas to try when resolving partitions. Empty = auto-detect.
    pub annotation_schemas: Vec<AnnotationSchema>,
}

/// Options for fastboot flash operations
#[derive(Debug, Clone)]
pub struct FastbootOptions {
    pub http: HttpClientOptions,
    pub device_serial: Option<String>,
    pub partition_mappings: Vec<(String, String)>, // (partition_name, file_pattern) - fallback for manual mapping
    pub timeout_secs: u32,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Default for FastbootOptions {
    fn default() -> Self {
        Self {
            http: HttpClientOptions::default(),
            device_serial: None,
            partition_mappings: Vec::new(),
            timeout_secs: 1200,
            username: None,
            password: None,
        }
    }
}

/// Options for HTTP client setup (subset of FlashOptions)
#[derive(Debug, Clone, Default)]
pub struct HttpClientOptions {
    pub insecure_tls: bool,
    pub cacert: Option<PathBuf>,
    pub debug: bool,
}

impl From<&FlashOptions> for HttpClientOptions {
    fn from(opts: &FlashOptions) -> Self {
        Self {
            insecure_tls: opts.insecure_tls,
            cacert: opts.cacert.clone(),
            debug: opts.debug,
        }
    }
}

impl From<&BlockFlashOptions> for HttpClientOptions {
    fn from(opts: &BlockFlashOptions) -> Self {
        Self::from(&opts.common)
    }
}

impl From<&OciOptions> for HttpClientOptions {
    fn from(opts: &OciOptions) -> Self {
        Self::from(&opts.common)
    }
}

impl From<&FastbootOptions> for HttpClientOptions {
    fn from(opts: &FastbootOptions) -> Self {
        opts.http.clone()
    }
}
