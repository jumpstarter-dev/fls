use std::path::PathBuf;

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
            max_retries: 10,
            retry_delay_secs: 2,
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
}

/// Options for fastboot flash operations
#[derive(Debug, Clone)]
pub struct FastbootOptions {
    pub common: FlashOptions,
    pub device_serial: Option<String>,
    pub target: Option<String>, // Target platform (e.g., "ridesx4")
    pub partition_mappings: Vec<(String, String)>, // (partition_name, file_pattern) - fallback for manual mapping
    pub timeout_secs: u32,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Default for FastbootOptions {
    fn default() -> Self {
        Self {
            common: FlashOptions::default(),
            device_serial: None,
            target: None,
            partition_mappings: Vec::new(),
            timeout_secs: 30,
            username: None,
            password: None,
        }
    }
}

/// Options for HTTP client setup (subset of FlashOptions)
#[derive(Debug, Clone)]
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
        Self::from(&opts.common)
    }
}
