use std::path::PathBuf;

pub struct BlockFlashOptions {
    pub ignore_certificates: bool,
    pub ca_cert: Option<PathBuf>,
    pub device: String,
    pub buffer_size_mb: usize,
    pub max_retries: usize,
    pub retry_delay_secs: u64,
    pub debug: bool,
}

impl Default for BlockFlashOptions {
    fn default() -> Self {
        Self {
            ignore_certificates: false,
            ca_cert: None,
            device: String::new(),
            buffer_size_mb: 1024,
            max_retries: 10,
            retry_delay_secs: 2,
            debug: false,
        }
    }
}
