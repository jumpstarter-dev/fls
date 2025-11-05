use std::path::PathBuf;

pub struct BlockFlashOptions {
    pub insecure_tls: bool,
    pub cacert: Option<PathBuf>,
    pub device: String,
    pub buffer_size_mb: usize,
    pub max_retries: usize,
    pub retry_delay_secs: u64,
    pub debug: bool,
    pub headers: Vec<(String, String)>,
}

impl Default for BlockFlashOptions {
    fn default() -> Self {
        Self {
            insecure_tls: false,
            cacert: None,
            device: String::new(),
            buffer_size_mb: 1024,
            max_retries: 10,
            retry_delay_secs: 2,
            debug: false,
            headers: Vec::new(),
        }
    }
}
