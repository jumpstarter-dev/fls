use std::path::PathBuf;

pub struct BlockFlashOptions {
    pub insecure_tls: bool,
    pub cacert: Option<PathBuf>,
    pub device: String,
    pub buffer_size_mb: usize,
    pub write_buffer_size_mb: usize,
    pub max_retries: usize,
    pub retry_delay_secs: u64,
    pub debug: bool,
    pub o_direct: bool,
    pub headers: Vec<(String, String)>,
    pub progress_interval_secs: f64,
    pub newline_progress: bool,
    pub show_memory: bool,
}

impl Default for BlockFlashOptions {
    fn default() -> Self {
        Self {
            insecure_tls: false,
            cacert: None,
            device: String::new(),
            buffer_size_mb: 1024,
            write_buffer_size_mb: 128,
            max_retries: 10,
            retry_delay_secs: 2,
            debug: false,
            o_direct: false,
            headers: Vec::new(),
            progress_interval_secs: 0.1,
            newline_progress: false,
            show_memory: false,
        }
    }
}
