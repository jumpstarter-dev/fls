use clap::{Parser, Subcommand};
use std::path::PathBuf;

// Use the library module
use fls::fls;

/// Parse target argument in the format "partition:filename"
fn parse_target_mapping(s: &str) -> Result<(String, String), String> {
    match s.split_once(':') {
        Some((partition, filename)) => {
            if partition.is_empty() {
                Err("Partition name cannot be empty".to_string())
            } else if filename.is_empty() {
                Err("Filename cannot be empty".to_string())
            } else {
                Ok((partition.to_string(), filename.to_string()))
            }
        }
        None => Err("Format should be 'partition:filename' (e.g., 'boot_a:aboot.img')".to_string()),
    }
}

#[derive(Parser)]
#[command(name = "fls")]
#[command(about = "A small Rust utility for flashing devices")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Flash a block device from a URL (supports http://, https://, oci://)
    FromUrl {
        /// URL to download the image from (http://, https://, oci://)
        url: String,
        /// Destination device path (e.g., /dev/sdb)
        device: String,
        /// Path to CA certificate PEM file for TLS validation
        #[arg(long)]
        cacert: Option<PathBuf>,
        /// Ignore SSL certificate verification
        #[arg(short = 'k', long = "insecure-tls")]
        insecure_tls: bool,
        /// Buffer size in MB for download buffering (default: 128 MB)
        #[arg(long, default_value = "128")]
        buffer_size: usize,
        /// Write buffer size in MB for decompressed data buffering (default: 128 MB)
        #[arg(long, default_value = "128")]
        write_buffer_size: usize,
        /// Maximum number of retry attempts for failed downloads (default: 10)
        #[arg(long, default_value = "10")]
        max_retries: usize,
        /// Delay in seconds between retry attempts (default: 2)
        #[arg(long, default_value = "2")]
        retry_delay: u64,
        /// Enable debug output (prints all dd messages)
        #[arg(long)]
        debug: bool,
        /// Enable O_DIRECT mode for direct I/O (bypasses OS cache)
        #[arg(long)]
        o_direct: bool,
        /// Custom HTTP headers (can be used multiple times, format: 'Header: value')
        #[arg(short = 'H', long = "header")]
        headers: Vec<String>,
        /// Progress update interval in seconds (default: 0.5, accepts float values like 1.0 or 0.5)
        #[arg(short = 'i', long, default_value = "0.5")]
        progress_interval: f64,
        /// Print progress on new lines instead of clearing and rewriting the same line
        #[arg(short = 'n', long)]
        newline_progress: bool,
        /// Show memory statistics in progress display
        #[arg(long)]
        show_memory: bool,
        /// Registry username for OCI authentication
        #[arg(short = 'u', long, env = "FLS_REGISTRY_USERNAME")]
        username: Option<String>,
        /// Registry password for OCI authentication (or use FLS_REGISTRY_PASSWORD env)
        #[arg(short = 'p', long, env = "FLS_REGISTRY_PASSWORD")]
        password: Option<String>,
        /// Glob pattern to match disk image file inside OCI layer tar (e.g., "*.img.xz")
        #[arg(long)]
        file_pattern: Option<String>,
    },
    /// Flash an OCI image to fastboot partitions via USB
    Fastboot {
        /// OCI image reference to download and flash (must be prefixed with "oci://")
        image_ref: String,
        /// Device serial number (optional, will use first device if not specified)
        #[arg(short = 's', long)]
        serial: Option<String>,
        /// Target partition and file override (e.g., "boot_a:boot_a.simg"), can be used multiple times
        #[arg(short = 't', long = "target", value_parser = parse_target_mapping)]
        targets: Vec<(String, String)>,
        /// Fastboot operation timeout in seconds (default: 1200)
        #[arg(long, default_value = "1200")]
        timeout: u32,
        /// Path to CA certificate PEM file for TLS validation
        #[arg(long)]
        cacert: Option<PathBuf>,
        /// Ignore SSL certificate verification
        #[arg(short = 'k', long = "insecure-tls")]
        insecure_tls: bool,
        /// Enable debug output
        #[arg(long)]
        debug: bool,
        /// Registry username for OCI authentication
        #[arg(short = 'u', long)]
        username: Option<String>,
        /// Registry password for OCI authentication (or use FLS_REGISTRY_PASSWORD env)
        #[arg(short = 'p', long, env = "FLS_REGISTRY_PASSWORD")]
        password: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::FromUrl {
            url,
            device,
            cacert,
            insecure_tls,
            buffer_size,
            write_buffer_size,
            max_retries,
            retry_delay,
            debug,
            o_direct,
            headers,
            progress_interval,
            newline_progress,
            show_memory,
            username,
            password,
            file_pattern,
        } => {
            // Detect URL scheme to determine handler
            let is_oci = url.starts_with("oci://");

            if debug {
                eprintln!("[DEBUG] URL: '{}', is_oci: {}", url, is_oci);
            }

            if is_oci {
                // OCI image - strip scheme prefix
                let image_ref = url
                    .strip_prefix("oci://")
                    .expect("URL should start with 'oci://' as verified above");

                println!("OCI flash command:");
                println!("  Image: {}", image_ref);
                println!("  Device: {}", device);
                match (&username, &password) {
                    (Some(_), None) | (None, Some(_)) => {
                        eprintln!(
                            "Error: OCI authentication requires both --username and --password"
                        );
                        std::process::exit(1);
                    }
                    (Some(_), Some(_)) => println!("  Auth: Using provided credentials"),
                    (None, None) => println!("  Auth: Anonymous"),
                }
                if let Some(ref pattern) = file_pattern {
                    println!("  File pattern: {}", pattern);
                }
                if let Some(ref cert_path) = cacert {
                    println!("  CA Certificate: {}", cert_path.display());
                }
                println!("  Ignore certificates: {}", insecure_tls);
                println!("  Buffer size: {} MB", buffer_size);
                println!("  Write buffer size: {} MB", write_buffer_size);
                println!("  Debug: {}", debug);
                println!("  O_DIRECT mode: {}", o_direct);
                println!();

                let options = fls::OciOptions {
                    common: fls::FlashOptions {
                        insecure_tls,
                        cacert,
                        device: device.clone(),
                        buffer_size_mb: buffer_size,
                        write_buffer_size_mb: write_buffer_size,
                        debug,
                        o_direct,
                        progress_interval_secs: progress_interval,
                        newline_progress,
                        show_memory,
                    },
                    username,
                    password,
                    file_pattern,
                };

                match fls::flash_from_oci(image_ref, options).await {
                    Ok(_) => {
                        println!("Result: FLASH_COMPLETED");
                        std::process::exit(0);
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        println!("Result: FLASH_FAILED");
                        std::process::exit(1);
                    }
                }
            } else {
                // HTTP/HTTPS URL
                println!("Block flash command:");
                println!("  URL: {}", url);
                println!("  Device: {}", device);
                if let Some(ref cert_path) = cacert {
                    println!("  CA Certificate: {}", cert_path.display());
                }
                println!("  Ignore certificates: {}", insecure_tls);
                println!("  Buffer size: {} MB", buffer_size);
                println!("  Write buffer size: {} MB", write_buffer_size);
                println!("  Max retries: {}", max_retries);
                println!("  Retry delay: {} seconds", retry_delay);
                println!("  Debug: {}", debug);
                println!("  O_DIRECT mode: {}", o_direct);

                // Parse headers in the format "Header: value"
                let parsed_headers: Vec<(String, String)> = headers
                    .iter()
                    .filter_map(|h| {
                        let Some((name, value)) = h.split_once(':') else {
                            eprintln!("Warning: Ignoring invalid header format: {}", h);
                            return None;
                        };
                        let name = name.trim().to_string();
                        let value = value.trim().to_string();

                        if name.is_empty() {
                            eprintln!("Warning: Ignoring header with empty name: {}", h);
                            return None;
                        }
                        if value.is_empty() {
                            eprintln!("Warning: Header '{}' has empty value", name);
                        }
                        Some((name, value))
                    })
                    .collect();

                if !parsed_headers.is_empty() {
                    println!("  Custom headers:");
                    for (name, value) in &parsed_headers {
                        println!("    {}: {}", name, value);
                    }
                }
                println!();

                let options = fls::BlockFlashOptions {
                    common: fls::FlashOptions {
                        insecure_tls,
                        cacert,
                        device: device.clone(),
                        buffer_size_mb: buffer_size,
                        write_buffer_size_mb: write_buffer_size,
                        debug,
                        o_direct,
                        progress_interval_secs: progress_interval,
                        newline_progress,
                        show_memory,
                    },
                    max_retries,
                    retry_delay_secs: retry_delay,
                    headers: parsed_headers,
                };

                match fls::flash_from_url(&url, options).await {
                    Ok(_) => {
                        println!("Result: FLASH_COMPLETED");
                        std::process::exit(0);
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        println!("Result: FLASH_FAILED");
                        std::process::exit(1);
                    }
                }
            }
        }
        Commands::Fastboot {
            image_ref,
            serial,
            targets,
            timeout,
            cacert,
            insecure_tls,
            debug,
            username,
            password,
        } => {
            let image_ref_input = image_ref;
            let image_ref = match image_ref_input.strip_prefix("oci://") {
                Some(reference) => reference,
                None => {
                    eprintln!(
                        "Error: fastboot expects an OCI image reference prefixed with 'oci://'"
                    );
                    eprintln!("  Example: fls fastboot oci://quay.io/org/image:latest");
                    std::process::exit(1);
                }
            };

            println!("Fastboot flash command:");
            println!("  Image: {}", image_ref_input);
            if let Some(ref serial) = serial {
                println!("  Device serial: {}", serial);
            }
            if !targets.is_empty() {
                println!("  Target partitions:");
                for (partition, filename) in &targets {
                    println!("    {} → {}", partition, filename);
                }
            }
            if let Some(ref cert_path) = cacert {
                println!("  CA Certificate: {}", cert_path.display());
            }
            println!("  Ignore certificates: {}", insecure_tls);
            println!("  Timeout: {} seconds", timeout);
            println!("  Debug: {}", debug);
            match (&username, &password) {
                (Some(_), None) | (None, Some(_)) => {
                    eprintln!("Error: OCI authentication requires both --username and --password");
                    std::process::exit(1);
                }
                (Some(_), Some(_)) => println!("  Auth: Using provided credentials"),
                (None, None) => println!("  Auth: Anonymous"),
            }
            println!();

            let options = fls::FastbootOptions {
                http: fls::HttpClientOptions {
                    insecure_tls,
                    cacert,
                    debug,
                },
                device_serial: serial,
                partition_mappings: targets,
                timeout_secs: timeout,
                username,
                password,
            };

            match fls::flash_from_fastboot(image_ref, options).await {
                Ok(_) => {
                    println!("Result: FLASH_COMPLETED");
                    std::process::exit(0);
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    println!("Result: FLASH_FAILED");
                    std::process::exit(1);
                }
            }
        }
    }
}
