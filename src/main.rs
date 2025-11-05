use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod fls;

#[derive(Parser)]
#[command(name = "fls")]
#[command(about = "A small Rust utility for flashing devices")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Flash a block device from a URL
    FromUrl {
        /// URL to download the image from
        url: String,
        /// Destination device path (e.g., /dev/sdb)
        device: String,
        /// Path to CA certificate PEM file for TLS validation
        #[arg(long)]
        ca_cert: Option<PathBuf>,
        /// Ignore SSL certificate verification
        #[arg(long)]
        ignore_certificates: bool,
        /// Buffer size in MB for download buffering (default: 1024 MB)
        #[arg(long, default_value = "1024")]
        buffer_size: usize,
        /// Maximum number of retry attempts for failed downloads (default: 10)
        #[arg(long, default_value = "10")]
        max_retries: usize,
        /// Delay in seconds between retry attempts (default: 2)
        #[arg(long, default_value = "2")]
        retry_delay: u64,
        /// Enable debug output (prints all dd messages)
        #[arg(long)]
        debug: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::FromUrl {
            url,
            device,
            ca_cert,
            ignore_certificates,
            buffer_size,
            max_retries,
            retry_delay,
            debug,
        } => {
            println!("Block flash command:");
            println!("  URL: {}", url);
            println!("  Device: {}", device);
            if let Some(ref cert_path) = ca_cert {
                println!("  CA Certificate: {}", cert_path.display());
            }
            println!("  Ignore certificates: {}", ignore_certificates);
            println!("  Buffer size: {} MB", buffer_size);
            println!("  Max retries: {}", max_retries);
            println!("  Retry delay: {} seconds", retry_delay);
            println!("  Debug: {}", debug);
            println!();

            let options = fls::BlockFlashOptions {
                ignore_certificates,
                ca_cert,
                device: device.clone(),
                buffer_size_mb: buffer_size,
                max_retries,
                retry_delay_secs: retry_delay,
                debug,
            };

            fls::flash_from_url(&url, options).await?;
        }
    }

    Ok(())
}
