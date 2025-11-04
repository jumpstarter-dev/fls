use clap::{Parser, Subcommand};

mod block_flash;

#[derive(Parser)]
#[command(name = "smallrs")]
#[command(about = "A small Rust utility for flashing devices")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Flash a block device from a URL
    BlockFlash {
        /// URL to download the image from
        url: String,
        /// Destination device path (e.g., /dev/sdb)
        device: String,
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
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::BlockFlash { url, device, ignore_certificates, buffer_size, max_retries, retry_delay } => {
            println!("Block flash command:");
            println!("  URL: {}", url);
            println!("  Device: {}", device);
            println!("  Ignore certificates: {}", ignore_certificates);
            println!("  Buffer size: {} MB", buffer_size);
            println!("  Max retries: {}", max_retries);
            println!("  Retry delay: {} seconds", retry_delay);
            
            let options = block_flash::BlockFlashOptions {
                ignore_certificates,
                device: device.clone(),
                buffer_size_mb: buffer_size,
                max_retries,
                retry_delay_secs: retry_delay,
            };
            
            block_flash::stream_and_decompress(&url, options).await?;
        }
    }
    
    Ok(())
}
