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
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::BlockFlash { url, device, ignore_certificates, buffer_size } => {
            println!("Block flash command:");
            println!("  URL: {}", url);
            println!("  Device: {}", device);
            println!("  Ignore certificates: {}", ignore_certificates);
            println!("  Buffer size: {} MB", buffer_size);
            
            let options = block_flash::BlockFlashOptions {
                ignore_certificates,
                device: device.clone(),
                buffer_size_mb: buffer_size,
            };
            
            block_flash::stream_and_decompress(&url, options).await?;
        }
    }
    
    Ok(())
}
