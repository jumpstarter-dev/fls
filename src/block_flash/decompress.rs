use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Command, Child};
use tokio::sync::mpsc;

/// Determines the appropriate decompression command based on URL extension
fn get_decompressor_command(url: &str) -> &'static str {
    let extension = url.rsplit('.').next().unwrap_or("").to_lowercase();
    match extension.as_str() {
        "gz" => "zcat",
        "xz" => "xzcat",
        "bz" | "bz2" => "bzcat",
        _ => "cat", // Unknown extension, assume uncompressed
    }
}

/// Checks if a binary is available on the system
pub(crate) fn check_binary_available(cmd: &str) -> Result<(), String> {
    match std::process::Command::new(cmd)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
    {
        Ok(_) => Ok(()),
        Err(_) => Err(format!(
            "Required binary '{}' not found in PATH. Please install it and try again.",
            cmd
        )),
    }
}

/// Starts the appropriate decompression process based on URL extension
pub(crate) async fn start_decompressor_process(url: &str) -> Result<(Child, &'static str), Box<dyn std::error::Error>> {
    let cmd = get_decompressor_command(url);
    
    // Check if binary is available before attempting to spawn
    check_binary_available(cmd)?;
    
    println!("Using decompressor: {}", cmd);
    
    let process = Command::new(cmd)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    
    Ok((process, cmd))
}

pub(crate) async fn spawn_decompressor_stdout_reader(
    mut stdout: tokio::process::ChildStdout,
    decompressed_tx: mpsc::UnboundedSender<Vec<u8>>,
    mut dd_stdin: tokio::process::ChildStdin,
    error_tx: mpsc::UnboundedSender<String>,
) {
    let mut buffer = [0u8; 8 * 1024 * 1024]; // 8MB buffer for better performance
    loop {
        match stdout.read(&mut buffer).await {
            Ok(0) => break, // EOF
            Ok(n) => {
                // Send to progress tracking
                if decompressed_tx.send(buffer[..n].to_vec()).is_err() {
                    break;
                }
                // Write to dd stdin
                if let Err(e) = dd_stdin.write_all(&buffer[..n]).await {
                    let _ = error_tx.send(format!("Error writing to dd stdin: {}", e));
                    break;
                }
            }
            Err(e) => {
                let _ = error_tx.send(format!("Error reading from decompressor stdout: {}", e));
                break;
            }
        }
    }
    // Close dd stdin when decompressor is done
    let _ = dd_stdin.shutdown().await;
}

pub(crate) async fn spawn_stderr_reader(
    mut stderr: tokio::process::ChildStderr,
    error_tx: mpsc::UnboundedSender<String>,
    process_name: &'static str,
) {
    let mut buffer = [0u8; 1024];
    loop {
        match stderr.read(&mut buffer).await {
            Ok(0) => break, // EOF
            Ok(n) => {
                if let Ok(s) = String::from_utf8(buffer[..n].to_vec()) {
                    let _ = error_tx.send(format!("{}: {}", process_name, s.trim()));
                }
            }
            Err(_) => break,
        }
    }
}

