use tokio::io::AsyncReadExt;
use tokio::process::{Command, Child};
use tokio::sync::mpsc;

pub(crate) async fn start_dd_process(device: &str) -> Result<Child, Box<dyn std::error::Error>> {
    let dd = Command::new("dd")
        .arg(&format!("of={}", device))
        .arg("bs=64k")
        .arg("iflag=fullblock")
        .arg("oflag=direct")
        .arg("status=progress")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())  // Redirect stdout to /dev/null
        .stderr(std::process::Stdio::piped())
        .spawn()?;
    Ok(dd)
}

pub(crate) async fn handle_dd_progress(
    mut dd_stderr: tokio::process::ChildStderr,
    dd_error_tx: mpsc::UnboundedSender<String>,
) {
    let mut buffer = [0u8; 1024];
    loop {
        match dd_stderr.read(&mut buffer).await {
            Ok(0) => break, // EOF
            Ok(n) => {
                if let Ok(s) = String::from_utf8(buffer[..n].to_vec()) {
                    let _ = dd_error_tx.send(s);
                }
            }
            Err(_) => break,
        }
    }
}

pub(crate) async fn process_dd_messages(
    mut dd_error_rx: mpsc::UnboundedReceiver<String>,
    written_tx: mpsc::UnboundedSender<u64>,
    debug: bool,
) -> Result<(), String> {
    while let Some(dd_msg) = dd_error_rx.recv().await {
        if debug {
            eprintln!("[DEBUG] dd output: {}", dd_msg.trim());
        }
        
        // Parse dd progress messages to extract bytes written
        if dd_msg.contains("bytes") && (dd_msg.contains("transferred") || dd_msg.contains("copied")) {
            // Parse format like: "13238272 bytes (13 MB, 13 MiB) transferred 13.725s, 965 kB/s"
            // or: "9895936 bytes (9896 kB, 9664 KiB) transferred 10.146s, 975 kB/s"
            // We want the second number in the tuple with its unit
            if let Some(paren_start) = dd_msg.find('(') {
                if let Some(paren_end) = dd_msg.find(')') {
                    let tuple_content = &dd_msg[paren_start + 1..paren_end];
                    // Split by comma and get the second part
                    let parts: Vec<&str> = tuple_content.split(',').collect();
                    if parts.len() >= 2 {
                        let second_part = parts[1].trim();
                        // Extract number and unit
                        if let Some(space_pos) = second_part.find(' ') {
                            let number_str = &second_part[..space_pos];
                            let unit = &second_part[space_pos + 1..];
                            
                            // Try to parse as float first (handles both integers and decimals like "1.5")
                            if let Ok(value) = number_str.parse::<f64>() {
                                // Convert to bytes based on unit
                                let bytes_written = match unit {
                                    "B" => value,
                                    "kB" => value * 1000.0,
                                    "kiB" => value * 1024.0,
                                    "MB" => value * 1000.0 * 1000.0,
                                    "MiB" => value * 1024.0 * 1024.0,
                                    "GB" => value * 1000.0 * 1000.0 * 1000.0,
                                    "GiB" => value * 1024.0 * 1024.0 * 1024.0,
                                    "TB" => value * 1000.0 * 1000.0 * 1000.0 * 1000.0,
                                    "TiB" => value * 1024.0 * 1024.0 * 1024.0 * 1024.0,
                                    _ => value, // fallback to raw value (treat as bytes)
                                };
                                let _ = written_tx.send(bytes_written as u64);
                            }
                        } else {
                            // No space found, treat the entire second_part as a number (bytes)
                            if let Ok(value) = second_part.parse::<f64>() {
                                let _ = written_tx.send(value as u64);
                            }
                        }
                    }
                }
            }
        } else if !dd_msg.trim().is_empty() {
            // Skip "records in/out" messages from dd's final summary
            if !dd_msg.contains("records in") && !dd_msg.contains("records out") {
                eprintln!("dd: {}", dd_msg.trim());
            }
        }
    }
    Ok(())
}

