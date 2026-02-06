//! Fastboot flashing implementation
//!
//! This module uses the system fastboot CLI for flashing partitions.
//! OCI images are downloaded and extracted, then written to a temporary
//! directory and flashed via `fastboot flash`.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::process::Command;
use tokio::time::timeout;

use super::options::FastbootOptions;

#[derive(Debug)]
#[allow(dead_code)]
pub enum FastbootError {
    DeviceNotFound(Option<String>),
    CommandError(String),
    PartitionError(String),
    TimeoutError,
}

impl fmt::Display for FastbootError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FastbootError::DeviceNotFound(serial) => {
                if let Some(s) = serial {
                    write!(f, "Fastboot device with serial '{}' not found", s)
                } else {
                    write!(f, "No fastboot devices found")
                }
            }
            FastbootError::CommandError(msg) => write!(f, "Fastboot command error: {}", msg),
            FastbootError::PartitionError(msg) => write!(f, "Partition error: {}", msg),
            FastbootError::TimeoutError => write!(f, "Operation timed out"),
        }
    }
}

impl Error for FastbootError {}

fn temp_base_dir() -> Result<PathBuf, FastbootError> {
    let env_value = std::env::var("FLS_TMP_DIR").ok();
    let base = match env_value
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        Some(value) => PathBuf::from(value),
        None => PathBuf::from("/var/lib/fls"),
    };

    std::fs::create_dir_all(&base).map_err(|e| {
        let hint = if env_value.is_some() {
            format!(
                "Failed to create temp base directory {}: {}",
                base.display(),
                e
            )
        } else {
            format!(
                "Failed to create temp base directory {}: {}. Set FLS_TMP_DIR to override.",
                base.display(),
                e
            )
        };
        FastbootError::CommandError(hint)
    })?;

    Ok(base)
}

fn build_oci_options(options: &FastbootOptions) -> crate::fls::options::OciOptions {
    crate::fls::options::OciOptions {
        common: crate::fls::options::FlashOptions {
            insecure_tls: options.http.insecure_tls,
            cacert: options.http.cacert.clone(),
            debug: options.http.debug,
            ..crate::fls::options::FlashOptions::default()
        },
        username: options.username.clone(),
        password: options.password.clone(),
        file_pattern: None,
    }
}

/// Main entry point for fastboot flashing
///
/// Downloads an OCI image and flashes it to a fastboot device via the system fastboot CLI.
/// The image_ref should be an OCI image reference without a scheme
/// (e.g., "registry.example.com/my-image:latest").
pub async fn flash_from_fastboot(
    image_ref: &str,
    options: FastbootOptions,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let temp_dir = TempDirGuard::new("fls-fastboot")?;
    let partition_map = process_oci_image_to_dir(image_ref, &options, temp_dir.path()).await?;

    if partition_map.is_empty() {
        return Err("No partition files found for flashing".into());
    }

    flash_partitions_with_fastboot_cli(&partition_map, &options).await?;

    println!("Fastboot flash completed successfully!");
    Ok(())
}

struct TempDirGuard {
    path: PathBuf,
}

impl TempDirGuard {
    fn new(prefix: &str) -> Result<Self, FastbootError> {
        let base = temp_base_dir()?;
        let pid = std::process::id();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let dir = base.join(format!("{}-{}-{}", prefix, pid, timestamp));
        std::fs::create_dir(&dir).map_err(|e| {
            FastbootError::CommandError(format!(
                "Failed to create temp directory {}: {}",
                dir.display(),
                e
            ))
        })?;
        Ok(Self { path: dir })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

async fn run_fastboot_command(
    fastboot_path: &str,
    args: &[String],
    timeout_duration: Duration,
) -> Result<std::process::Output, FastbootError> {
    let mut cmd = Command::new(fastboot_path);
    cmd.args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let output = match timeout(timeout_duration, cmd.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(e)) => {
            return Err(FastbootError::CommandError(format!(
                "Failed to run fastboot: {}",
                e
            )))
        }
        Err(_) => return Err(FastbootError::TimeoutError),
    };

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(FastbootError::CommandError(format!(
            "fastboot {:?} failed (code {:?}). stdout: '{}' stderr: '{}'",
            args,
            output.status.code(),
            stdout.trim(),
            stderr.trim()
        )));
    }

    Ok(output)
}

async fn detect_fastboot_device_id(
    fastboot_path: &str,
    timeout_duration: Duration,
) -> Result<String, FastbootError> {
    let output = run_fastboot_command(
        fastboot_path,
        &["devices".to_string(), "-l".to_string()],
        timeout_duration,
    )
    .await?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut ids: Vec<String> = stdout
        .lines()
        .filter_map(|line| line.split_whitespace().next().map(|s| s.to_string()))
        .collect();

    if ids.is_empty() {
        return Err(FastbootError::DeviceNotFound(None));
    }

    if ids.len() > 1 {
        println!(
            "Warning: Multiple fastboot devices found, using first: {}",
            ids[0]
        );
    }

    Ok(ids.remove(0))
}

async fn flash_partitions_with_fastboot_cli(
    partition_map: &HashMap<String, PathBuf>,
    options: &FastbootOptions,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let fastboot_path = "fastboot";
    let timeout_duration = Duration::from_secs(options.timeout_secs as u64);

    let device_id = if let Some(serial) = options.device_serial.as_deref() {
        serial.to_string()
    } else {
        detect_fastboot_device_id(fastboot_path, timeout_duration).await?
    };

    println!("Using fastboot CLI: {}", fastboot_path);
    println!("Using device: {}", device_id);

    let mut partitions: Vec<(&String, &PathBuf)> = partition_map.iter().collect();
    partitions.sort_by(|a, b| a.0.cmp(b.0));

    for (partition, path) in &partitions {
        println!("Flashing partition '{}' from {}", partition, path.display());
        let mut args = vec!["-s".to_string(), device_id.clone()];
        args.push("flash".to_string());
        args.push((*partition).clone());
        args.push(path.display().to_string());
        run_fastboot_command(fastboot_path, &args, timeout_duration).await?;
    }

    println!("Running fastboot continue...");
    let args = vec!["-s".to_string(), device_id.clone(), "continue".to_string()];
    run_fastboot_command(fastboot_path, &args, timeout_duration).await?;

    println!("Fastboot CLI flash completed successfully.");
    Ok(())
}

/// Process OCI image and extract files for flashing
async fn process_oci_image_to_dir(
    image_ref: &str,
    options: &FastbootOptions,
    output_dir: &Path,
) -> Result<HashMap<String, PathBuf>, Box<dyn Error + Send + Sync>> {
    println!("Processing OCI image for fastboot: {}", image_ref);

    if options.partition_mappings.is_empty() {
        println!("No explicit partition mappings provided, attempting auto-detection from OCI annotations");
        return extract_files_by_auto_detection_to_dir(image_ref, options, output_dir).await;
    }

    println!("Partition mappings provided; applying overrides on top of OCI annotations");

    let oci_options = build_oci_options(options);

    match super::oci::extract_files_by_annotations_with_overrides_to_dir(
        image_ref,
        &oci_options,
        output_dir,
        &options.partition_mappings,
    )
    .await
    {
        Ok(Some(partition_files)) => return Ok(partition_files),
        Ok(None) => {
            println!("No OCI annotations found, falling back to file-pattern extraction");
        }
        Err(e) => {
            return Err(format!("Annotation-based extraction failed: {}", e).into());
        }
    }

    extract_files_by_patterns_to_dir(image_ref, options, output_dir).await
}

async fn extract_files_by_auto_detection_to_dir(
    image_ref: &str,
    options: &FastbootOptions,
    output_dir: &Path,
) -> Result<HashMap<String, PathBuf>, Box<dyn Error + Send + Sync>> {
    println!("Auto-detecting partitions from OCI layer annotations...");

    // Create OCI options
    let oci_options = build_oci_options(options);

    // Use annotation-aware extraction to get files from correct layers
    let partition_files =
        super::oci::extract_files_by_annotations_with_overrides_to_dir(
            image_ref,
            &oci_options,
            output_dir,
            &[],
        )
        .await
        .map_err(|e| format!("Annotation-based extraction failed: {}", e))?
        .ok_or_else(|| {
            "No partitions found in OCI annotations. Expected layers with 'automotive.sdv.cloud.redhat.com/partition' annotations".to_string()
        })?;

    if partition_files.is_empty() {
        return Err("No partitions found in OCI annotations. Expected layers with 'automotive.sdv.cloud.redhat.com/partition' annotations".into());
    }

    println!(
        "Successfully extracted {} partitions:",
        partition_files.len()
    );
    for (partition, path) in &partition_files {
        println!("  {} -> {}", partition, path.display());
    }

    Ok(partition_files)
}

async fn extract_files_by_patterns_to_dir(
    image_ref: &str,
    options: &FastbootOptions,
    output_dir: &Path,
) -> Result<HashMap<String, PathBuf>, Box<dyn Error + Send + Sync>> {
    let mut partition_files = HashMap::new();
    let target_files: HashSet<String> = options
        .partition_mappings
        .iter()
        .map(|(_, filename)| filename.clone())
        .collect();

    println!("Extracting files using partition mappings:");
    for (partition_name, file_pattern) in &options.partition_mappings {
        println!("  {} = {}", partition_name, file_pattern);
        println!("    Searching for file: {}", file_pattern);
    }

    let file_map =
        extract_files_from_oci_to_dir(image_ref, options, output_dir, &target_files).await?;

    for (partition_name, file_pattern) in &options.partition_mappings {
        match file_map.get(file_pattern) {
            Some(path) => {
                println!(
                    "    Found file for partition '{}' at {}",
                    partition_name,
                    path.display()
                );
                partition_files.insert(partition_name.clone(), path.clone());
            }
            None => {
                let available_files: Vec<&String> = file_map.keys().collect();
                return Err(format!(
                    "Missing partition file: {} (pattern: {}): File '{}' not found in OCI image. Available files: {:?}",
                    partition_name, file_pattern, file_pattern, available_files
                )
                .into());
            }
        }
    }

    if partition_files.is_empty() {
        return Err("No partition files found matching the specified patterns".into());
    }

    Ok(partition_files)
}

async fn extract_files_from_oci_to_dir(
    image_ref: &str,
    options: &FastbootOptions,
    output_dir: &Path,
    target_files: &HashSet<String>,
) -> Result<HashMap<String, PathBuf>, Box<dyn Error + Send + Sync>> {
    println!("Downloading OCI image: {}", image_ref);

    println!("Looking for files: {:?}", target_files);

    let oci_options = build_oci_options(options);

    let file_data = super::oci::extract_files_from_oci_image_to_dir(
        image_ref,
        target_files,
        &oci_options,
        output_dir,
    )
    .await
    .map_err(|e| -> Box<dyn Error + Send + Sync> {
        format!("OCI extraction failed: {}", e).into()
    })?;

    println!(
        "Successfully extracted {} files from OCI image",
        file_data.len()
    );
    Ok(file_data)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::fs;
    #[cfg(unix)]
    use std::future::Future;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    #[cfg(unix)]
    use std::sync::{Mutex, OnceLock};
    #[cfg(unix)]
    use tempfile::tempdir;

    #[test]
    fn test_fastboot_options_default() {
        let options = FastbootOptions::default();
        assert_eq!(options.timeout_secs, 1200);
        assert!(options.partition_mappings.is_empty());
        assert!(options.username.is_none());
        assert!(options.password.is_none());
    }

    #[test]
    fn test_fastboot_error_display() {
        let err = FastbootError::DeviceNotFound(Some("ABC123".to_string()));
        assert_eq!(
            err.to_string(),
            "Fastboot device with serial 'ABC123' not found"
        );
    }

    #[cfg(unix)]
    fn create_fastboot_script(contents: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempdir().expect("create temp dir");
        let path = dir.path().join("fastboot");
        fs::write(&path, contents).expect("write fastboot script");
        let mut perms = fs::metadata(&path)
            .expect("read script metadata")
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&path, perms).expect("set script permissions");
        (dir, path)
    }

    #[cfg(unix)]
    fn path_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[cfg(unix)]
    struct EnvVarGuard {
        key: &'static str,
        old: Option<String>,
    }

    #[cfg(unix)]
    impl EnvVarGuard {
        fn set(key: &'static str, value: String) -> Self {
            let old = std::env::var(key).ok();
            std::env::set_var(key, &value);
            Self { key, old }
        }
    }

    #[cfg(unix)]
    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(old) = &self.old {
                std::env::set_var(self.key, old);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    #[cfg(unix)]
    fn prepend_path(dir: &Path) -> String {
        let current = std::env::var("PATH").unwrap_or_default();
        format!("{}:{}", dir.display(), current)
    }

    #[cfg(unix)]
    fn block_on<F: Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build tokio runtime")
            .block_on(future)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_detect_fastboot_device_id_parses_first_device() {
        let script = r#"#!/bin/sh
if [ "$1" = "devices" ]; then
  echo "ABC123 fastboot"
  echo "DEF456 fastboot"
fi
"#;
        let (_dir, fastboot_path) = create_fastboot_script(script);
        let fastboot_path = fastboot_path.to_string_lossy().to_string();

        let device_id = detect_fastboot_device_id(&fastboot_path, Duration::from_secs(2)).await;

        assert_eq!(device_id.unwrap(), "ABC123");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_detect_fastboot_device_id_no_devices() {
        let script = r#"#!/bin/sh
if [ "$1" = "devices" ]; then
  exit 0
fi
"#;
        let (_dir, fastboot_path) = create_fastboot_script(script);
        let fastboot_path = fastboot_path.to_string_lossy().to_string();

        let err = detect_fastboot_device_id(&fastboot_path, Duration::from_secs(2)).await;

        assert!(matches!(err, Err(FastbootError::DeviceNotFound(None))));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_run_fastboot_command_failure_includes_output() {
        let script = r#"#!/bin/sh
if [ "$1" = "fail" ]; then
  echo "stdout message"
  echo "stderr message" >&2
  exit 7
fi
"#;
        let (_dir, fastboot_path) = create_fastboot_script(script);
        let fastboot_path = fastboot_path.to_string_lossy().to_string();

        let err = run_fastboot_command(
            &fastboot_path,
            &["fail".to_string()],
            Duration::from_secs(2),
        )
        .await
        .unwrap_err();

        match err {
            FastbootError::CommandError(msg) => {
                assert!(msg.contains("stdout message"));
                assert!(msg.contains("stderr message"));
            }
            other => panic!("Unexpected error: {:?}", other),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_run_fastboot_command_timeout() {
        let script = r#"#!/bin/sh
if [ "$1" = "sleep" ]; then
  sleep 2
fi
"#;
        let (_dir, fastboot_path) = create_fastboot_script(script);
        let fastboot_path = fastboot_path.to_string_lossy().to_string();

        let err = run_fastboot_command(
            &fastboot_path,
            &["sleep".to_string()],
            Duration::from_millis(100),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, FastbootError::TimeoutError));
    }

    #[cfg(unix)]
    #[test]
    fn test_flash_partitions_with_fastboot_cli_runs_flash_and_continue() {
        let _lock = path_lock().lock().expect("lock PATH");
        let script = r#"#!/bin/sh
if [ -n "$LOG_FILE" ]; then
  echo "$@" >> "$LOG_FILE"
fi
if [ "$1" = "devices" ]; then
  echo "ABC123 fastboot"
fi
exit 0
"#;
        let (dir, _fastboot_path) = create_fastboot_script(script);
        let log_path = dir.path().join("fastboot.log");
        let _log_guard = EnvVarGuard::set("LOG_FILE", log_path.to_string_lossy().to_string());
        let _path_guard = EnvVarGuard::set("PATH", prepend_path(dir.path()));

        let images_dir = tempdir().expect("create images dir");
        let boot_path = images_dir.path().join("boot_a.simg");
        let system_path = images_dir.path().join("system_a.simg");
        fs::write(&boot_path, b"boot image").expect("write boot image");
        fs::write(&system_path, b"system image").expect("write system image");

        let mut partition_map = HashMap::new();
        partition_map.insert("system_a".to_string(), system_path.clone());
        partition_map.insert("boot_a".to_string(), boot_path.clone());

        let options = FastbootOptions {
            device_serial: Some("SER123".to_string()),
            timeout_secs: 2,
            ..Default::default()
        };

        block_on(async {
            flash_partitions_with_fastboot_cli(&partition_map, &options)
                .await
                .expect("fastboot flash should succeed");
        });

        let log = fs::read_to_string(&log_path).expect("read fastboot log");
        let lines: Vec<String> = log.lines().map(|line| line.to_string()).collect();

        let expected = vec![
            format!("-s SER123 flash boot_a {}", boot_path.display()),
            format!("-s SER123 flash system_a {}", system_path.display()),
            "-s SER123 continue".to_string(),
        ];

        assert_eq!(lines, expected);
    }

    #[cfg(unix)]
    #[test]
    fn test_temp_dir_guard_cleans_up() {
        let base_dir = tempdir().expect("create temp base dir");
        let _env_guard =
            EnvVarGuard::set("FLS_TMP_DIR", base_dir.path().to_string_lossy().to_string());
        let path = {
            let guard = TempDirGuard::new("fls-fastboot-test").expect("create temp dir guard");
            let path = guard.path().to_path_buf();
            assert!(path.exists(), "temp dir should exist");
            path
        };

        assert!(!path.exists(), "temp dir should be removed on drop");
    }
}
