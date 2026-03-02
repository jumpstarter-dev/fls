/// Flash from OCI image
///
/// Implements the streaming pipeline:
/// Registry blob -> gzip decompress -> tar extract -> xzcat -> block device
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use bytes::Bytes;
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use xz2::read::XzDecoder;

use crate::fls::byte_channel::{byte_bounded_channel, ByteBoundedReceiver, ByteBoundedSender};

use super::manifest::{LayerCompression, Manifest};
use super::reference::ImageReference;
use super::registry::RegistryClient;
use crate::fls::automotive::annotations as automotive_annotations;
use crate::fls::block_writer::AsyncBlockWriter;
use crate::fls::compression::Compression;
use crate::fls::decompress::{spawn_stderr_reader, start_decompressor_process};
use crate::fls::error_handling::process_error_messages;
use crate::fls::format_detector::{DetectionResult, FileFormat, FormatDetector};
use crate::fls::magic_bytes::{detect_compression, detect_content_and_compression, ContentType};
use crate::fls::options::OciOptions;
use crate::fls::progress::ProgressTracker;
use crate::fls::simg::{SparseParser, WriteCommand};
use crate::fls::stream_utils::ChannelReader;

use std::collections::{HashMap, HashSet};

const OCI_TITLE_ANNOTATION: &str = "org.opencontainers.image.title";

/// Parameters for download coordination functions
struct DownloadCoordinationParams {
    http_tx: ByteBoundedSender<bytes::Bytes>,
    decompressed_progress_rx: mpsc::UnboundedReceiver<u64>,
    written_progress_rx: mpsc::UnboundedReceiver<u64>,
    decompressor_written_progress_rx: mpsc::UnboundedReceiver<u64>,
}

/// Download context parameters
struct DownloadContext {
    content_detection_buffer: Vec<u8>,
    content_length: Option<u64>,
    decompressor_name: &'static str,
}

/// Parameters for raw disk download coordination
struct RawDiskDownloadParams {
    http_tx: ByteBoundedSender<bytes::Bytes>,
    writer_handle: tokio::task::JoinHandle<Result<u64, std::io::Error>>,
    external_decompressor: Option<tokio::process::Child>,
    decompressed_progress_rx: mpsc::UnboundedReceiver<u64>,
    raw_written_progress_rx: mpsc::UnboundedReceiver<u64>,
}

/// Processing handles for coordination functions
struct ProcessingHandles {
    writer_handle: tokio::task::JoinHandle<Result<u64, std::io::Error>>,
    decompressor_writer_handle: tokio::task::JoinHandle<Result<(), String>>,
    error_processor: tokio::task::JoinHandle<()>,
    tar_extractor_handle: tokio::task::JoinHandle<Result<(), String>>,
}

/// Components returned by external decompressor pipeline setup
struct ExternalDecompressorPipeline {
    writer_handle: tokio::task::JoinHandle<Result<u64, std::io::Error>>,
    decompressor: tokio::process::Child,
}

/// Components returned by pipeline setup
struct TarPipelineComponents {
    http_tx: ByteBoundedSender<bytes::Bytes>,
    http_rx: ByteBoundedReceiver<bytes::Bytes>,
    tar_tx: mpsc::Sender<Vec<u8>>,
    decompressed_progress_rx: mpsc::UnboundedReceiver<u64>,
    written_progress_rx: mpsc::UnboundedReceiver<u64>,
    decompressor_written_progress_rx: mpsc::UnboundedReceiver<u64>,
    writer_handle: tokio::task::JoinHandle<Result<u64, std::io::Error>>,
    decompressor_writer_handle: tokio::task::JoinHandle<Result<(), String>>,
    error_processor: tokio::task::JoinHandle<()>,
    decompressor: tokio::process::Child,
    decompressor_name: &'static str,
}

/// Connect to an OCI registry and resolve the image manifest.
///
/// Handles image reference parsing, client creation, authentication, and
/// manifest resolution (including multi-platform index negotiation).
async fn connect_and_resolve(
    image: &str,
    options: &OciOptions,
) -> Result<(RegistryClient, Manifest), Box<dyn std::error::Error>> {
    let image_ref = ImageReference::parse(image)?;
    println!("Pulling OCI image: {}", image_ref);

    let mut client = RegistryClient::new(image_ref.clone(), options).await?;
    println!("Connecting to registry: {}", image_ref.registry);
    client.authenticate().await?;

    let manifest = resolve_manifest(&mut client).await?;
    Ok((client, manifest))
}

async fn stream_blob_to_file(
    mut stream: impl futures_util::Stream<Item = reqwest::Result<Bytes>> + Unpin + Send + 'static,
    output_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut prefix = Vec::new();
    let mut initial_chunks: Vec<Bytes> = Vec::new();
    while prefix.len() < 6 {
        match stream.next().await {
            Some(Ok(chunk)) => {
                if prefix.len() < 6 {
                    let needed = 6 - prefix.len();
                    let take = needed.min(chunk.len());
                    prefix.extend_from_slice(&chunk[..take]);
                }
                initial_chunks.push(chunk);
            }
            Some(Err(e)) => return Err(format!("Stream error: {}", e).into()),
            None => break,
        }
    }

    if initial_chunks.is_empty() {
        return Err("Empty OCI layer stream".into());
    }

    let blob_compression = detect_compression(&prefix);

    let (tx, rx) = mpsc::channel::<Bytes>(16);
    for chunk in initial_chunks {
        tx.send(chunk)
            .await
            .map_err(|_| "Failed to send initial chunk to reader")?;
    }

    let forward_handle = tokio::spawn(async move {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| format!("Stream error: {}", e))?;
            tx.send(chunk)
                .await
                .map_err(|_| "Reader channel closed".to_string())?;
        }
        Ok::<(), String>(())
    });

    let writer_handle = tokio::task::spawn_blocking(move || -> Result<(), String> {
        let reader = ChannelReader::new(rx);
        let mut file = File::create(&output_path)
            .map_err(|e| format!("Failed to create {}: {}", output_path.display(), e))?;

        match blob_compression {
            Compression::Gzip => {
                let mut decoder = GzDecoder::new(reader);
                std::io::copy(&mut decoder, &mut file)
                    .map_err(|e| format!("Gzip decompression failed: {}", e))?;
            }
            Compression::Xz => {
                let mut decoder = XzDecoder::new(reader);
                std::io::copy(&mut decoder, &mut file)
                    .map_err(|e| format!("XZ decompression failed: {}", e))?;
            }
            _ => {
                let mut reader = reader;
                std::io::copy(&mut reader, &mut file)
                    .map_err(|e| format!("Stream copy failed: {}", e))?;
            }
        }

        file.flush()
            .map_err(|e| format!("Failed to flush {}: {}", output_path.display(), e))?;
        Ok(())
    });

    let (forward_result, writer_result) = tokio::join!(forward_handle, writer_handle);

    // Prioritize writer errors — a writer failure (e.g. disk full, decompression
    // error) drops rx, which causes the forwarder's tx.send to fail with a
    // misleading "Reader channel closed" error.
    writer_result
        .map_err(|e| format!("Writer task failed: {}", e))?
        .map_err(|e| format!("Writer task error: {}", e))?;
    forward_result
        .map_err(|e| format!("Stream task failed: {}", e))?
        .map_err(|e| format!("Stream task error: {}", e))?;

    Ok(())
}

async fn stream_blob_to_tar_files(
    mut stream: impl futures_util::Stream<Item = reqwest::Result<Bytes>> + Unpin + Send + 'static,
    target_files: std::collections::HashSet<String>,
    output_dir: PathBuf,
) -> Result<HashMap<String, PathBuf>, Box<dyn std::error::Error>> {
    let mut prefix = Vec::new();
    let mut initial_chunks: Vec<Bytes> = Vec::new();
    while prefix.len() < 6 {
        match stream.next().await {
            Some(Ok(chunk)) => {
                if prefix.len() < 6 {
                    let needed = 6 - prefix.len();
                    let take = needed.min(chunk.len());
                    prefix.extend_from_slice(&chunk[..take]);
                }
                initial_chunks.push(chunk);
            }
            Some(Err(e)) => return Err(format!("Stream error: {}", e).into()),
            None => break,
        }
    }

    if initial_chunks.is_empty() {
        return Err("Empty OCI layer stream".into());
    }

    let blob_compression = detect_compression(&prefix);

    let (tx, rx) = mpsc::channel::<Bytes>(16);
    for chunk in initial_chunks {
        tx.send(chunk)
            .await
            .map_err(|_| "Failed to send initial chunk to reader")?;
    }

    let forward_handle = tokio::spawn(async move {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| format!("Stream error: {}", e))?;
            tx.send(chunk)
                .await
                .map_err(|_| "Reader channel closed".to_string())?;
        }
        Ok::<(), String>(())
    });

    let writer_handle =
        tokio::task::spawn_blocking(move || -> Result<HashMap<String, PathBuf>, String> {
            let reader = ChannelReader::new(rx);
            let reader: Box<dyn Read + Send> = match blob_compression {
                Compression::Gzip => Box::new(GzDecoder::new(reader)),
                Compression::Xz => Box::new(XzDecoder::new(reader)),
                _ => Box::new(reader),
            };

            let mut found = HashMap::new();
            let mut archive = tar::Archive::new(reader);
            for entry_result in archive.entries().map_err(|e| format!("Tar error: {}", e))? {
                let mut entry = entry_result.map_err(|e| format!("Tar entry error: {}", e))?;
                let path = entry.path().map_err(|e| format!("Invalid path: {}", e))?;
                let file_name = match path.file_name() {
                    Some(name) => name.to_string_lossy().to_string(),
                    None => continue,
                };

                if target_files.contains(&file_name) {
                    let output_path = output_dir.join(&file_name);
                    let mut file = File::create(&output_path).map_err(|e| {
                        format!("Failed to create {}: {}", output_path.display(), e)
                    })?;
                    let mut prefix = [0u8; 6];
                    let prefix_len = entry
                        .read(&mut prefix)
                        .map_err(|e| format!("Failed to read {}: {}", file_name, e))?;
                    let prefix_slice = &prefix[..prefix_len];
                    let cursor = std::io::Cursor::new(prefix_slice.to_vec());
                    let mut combined = cursor.chain(entry);

                    match detect_compression(prefix_slice) {
                        Compression::Gzip => {
                            let mut decoder = GzDecoder::new(combined);
                            std::io::copy(&mut decoder, &mut file).map_err(|e| {
                                format!("Failed to write {}: {}", output_path.display(), e)
                            })?;
                        }
                        Compression::Xz => {
                            let mut decoder = XzDecoder::new(combined);
                            std::io::copy(&mut decoder, &mut file).map_err(|e| {
                                format!("Failed to write {}: {}", output_path.display(), e)
                            })?;
                        }
                        _ => {
                            std::io::copy(&mut combined, &mut file).map_err(|e| {
                                format!("Failed to write {}: {}", output_path.display(), e)
                            })?;
                        }
                    }
                    found.insert(file_name, output_path);

                    if found.len() == target_files.len() {
                        break;
                    }
                }
            }

            Ok(found)
        });

    let (forward_result, writer_result) = tokio::join!(forward_handle, writer_handle);

    // Prioritize writer errors (see stream_blob_to_file for rationale).
    let found = writer_result
        .map_err(|e| format!("Writer task failed: {}", e))?
        .map_err(|e| format!("Writer task error: {}", e))?;
    forward_result
        .map_err(|e| format!("Stream task failed: {}", e))?
        .map_err(|e| format!("Stream task error: {}", e))?;

    Ok(found)
}

/// Extract specific files from an OCI image and write them to output_dir
pub async fn extract_files_from_oci_image_to_dir(
    image: &str,
    target_files: &std::collections::HashSet<String>,
    options: &OciOptions,
    output_dir: &Path,
) -> Result<HashMap<String, PathBuf>, Box<dyn std::error::Error>> {
    let (client, manifest) = connect_and_resolve(image, options).await?;

    // Get the layer to download
    let layer = manifest.get_single_layer()?;
    let layer_size = layer.size;

    println!("Layer digest: {}", layer.digest);
    println!(
        "Layer size: {} bytes ({:.2} MB)",
        layer_size,
        layer_size as f64 / (1024.0 * 1024.0)
    );

    ensure_supported_layer_compression(layer.compression(), &layer.media_type)?;

    // Start blob download
    println!("Starting download...");
    let response = client.get_blob_stream(&layer.digest).await?;
    let stream = response.bytes_stream();

    let is_tar_layer = layer.media_type.contains("tar") || looks_like_tar_layer(layer);
    let should_use_tar = is_tar_layer || target_files.len() > 1;

    if should_use_tar {
        if is_tar_layer {
            println!("Processing as tar archive based on layer media type...");
        } else {
            println!("Processing as tar archive due to multiple target files...");
        }
        let file_map =
            stream_blob_to_tar_files(stream, target_files.clone(), output_dir.to_path_buf())
                .await?;
        return Ok(file_map);
    }

    println!("Processing as direct file based on layer content...");
    if target_files.len() != 1 {
        return Err("Multiple target files specified but OCI layer contains a single direct file. Use separate layers for each file.".into());
    }

    let filename = target_files.iter().next().unwrap();
    let basename = Path::new(filename)
        .file_name()
        .ok_or_else(|| format!("Invalid filename '{}'", filename))?;
    let output_path = output_dir.join(basename);

    stream_blob_to_file(stream, output_path.clone()).await?;

    let mut map = HashMap::new();
    map.insert(filename.clone(), output_path);
    Ok(map)
}

/// Extract files based on layer annotations for automotive images and write to output_dir
pub async fn extract_files_by_annotations_to_dir(
    image: &str,
    options: &OciOptions,
    output_dir: &Path,
) -> Result<HashMap<String, PathBuf>, Box<dyn std::error::Error>> {
    let (client, manifest) = connect_and_resolve(image, options).await?;

    // Get layers and extract from each based on annotations
    let layers = manifest.get_layers()?;
    let mut partition_files = HashMap::new();

    for layer in layers {
        if let Some(ref annotations) = layer.annotations {
            if let Some(partition) = annotations.get(automotive_annotations::PARTITION_ANNOTATION) {
                ensure_supported_layer_compression(layer.compression(), &layer.media_type)?;
                let sanitized_name = sanitize_partition_name(partition)
                    .map_err(|e| format!("Invalid partition annotation '{}': {}", partition, e))?;
                let title = annotations
                    .get(OCI_TITLE_ANNOTATION)
                    .map(|s| s.as_str())
                    .unwrap_or("layer");
                println!(
                    "Extracting {} from layer {} for partition {}",
                    title,
                    &layer.digest[0..12],
                    partition
                );

                // Download this specific layer
                let response = client.get_blob_stream(&layer.digest).await?;
                let stream = response.bytes_stream();

                let output_path = output_dir.join(format!("{}.img", sanitized_name));
                stream_blob_to_file(stream, output_path.clone()).await?;
                partition_files.insert(sanitized_name, output_path);
            }
        }
    }

    println!(
        "Extracted {} partitions by annotations",
        partition_files.len()
    );
    Ok(partition_files)
}

fn parse_default_partitions(manifest: &Manifest) -> Result<Option<HashSet<String>>, String> {
    let Manifest::Image(image) = manifest else {
        return Ok(None);
    };

    let Some(ref annotations) = image.annotations else {
        return Ok(None);
    };

    let Some(raw) = annotations.get(automotive_annotations::DEFAULT_PARTITIONS) else {
        return Ok(None);
    };

    let mut partitions = HashSet::new();
    for entry in raw.split(',') {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        let sanitized = sanitize_partition_name(trimmed)
            .map_err(|e| format!("Invalid default partition '{}': {}", trimmed, e))?;
        partitions.insert(sanitized);
    }

    if partitions.is_empty() {
        return Err(format!(
            "Default partition annotation '{}' is empty",
            automotive_annotations::DEFAULT_PARTITIONS
        ));
    }

    Ok(Some(partitions))
}

fn looks_like_tar_layer(layer: &super::manifest::Descriptor) -> bool {
    if layer.media_type.contains("tar") {
        return true;
    }

    let Some(ref annotations) = layer.annotations else {
        return false;
    };

    let Some(title) = annotations.get(OCI_TITLE_ANNOTATION) else {
        return false;
    };

    let title = title.to_ascii_lowercase();
    title.ends_with(".tar")
        || title.ends_with(".tar.gz")
        || title.ends_with(".tgz")
        || title.ends_with(".tar.xz")
        || title.ends_with(".tar.lz4")
}

/// Extract files based on layer annotations, applying optional overrides.
///
/// When overrides are provided, each entry maps a target partition to a layer
/// title (org.opencontainers.image.title). Auto-detected partitions remain
/// unless explicitly overridden. If the manifest provides default partitions,
/// only those partitions are auto-extracted. Returns Ok(None) when no
/// annotations exist and overrides were provided, allowing callers to fall back
/// to pattern-based extraction.
pub async fn extract_files_by_annotations_with_overrides_to_dir(
    image: &str,
    options: &OciOptions,
    output_dir: &Path,
    overrides: &[(String, String)],
) -> Result<Option<HashMap<String, PathBuf>>, Box<dyn std::error::Error>> {
    let (client, manifest) = connect_and_resolve(image, options).await?;

    let default_partitions = parse_default_partitions(&manifest)
        .map_err(|e| format!("Invalid default partitions annotation: {}", e))?;
    if let Some(ref partitions) = default_partitions {
        println!("Using default partitions from manifest: {:?}", partitions);
    }

    // Get layers and build lookup tables
    let layers = manifest.get_layers()?;
    let mut partition_files = HashMap::new();
    let mut title_to_layer: HashMap<String, &super::manifest::Descriptor> = HashMap::new();
    let mut title_to_path = HashMap::new();
    let mut has_partition_annotations = false;
    let mut available_partitions = HashSet::new();
    let overridden_partitions: HashSet<String> = overrides
        .iter()
        .map(|(partition, _)| partition.clone())
        .collect();
    let allowed_partitions = default_partitions.as_ref();
    let is_single_tar_layer = layers.len() == 1 && looks_like_tar_layer(&layers[0]);

    for layer in layers {
        if let Some(ref annotations) = layer.annotations {
            if let Some(title) = annotations.get(OCI_TITLE_ANNOTATION) {
                if let Some(existing) = title_to_layer.get(title) {
                    eprintln!(
                        "Warning: duplicate OCI title '{}': keeping layer {}, ignoring layer {}",
                        title,
                        &existing.digest[..12.min(existing.digest.len())],
                        &layer.digest[..12.min(layer.digest.len())],
                    );
                } else {
                    title_to_layer.insert(title.clone(), layer);
                }
            }

            if let Some(partition) = annotations.get(automotive_annotations::PARTITION_ANNOTATION) {
                has_partition_annotations = true;
                available_partitions.insert(partition.clone());
                if overridden_partitions.contains(partition) {
                    continue;
                }
                if let Some(allowed) = allowed_partitions {
                    if !allowed.contains(partition) {
                        continue;
                    }
                }
                ensure_supported_layer_compression(layer.compression(), &layer.media_type)?;
                let sanitized_name = sanitize_partition_name(partition)
                    .map_err(|e| format!("Invalid partition annotation '{}': {}", partition, e))?;
                let title = annotations
                    .get(OCI_TITLE_ANNOTATION)
                    .map(|s| s.as_str())
                    .unwrap_or("layer");
                println!(
                    "Extracting {} from layer {} for partition {}",
                    title,
                    &layer.digest[0..12],
                    partition
                );

                // Download this specific layer
                let response = client.get_blob_stream(&layer.digest).await?;
                let stream = response.bytes_stream();

                let output_path = output_dir.join(format!("{}.img", sanitized_name));
                stream_blob_to_file(stream, output_path.clone()).await?;
                partition_files.insert(sanitized_name, output_path.clone());
                if let Some(title) = annotations.get(OCI_TITLE_ANNOTATION) {
                    title_to_path.insert(title.clone(), output_path);
                }
            }
        }
    }

    if !has_partition_annotations && title_to_layer.is_empty() {
        if overrides.is_empty() {
            return Err(
                "No partitions found in OCI annotations. Expected layers with 'automotive.sdv.cloud.redhat.com/partition' annotations"
                    .into(),
            );
        }
        return Ok(None);
    }

    if !has_partition_annotations && !overrides.is_empty() && is_single_tar_layer {
        let single_title = layers[0]
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(OCI_TITLE_ANNOTATION));
        let override_titles: HashSet<&str> = overrides
            .iter()
            .map(|(_, filename)| filename.as_str())
            .collect();

        if single_title
            .map(|title| !override_titles.contains(title.as_str()))
            .unwrap_or(true)
        {
            return Ok(None);
        }
    }

    // Apply overrides by layer title
    for (partition, filename) in overrides {
        let sanitized_partition = sanitize_partition_name(partition)
            .map_err(|e| format!("Invalid partition mapping '{}': {}", partition, e))?;

        if let Some(path) = title_to_path.get(filename) {
            partition_files.insert(sanitized_partition, path.clone());
            continue;
        }

        let layer = match title_to_layer.get(filename) {
            Some(layer) => *layer,
            None => {
                let available: Vec<&String> = title_to_layer.keys().collect();
                return Err(format!(
                    "Override file '{}' not found in OCI layer titles. Available files: {:?}",
                    filename, available
                )
                .into());
            }
        };

        ensure_supported_layer_compression(layer.compression(), &layer.media_type)?;
        let output_path = output_dir.join(format!("{}.img", sanitized_partition));

        let response = client.get_blob_stream(&layer.digest).await?;
        let stream = response.bytes_stream();
        stream_blob_to_file(stream, output_path.clone()).await?;

        title_to_path.insert(filename.clone(), output_path.clone());
        partition_files.insert(sanitized_partition, output_path);
    }

    if partition_files.is_empty() {
        if let Some(allowed) = allowed_partitions {
            return Err(format!(
                "No partitions matched default partitions {:?}. Available partitions: {:?}",
                allowed, available_partitions
            )
            .into());
        }
        return Err(
            "No partitions found in OCI annotations. Expected layers with 'automotive.sdv.cloud.redhat.com/partition' annotations"
                .into(),
        );
    }

    Ok(Some(partition_files))
}

fn ensure_supported_layer_compression(
    compression: LayerCompression,
    media_type: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    match compression {
        LayerCompression::None | LayerCompression::Gzip => Ok(()),
        other => Err(format!(
            "Unsupported OCI layer compression {:?} (media type: {}). Supported: uncompressed, gzip",
            other, media_type
        )
        .into()),
    }
}

fn sanitize_partition_name(name: &str) -> Result<String, String> {
    if name.is_empty() {
        return Err("partition name is empty".to_string());
    }
    if std::path::Path::new(name).is_absolute() {
        return Err("partition name is an absolute path".to_string());
    }
    if name.contains("..") {
        return Err("partition name contains '..'".to_string());
    }
    if name.contains('/') || name.contains('\\') {
        return Err("partition name contains path separators".to_string());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return Err("partition name contains invalid characters".to_string());
    }
    Ok(name.to_string())
}

/// Execute a sequence of write commands on the block writer
async fn execute_write_commands(
    commands: Vec<WriteCommand>,
    writer: &AsyncBlockWriter,
    debug: bool,
) -> std::io::Result<()> {
    for cmd in commands {
        match cmd {
            WriteCommand::Write(data) => writer.write(data).await?,
            WriteCommand::Seek(offset) => {
                if debug {
                    eprintln!("[DEBUG] Sparse: seeking to offset {}", offset);
                }
                writer.seek(offset).await?;
            }
            WriteCommand::Fill { pattern, bytes } => {
                if debug {
                    eprintln!(
                        "[DEBUG] Sparse: fill pattern {:02x}{:02x}{:02x}{:02x} for {} bytes",
                        pattern[0], pattern[1], pattern[2], pattern[3], bytes
                    );
                }
                writer.fill(pattern, bytes).await?;
            }
            WriteCommand::Complete { expected_size } => {
                if debug {
                    eprintln!(
                        "[DEBUG] Sparse: complete, expected output size {} bytes",
                        expected_size
                    );
                }
            }
        }
    }
    Ok(())
}

/// Process data through sparse parser and execute resulting write commands
async fn process_sparse_data(
    parser: &mut SparseParser,
    data: &[u8],
    writer: &AsyncBlockWriter,
    debug: bool,
) -> std::io::Result<()> {
    let (commands, _consumed) = parser.process(data).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Sparse image parse error: {}", e),
        )
    })?;
    execute_write_commands(commands, writer, debug).await
}

/// Handle detected format: process initial data and return parser if sparse
async fn handle_detected_format(
    format: FileFormat,
    consumed_bytes: Vec<u8>,
    remaining_data: &[u8],
    writer: &AsyncBlockWriter,
    debug: bool,
) -> std::io::Result<Option<SparseParser>> {
    match format {
        FileFormat::SparseImage => {
            println!("Sparse image (simg) format detected");
            if debug {
                eprintln!("[DEBUG] Auto-detect: Detected sparse image format");
            }
            let mut parser = SparseParser::new();

            // Process the accumulated detection data
            process_sparse_data(&mut parser, &consumed_bytes, writer, debug).await?;

            // Process any remaining data in current buffer
            if !remaining_data.is_empty() {
                process_sparse_data(&mut parser, remaining_data, writer, debug).await?;
            }

            Ok(Some(parser))
        }
        FileFormat::Regular => {
            if debug {
                eprintln!("[DEBUG] Auto-detect: Detected regular file format");
            }
            // Write the accumulated detection data
            writer.write(consumed_bytes).await?;

            // Write any remaining data
            if !remaining_data.is_empty() {
                writer.write(remaining_data.to_vec()).await?;
            }

            Ok(None)
        }
    }
}

/// Process a single buffer through format detection and writing
///
/// This is the core logic for handling incoming data in the write pipeline.
/// It handles format detection (sparse vs regular), and once determined,
/// routes data appropriately to either the sparse parser or direct write.
async fn process_buffer_with_format_detection(
    buffer: &[u8],
    n: usize,
    detector: &mut FormatDetector,
    parser: &mut Option<SparseParser>,
    format_determined: &mut bool,
    writer: &AsyncBlockWriter,
    debug: bool,
) -> std::io::Result<()> {
    if !*format_determined {
        match detector.process(&buffer[..n]) {
            DetectionResult::NeedMoreData => {
                if debug {
                    eprintln!("[DEBUG] Auto-detect: Need more data for format detection");
                }
                return Ok(());
            }
            DetectionResult::Detected {
                format,
                consumed_bytes,
                consumed_from_input,
            } => {
                let remaining = &buffer[consumed_from_input..n];
                *parser = handle_detected_format(format, consumed_bytes, remaining, writer, debug)
                    .await?;
                *format_determined = true;
            }
            DetectionResult::Error(msg) => {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, msg));
            }
        }
    } else if let Some(ref mut p) = parser {
        process_sparse_data(p, &buffer[..n], writer, debug).await?;
    } else {
        writer.write(buffer[..n].to_vec()).await?;
    }
    Ok(())
}

/// Handle EOF when format detection is incomplete
async fn finalize_format_at_eof(
    detector: &mut FormatDetector,
    format_determined: bool,
    writer: &AsyncBlockWriter,
    debug: bool,
) -> std::io::Result<()> {
    if !format_determined {
        if let Some(buffered_data) = detector.finalize_at_eof() {
            if debug {
                eprintln!(
                    "[DEBUG] EOF before format detection complete, writing {} buffered bytes as regular data",
                    buffered_data.len()
                );
            }
            writer.write(buffered_data).await?;
        }
    }
    Ok(())
}

/// Resolve the manifest, handling multi-platform manifest indexes
async fn resolve_manifest(
    client: &mut RegistryClient,
) -> Result<Manifest, Box<dyn std::error::Error>> {
    println!("Fetching manifest...");
    let manifest = client.fetch_manifest().await?;

    // Handle manifest index (multi-platform images)
    let manifest = match manifest {
        Manifest::Index(idx) => {
            println!(
                "Found manifest index with {} platforms",
                idx.manifests.len()
            );
            let platform = idx
                .find_linux_manifest()
                .ok_or("No linux/arm64 or linux/amd64 manifest found")?;
            println!(
                "Selected platform: {:?}",
                platform
                    .platform
                    .as_ref()
                    .map(|p| format!("{}/{}", p.os, p.architecture))
            );
            client.fetch_manifest_by_digest(&platform.digest).await?
        }
        m => m,
    };

    Ok(manifest)
}

/// Setup the tar processing pipeline with channels, decompressor, and async tasks
async fn setup_tar_processing_pipeline(
    content_type: ContentType,
    compression: LayerCompression,
    compression_type: Compression,
    options: &OciOptions,
    buffer_size_mb: usize,
    buffer_capacity: usize,
) -> Result<TarPipelineComponents, Box<dyn std::error::Error>> {
    let max_buffer_bytes = buffer_size_mb * 1024 * 1024;
    println!(
        "Using download buffer: {} MB (byte-bounded)",
        buffer_size_mb
    );

    let (http_tx, http_rx) =
        byte_bounded_channel::<bytes::Bytes>(max_buffer_bytes, buffer_capacity);

    // Channel for tar entry data -> decompressor stdin
    let (tar_tx, mut tar_rx) = mpsc::channel::<Vec<u8>>(16); // 16 * 8MB = 128MB buffer

    // Channels for progress tracking
    let (decompressed_progress_tx, decompressed_progress_rx) = mpsc::unbounded_channel::<u64>();
    let (error_tx, error_rx) = mpsc::unbounded_channel::<String>();
    let (written_progress_tx, written_progress_rx) = mpsc::unbounded_channel::<u64>();
    // Channel for tracking bytes actually written to decompressor (for progress bar)
    let (decompressor_written_progress_tx, decompressor_written_progress_rx) =
        mpsc::unbounded_channel::<u64>();

    // Choose decompressor based on content type and compression detection
    let initial_decompressor_hint = get_decompressor_hint(
        content_type.clone(),
        compression,
        compression_type,
        options.file_pattern.as_deref(),
    );
    if options.common.debug {
        eprintln!(
            "[DEBUG] Selected decompressor hint: '{}' (content={:?}, layer_compression={:?}, content_compression={:?})",
            initial_decompressor_hint, content_type, compression, compression_type
        );
    }
    let (mut decompressor, decompressor_name) =
        start_decompressor_process(initial_decompressor_hint).await?;

    let mut decompressor_stdin = decompressor.stdin.take().unwrap();
    let decompressor_stdout = decompressor.stdout.take().unwrap();
    let decompressor_stderr = decompressor.stderr.take().unwrap();

    println!(
        "Opening block device for writing: {}",
        options.common.device
    );

    // Create block writer
    let block_writer = AsyncBlockWriter::new(
        options.common.device.clone(),
        written_progress_tx.clone(),
        options.common.debug,
        options.common.o_direct,
        options.common.write_buffer_size_mb,
    )?;

    // Spawn task: decompressor stdout -> block writer with sparse image detection
    let error_tx_clone = error_tx.clone();
    let debug = options.common.debug;
    let writer_handle = {
        let writer = block_writer;
        tokio::spawn(async move {
            let mut stdout = decompressor_stdout;
            let mut buffer = vec![0u8; 8 * 1024 * 1024]; // 8MB buffer

            // Auto-detect sparse image format from initial data
            let mut detector = FormatDetector::new();
            let mut parser: Option<SparseParser> = None;
            let mut format_determined = false;

            loop {
                let n = match tokio::io::AsyncReadExt::read(&mut stdout, &mut buffer).await {
                    Ok(0) => {
                        finalize_format_at_eof(&mut detector, format_determined, &writer, debug)
                            .await?;
                        break;
                    }
                    Ok(n) => n,
                    Err(e) => {
                        let _ =
                            error_tx_clone.send(format!("Error reading from decompressor: {}", e));
                        return Err(e);
                    }
                };

                if decompressed_progress_tx.send(n as u64).is_err() {
                    break;
                }

                process_buffer_with_format_detection(
                    &buffer,
                    n,
                    &mut detector,
                    &mut parser,
                    &mut format_determined,
                    &writer,
                    debug,
                )
                .await
                .map_err(|e| {
                    let _ = error_tx_clone.send(format!("Write pipeline error: {}", e));
                    e
                })?;
            }
            writer.close().await
        })
    };

    // Spawn stderr reader for decompressor
    tokio::spawn(spawn_stderr_reader(
        decompressor_stderr,
        error_tx.clone(),
        decompressor_name,
    ));

    // Spawn error processor
    let error_processor = tokio::spawn(process_error_messages(error_rx));

    // Spawn task: tar channel -> decompressor stdin
    let decompressor_writer_handle = tokio::spawn(async move {
        while let Some(chunk) = tar_rx.recv().await {
            let chunk_len = chunk.len() as u64;
            if let Err(e) = decompressor_stdin.write_all(&chunk).await {
                return Err(format!("Error writing to decompressor: {}", e));
            }
            // Notify that bytes were written to decompressor (for progress bar)
            let _ = decompressor_written_progress_tx.send(chunk_len);
        }
        // Close stdin to signal EOF
        drop(decompressor_stdin);
        Ok::<(), String>(())
    });

    Ok(TarPipelineComponents {
        http_tx,
        http_rx,
        tar_tx,
        decompressed_progress_rx,
        written_progress_rx,
        decompressor_written_progress_rx,
        writer_handle,
        decompressor_writer_handle,
        error_processor,
        decompressor,
        decompressor_name,
    })
}

/// Coordinate the download process with progress tracking and cleanup
async fn coordinate_download_and_processing(
    mut stream: impl futures_util::Stream<Item = reqwest::Result<bytes::Bytes>> + std::marker::Unpin,
    context: DownloadContext,
    mut params: DownloadCoordinationParams,
    handles: ProcessingHandles,
    mut decompressor: tokio::process::Child,
    options: &OciOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    // Get the buffer size before moving it
    let detection_buffer_size = context.content_detection_buffer.len() as u64;

    // Send the already-downloaded detection buffer first
    if params
        .http_tx
        .send(bytes::Bytes::from(context.content_detection_buffer))
        .await
        .is_err()
    {
        return Err("Failed to send detection buffer to tar extractor".into());
    }

    // Main download loop
    let mut progress =
        ProgressTracker::new(options.common.newline_progress, options.common.show_memory);
    progress.set_content_length(context.content_length);
    progress.set_is_compressed(context.decompressor_name != "cat");
    progress.bytes_received = detection_buffer_size; // Account for detection buffer
    let update_interval = Duration::from_secs_f64(options.common.progress_interval_secs);
    let debug = options.common.debug;

    // Download and send chunks (using stream from earlier)
    let mut chunk_count = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(chunk_result)) => {
                match chunk_result {
                    Ok(chunk) => {
                        let chunk_len = chunk.len() as u64;
                        progress.bytes_received += chunk_len;
                        chunk_count += 1;

                        if debug && chunk_count <= 5 {
                            eprintln!(
                                "[DEBUG] Received chunk {}: {} bytes (total: {} MB)",
                                chunk_count,
                                chunk_len,
                                progress.bytes_received / (1024 * 1024)
                            );
                        }

                        if params.http_tx.send(chunk).await.is_err() {
                            eprintln!("\nTar extractor channel closed");
                            if debug {
                                eprintln!(
                                    "[DEBUG] Channel closed after {} chunks, {} bytes",
                                    chunk_count, progress.bytes_received
                                );
                            }
                            break;
                        }

                        // Update progress
                        while let Ok(byte_count) = params.decompressed_progress_rx.try_recv() {
                            progress.bytes_decompressed += byte_count;
                        }
                        // Track bytes actually written to decompressor (for progress bar)
                        while let Ok(written_len) =
                            params.decompressor_written_progress_rx.try_recv()
                        {
                            progress.bytes_sent_to_decompressor += written_len;
                        }
                        while let Ok(written_bytes) = params.written_progress_rx.try_recv() {
                            progress.bytes_written = written_bytes;
                        }

                        if let Err(e) =
                            progress.update_progress(context.content_length, update_interval, false)
                        {
                            eprintln!();
                            return Err(e);
                        }
                    }
                    Err(e) => {
                        return Err(format!("Download error: {}", e).into());
                    }
                }
            }
            Ok(None) => {
                // Stream ended
                break;
            }
            Err(_) => {
                return Err("Download timeout".into());
            }
        }
    }

    // Close HTTP channel to signal download complete
    drop(params.http_tx);

    progress.download_duration = Some(progress.start_time.elapsed());

    if debug {
        eprintln!(
            "[DEBUG] Download completed, {} bytes received",
            progress.bytes_received
        );
    }

    // Wait for tar extractor (now only handles tar archives)
    let tar_result = handles.tar_extractor_handle.await?;
    if let Err(e) = tar_result {
        return Err(format!("Tar extraction failed: {}", e).into());
    }

    // Wait for decompressor writer
    if let Err(e) = handles.decompressor_writer_handle.await? {
        return Err(format!("Decompressor write failed: {}", e).into());
    }

    // Wait for decompressor process
    loop {
        while let Ok(byte_count) = params.decompressed_progress_rx.try_recv() {
            progress.bytes_decompressed += byte_count;
        }
        while let Ok(written_bytes) = params.written_progress_rx.try_recv() {
            progress.bytes_written = written_bytes;
        }
        let _ = progress.update_progress(None, update_interval, false);

        match decompressor.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    return Err(format!(
                        "{} failed with status: {:?}",
                        context.decompressor_name,
                        status.code()
                    )
                    .into());
                }
                break;
            }
            Ok(None) => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }

    progress.decompress_duration = Some(progress.start_time.elapsed());

    // Wait for block writer
    loop {
        while let Ok(written_bytes) = params.written_progress_rx.try_recv() {
            progress.bytes_written = written_bytes;
        }
        let _ = progress.update_progress(None, update_interval, false);

        if handles.writer_handle.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    match handles.writer_handle.await {
        Ok(Ok(final_bytes)) => {
            progress.bytes_written = final_bytes;
        }
        Ok(Err(e)) => return Err(Box::new(e)),
        Err(e) => return Err(e.into()),
    }

    progress.write_duration = Some(progress.start_time.elapsed());

    // Final progress update
    let _ = progress.update_progress(None, update_interval, true);

    // Wait for error processor
    let _ = tokio::time::timeout(Duration::from_secs(2), handles.error_processor).await;

    // Print final stats
    progress.print_final_stats();

    Ok(())
}

/// Setup external decompressor pipeline for XZ compression
async fn setup_external_decompressor_pipeline(
    http_rx: ByteBoundedReceiver<bytes::Bytes>,
    block_writer: AsyncBlockWriter,
    decompressed_progress_tx: mpsc::UnboundedSender<u64>,
    debug: bool,
) -> Result<ExternalDecompressorPipeline, Box<dyn std::error::Error>> {
    // XZ: Use external xzcat process
    let (mut decompressor, decompressor_name) = start_decompressor_process("disk.img.xz").await?;

    let decompressor_stdin = decompressor.stdin.take().unwrap();
    let decompressor_stdout = decompressor.stdout.take().unwrap();
    let decompressor_stderr = decompressor.stderr.take().unwrap();

    let (error_tx, error_rx) = mpsc::unbounded_channel::<String>();

    // Spawn stderr reader
    tokio::spawn(spawn_stderr_reader(
        decompressor_stderr,
        error_tx.clone(),
        decompressor_name,
    ));

    // Spawn error processor
    tokio::spawn(process_error_messages(error_rx));

    // Spawn blocking task: read from channel and write to xzcat stdin
    // First, create a sync file handle from the async stdin
    #[cfg(unix)]
    let stdin_fd = {
        use std::os::unix::io::{AsRawFd, FromRawFd};
        let raw_fd = decompressor_stdin.as_raw_fd();
        // Duplicate the fd so we can use it in blocking context
        let dup_fd = unsafe { libc::dup(raw_fd) };
        if dup_fd == -1 {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: dup_fd is a valid file descriptor (we checked above)
        unsafe { std::fs::File::from_raw_fd(dup_fd) }
    };
    #[cfg(not(unix))]
    let stdin_fd: std::fs::File = {
        return Err("XZ streaming decompression is not supported on non-unix platforms".into());
    };

    // Drop the original async stdin (the dup'd fd still points to the pipe)
    drop(decompressor_stdin);

    let stdin_writer_handle = {
        tokio::task::spawn_blocking(move || {
            use std::io::Write as _;
            let reader = ChannelReader::new_byte_bounded(http_rx);
            let mut reader = reader;
            let mut stdin = stdin_fd;
            let mut buffer = vec![0u8; 1024 * 1024]; // 1MB chunks

            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => break, // EOF
                    Ok(n) => {
                        if let Err(e) = stdin.write_all(&buffer[..n]) {
                            return Err(format!("Error writing to xzcat: {}", e));
                        }
                    }
                    Err(e) => return Err(format!("Error reading stream: {}", e)),
                }
            }

            drop(stdin);
            Ok::<(), String>(())
        })
    };

    // Spawn task: xzcat stdout -> block writer with sparse detection
    let writer = block_writer;
    let progress_tx = decompressed_progress_tx;
    let writer_handle = tokio::spawn(async move {
        let mut stdout = decompressor_stdout;
        let mut buffer = vec![0u8; 8 * 1024 * 1024]; // 8MB buffer

        // Auto-detect sparse image format from initial data
        let mut detector = FormatDetector::new();
        let mut parser: Option<SparseParser> = None;
        let mut format_determined = false;

        loop {
            let n = match tokio::io::AsyncReadExt::read(&mut stdout, &mut buffer).await {
                Ok(0) => {
                    finalize_format_at_eof(&mut detector, format_determined, &writer, debug)
                        .await?;
                    break;
                }
                Ok(n) => n,
                Err(e) => return Err(e),
            };

            let _ = progress_tx.send(n as u64);

            process_buffer_with_format_detection(
                &buffer,
                n,
                &mut detector,
                &mut parser,
                &mut format_determined,
                &writer,
                debug,
            )
            .await?;
        }

        // Wait for stdin writer to finish and propagate any errors
        stdin_writer_handle
            .await
            .map_err(|e| std::io::Error::other(format!("Stdin writer task failed: {}", e)))?
            .map_err(|e| std::io::Error::other(format!("Stdin writer error: {}", e)))?;

        writer.close().await
    });

    Ok(ExternalDecompressorPipeline {
        writer_handle,
        decompressor,
    })
}

/// Setup in-process decompression pipeline for Gzip or None compression
async fn setup_inprocess_decompression_pipeline(
    http_rx: ByteBoundedReceiver<bytes::Bytes>,
    block_writer: AsyncBlockWriter,
    decompressed_progress_tx: mpsc::UnboundedSender<u64>,
    compression_type: Compression,
    debug: bool,
) -> Result<tokio::task::JoinHandle<Result<u64, std::io::Error>>, Box<dyn std::error::Error>> {
    // Gzip or None: decompress in-process and write directly to block writer
    let writer = block_writer;
    let progress_tx = decompressed_progress_tx;

    // Create an async channel for decompressed data
    let (data_tx, mut data_rx) = mpsc::channel::<Vec<u8>>(16);

    // Spawn blocking task: read, decompress, send to async channel
    let reader_handle = tokio::task::spawn_blocking(move || {
        let reader = ChannelReader::new_byte_bounded(http_rx);

        // Apply in-process gzip decompression if needed
        let processed_reader: Box<dyn std::io::Read + Send> = match compression_type {
            Compression::Gzip => {
                if debug {
                    eprintln!("[DEBUG] Applying in-process gzip decompression");
                }
                Box::new(flate2::read::GzDecoder::new(reader))
            }
            _ => Box::new(reader),
        };

        let mut reader = processed_reader;
        let mut buffer = vec![0u8; 1024 * 1024]; // 1MB chunks

        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break, // EOF
                Ok(n) => {
                    // Use blocking_send for proper backpressure handling
                    // This blocks the thread until space is available, avoiding deadlocks
                    // that could occur with block_on, while still providing backpressure
                    data_tx
                        .blocking_send(buffer[..n].to_vec())
                        .map_err(|_| "Data channel closed")?;
                }
                Err(e) => return Err(format!("Error reading/decompressing: {}", e)),
            }
        }

        Ok::<(), String>(())
    });

    // Spawn async task: receive from channel and write to block writer with sparse detection
    let handle = tokio::spawn(async move {
        // Auto-detect sparse image format from initial data
        let mut detector = FormatDetector::new();
        let mut parser: Option<SparseParser> = None;
        let mut format_determined = false;

        while let Some(data) = data_rx.recv().await {
            let len = data.len();
            let _ = progress_tx.send(len as u64);

            process_buffer_with_format_detection(
                &data,
                len,
                &mut detector,
                &mut parser,
                &mut format_determined,
                &writer,
                debug,
            )
            .await?;
        }

        // Handle EOF with incomplete format detection
        finalize_format_at_eof(&mut detector, format_determined, &writer, debug).await?;

        // Wait for reader and propagate any errors
        match reader_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                return Err(std::io::Error::other(format!(
                    "Reader/decompression error: {}",
                    e
                )));
            }
            Err(e) => {
                return Err(std::io::Error::other(format!("Reader task failed: {}", e)));
            }
        }

        writer.close().await
    });

    Ok(handle)
}

/// Coordinate raw disk image download with progress tracking and cleanup
async fn coordinate_raw_disk_download(
    mut stream: impl futures_util::Stream<Item = reqwest::Result<bytes::Bytes>> + std::marker::Unpin,
    initial_buffer: Vec<u8>,
    layer_size: u64,
    mut params: RawDiskDownloadParams,
    options: &OciOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create progress tracking
    let mut progress =
        ProgressTracker::new(options.common.newline_progress, options.common.show_memory);
    progress.set_content_length(Some(layer_size));
    progress.set_is_compressed(params.external_decompressor.is_some());
    progress.bytes_received = initial_buffer.len() as u64;
    let update_interval = Duration::from_secs_f64(options.common.progress_interval_secs);

    // Send the already-downloaded detection buffer first
    if params
        .http_tx
        .send(bytes::Bytes::from(initial_buffer))
        .await
        .is_err()
    {
        return Err("Failed to send detection buffer to streaming pipeline".into());
    }

    // Main download loop - stream chunks to processing pipeline
    let mut chunk_count = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(chunk_result)) => {
                match chunk_result {
                    Ok(chunk) => {
                        let chunk_len = chunk.len() as u64;
                        progress.bytes_received += chunk_len;
                        chunk_count += 1;

                        if options.common.debug && chunk_count <= 5 {
                            eprintln!(
                                "[DEBUG] Received chunk {}: {} bytes (total: {} MB)",
                                chunk_count,
                                chunk_len,
                                progress.bytes_received / (1024 * 1024)
                            );
                        }

                        if params.http_tx.send(chunk).await.is_err() {
                            eprintln!("\nStreaming pipeline closed");
                            break;
                        }

                        // Update progress
                        while let Ok(byte_count) = params.decompressed_progress_rx.try_recv() {
                            progress.bytes_decompressed += byte_count;
                        }
                        while let Ok(written_bytes) = params.raw_written_progress_rx.try_recv() {
                            progress.bytes_written = written_bytes;
                        }
                        // For raw disk images, bytes_sent_to_decompressor tracks bytes_received
                        // since data is immediately forwarded to the decompression pipeline
                        progress.bytes_sent_to_decompressor = progress.bytes_received;

                        if let Err(e) =
                            progress.update_progress(Some(layer_size), update_interval, false)
                        {
                            eprintln!();
                            return Err(e);
                        }
                    }
                    Err(e) => {
                        return Err(format!("Download error: {}", e).into());
                    }
                }
            }
            Ok(None) => {
                // Stream ended
                break;
            }
            Err(_) => {
                return Err("Download timeout".into());
            }
        }
    }

    // Close HTTP channel to signal download complete
    drop(params.http_tx);

    progress.download_duration = Some(progress.start_time.elapsed());

    if options.common.debug {
        eprintln!(
            "[DEBUG] Download completed, {} bytes received",
            progress.bytes_received
        );
    }

    // Wait for the processing pipeline to complete
    loop {
        // Update progress from decompression and writes
        while let Ok(byte_count) = params.decompressed_progress_rx.try_recv() {
            progress.bytes_decompressed += byte_count;
        }
        while let Ok(written_bytes) = params.raw_written_progress_rx.try_recv() {
            progress.bytes_written = written_bytes;
        }
        let _ = progress.update_progress(Some(layer_size), update_interval, false);

        if params.writer_handle.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Get final result
    match params.writer_handle.await {
        Ok(Ok(final_bytes)) => {
            progress.bytes_written = final_bytes;
            progress.bytes_decompressed = final_bytes;
        }
        Ok(Err(e)) => return Err(Box::new(e)),
        Err(e) => return Err(e.into()),
    }

    let elapsed = progress.start_time.elapsed();
    progress.decompress_duration = Some(elapsed);
    progress.write_duration = Some(elapsed);

    // Wait for external decompressor process and check exit status
    if let Some(mut decompressor) = params.external_decompressor {
        match decompressor.wait().await {
            Ok(status) => {
                if !status.success() {
                    return Err(format!(
                        "Decompressor process failed with exit code: {:?}",
                        status.code()
                    )
                    .into());
                }
            }
            Err(e) => {
                return Err(format!("Failed to wait for decompressor process: {}", e).into());
            }
        }
    }

    // Final progress update
    let _ = progress.update_progress(Some(layer_size), update_interval, true);

    // Print final stats
    progress.print_final_stats();

    Ok(())
}

/// Flash a block device from an OCI image
pub async fn flash_from_oci(
    image: &str,
    options: OciOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    // Parse image reference
    let image_ref = ImageReference::parse(image)?;
    println!("Pulling image: {}", image_ref);

    // Create registry client and authenticate
    let mut client = RegistryClient::new(image_ref.clone(), &options).await?;
    println!("Connecting to registry: {}", image_ref.registry);
    client.authenticate().await?;

    // Resolve manifest (handling multi-platform indexes)
    let manifest = resolve_manifest(&mut client).await?;

    // Get the layer to download
    let layer = manifest.get_single_layer()?;
    let layer_size = layer.size;
    let compression = layer.compression();

    println!("Layer digest: {}", layer.digest);
    println!(
        "Layer size: {} bytes ({:.2} MB)",
        layer_size,
        layer_size as f64 / (1024.0 * 1024.0)
    );
    println!("Layer media type: {}", layer.media_type);
    println!("Layer compression: {:?}", compression);

    ensure_supported_layer_compression(compression, &layer.media_type)?;

    // Start blob download
    println!("\nStarting download...");
    let response = client.get_blob_stream(&layer.digest).await?;
    let content_length = response.content_length();

    // We'll detect actual compression from the data stream since
    // some registries don't set the media type correctly

    // First, do a small download to detect content type before setting up pipeline
    println!("\nDetecting content type...");
    let mut content_detection_buffer = Vec::new();
    let detection_size = 2 * 1024 * 1024; // 2MB should be enough for detection

    let mut stream = response.bytes_stream();
    while content_detection_buffer.len() < detection_size {
        match tokio::time::timeout(Duration::from_secs(30), stream.next()).await {
            Ok(Some(chunk_result)) => match chunk_result {
                Ok(chunk) => {
                    content_detection_buffer.extend_from_slice(&chunk);
                }
                Err(e) => {
                    return Err(format!("Download error during detection: {}", e).into());
                }
            },
            Ok(None) => break, // Stream ended early
            Err(_) => return Err("Detection timeout".into()),
        }
    }

    // Detect content and compression type
    let (content_type, compression_type) =
        detect_content_and_compression(&content_detection_buffer, options.common.debug)?;

    if options.common.debug {
        eprintln!(
            "[DEBUG] Detected content: {:?}, compression: {:?}",
            content_type, compression_type
        );
    }

    // Handle raw disk images with a separate, simpler pipeline
    if content_type == ContentType::RawDiskImage {
        return flash_raw_disk_image_directly(
            content_detection_buffer,
            stream,
            compression_type,
            options,
            layer_size,
        )
        .await;
    }

    // Handle XZ-compressed data that is NOT a tar archive - bypass tar extraction
    if compression_type == Compression::Xz && content_type != ContentType::TarArchive {
        if options.common.debug {
            eprintln!("[DEBUG] XZ-compressed layer detected - bypassing tar extraction, passing directly to main pipeline");
        }
        return flash_raw_disk_image_directly(
            content_detection_buffer,
            stream,
            compression_type,
            options,
            layer_size,
        )
        .await;
    }

    // For tar archives, continue with the complex pipeline
    println!("Processing tar archive...");

    // Set up buffer capacity for the streaming pipeline
    let buffer_size_mb = options.common.buffer_size_mb;
    let avg_chunk_size_kb = 16;
    let mut buffer_capacity = (buffer_size_mb * 1024) / avg_chunk_size_kb;
    buffer_capacity = buffer_capacity.max(1000);

    // For compressed layers, use much larger buffering to ensure gzip decoder gets enough data
    if compression != LayerCompression::None {
        buffer_capacity = buffer_capacity.max(10000); // Ensure substantial buffering for compression
        if options.common.debug {
            eprintln!(
                "[DEBUG] Using enhanced buffering for compressed layer: {} chunks",
                buffer_capacity
            );
        }
    }

    // Setup the complete tar processing pipeline
    let pipeline = setup_tar_processing_pipeline(
        content_type.clone(),
        compression,
        compression_type,
        &options,
        buffer_size_mb,
        buffer_capacity,
    )
    .await?;

    // Extract components for use in the download loop
    let TarPipelineComponents {
        http_tx,
        http_rx,
        tar_tx,
        decompressed_progress_rx,
        written_progress_rx,
        decompressor_written_progress_rx,
        writer_handle,
        decompressor_writer_handle,
        error_processor,
        decompressor,
        decompressor_name,
    } = pipeline;

    // Spawn blocking task: HTTP rx -> gzip -> tar -> tar tx
    let file_pattern = options.file_pattern.clone();
    let debug = options.common.debug;
    let tar_extractor_handle = tokio::task::spawn_blocking(move || {
        extract_tar_archive_from_stream(
            http_rx,
            tar_tx,
            file_pattern.as_deref(),
            compression,
            compression_type,
            debug,
        )
    });

    // Coordinate download processing and cleanup
    let context = DownloadContext {
        content_detection_buffer,
        content_length,
        decompressor_name,
    };

    let params = DownloadCoordinationParams {
        http_tx,
        decompressed_progress_rx,
        written_progress_rx,
        decompressor_written_progress_rx,
    };

    let handles = ProcessingHandles {
        writer_handle,
        decompressor_writer_handle,
        error_processor,
        tar_extractor_handle,
    };

    coordinate_download_and_processing(stream, context, params, handles, decompressor, &options)
        .await
}

/// Common implementation for tar stream extraction
fn extract_tar_stream_impl<R: Read + Send>(
    reader: R,
    tar_tx: mpsc::Sender<Vec<u8>>,
    file_pattern: Option<&str>,
    debug: bool,
) -> Result<(), String> {
    if debug {
        eprintln!("[DEBUG] Tar extractor starting");
    }
    // Create a reader that auto-detects compression from magic bytes
    // Add substantial buffering for streaming gzip decompression
    let buffered_reader = std::io::BufReader::with_capacity(1024 * 1024, reader); // 1MB buffer
    let mut magic_reader = MagicDetectingReader::new(buffered_reader, debug);

    // Read first few bytes to detect compression
    let mut magic_buf = [0u8; 4];
    magic_reader
        .peek_bytes(&mut magic_buf)
        .map_err(|e| format!("Failed to peek magic bytes: {}", e))?;

    let is_gzipped = magic_buf[0] == 0x1f && magic_buf[1] == 0x8b;
    let is_xz = magic_buf[0] == 0xfd
        && magic_buf[1] == 0x37
        && magic_buf[2] == 0x7a
        && magic_buf[3] == 0x58;

    let reader: Box<dyn Read + Send> = if is_gzipped {
        if debug {
            eprintln!("[DEBUG] Auto-detected gzip compression from magic bytes");
            // Save first 1MB of raw gzip data for debugging
            let tee_reader = TeeReader::new(magic_reader, "/tmp/debug_gzip_data.gz", 1024 * 1024);
            let gz_decoder = GzDecoder::new(tee_reader);
            Box::new(DebugGzReader::new(gz_decoder, debug))
        } else {
            let gz_decoder = GzDecoder::new(magic_reader);
            Box::new(gz_decoder)
        }
    } else if is_xz {
        if debug {
            eprintln!("[DEBUG] Auto-detected XZ compression from magic bytes - decompressing for tar extraction");
        }
        // Decompress XZ before tar extraction (like gzip)
        let xz_decoder = XzDecoder::new(magic_reader);
        Box::new(xz_decoder)
    } else {
        if debug {
            eprintln!("[DEBUG] No compression detected, treating as raw tar");
        }
        Box::new(magic_reader)
    };

    // Wrap with debug reader if debug mode
    let reader: Box<dyn Read + Send> = if debug {
        Box::new(DebugReader::new(reader, debug))
    } else {
        reader
    };

    // Parse as tar archive (content detection already done by caller)
    let mut archive = tar::Archive::new(reader);

    if debug {
        eprintln!("[DEBUG] Starting tar archive parsing");
    }

    // Find and extract the disk image
    for entry_result in archive.entries().map_err(|e| format!("Tar error: {}", e))? {
        let mut entry = entry_result.map_err(|e| format!("Tar entry error: {}", e))?;

        let path = entry
            .path()
            .map_err(|e| format!("Invalid path: {}", e))?
            .to_path_buf();

        let size = entry.size();

        if debug {
            eprintln!("[DEBUG] Tar entry: {:?} ({} bytes)", path, size);
        }

        // Check if this is the disk image we're looking for
        if is_disk_image(&path, file_pattern) {
            println!("Found disk image: {:?} ({} bytes)", path, size);

            // Stream entry contents
            let mut buffer = vec![0u8; 8 * 1024 * 1024]; // 8MB chunks
            loop {
                let n = entry
                    .read(&mut buffer)
                    .map_err(|e| format!("Read error: {}", e))?;
                if n == 0 {
                    break;
                }
                tar_tx
                    .blocking_send(buffer[..n].to_vec())
                    .map_err(|_| "Channel closed")?;
            }

            // Done - we only extract one file
            return Ok(());
        }
    }

    Err("No disk image found in tar archive".to_string())
}

/// Determine the appropriate decompressor hint based on content and compression types.
///
/// Returns a file pattern string that `start_decompressor_process` uses to select
/// the appropriate decompressor command (e.g., "disk.img" -> cat, "disk.img.xz" -> xzcat).
fn get_decompressor_hint(
    content_type: ContentType,
    _layer_compression: LayerCompression,
    content_compression: Compression,
    file_pattern: Option<&str>,
) -> &'static str {
    // Priority 1: If user specified a file pattern, use its extension
    // This handles cases like gzip-compressed tar containing xz-compressed disk images
    if let Some(p) = file_pattern {
        if p.ends_with(".xz") {
            return "disk.img.xz";
        }
        if p.ends_with(".gz") {
            return "disk.img.gz";
        }
        // Pattern specified but no compression extension - assume uncompressed
        return "disk.img";
    }

    // Priority 2: For tar archives, use content compression to determine
    // what decompressor the extracted file needs
    if content_type == ContentType::TarArchive {
        return match content_compression {
            Compression::Xz => "disk.img.xz",
            Compression::Gzip => "disk.img.gz",
            Compression::Zstd => "disk.img", // Zstd not supported for file-level decompression
            Compression::None => "disk.img",
        };
    }

    // Default: assume XZ compressed disk image (conservative choice)
    "disk.img.xz"
}

/// Check if a path matches a disk image
fn is_disk_image(path: &Path, pattern: Option<&str>) -> bool {
    let Some(os_name) = path.file_name() else {
        return false;
    };
    let name = os_name.to_string_lossy();

    // Skip hidden files
    if name.starts_with('.') {
        return false;
    }

    // If pattern specified, use it
    if let Some(pattern) = pattern {
        return matches_pattern(&name, pattern);
    }

    // Default: look for common disk image extensions
    const DISK_IMAGE_EXTENSIONS: &[&str] = &[
        ".img.xz", ".img.gz", ".img.bz2", ".raw.xz", ".raw.gz", ".raw.bz2", ".xz", ".gz", ".bz2",
        ".img", ".raw",
    ];

    DISK_IMAGE_EXTENSIONS.iter().any(|ext| name.ends_with(ext))
}

/// Simple glob pattern matching supporting `*.ext`, `prefix*`, or exact match.
///
/// Does not support wildcards in the middle of patterns (e.g., `disk*.img`).
fn matches_pattern(name: &str, pattern: &str) -> bool {
    if let Some(suffix) = pattern.strip_prefix('*') {
        name.ends_with(suffix)
    } else if let Some(prefix) = pattern.strip_suffix('*') {
        name.starts_with(prefix)
    } else {
        name == pattern
    }
}

/// Reader that can peek at magic bytes without consuming them
struct MagicDetectingReader<R: Read> {
    inner: R,
    peeked: Vec<u8>,
    peek_pos: usize,
    debug: bool,
}

impl<R: Read> MagicDetectingReader<R> {
    fn new(inner: R, debug: bool) -> Self {
        Self {
            inner,
            peeked: Vec::new(),
            peek_pos: 0,
            debug,
        }
    }

    fn peek_bytes(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        if self.debug {
            eprintln!(
                "[DEBUG] MagicDetectingReader::peek_bytes called for {} bytes",
                buf.len()
            );
            eprintln!(
                "[DEBUG]   Current peeked.len(): {}, peek_pos: {}",
                self.peeked.len(),
                self.peek_pos
            );
        }

        // Read enough bytes to fill the peek buffer
        while self.peeked.len() < buf.len() {
            let mut temp_buf = [0u8; 1024];
            let n = self.inner.read(&mut temp_buf)?;
            if self.debug {
                eprintln!("[DEBUG]   Read {} bytes from inner reader", n);
            }
            if n == 0 {
                break; // EOF
            }
            if n > 0 && self.debug {
                eprintln!(
                    "[DEBUG]   First 8 bytes read: {:02x?}",
                    &temp_buf[..n.min(8)]
                );
            }
            self.peeked.extend_from_slice(&temp_buf[..n]);
        }

        // Copy the requested bytes
        let to_copy = buf.len().min(self.peeked.len());
        buf[..to_copy].copy_from_slice(&self.peeked[..to_copy]);
        if self.debug {
            eprintln!(
                "[DEBUG]   Copying {} bytes to peek buffer: {:02x?}",
                to_copy,
                &self.peeked[..to_copy]
            );
            eprintln!(
                "[DEBUG]   Total peeked buffer now: {} bytes",
                self.peeked.len()
            );
        }
        Ok(())
    }
}

impl<R: Read> Read for MagicDetectingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.debug {
            eprintln!(
                "[DEBUG] MagicDetectingReader::read called for {} bytes",
                buf.len()
            );
            eprintln!(
                "[DEBUG]   peek_pos: {}, peeked.len(): {}",
                self.peek_pos,
                self.peeked.len()
            );
        }

        // First, drain any peeked bytes
        if self.peek_pos < self.peeked.len() {
            let available = self.peeked.len() - self.peek_pos;
            let to_copy = buf.len().min(available);
            buf[..to_copy].copy_from_slice(&self.peeked[self.peek_pos..self.peek_pos + to_copy]);
            self.peek_pos += to_copy;
            if self.debug {
                eprintln!(
                    "[DEBUG]   Returning {} peeked bytes: {:02x?}",
                    to_copy,
                    &buf[..to_copy.min(8)]
                );
            }
            return Ok(to_copy);
        }

        // No more peeked data, read directly from inner
        let n = self.inner.read(buf)?;
        if self.debug {
            if n > 0 {
                eprintln!(
                    "[DEBUG]   Read {} bytes directly from inner: {:02x?}",
                    n,
                    &buf[..n.min(8)]
                );
            } else {
                eprintln!("[DEBUG]   EOF from inner reader");
            }
        }
        Ok(n)
    }
}

/// Reader that logs the first few bytes for debugging
struct DebugReader<R: Read> {
    inner: R,
    logged_first: bool,
    debug: bool,
}

/// Reader that saves data to file while passing it through (for debugging)
struct TeeReader<R: Read> {
    inner: R,
    file: Option<std::fs::File>,
    bytes_written: usize,
    max_bytes: usize,
}

impl<R: Read> TeeReader<R> {
    fn new(inner: R, path: &str, max_bytes: usize) -> Self {
        let file = match std::fs::File::create(path) {
            Ok(f) => {
                eprintln!("[DEBUG] TeeReader: Saving raw gzip data to {}", path);
                Some(f)
            }
            Err(e) => {
                eprintln!("[DEBUG] TeeReader: Failed to create {}: {}", path, e);
                None
            }
        };

        Self {
            inner,
            file,
            bytes_written: 0,
            max_bytes,
        }
    }
}

impl<R: Read> Read for TeeReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;

        // Save data to file if still under limit
        if n > 0 && self.bytes_written < self.max_bytes {
            if let Some(ref mut file) = self.file {
                let to_write = (self.max_bytes - self.bytes_written).min(n);
                if let Err(e) = file.write_all(&buf[..to_write]) {
                    eprintln!("[DEBUG] TeeReader: Failed to write to debug file: {}", e);
                    self.file = None;
                } else {
                    self.bytes_written += to_write;
                    if self.bytes_written >= self.max_bytes {
                        eprintln!(
                            "[DEBUG] TeeReader: Finished saving {} bytes to debug file",
                            self.bytes_written
                        );
                        self.file = None;
                    }
                }
            }
        }

        Ok(n)
    }
}

/// Reader that specifically debugs gzip decoder output
struct DebugGzReader<R: Read> {
    inner: R,
    bytes_read: u64,
    last_log: u64,
    debug: bool,
}

impl<R: Read> DebugGzReader<R> {
    fn new(inner: R, debug: bool) -> Self {
        if debug {
            eprintln!("[DEBUG] DebugGzReader created");
        }
        Self {
            inner,
            bytes_read: 0,
            last_log: 0,
            debug,
        }
    }
}

impl<R: Read> Read for DebugGzReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.inner.read(buf) {
            Ok(n) => {
                if n == 0 {
                    if self.debug {
                        eprintln!(
                            "[DEBUG] DebugGzReader: EOF reached after {} bytes",
                            self.bytes_read
                        );
                    }
                    return Ok(0);
                }

                self.bytes_read += n as u64;

                // Log every 10MB or first read
                if self.debug
                    && (self.bytes_read - self.last_log >= 10 * 1024 * 1024 || self.last_log == 0)
                {
                    eprintln!(
                        "[DEBUG] DebugGzReader: Read {} bytes (total: {} MB)",
                        n,
                        self.bytes_read / (1024 * 1024)
                    );
                    eprintln!(
                        "[DEBUG] DebugGzReader: First 8 bytes: {:02x?}",
                        &buf[..n.min(8)]
                    );

                    // Check if we're getting all zeros
                    let all_zeros = buf[..n].iter().all(|&b| b == 0);
                    if all_zeros {
                        eprintln!("[DEBUG] DebugGzReader: *** WARNING: All bytes are zero! ***");
                        eprintln!(
                            "[DEBUG] DebugGzReader: This suggests gzip decompression is failing!"
                        );
                    }

                    self.last_log = self.bytes_read;
                }

                Ok(n)
            }
            Err(e) => {
                if self.debug {
                    eprintln!(
                        "[DEBUG] DebugGzReader: ERROR during read: {} (after {} bytes)",
                        e, self.bytes_read
                    );
                }
                Err(e)
            }
        }
    }
}

impl<R: Read> DebugReader<R> {
    fn new(inner: R, debug: bool) -> Self {
        Self {
            inner,
            logged_first: false,
            debug,
        }
    }
}

impl<R: Read> Read for DebugReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.debug {
            eprintln!("[DEBUG] DebugReader::read called for {} bytes", buf.len());
        }
        let n = self.inner.read(buf)?;
        if self.debug {
            eprintln!("[DEBUG] DebugReader got {} bytes from inner", n);

            if !self.logged_first && n > 0 {
                eprintln!(
                    "[DEBUG] First 16 bytes after decompression: {:02x?}",
                    &buf[..n.min(16)]
                );
                self.logged_first = true;
            } else if n > 0 {
                eprintln!(
                    "[DEBUG] Subsequent read: first 8 bytes: {:02x?}",
                    &buf[..n.min(8)]
                );
            }
        }

        Ok(n)
    }
}

/// Flash raw disk image directly without tar extraction
async fn flash_raw_disk_image_directly(
    initial_buffer: Vec<u8>,
    stream: impl futures_util::Stream<Item = reqwest::Result<bytes::Bytes>> + std::marker::Unpin,
    compression_type: Compression,
    options: OciOptions,
    layer_size: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Disk image detected - streaming directly to device");
    println!("Compression: {:?}", compression_type);
    println!("Opening device: {}", options.common.device);

    // Create progress tracking
    let mut _progress =
        ProgressTracker::new(options.common.newline_progress, options.common.show_memory);
    _progress.set_content_length(Some(layer_size));
    _progress.set_is_compressed(compression_type != Compression::None);
    let _update_interval = Duration::from_secs_f64(options.common.progress_interval_secs);

    // Set up single-purpose block writer with its own progress channel
    let (raw_written_progress_tx, raw_written_progress_rx) = mpsc::unbounded_channel::<u64>();

    // Create block writer
    let block_writer = AsyncBlockWriter::new(
        options.common.device.clone(),
        raw_written_progress_tx,
        options.common.debug,
        options.common.o_direct,
        options.common.write_buffer_size_mb,
    )?;

    // Set up byte-bounded streaming pipeline
    let buffer_size_mb = options.common.buffer_size_mb;
    let max_buffer_bytes = buffer_size_mb * 1024 * 1024;
    let buffer_capacity = ((buffer_size_mb * 1024) / 16).max(1000); // item cap for mpsc

    let (http_tx, http_rx) =
        byte_bounded_channel::<bytes::Bytes>(max_buffer_bytes, buffer_capacity);
    let (decompressed_progress_tx, decompressed_progress_rx) = mpsc::unbounded_channel::<u64>();

    // For gzip and none, we can decompress in-process and write directly to block writer
    // For XZ, we need the external xzcat process
    let needs_external_decompressor = compression_type == Compression::Xz;

    if options.common.debug {
        eprintln!(
            "[DEBUG] Compression type: {:?}, using external decompressor: {}",
            compression_type, needs_external_decompressor
        );
    }

    // Spawn the processing pipeline based on compression type
    let (writer_handle, external_decompressor) = if needs_external_decompressor {
        // XZ: Use external xzcat process
        let pipeline = setup_external_decompressor_pipeline(
            http_rx,
            block_writer,
            decompressed_progress_tx,
            options.common.debug,
        )
        .await?;

        (pipeline.writer_handle, Some(pipeline.decompressor))
    } else {
        // Gzip or None: decompress in-process and write directly to block writer
        let handle = setup_inprocess_decompression_pipeline(
            http_rx,
            block_writer,
            decompressed_progress_tx,
            compression_type,
            options.common.debug,
        )
        .await?;

        (handle, None)
    };

    // Coordinate download and processing
    let params = RawDiskDownloadParams {
        http_tx,
        writer_handle,
        external_decompressor,
        decompressed_progress_rx,
        raw_written_progress_rx,
    };

    coordinate_raw_disk_download(stream, initial_buffer, layer_size, params, &options).await
}

/// Simple tar archive extraction without the complex buffering logic
fn extract_tar_archive_from_stream(
    http_rx: ByteBoundedReceiver<bytes::Bytes>,
    tar_tx: mpsc::Sender<Vec<u8>>,
    file_pattern: Option<&str>,
    compression: LayerCompression,
    compression_type: Compression,
    debug: bool,
) -> Result<(), String> {
    let reader = ChannelReader::new_byte_bounded(http_rx);

    // Handle layer compression before tar extraction
    // Use both manifest compression and content-detected compression
    let decompressed_reader: Box<dyn Read + Send> = match compression {
        LayerCompression::Gzip => {
            if debug {
                eprintln!("[DEBUG] Layer is gzip compressed (manifest), decompressing before tar extraction");
            }
            Box::new(GzDecoder::new(reader))
        }
        LayerCompression::Zstd => {
            return Err("Zstd layer compression is not supported yet".to_string());
        }
        LayerCompression::None => {
            // When manifest says no compression, use content detection result
            match compression_type {
                Compression::Gzip => {
                    if debug {
                        eprintln!("[DEBUG] Content is gzip compressed (detected), decompressing before tar extraction");
                    }
                    Box::new(GzDecoder::new(reader))
                }
                Compression::Xz => {
                    if debug {
                        eprintln!("[DEBUG] Content is XZ compressed layer (detected), will be decompressed during tar extraction");
                    }
                    // XZ decompression happens in extract_tar_stream_impl via magic byte detection
                    Box::new(reader)
                }
                Compression::None => {
                    if debug {
                        eprintln!("[DEBUG] No compression detected, processing tar directly");
                    }
                    Box::new(reader)
                }
                Compression::Zstd => {
                    return Err("Zstd content compression is not supported yet".to_string());
                }
            }
        }
    };

    extract_tar_stream_impl(decompressed_reader, tar_tx, file_pattern, debug)
}

#[cfg(test)]
mod tests {
    use super::sanitize_partition_name;

    #[test]
    fn sanitize_partition_name_accepts_valid() {
        let valid = ["boot_a", "system-a", "slot_1", "boot_a.1", "A1-b_c.2"];
        for name in valid {
            assert_eq!(sanitize_partition_name(name).unwrap(), name);
        }
    }

    #[test]
    fn sanitize_partition_name_rejects_empty() {
        let err = sanitize_partition_name("").unwrap_err();
        assert!(err.contains("empty"));
    }

    #[test]
    fn sanitize_partition_name_rejects_path_separators() {
        for name in ["boot/a", "boot\\a"] {
            let err = sanitize_partition_name(name).unwrap_err();
            assert!(err.contains("path separators"));
        }
    }

    #[test]
    fn sanitize_partition_name_rejects_parent_traversal() {
        for name in ["..", "../boot", "boot/..", "boot.."] {
            let err = sanitize_partition_name(name).unwrap_err();
            assert!(err.contains(".."));
        }
    }

    #[test]
    fn sanitize_partition_name_rejects_absolute_path() {
        let err = sanitize_partition_name("/tmp/boot").unwrap_err();
        assert!(err.contains("absolute path"));
    }

    #[test]
    fn sanitize_partition_name_rejects_invalid_chars() {
        for name in ["boot:1", "boot a", "boot@a"] {
            let err = sanitize_partition_name(name).unwrap_err();
            assert!(err.contains("invalid characters"));
        }
    }
}
