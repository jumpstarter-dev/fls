mod common;

use fls::fls::oci::extract_files_from_oci_image_to_dir;
use fls::{FlashOptions, OciOptions};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request as HyperRequest, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tar::Header;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// Spawn an HTTPS server that serves OCI registry endpoints: /v2/, manifest at manifest_path, blob at blob_path.
/// Returns (socket_addr, server_join_handle). Caller should abort the handle when done.
async fn spawn_oci_server(
    manifest_path: String,
    blob_path: String,
    manifest_body: Vec<u8>,
    blob_body: Vec<u8>,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let cert_dir = PathBuf::from("tests/test_certs");
    let server_cert_pem =
        fs::read_to_string(cert_dir.join("server-cert.pem")).expect("read server cert");
    let server_key_pem =
        fs::read_to_string(cert_dir.join("server-key.pem")).expect("read server key");
    let server_cert = rustls_pemfile::certs(&mut server_cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .expect("parse server cert");
    let server_key = rustls_pemfile::private_key(&mut server_key_pem.as_bytes())
        .expect("parse server key")
        .expect("missing server key");
    let mut server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(server_cert, server_key)
        .expect("build server config");
    server_config.alpn_protocols = vec![b"http/1.1".to_vec()];
    let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let manifest_body = Arc::new(manifest_body);
    let blob_body = Arc::new(blob_body);
    let manifest_path = Arc::new(manifest_path);
    let blob_path = Arc::new(blob_path);

    let handle = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let tls_acceptor = tls_acceptor.clone();
            let manifest_body = manifest_body.clone();
            let blob_body = blob_body.clone();
            let manifest_path = manifest_path.clone();
            let blob_path = blob_path.clone();

            tokio::spawn(async move {
                let tls_stream = tls_acceptor.accept(stream).await.unwrap();
                let io = TokioIo::new(tls_stream);
                let service = service_fn(move |req: HyperRequest<hyper::body::Incoming>| {
                    let manifest_body = manifest_body.clone();
                    let blob_body = blob_body.clone();
                    let manifest_path = manifest_path.clone();
                    let blob_path = blob_path.clone();
                    async move {
                        let path = req.uri().path();
                        let response = if path == "/v2/" {
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(Bytes::new()))
                                .unwrap()
                        } else if path == manifest_path.as_str() {
                            Response::builder()
                                .status(StatusCode::OK)
                                .header(
                                    "Content-Type",
                                    "application/vnd.oci.image.manifest.v1+json",
                                )
                                .body(Full::new(Bytes::copy_from_slice(&manifest_body)))
                                .unwrap()
                        } else if path == blob_path.as_str() {
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(Bytes::copy_from_slice(&blob_body)))
                                .unwrap()
                        } else {
                            Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .body(Full::new(Bytes::new()))
                                .unwrap()
                        };
                        Ok::<_, std::convert::Infallible>(response)
                    }
                });

                let _ = http1::Builder::new().serve_connection(io, service).await;
            });
        }
    });

    (local_addr, handle)
}

fn build_tar(entries: Vec<(&str, Vec<u8>)>) -> Vec<u8> {
    let mut tar_buf = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut tar_buf);
        for (name, data) in entries {
            let mut header = Header::new_gnu();
            header.set_mode(0o644);
            header.set_size(data.len() as u64);
            header.set_cksum();
            builder
                .append_data(&mut header, name, data.as_slice())
                .expect("append tar entry");
        }
        builder.finish().expect("finish tar");
    }
    tar_buf
}

const REPO: &str = "test/repo";
const TAG: &str = "latest";

fn default_options(cert_dir: &Path) -> OciOptions {
    OciOptions {
        common: FlashOptions {
            insecure_tls: false,
            cacert: Some(cert_dir.join("ca-cert.pem")),
            ..Default::default()
        },
        username: None,
        password: None,
        file_pattern: None,
    }
}

#[tokio::test]
async fn test_extract_files_from_oci_image_to_dir_tar_layer() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let boot_data = common::create_test_data(1024);
    let system_data = common::create_test_data(2048);
    let tar_bytes = build_tar(vec![
        ("boot_a.simg", boot_data.clone()),
        ("system_a.simg", system_data.clone()),
    ]);
    let blob_bytes = common::compress_gz(&tar_bytes);
    let blob_digest = "sha256:layer";

    let manifest_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:config",
            "size": 2
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": blob_digest,
                "size": blob_bytes.len()
            }
        ]
    })
    .to_string();

    let (local_addr, server_handle) = spawn_oci_server(
        format!("/v2/{}/manifests/{}", REPO, TAG),
        format!("/v2/{}/blobs/{}", REPO, blob_digest),
        manifest_json.into_bytes(),
        blob_bytes,
    )
    .await;

    let cert_dir = PathBuf::from("tests/test_certs");
    let host = format!("127.0.0.1.nip.io:{}", local_addr.port());
    let image_ref = format!("{}/{}:{}", host, REPO, TAG);
    let options = default_options(&cert_dir);

    let out_dir = tempdir().expect("create temp dir");
    let target_files: HashSet<String> = ["boot_a.simg".to_string(), "system_a.simg".to_string()]
        .into_iter()
        .collect();

    let result: Result<HashMap<String, PathBuf>, Box<dyn std::error::Error>> =
        extract_files_from_oci_image_to_dir(&image_ref, &target_files, &options, out_dir.path())
            .await;

    server_handle.abort();

    assert!(result.is_ok(), "Extraction failed: {:?}", result.err());

    let boot_path = out_dir.path().join("boot_a.simg");
    let system_path = out_dir.path().join("system_a.simg");
    let boot_written = fs::read(boot_path).expect("read boot output");
    let system_written = fs::read(system_path).expect("read system output");

    assert_eq!(boot_written, boot_data);
    assert_eq!(system_written, system_data);
}

/// Direct-file (non-tar) layer: media type without "tar", single target file, blob is raw gzip content.
#[tokio::test]
async fn test_extract_direct_file_layer() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let file_data = common::create_test_data(512);
    let blob_bytes = common::compress_gz(&file_data);
    let blob_digest = "sha256:direct";

    // Media type without "tar" so the code uses stream_blob_to_file instead of tar extraction.
    let manifest_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:config",
            "size": 2
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1+gzip",
                "digest": blob_digest,
                "size": blob_bytes.len()
            }
        ]
    })
    .to_string();

    let (local_addr, server_handle) = spawn_oci_server(
        format!("/v2/{}/manifests/{}", REPO, TAG),
        format!("/v2/{}/blobs/{}", REPO, blob_digest),
        manifest_json.into_bytes(),
        blob_bytes,
    )
    .await;

    let cert_dir = PathBuf::from("tests/test_certs");
    let host = format!("127.0.0.1.nip.io:{}", local_addr.port());
    let image_ref = format!("{}/{}:{}", host, REPO, TAG);
    let options = default_options(&cert_dir);

    let out_dir = tempdir().expect("create temp dir");
    let target_files: HashSet<String> = ["single.img".to_string()].into_iter().collect();

    let result =
        extract_files_from_oci_image_to_dir(&image_ref, &target_files, &options, out_dir.path())
            .await;

    server_handle.abort();

    assert!(result.is_ok(), "Extraction failed: {:?}", result.err());
    let map = result.unwrap();
    assert_eq!(map.len(), 1);
    assert!(map.contains_key("single.img"));

    let path = out_dir.path().join("single.img");
    let written = fs::read(&path).expect("read output");
    assert_eq!(written, file_data);
}

/// Tar layer but request only one of two files in the layer.
#[tokio::test]
async fn test_extract_tar_single_requested_file() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let boot_data = common::create_test_data(256);
    let system_data = common::create_test_data(512);
    let tar_bytes = build_tar(vec![
        ("boot_a.simg", boot_data.clone()),
        ("system_a.simg", system_data.clone()),
    ]);
    let blob_bytes = common::compress_gz(&tar_bytes);
    let blob_digest = "sha256:layer";

    let manifest_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": "sha256:config", "size": 2 },
        "layers": [{ "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": blob_digest, "size": blob_bytes.len() }]
    })
    .to_string();

    let (local_addr, server_handle) = spawn_oci_server(
        format!("/v2/{}/manifests/{}", REPO, TAG),
        format!("/v2/{}/blobs/{}", REPO, blob_digest),
        manifest_json.into_bytes(),
        blob_bytes,
    )
    .await;

    let cert_dir = PathBuf::from("tests/test_certs");
    let options = default_options(&cert_dir);
    let out_dir = tempdir().expect("create temp dir");
    let host = format!("127.0.0.1.nip.io:{}", local_addr.port());
    let image_ref = format!("{}/{}:{}", host, REPO, TAG);

    // Request only one file; layer contains two.
    let target_files: HashSet<String> = ["system_a.simg".to_string()].into_iter().collect();

    let result =
        extract_files_from_oci_image_to_dir(&image_ref, &target_files, &options, out_dir.path())
            .await;

    server_handle.abort();

    assert!(result.is_ok(), "Extraction failed: {:?}", result.err());
    let map = result.unwrap();
    assert_eq!(map.len(), 1);
    assert!(map.contains_key("system_a.simg"));
    let written = fs::read(out_dir.path().join("system_a.simg")).expect("read");
    assert_eq!(written, system_data);
    // boot_a.simg was not requested so should not be present
    assert!(!out_dir.path().join("boot_a.simg").exists());
}

/// Tar layer contains one file; request two. Only the present file is returned (partial success).
#[tokio::test]
async fn test_extract_tar_missing_file() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let boot_data = common::create_test_data(256);
    // Tar has only boot_a.simg
    let tar_bytes = build_tar(vec![("boot_a.simg", boot_data.clone())]);
    let blob_bytes = common::compress_gz(&tar_bytes);
    let blob_digest = "sha256:layer";

    let manifest_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": "sha256:config", "size": 2 },
        "layers": [{ "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": blob_digest, "size": blob_bytes.len() }]
    })
    .to_string();

    let (local_addr, server_handle) = spawn_oci_server(
        format!("/v2/{}/manifests/{}", REPO, TAG),
        format!("/v2/{}/blobs/{}", REPO, blob_digest),
        manifest_json.into_bytes(),
        blob_bytes,
    )
    .await;

    let cert_dir = PathBuf::from("tests/test_certs");
    let options = default_options(&cert_dir);
    let out_dir = tempdir().expect("create temp dir");
    let host = format!("127.0.0.1.nip.io:{}", local_addr.port());
    let image_ref = format!("{}/{}:{}", host, REPO, TAG);

    // Request both; only boot_a.simg exists in the layer.
    let target_files: HashSet<String> = ["boot_a.simg".to_string(), "system_a.simg".to_string()]
        .into_iter()
        .collect();

    let result =
        extract_files_from_oci_image_to_dir(&image_ref, &target_files, &options, out_dir.path())
            .await;

    server_handle.abort();

    // Current behavior: returns Ok with only the files that were found.
    assert!(result.is_ok(), "Extraction failed: {:?}", result.err());
    let map = result.unwrap();
    assert_eq!(map.len(), 1, "only boot_a.simg should be found");
    assert!(map.contains_key("boot_a.simg"));
    assert!(!map.contains_key("system_a.simg"));

    let written = fs::read(out_dir.path().join("boot_a.simg")).expect("read");
    assert_eq!(written, boot_data);
    assert!(!out_dir.path().join("system_a.simg").exists());
}
