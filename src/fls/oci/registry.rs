/// OCI Registry client
///
/// Implements the OCI Distribution Specification for pulling images:
/// - GET /v2/ - API version check
/// - GET /v2/<name>/manifests/<reference> - Fetch manifest
/// - GET /v2/<name>/blobs/<digest> - Fetch blob
use reqwest::{Client, Response, StatusCode};

use super::auth::{request_token, Credentials, WwwAuthenticate};
use super::manifest::{media_types, Manifest};
use super::reference::ImageReference;
use crate::fls::options::BlockFlashOptions;

/// OCI Registry client
pub struct RegistryClient {
    client: Client,
    image_ref: ImageReference,
    credentials: Credentials,
    token: Option<String>,
    debug: bool,
}

impl RegistryClient {
    /// Create a new registry client
    pub async fn new(
        image_ref: ImageReference,
        options: &OciOptions,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Reuse the HTTP client setup from the existing module
        let client = crate::fls::http::setup_http_client(&BlockFlashOptions {
            insecure_tls: options.insecure_tls,
            cacert: options.cacert.clone(),
            device: String::new(),
            buffer_size_mb: options.buffer_size_mb,
            write_buffer_size_mb: options.write_buffer_size_mb,
            max_retries: 10,
            retry_delay_secs: 2,
            debug: options.debug,
            o_direct: false,
            headers: Vec::new(),
            progress_interval_secs: 0.5,
            newline_progress: false,
            show_memory: false,
        })
        .await?;

        let credentials = Credentials::new(options.username.clone(), options.password.clone());

        Ok(Self {
            client,
            image_ref,
            credentials,
            token: None,
            debug: options.debug,
        })
    }

    /// Authenticate with the registry
    pub async fn authenticate(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let base_url = self.image_ref.registry_url();
        let v2_url = format!("{}/v2/", base_url);

        if self.debug {
            eprintln!("[DEBUG] Checking registry API: {}", v2_url);
        }

        // Try anonymous access first
        let response = self.client.get(&v2_url).send().await?;

        match response.status() {
            StatusCode::OK => {
                if self.debug {
                    eprintln!("[DEBUG] Registry allows anonymous access");
                }
                // Anonymous access works, no token needed
                Ok(())
            }
            StatusCode::UNAUTHORIZED => {
                // Need to authenticate
                let www_auth_header = response
                    .headers()
                    .get("www-authenticate")
                    .or_else(|| response.headers().get("WWW-Authenticate"))
                    .ok_or("No WWW-Authenticate header in 401 response")?
                    .to_str()
                    .map_err(|e| format!("Invalid WWW-Authenticate header: {}", e))?;

                if self.debug {
                    eprintln!("[DEBUG] WWW-Authenticate: {}", www_auth_header);
                }

                let www_auth = WwwAuthenticate::parse(www_auth_header)?;

                let token = request_token(
                    &self.client,
                    &www_auth,
                    self.image_ref.api_repository(),
                    &self.credentials,
                    self.debug,
                )
                .await?;

                if self.debug {
                    eprintln!("[DEBUG] Obtained bearer token");
                }

                self.token = Some(token);
                Ok(())
            }
            status => Err(format!("Unexpected status from /v2/: {}", status).into()),
        }
    }

    /// Add authorization header to request if we have a token
    fn add_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(token) = &self.token {
            request.header("Authorization", format!("Bearer {}", token))
        } else if let Some(basic) = self.credentials.basic_auth_header() {
            request.header("Authorization", basic)
        } else {
            request
        }
    }

    /// Fetch the image manifest
    pub async fn fetch_manifest(&self) -> Result<Manifest, Box<dyn std::error::Error>> {
        let url = format!(
            "{}/v2/{}/manifests/{}",
            self.image_ref.registry_url(),
            self.image_ref.api_repository(),
            self.image_ref.reference_string()
        );

        if self.debug {
            eprintln!("[DEBUG] Fetching manifest: {}", url);
        }

        let request = self.client.get(&url).header(
            "Accept",
            format!(
                "{}, {}, {}, {}",
                media_types::OCI_MANIFEST,
                media_types::DOCKER_MANIFEST_V2,
                media_types::OCI_INDEX,
                media_types::DOCKER_MANIFEST_LIST
            ),
        );

        let response = self.add_auth(request).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("Failed to fetch manifest: {} - {}", status, body).into());
        }

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let bytes = response.bytes().await?;

        if self.debug {
            eprintln!("[DEBUG] Manifest content-type: {:?}", content_type);
            eprintln!("[DEBUG] Manifest size: {} bytes", bytes.len());
        }

        Manifest::parse(&bytes, content_type.as_deref())
            .map_err(|e| format!("Failed to parse manifest: {}", e).into())
    }

    /// Fetch manifest for a specific digest (used for resolving manifest indexes)
    pub async fn fetch_manifest_by_digest(
        &self,
        digest: &str,
    ) -> Result<Manifest, Box<dyn std::error::Error>> {
        let url = format!(
            "{}/v2/{}/manifests/{}",
            self.image_ref.registry_url(),
            self.image_ref.api_repository(),
            digest
        );

        if self.debug {
            eprintln!("[DEBUG] Fetching manifest by digest: {}", url);
        }

        let request = self.client.get(&url).header(
            "Accept",
            format!(
                "{}, {}",
                media_types::OCI_MANIFEST,
                media_types::DOCKER_MANIFEST_V2
            ),
        );

        let response = self.add_auth(request).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("Failed to fetch manifest: {} - {}", status, body).into());
        }

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let bytes = response.bytes().await?;
        Manifest::parse(&bytes, content_type.as_deref())
            .map_err(|e| format!("Failed to parse manifest: {}", e).into())
    }

    /// Start streaming a blob
    pub async fn get_blob_stream(
        &self,
        digest: &str,
    ) -> Result<Response, Box<dyn std::error::Error>> {
        let url = format!(
            "{}/v2/{}/blobs/{}",
            self.image_ref.registry_url(),
            self.image_ref.api_repository(),
            digest
        );

        if self.debug {
            eprintln!("[DEBUG] Starting blob download: {}", url);
        }

        let request = self.client.get(&url);
        let response = self.add_auth(request).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("Failed to fetch blob: {} - {}", status, body).into());
        }

        if self.debug {
            if let Some(len) = response.content_length() {
                eprintln!("[DEBUG] Blob size: {} bytes", len);
            }
        }

        Ok(response)
    }

    /// Get the image reference
    #[allow(dead_code)]
    pub fn image_ref(&self) -> &ImageReference {
        &self.image_ref
    }
}

/// Options for OCI operations
#[derive(Debug, Clone)]
pub struct OciOptions {
    pub username: Option<String>,
    pub password: Option<String>,
    pub insecure_tls: bool,
    pub cacert: Option<std::path::PathBuf>,
    pub buffer_size_mb: usize,
    pub write_buffer_size_mb: usize,
    pub debug: bool,
    pub o_direct: bool,
    pub progress_interval_secs: f64,
    pub newline_progress: bool,
    pub show_memory: bool,
    pub file_pattern: Option<String>,
    pub device: String,
}

impl Default for OciOptions {
    fn default() -> Self {
        Self {
            username: None,
            password: None,
            insecure_tls: false,
            cacert: None,
            buffer_size_mb: 128,
            write_buffer_size_mb: 128,
            debug: false,
            o_direct: false,
            progress_interval_secs: 0.5,
            newline_progress: false,
            show_memory: false,
            file_pattern: None,
            device: String::new(),
        }
    }
}
