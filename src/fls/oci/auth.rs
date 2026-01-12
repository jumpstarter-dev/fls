/// OCI registry authentication
///
/// Implements the Docker/OCI token authentication flow:
/// 1. Try to access resource
/// 2. On 401, parse WWW-Authenticate header
/// 3. Request token from auth service
/// 4. Use token for subsequent requests
use base64::Engine;
use reqwest::Client;
use std::collections::HashMap;

/// Authentication credentials
#[derive(Debug, Clone)]
pub enum Credentials {
    Anonymous,
    Basic { username: String, password: String },
}

impl Credentials {
    pub fn new(username: Option<String>, password: Option<String>) -> Self {
        match (username, password) {
            (Some(u), Some(p)) => Credentials::Basic {
                username: u,
                password: p,
            },
            _ => Credentials::Anonymous,
        }
    }

    /// Encode as basic auth header value
    pub fn basic_auth_header(&self) -> Option<String> {
        match self {
            Credentials::Anonymous => None,
            Credentials::Basic { username, password } => {
                let encoded = base64::engine::general_purpose::STANDARD
                    .encode(format!("{}:{}", username, password));
                Some(format!("Basic {}", encoded))
            }
        }
    }
}

/// Parsed WWW-Authenticate header
#[derive(Debug)]
pub struct WwwAuthenticate {
    pub realm: String,
    pub service: Option<String>,
    pub scope: Option<String>,
}

impl WwwAuthenticate {
    /// Parse WWW-Authenticate header value
    ///
    /// Format: Bearer realm="...",service="...",scope="..."
    pub fn parse(header: &str) -> Result<Self, String> {
        // Remove "Bearer " prefix if present
        let params_str = header
            .strip_prefix("Bearer ")
            .or_else(|| header.strip_prefix("bearer "))
            .unwrap_or(header);

        let params = parse_auth_params(params_str);

        let realm = params
            .get("realm")
            .ok_or("Missing realm in WWW-Authenticate header")?
            .clone();

        Ok(Self {
            realm,
            service: params.get("service").cloned(),
            scope: params.get("scope").cloned(),
        })
    }

    /// Build token request URL
    pub fn token_url(&self, repository: &str) -> String {
        let mut url = self.realm.clone();

        // Add query parameters
        let mut params = Vec::new();

        if let Some(service) = &self.service {
            params.push(format!("service={}", urlencoding_encode(service)));
        }

        // Use the scope from the header, or construct one for pull access
        let scope = self
            .scope
            .clone()
            .unwrap_or_else(|| format!("repository:{}:pull", repository));
        params.push(format!("scope={}", urlencoding_encode(&scope)));

        if !params.is_empty() {
            if url.contains('?') {
                url.push('&');
            } else {
                url.push('?');
            }
            url.push_str(&params.join("&"));
        }

        url
    }
}

/// Parse key="value" pairs from auth header
fn parse_auth_params(s: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();

    // Simple state machine parser for key="value" pairs
    let mut key = String::new();
    let mut value = String::new();
    let mut in_value = false;
    let mut in_quotes = false;

    for c in s.chars() {
        match c {
            '=' if !in_value => {
                in_value = true;
            }
            '"' if in_value => {
                if in_quotes {
                    // End of quoted value
                    params.insert(key.clone(), value.clone());
                    key.clear();
                    value.clear();
                    in_value = false;
                    in_quotes = false;
                } else {
                    in_quotes = true;
                }
            }
            ',' if !in_quotes => {
                // Value separator (for unquoted values)
                if in_value && !value.is_empty() {
                    params.insert(key.clone(), value.clone());
                    key.clear();
                    value.clear();
                    in_value = false;
                }
            }
            ' ' if !in_quotes && !in_value => {
                // Skip whitespace between params
            }
            _ if in_value => {
                value.push(c);
            }
            _ => {
                key.push(c);
            }
        }
    }

    // Handle last value if not quoted
    if in_value && !value.is_empty() {
        params.insert(key, value);
    }

    params
}

/// Simple URL encoding for query parameters
fn urlencoding_encode(s: &str) -> String {
    let mut result = String::new();
    for c in s.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                result.push(c);
            }
            ':' => result.push_str("%3A"),
            '/' => result.push_str("%2F"),
            ' ' => result.push_str("%20"),
            _ => {
                for byte in c.to_string().as_bytes() {
                    result.push_str(&format!("%{:02X}", byte));
                }
            }
        }
    }
    result
}

/// Token response from auth service
#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub struct TokenResponse {
    pub token: Option<String>,
    pub access_token: Option<String>,
    pub expires_in: Option<u64>,
}

impl TokenResponse {
    /// Get the token value (some registries use 'token', others use 'access_token')
    pub fn get_token(&self) -> Option<&str> {
        self.token.as_deref().or(self.access_token.as_deref())
    }
}

/// Request a bearer token from the auth service
pub async fn request_token(
    client: &Client,
    www_auth: &WwwAuthenticate,
    repository: &str,
    credentials: &Credentials,
    debug: bool,
) -> Result<String, String> {
    let token_url = www_auth.token_url(repository);

    if debug {
        eprintln!("[DEBUG] Requesting token from: {}", token_url);
    }

    let mut request = client.get(&token_url);

    // Add basic auth if credentials provided
    if let Some(auth_header) = credentials.basic_auth_header() {
        request = request.header("Authorization", auth_header);
        if debug {
            eprintln!("[DEBUG] Using basic auth for token request");
        }
    }

    let response = request
        .send()
        .await
        .map_err(|e| format!("Token request failed: {}", e))?;

    if !response.status().is_success() {
        return Err(format!(
            "Token request failed with status: {}",
            response.status()
        ));
    }

    let token_response: TokenResponse = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse token response: {}", e))?;

    token_response
        .get_token()
        .map(String::from)
        .ok_or_else(|| "No token in response".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_www_authenticate() {
        let header = r#"Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:org/repo:pull""#;
        let auth = WwwAuthenticate::parse(header).unwrap();
        assert_eq!(auth.realm, "https://ghcr.io/token");
        assert_eq!(auth.service, Some("ghcr.io".to_string()));
        assert_eq!(auth.scope, Some("repository:org/repo:pull".to_string()));
    }

    #[test]
    fn test_parse_www_authenticate_minimal() {
        let header = r#"Bearer realm="https://auth.docker.io/token""#;
        let auth = WwwAuthenticate::parse(header).unwrap();
        assert_eq!(auth.realm, "https://auth.docker.io/token");
        assert_eq!(auth.service, None);
    }

    #[test]
    fn test_token_url() {
        let auth = WwwAuthenticate {
            realm: "https://auth.docker.io/token".to_string(),
            service: Some("registry.docker.io".to_string()),
            scope: None,
        };
        let url = auth.token_url("library/ubuntu");
        assert!(url.contains("service=registry.docker.io"));
        assert!(url.contains("scope=repository%3Alibrary%2Fubuntu%3Apull"));
    }

    #[test]
    fn test_basic_auth_header() {
        let creds = Credentials::Basic {
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        let header = creds.basic_auth_header().unwrap();
        assert!(header.starts_with("Basic "));
    }
}
