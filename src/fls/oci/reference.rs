/// OCI image reference parsing
///
/// Supports formats:
/// - registry.example.com/namespace/repo:tag
/// - registry.example.com/namespace/repo@sha256:digest
/// - namespace/repo:tag (Docker Hub)
/// - repo:tag (Docker Hub library)
use std::fmt;

/// Parsed OCI image reference
#[derive(Debug, Clone)]
pub struct ImageReference {
    /// Registry host (e.g., "ghcr.io", "docker.io")
    pub registry: String,
    /// Repository path (e.g., "myorg/myimage")
    pub repository: String,
    /// Tag or digest reference
    pub reference: Reference,
}

/// Reference type - either a tag or a digest
#[derive(Debug, Clone)]
pub enum Reference {
    Tag(String),
    Digest(String),
}

impl fmt::Display for Reference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Reference::Tag(t) => write!(f, "{}", t),
            Reference::Digest(d) => write!(f, "{}", d),
        }
    }
}

impl fmt::Display for ImageReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.reference {
            Reference::Tag(t) => write!(f, "{}/{}:{}", self.registry, self.repository, t),
            Reference::Digest(d) => write!(f, "{}/{}@{}", self.registry, self.repository, d),
        }
    }
}

impl ImageReference {
    /// Parse an image reference string
    ///
    /// Examples:
    /// - "ghcr.io/org/repo:tag" -> ghcr.io, org/repo, tag
    /// - "docker.io/library/ubuntu:22.04" -> docker.io, library/ubuntu, 22.04
    /// - "myrepo:latest" -> docker.io, library/myrepo, latest
    /// - "myorg/myrepo:v1" -> docker.io, myorg/myrepo, v1
    /// - "registry.example.com:5000/repo:tag" -> registry.example.com:5000, repo, tag
    pub fn parse(input: &str) -> Result<Self, String> {
        let input = input.trim();

        if input.is_empty() {
            return Err("Empty image reference".to_string());
        }

        // Split by @ for digest references
        let (name_part, reference) = if let Some(at_pos) = input.rfind('@') {
            let digest = &input[at_pos + 1..];
            validate_digest(digest)?;
            let name_part = &input[..at_pos];

            // Disallow tag mixed with digest (e.g. repo:tag@sha256:...)
            // but allow registry ports (e.g. registry:5000/repo@sha256:...)
            if let Some(last_colon) = name_part.rfind(':') {
                if let Some(last_slash) = name_part.rfind('/') {
                    // If colon comes after the last slash, it's a tag (invalid with digest)
                    if last_colon > last_slash {
                        return Err("Invalid reference: cannot combine tag and digest".to_string());
                    }
                } else {
                    // No slash at all means the colon would be a tag, not a port
                    // (e.g., "repo:tag@sha256:...")
                    return Err("Invalid reference: cannot combine tag and digest".to_string());
                }
            }

            (name_part, Reference::Digest(digest.to_string()))
        } else {
            // Split by : for tag, but be careful about registry port
            let (name, tag) = split_name_and_tag(input)?;
            (name, Reference::Tag(tag.unwrap_or("latest").to_string()))
        };

        // Parse registry and repository from name
        let (registry, repository) = parse_registry_and_repo(name_part)?;

        Ok(Self {
            registry,
            repository,
            reference,
        })
    }

    /// Get the full repository path for API calls (registry/repository)
    pub fn api_repository(&self) -> &str {
        &self.repository
    }

    /// Get the reference string for manifest requests
    pub fn reference_string(&self) -> String {
        match &self.reference {
            Reference::Tag(t) => t.clone(),
            Reference::Digest(d) => d.clone(),
        }
    }

    /// Get the registry URL (with https:// prefix)
    pub fn registry_url(&self) -> String {
        // Docker Hub uses registry-1.docker.io for the registry API
        if self.registry == "docker.io" {
            "https://registry-1.docker.io".to_string()
        } else {
            format!("https://{}", self.registry)
        }
    }
}

/// Validate digest format according to OCI spec
/// Digests must be in format `algorithm:hex` where:
/// - sha256 requires exactly 64 hex characters
/// - sha512 requires exactly 128 hex characters
fn validate_digest(digest: &str) -> Result<(), String> {
    let (algorithm, hash) = digest
        .split_once(':')
        .ok_or_else(|| format!("Invalid digest format (missing ':'): {}", digest))?;

    let expected_len = match algorithm {
        "sha256" => 64,
        "sha512" => 128,
        _ => {
            return Err(format!(
                "Unsupported digest algorithm '{}' (expected sha256 or sha512)",
                algorithm
            ))
        }
    };

    if hash.len() != expected_len {
        return Err(format!(
            "Invalid {} digest: expected {} hex characters, got {}",
            algorithm,
            expected_len,
            hash.len()
        ));
    }

    // Validate hex characters (lowercase per OCI spec)
    if !hash
        .chars()
        .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    {
        return Err(format!(
            "Invalid {} digest: must contain only lowercase hex characters (0-9, a-f)",
            algorithm
        ));
    }

    Ok(())
}

/// Check if a string looks like a hostname (contains dot, is localhost, or is IP literal)
fn looks_like_hostname(authority: &str) -> bool {
    authority.contains('.') || authority == "localhost" || authority.starts_with('[')
}

/// Split image name and tag, handling registry ports correctly
fn split_name_and_tag(input: &str) -> Result<(&str, Option<&str>), String> {
    // Find the last colon
    if let Some(colon_pos) = input.rfind(':') {
        // Check if this colon is part of a port number (registry:port/repo)
        let after_colon = &input[colon_pos + 1..];
        let before_colon = &input[..colon_pos];

        // If there's a slash after the colon, this is a port number
        if after_colon.contains('/') {
            // This is registry:port/repo format, no tag
            return Ok((input, None));
        }

        // If before the colon contains a slash, this is a tag
        if before_colon.contains('/') || !after_colon.chars().all(|c| c.is_ascii_digit()) {
            return Ok((before_colon, Some(after_colon)));
        }

        // Could be either "registry:port" or "image:tag"
        // Only treat all-digit short suffix as port if authority looks like a host
        if after_colon.len() <= 5 && after_colon.chars().all(|c| c.is_ascii_digit()) {
            // Check if the portion before colon looks like a hostname
            // (contains a dot, is "localhost", or is an IP literal)
            if looks_like_hostname(before_colon) {
                // Likely a port number, no tag
                return Ok((input, None));
            }
        }

        Ok((before_colon, Some(after_colon)))
    } else {
        Ok((input, None))
    }
}

/// Parse registry and repository from the name part
fn parse_registry_and_repo(name: &str) -> Result<(String, String), String> {
    let parts: Vec<&str> = name.splitn(2, '/').collect();

    if parts.len() == 1 {
        // Just a repo name, use Docker Hub library
        return Ok(("docker.io".to_string(), format!("library/{}", parts[0])));
    }

    let first = parts[0];
    let rest = parts[1];

    // Determine if first part is a registry or namespace
    // A registry typically has: a dot, a colon (port), or is "localhost"
    let is_registry = first.contains('.') || first.contains(':') || first == "localhost";

    if is_registry {
        // First part is a registry
        if rest.is_empty() {
            return Err("Empty repository name".to_string());
        }
        Ok((first.to_string(), rest.to_string()))
    } else {
        // First part is a namespace on Docker Hub
        Ok(("docker.io".to_string(), name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_full_reference() {
        let r = ImageReference::parse("ghcr.io/myorg/myimage:v1.0").unwrap();
        assert_eq!(r.registry, "ghcr.io");
        assert_eq!(r.repository, "myorg/myimage");
        assert!(matches!(r.reference, Reference::Tag(t) if t == "v1.0"));
    }

    #[test]
    fn test_parse_docker_hub_library() {
        let r = ImageReference::parse("ubuntu:22.04").unwrap();
        assert_eq!(r.registry, "docker.io");
        assert_eq!(r.repository, "library/ubuntu");
        assert!(matches!(r.reference, Reference::Tag(t) if t == "22.04"));
    }

    #[test]
    fn test_parse_docker_hub_namespace() {
        let r = ImageReference::parse("myuser/myrepo:latest").unwrap();
        assert_eq!(r.registry, "docker.io");
        assert_eq!(r.repository, "myuser/myrepo");
    }

    #[test]
    fn test_parse_with_port() {
        let r = ImageReference::parse("registry.local:5000/myrepo:tag").unwrap();
        assert_eq!(r.registry, "registry.local:5000");
        assert_eq!(r.repository, "myrepo");
        assert!(matches!(r.reference, Reference::Tag(t) if t == "tag"));
    }

    #[test]
    fn test_parse_digest() {
        let r = ImageReference::parse(
            "ghcr.io/org/repo@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();
        assert_eq!(r.registry, "ghcr.io");
        assert_eq!(r.repository, "org/repo");
        assert!(matches!(r.reference, Reference::Digest(d) if d.starts_with("sha256:")));
    }

    #[test]
    fn test_parse_default_tag() {
        let r = ImageReference::parse("ghcr.io/org/repo").unwrap();
        assert!(matches!(r.reference, Reference::Tag(t) if t == "latest"));
    }

    #[test]
    fn test_parse_numeric_tag() {
        // Test that "myrepo:12345" yields tag "12345" (not treated as port)
        let r = ImageReference::parse("myrepo:12345").unwrap();
        assert_eq!(r.registry, "docker.io");
        assert_eq!(r.repository, "library/myrepo");
        assert!(matches!(r.reference, Reference::Tag(t) if t == "12345"));
    }

    #[test]
    fn test_parse_registry_port_no_tag() {
        // Test that "registry:5000/repo" yields no tag (port number)
        let r = ImageReference::parse("registry:5000/repo").unwrap();
        assert_eq!(r.registry, "registry:5000");
        assert_eq!(r.repository, "repo");
        assert!(matches!(r.reference, Reference::Tag(t) if t == "latest")); // should default to latest
    }

    #[test]
    fn test_valid_sha256_digest() {
        let r = ImageReference::parse(
            "ghcr.io/org/repo@sha256:a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4",
        )
        .unwrap();
        assert_eq!(r.registry, "ghcr.io");
        assert_eq!(r.repository, "org/repo");
        assert!(matches!(r.reference, Reference::Digest(d) if d.starts_with("sha256:")));
    }

    #[test]
    fn test_invalid_digest_too_short() {
        let result = ImageReference::parse("ghcr.io/org/repo@sha256:abc123");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expected 64 hex characters"));
    }

    #[test]
    fn test_invalid_digest_uppercase() {
        let result = ImageReference::parse(
            "ghcr.io/org/repo@sha256:A3ED95CAEB02FFE68CDD9FD84406680AE93D633CB16422D00E8A7C22955B46D4",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("lowercase hex"));
    }

    #[test]
    fn test_invalid_digest_algorithm() {
        let result = ImageReference::parse("ghcr.io/org/repo@md5:d41d8cd98f00b204e9800998ecf8427e");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unsupported digest algorithm"));
    }

    #[test]
    fn test_invalid_tag_with_digest() {
        // Tag + digest is not allowed
        let result = ImageReference::parse(
            "ghcr.io/org/repo:tag@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("cannot combine tag and digest"));
    }

    #[test]
    fn test_valid_registry_port_with_digest() {
        // Registry port + digest should be allowed
        let r = ImageReference::parse(
            "registry:5000/repo@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();
        assert_eq!(r.registry, "registry:5000");
        assert_eq!(r.repository, "repo");
        assert!(matches!(r.reference, Reference::Digest(_)));
    }
}
