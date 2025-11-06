use std::fmt;
use std::time::Duration;

#[derive(Debug)]
pub enum DownloadError {
    /// HTTP client error (4xx) - non-retryable
    HttpClientError(u16, String),
    /// HTTP server error (5xx) - retryable
    HttpServerError(u16, String),
    /// HTTP 429 Rate Limited - retryable with additional delay
    HttpRateLimited(Option<u64>), // Optional retry-after seconds
    /// TLS/Certificate error - non-retryable
    TlsError(String),
    /// DNS resolution error - retryable (might be transient)
    DnsError(String),
    /// Connection error - retryable
    ConnectionError(String),
    /// Timeout error - retryable
    TimeoutError(String),
    /// Other errors
    Other(String),
}

impl DownloadError {
    /// Get a human-readable description for an HTTP status code
    fn http_status_description(code: u16) -> String {
        match code {
            400 => "Bad Request".to_string(),
            401 => "Unauthorized - authentication required".to_string(),
            403 => "Forbidden - access denied".to_string(),
            404 => "Not Found - the requested file does not exist".to_string(),
            405 => "Method Not Allowed".to_string(),
            410 => "Gone - resource no longer available".to_string(),
            429 => "Too Many Requests - rate limited".to_string(),
            500 => "Internal Server Error - server error".to_string(),
            502 => "Bad Gateway - proxy/gateway error".to_string(),
            503 => "Service Unavailable - server temporarily unavailable".to_string(),
            504 => "Gateway Timeout".to_string(),
            _ => "Unknown".to_string(),
        }
    }

    /// Create a DownloadError from an HTTP status code
    pub fn from_http_status(status: reqwest::StatusCode) -> Self {
        let code = status.as_u16();

        // Handle 429 Rate Limited specially
        if code == 429 {
            return DownloadError::HttpRateLimited(None);
        }

        let description = Self::http_status_description(code);

        if (400..500).contains(&code) {
            DownloadError::HttpClientError(code, description)
        } else {
            DownloadError::HttpServerError(code, description)
        }
    }

    /// Create a DownloadError from an HTTP response (includes headers for retry-after)
    pub fn from_http_response(response: &reqwest::Response) -> Self {
        let status = response.status();
        let code = status.as_u16();

        // Handle 429 Rate Limited with Retry-After header
        if code == 429 {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            return DownloadError::HttpRateLimited(retry_after);
        }

        Self::from_http_status(status)
    }

    /// Create a DownloadError from a reqwest::Error
    pub fn from_reqwest(error: reqwest::Error) -> Self {
        if error.is_status() {
            if let Some(status) = error.status() {
                return Self::from_http_status(status);
            }
        }

        if error.is_timeout() {
            return DownloadError::TimeoutError(error.to_string());
        }

        if error.is_connect() {
            // Check the full error chain (including source errors) for TLS/SSL indicators
            let error_str = error.to_string();

            // Check error source chain for TLS/SSL/Certificate errors
            // We use a hybrid approach: check both type names and error messages
            // - Type names catch concrete types before trait object erasure
            // - Error messages catch issues when types are erased to dyn Error
            let mut current_error: Option<&dyn std::error::Error> = Some(&error);
            let mut is_tls_error = false;

            while let Some(err) = current_error {
                let error_msg = err.to_string().to_lowercase();

                // Check error message for TLS-related keywords
                // This works even when types are erased to trait objects
                let message_indicates_tls = error_msg.contains("certificate")
                    || error_msg.contains("tls")
                    || error_msg.contains("ssl")
                    || error_msg.contains("trust setting")  // macOS Security Framework
                    || error_msg.contains("trust policy"); // macOS Security Framework

                if message_indicates_tls {
                    is_tls_error = true;
                    break;
                }

                current_error = err.source();
            }

            if is_tls_error {
                return DownloadError::TlsError(error_str);
            }

            // Check for DNS errors
            if error_str.contains("dns") || error_str.contains("failed to lookup address") {
                return DownloadError::DnsError(error_str);
            }
            return DownloadError::ConnectionError(error_str);
        }

        // Fallback for other error types
        DownloadError::Other(error.to_string())
    }

    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            // HTTP 4xx client errors are not retryable
            DownloadError::HttpClientError(_, _) => false,
            // TLS errors are not retryable (need configuration change)
            DownloadError::TlsError(_) => false,
            // Everything else is potentially retryable
            DownloadError::HttpServerError(_, _) => true,
            DownloadError::HttpRateLimited(_) => true, // Rate limiting is retryable
            DownloadError::DnsError(_) => true,
            DownloadError::ConnectionError(_) => true,
            DownloadError::TimeoutError(_) => true,
            DownloadError::Other(_) => true,
        }
    }

    /// Get suggested additional retry delay for this error type
    /// Returns Some(duration) if this error type should have additional delay beyond normal retry
    pub fn suggested_retry_delay(&self) -> Option<Duration> {
        match self {
            // For rate limiting, use server's Retry-After if provided, otherwise use 60 seconds
            DownloadError::HttpRateLimited(retry_after_secs) => {
                let secs = retry_after_secs.unwrap_or(60);
                Some(Duration::from_secs(secs))
            }
            // No additional delay for other errors
            _ => None,
        }
    }

    /// Format the error with helpful context
    pub fn format_error(&self) -> String {
        match self {
            DownloadError::HttpClientError(code, desc) => {
                format!("HTTP error: {} {}", code, desc)
            }
            DownloadError::HttpServerError(code, desc) => {
                format!("HTTP error: {} {}", code, desc)
            }
            DownloadError::HttpRateLimited(retry_after) => {
                if let Some(secs) = retry_after {
                    format!(
                        "HTTP error: 429 Too Many Requests - rate limited (server requested {} second wait)",
                        secs
                    )
                } else {
                    "HTTP error: 429 Too Many Requests - rate limited".to_string()
                }
            }
            DownloadError::TlsError(msg) => {
                format!("TLS/SSL error - certificate validation failed. Try using -k to skip certificate verification ({})", msg)
            }
            DownloadError::DnsError(msg) => {
                format!(
                    "DNS resolution failed - unable to resolve hostname ({})",
                    msg
                )
            }
            DownloadError::ConnectionError(msg) => {
                if msg.contains("refused") {
                    format!(
                        "Connection refused - server is not accepting connections ({})",
                        msg
                    )
                } else if msg.contains("reset") || msg.contains("broken pipe") {
                    format!(
                        "Connection reset - network connection was interrupted ({})",
                        msg
                    )
                } else if msg.contains("unreachable") {
                    format!(
                        "Network unreachable - check your network connection ({})",
                        msg
                    )
                } else {
                    format!("Connection error: {}", msg)
                }
            }
            DownloadError::TimeoutError(msg) => {
                format!(
                    "Connection timeout - server did not respond in time ({})",
                    msg
                )
            }
            DownloadError::Other(msg) => msg.clone(),
        }
    }
}

impl fmt::Display for DownloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format_error())
    }
}

impl std::error::Error for DownloadError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_status_description() {
        assert_eq!(DownloadError::http_status_description(400), "Bad Request");
        assert_eq!(
            DownloadError::http_status_description(401),
            "Unauthorized - authentication required"
        );
        assert_eq!(
            DownloadError::http_status_description(403),
            "Forbidden - access denied"
        );
        assert_eq!(
            DownloadError::http_status_description(404),
            "Not Found - the requested file does not exist"
        );
        assert_eq!(
            DownloadError::http_status_description(500),
            "Internal Server Error - server error"
        );
        assert_eq!(
            DownloadError::http_status_description(502),
            "Bad Gateway - proxy/gateway error"
        );
        assert_eq!(
            DownloadError::http_status_description(503),
            "Service Unavailable - server temporarily unavailable"
        );
        assert_eq!(DownloadError::http_status_description(999), "Unknown");
    }

    #[test]
    fn test_from_http_status_client_errors() {
        let status_400 = reqwest::StatusCode::BAD_REQUEST;
        let error = DownloadError::from_http_status(status_400);
        assert!(matches!(error, DownloadError::HttpClientError(400, _)));
        assert!(!error.is_retryable());

        let status_404 = reqwest::StatusCode::NOT_FOUND;
        let error = DownloadError::from_http_status(status_404);
        assert!(matches!(error, DownloadError::HttpClientError(404, _)));
        assert!(!error.is_retryable());

        let status_403 = reqwest::StatusCode::FORBIDDEN;
        let error = DownloadError::from_http_status(status_403);
        assert!(matches!(error, DownloadError::HttpClientError(403, _)));
        assert!(!error.is_retryable());
    }

    #[test]
    fn test_from_http_status_server_errors() {
        let status_500 = reqwest::StatusCode::INTERNAL_SERVER_ERROR;
        let error = DownloadError::from_http_status(status_500);
        assert!(matches!(error, DownloadError::HttpServerError(500, _)));
        assert!(error.is_retryable());

        let status_502 = reqwest::StatusCode::BAD_GATEWAY;
        let error = DownloadError::from_http_status(status_502);
        assert!(matches!(error, DownloadError::HttpServerError(502, _)));
        assert!(error.is_retryable());

        let status_503 = reqwest::StatusCode::SERVICE_UNAVAILABLE;
        let error = DownloadError::from_http_status(status_503);
        assert!(matches!(error, DownloadError::HttpServerError(503, _)));
        assert!(error.is_retryable());
    }

    #[test]
    fn test_is_retryable_http_client_errors() {
        let error = DownloadError::HttpClientError(404, "Not Found".to_string());
        assert!(!error.is_retryable());

        let error = DownloadError::HttpClientError(401, "Unauthorized".to_string());
        assert!(!error.is_retryable());

        let error = DownloadError::HttpClientError(403, "Forbidden".to_string());
        assert!(!error.is_retryable());
    }

    #[test]
    fn test_is_retryable_http_server_errors() {
        let error = DownloadError::HttpServerError(500, "Internal Server Error".to_string());
        assert!(error.is_retryable());

        let error = DownloadError::HttpServerError(502, "Bad Gateway".to_string());
        assert!(error.is_retryable());

        let error = DownloadError::HttpServerError(503, "Service Unavailable".to_string());
        assert!(error.is_retryable());
    }

    #[test]
    fn test_is_retryable_tls_errors() {
        let error = DownloadError::TlsError("certificate verify failed".to_string());
        assert!(!error.is_retryable());

        let error = DownloadError::TlsError("ssl handshake failed".to_string());
        assert!(!error.is_retryable());
    }

    #[test]
    fn test_is_retryable_network_errors() {
        let error = DownloadError::DnsError("failed to resolve".to_string());
        assert!(error.is_retryable());

        let error = DownloadError::ConnectionError("connection refused".to_string());
        assert!(error.is_retryable());

        let error = DownloadError::TimeoutError("connection timeout".to_string());
        assert!(error.is_retryable());

        let error = DownloadError::Other("unknown error".to_string());
        assert!(error.is_retryable());
    }

    #[test]
    fn test_format_error_http_client() {
        let error = DownloadError::HttpClientError(404, "Not Found".to_string());
        assert_eq!(error.format_error(), "HTTP error: 404 Not Found");
    }

    #[test]
    fn test_format_error_http_server() {
        let error = DownloadError::HttpServerError(500, "Internal Server Error".to_string());
        assert_eq!(
            error.format_error(),
            "HTTP error: 500 Internal Server Error"
        );
    }

    #[test]
    fn test_format_error_tls() {
        let error = DownloadError::TlsError("certificate verify failed".to_string());
        let formatted = error.format_error();
        assert!(formatted.contains("TLS/SSL error"));
        assert!(formatted.contains("certificate validation failed"));
        assert!(formatted.contains("-k"));
    }

    #[test]
    fn test_format_error_dns() {
        let error = DownloadError::DnsError("failed to lookup address".to_string());
        let formatted = error.format_error();
        assert!(formatted.contains("DNS resolution failed"));
        assert!(formatted.contains("unable to resolve hostname"));
    }

    #[test]
    fn test_format_error_connection_refused() {
        let error = DownloadError::ConnectionError("connection refused".to_string());
        let formatted = error.format_error();
        assert!(formatted.contains("Connection refused"));
        assert!(formatted.contains("server is not accepting connections"));
    }

    #[test]
    fn test_format_error_connection_reset() {
        let error = DownloadError::ConnectionError("connection reset by peer".to_string());
        let formatted = error.format_error();
        assert!(formatted.contains("Connection reset"));
        assert!(formatted.contains("network connection was interrupted"));
    }

    #[test]
    fn test_format_error_network_unreachable() {
        let error = DownloadError::ConnectionError("network unreachable".to_string());
        let formatted = error.format_error();
        assert!(formatted.contains("Network unreachable"));
        assert!(formatted.contains("check your network connection"));
    }

    #[test]
    fn test_format_error_timeout() {
        let error = DownloadError::TimeoutError("operation timed out".to_string());
        let formatted = error.format_error();
        assert!(formatted.contains("Connection timeout"));
        assert!(formatted.contains("server did not respond in time"));
    }

    #[test]
    fn test_format_error_other() {
        let error = DownloadError::Other("some other error".to_string());
        assert_eq!(error.format_error(), "some other error");
    }

    #[test]
    fn test_display_trait() {
        let error = DownloadError::HttpClientError(404, "Not Found".to_string());
        assert_eq!(format!("{}", error), "HTTP error: 404 Not Found");

        let error = DownloadError::TlsError("cert failed".to_string());
        assert!(format!("{}", error).contains("TLS/SSL error"));
    }

    #[test]
    fn test_error_trait() {
        let error = DownloadError::HttpClientError(404, "Not Found".to_string());
        // Test that it implements std::error::Error
        let _: &dyn std::error::Error = &error;
    }

    #[test]
    fn test_http_rate_limited_description() {
        assert_eq!(
            DownloadError::http_status_description(429),
            "Too Many Requests - rate limited"
        );
    }

    #[test]
    fn test_from_http_status_rate_limited() {
        let status_429 = reqwest::StatusCode::TOO_MANY_REQUESTS;
        let error = DownloadError::from_http_status(status_429);
        assert!(matches!(error, DownloadError::HttpRateLimited(None)));
        assert!(error.is_retryable());
    }

    #[test]
    fn test_is_retryable_rate_limited() {
        let error = DownloadError::HttpRateLimited(None);
        assert!(error.is_retryable());

        let error = DownloadError::HttpRateLimited(Some(60));
        assert!(error.is_retryable());
    }

    #[test]
    fn test_suggested_retry_delay_rate_limited() {
        // With explicit retry-after
        let error = DownloadError::HttpRateLimited(Some(120));
        assert_eq!(
            error.suggested_retry_delay(),
            Some(Duration::from_secs(120))
        );

        // Without retry-after, should default to 60 seconds
        let error = DownloadError::HttpRateLimited(None);
        assert_eq!(error.suggested_retry_delay(), Some(Duration::from_secs(60)));
    }

    #[test]
    fn test_suggested_retry_delay_other_errors() {
        // Other error types should not have suggested delays
        let error = DownloadError::HttpClientError(404, "Not Found".to_string());
        assert_eq!(error.suggested_retry_delay(), None);

        let error = DownloadError::HttpServerError(500, "Internal Server Error".to_string());
        assert_eq!(error.suggested_retry_delay(), None);

        let error = DownloadError::TimeoutError("timeout".to_string());
        assert_eq!(error.suggested_retry_delay(), None);
    }

    #[test]
    fn test_format_error_rate_limited_without_retry_after() {
        let error = DownloadError::HttpRateLimited(None);
        let formatted = error.format_error();
        assert_eq!(
            formatted,
            "HTTP error: 429 Too Many Requests - rate limited"
        );
    }

    #[test]
    fn test_format_error_rate_limited_with_retry_after() {
        let error = DownloadError::HttpRateLimited(Some(120));
        let formatted = error.format_error();
        assert_eq!(
            formatted,
            "HTTP error: 429 Too Many Requests - rate limited (server requested 120 second wait)"
        );
    }
}
