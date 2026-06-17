/// Utility functions for URL redaction.
///
/// Redacts userinfo (username:password) from a NATS URL for safe logging.
///
/// # Examples
///
/// ```
/// use fabricfs_session_protocol::redact_nats_url;
///
/// assert_eq!(
///     redact_nats_url("nats://user:pass@localhost:4222"),
///     "nats://***:***@localhost:4222"
/// );
/// assert_eq!(
///     redact_nats_url("nats://localhost:4222"),
///     "nats://localhost:4222"
/// );
/// ```
pub fn redact_nats_url(url: &str) -> String {
    // Match pattern: nats://[userinfo]@host
    // Replace userinfo with ***:***
    if let Some(at_pos) = url.find('@') {
        if let Some(scheme_end) = url.find("://") {
            let scheme_end = scheme_end + 3; // Position after "://"
            if at_pos > scheme_end {
                // There is userinfo to redact
                let before = &url[..scheme_end];
                let after = &url[at_pos..];
                return format!("{}***:***{}", before, after);
            }
        }
    }
    // No userinfo found, return as-is
    url.to_string()
}

/// Strips userinfo (username:password) from a NATS URL, returning just the scheme + host.
///
/// This is used when connecting with a credential file to avoid passing credentials twice.
pub fn strip_userinfo(url: &str) -> String {
    if let Some(at_pos) = url.find('@') {
        if let Some(scheme_end) = url.find("://") {
            let scheme_end = scheme_end + 3;
            if at_pos > scheme_end {
                // Rebuild URL without userinfo
                let before = &url[..scheme_end];
                let after = &url[(at_pos + 1)..];
                return format!("{}{}", before, after);
            }
        }
    }
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact_nats_url_with_userinfo() {
        assert_eq!(
            redact_nats_url("nats://user:pass@localhost:4222"),
            "nats://***:***@localhost:4222"
        );
        assert_eq!(
            redact_nats_url("nats://token@example.com:4222"),
            "nats://***:***@example.com:4222"
        );
    }

    #[test]
    fn test_redact_nats_url_without_userinfo() {
        assert_eq!(
            redact_nats_url("nats://localhost:4222"),
            "nats://localhost:4222"
        );
        assert_eq!(
            redact_nats_url("nats://example.com:4222"),
            "nats://example.com:4222"
        );
    }

    #[test]
    fn test_strip_userinfo() {
        assert_eq!(
            strip_userinfo("nats://user:pass@localhost:4222"),
            "nats://localhost:4222"
        );
        assert_eq!(
            strip_userinfo("nats://token@example.com:4222"),
            "nats://example.com:4222"
        );
        assert_eq!(
            strip_userinfo("nats://localhost:4222"),
            "nats://localhost:4222"
        );
    }
}
