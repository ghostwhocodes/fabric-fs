/// Utility functions for NATS connection handling.
use std::io;

/// Establishes a NATS connection using either a credential file or a plain URL.
///
/// Priority order:
/// 1. If `creds_file` is provided, use it with the base URL (userinfo stripped)
/// 2. Otherwise, use the URL directly (may contain embedded credentials)
///
/// # Arguments
///
/// * `nats_url` - NATS server URL (e.g., "nats://localhost:4222")
/// * `creds_file` - Optional path to NATS credentials file
///
/// # Returns
///
/// A connected `nats::Connection` or an IO error with redacted URL in the message
///
/// # Examples
///
/// ```no_run
/// use fabricfs_transport::connect_nats;
///
/// // With credential file
/// let conn = connect_nats("nats://nats.example.com:4222", Some("/path/to/creds"))?;
///
/// // With plain URL
/// let conn = connect_nats("nats://localhost:4222", None)?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn connect_nats(
    nats_url: &str,
    creds_file: Option<&str>,
) -> Result<nats::Connection, io::Error> {
    let connection = if let Some(creds_path) = creds_file {
        // Use credential file - strip any userinfo from URL first
        let clean_url = strip_userinfo(nats_url);
        nats::Options::with_credentials(creds_path)
            .connect(&clean_url)
            .map_err(|e| {
                io::Error::other(format!(
                    "failed to connect to NATS at {} with credentials file {}: {}",
                    redact_nats_url(&clean_url),
                    creds_path,
                    e
                ))
            })?
    } else {
        // Use URL directly (may contain embedded credentials)
        nats::connect(nats_url).map_err(|e| {
            io::Error::other(format!(
                "failed to connect to NATS at {}: {}",
                redact_nats_url(nats_url),
                e
            ))
        })?
    };

    Ok(connection)
}

pub fn strip_userinfo(url: &str) -> String {
    if let Some((scheme, rest)) = url.split_once("://") {
        if let Some(at_pos) = rest.find('@') {
            return format!("{}://{}", scheme, &rest[at_pos + 1..]);
        }
    }
    url.to_string()
}

pub fn redact_nats_url(url: &str) -> String {
    if let Some((scheme, rest)) = url.split_once("://") {
        if let Some(at_pos) = rest.find('@') {
            return format!("{}://***:***@{}", scheme, &rest[at_pos + 1..]);
        }
    }
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connect_nats_redacts_url_in_error() {
        // This test verifies that URLs are redacted in error messages
        let result = connect_nats("nats://user:pass@nonexistent:9999", None);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        // Error should contain redacted URL, not raw credentials
        assert!(err_msg.contains("***:***"));
        assert!(!err_msg.contains("user:pass"));
    }
}
