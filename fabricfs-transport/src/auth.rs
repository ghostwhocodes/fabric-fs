use fs_core::RpcError;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

const AUTH_VERSION: &str = "v1";
const HEADER_AUTH_VERSION: &str = "X-FabricFs-Transport-Auth-Version";
const HEADER_KEY_ID: &str = "X-FabricFs-Transport-Key-Id";
const HEADER_SIGNATURE: &str = "X-FabricFs-Transport-Signature";
const PEER_IDENTITY_PREFIX: &str = "nats-auth:";

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct TransportAuth {
    key_id: String,
    secret: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedTransportPeer {
    pub peer_identity: String,
    pub replay_token: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportAuthStatus {
    Verified(VerifiedTransportPeer),
    Missing,
    Invalid(String),
}

impl TransportAuth {
    pub fn shared_secret(secret: impl AsRef<[u8]>) -> Result<Self, RpcError> {
        let secret = secret.as_ref();
        if secret.is_empty() {
            return Err(RpcError::Malformed(
                "transport authentication secret must not be empty".into(),
            ));
        }

        Ok(Self {
            key_id: key_id(secret),
            secret: secret.to_vec(),
        })
    }

    pub fn peer_identity(&self) -> String {
        format!("{PEER_IDENTITY_PREFIX}{}", self.key_id)
    }

    pub fn headers_for(&self, subject: &str, reply: &str, payload: &[u8]) -> nats::HeaderMap {
        let mut headers = nats::HeaderMap::new();
        headers.insert(HEADER_AUTH_VERSION, AUTH_VERSION);
        headers.insert(HEADER_KEY_ID, self.key_id.clone());
        headers.insert(
            HEADER_SIGNATURE,
            self.signature_hex(subject, reply, payload),
        );
        headers
    }

    pub fn authenticate_message(&self, message: &nats::Message) -> TransportAuthStatus {
        self.authenticate_headers(
            &message.subject,
            message.reply.as_deref().unwrap_or_default(),
            &message.data,
            message.headers.as_ref(),
        )
    }

    fn authenticate_headers(
        &self,
        subject: &str,
        reply: &str,
        payload: &[u8],
        headers: Option<&nats::HeaderMap>,
    ) -> TransportAuthStatus {
        let Some(headers) = headers else {
            return TransportAuthStatus::Missing;
        };

        if headers.get(HEADER_AUTH_VERSION).map(String::as_str) != Some(AUTH_VERSION) {
            return TransportAuthStatus::Invalid(
                "transport authentication version is missing or unsupported".into(),
            );
        }

        if headers.get(HEADER_KEY_ID).map(String::as_str) != Some(self.key_id.as_str()) {
            return TransportAuthStatus::Invalid(
                "transport authentication key id does not match server expectation".into(),
            );
        }

        let Some(signature_hex) = headers.get(HEADER_SIGNATURE) else {
            return TransportAuthStatus::Invalid(
                "transport authentication signature is missing".into(),
            );
        };
        let Ok(signature) = hex::decode(signature_hex) else {
            return TransportAuthStatus::Invalid(
                "transport authentication signature is not valid hex".into(),
            );
        };
        let Ok(mut mac) = HmacSha256::new_from_slice(&self.secret) else {
            return TransportAuthStatus::Invalid(
                "transport authentication secret is not usable".into(),
            );
        };
        mac.update(&canonical_signing_bytes(subject, reply, payload));
        if mac.verify_slice(&signature).is_err() {
            return TransportAuthStatus::Invalid(
                "transport authentication signature does not match request bytes".into(),
            );
        }

        TransportAuthStatus::Verified(VerifiedTransportPeer {
            peer_identity: self.peer_identity(),
            replay_token: hex::encode(signature),
        })
    }

    fn signature_hex(&self, subject: &str, reply: &str, payload: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(&self.secret).expect("checked in constructor");
        mac.update(&canonical_signing_bytes(subject, reply, payload));
        hex::encode(mac.finalize().into_bytes())
    }
}

pub fn is_verified_nats_peer_identity(peer_identity: &str) -> bool {
    peer_identity.starts_with(PEER_IDENTITY_PREFIX)
}

fn key_id(secret: &[u8]) -> String {
    let digest = Sha256::digest(secret);
    hex::encode(&digest[..8])
}

fn canonical_signing_bytes(subject: &str, reply: &str, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(subject.len() + reply.len() + payload.len() + 32);
    write_segment(&mut bytes, AUTH_VERSION.as_bytes());
    write_segment(&mut bytes, subject.as_bytes());
    write_segment(&mut bytes, reply.as_bytes());
    write_segment(&mut bytes, payload);
    bytes
}

fn write_segment(buf: &mut Vec<u8>, segment: &[u8]) {
    buf.extend_from_slice(&(segment.len() as u64).to_le_bytes());
    buf.extend_from_slice(segment);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_auth_round_trips_headers() {
        let auth = TransportAuth::shared_secret("shared-secret").expect("auth config");
        let headers = auth.headers_for("fabricfs.v1.demo.lookup", "_INBOX.123", b"payload");
        let message = nats::Message::new(
            "fabricfs.v1.demo.lookup",
            Some("_INBOX.123"),
            b"payload",
            Some(headers),
        );

        assert_eq!(
            auth.authenticate_message(&message),
            TransportAuthStatus::Verified(VerifiedTransportPeer {
                peer_identity: auth.peer_identity(),
                replay_token: auth.signature_hex(
                    "fabricfs.v1.demo.lookup",
                    "_INBOX.123",
                    b"payload"
                ),
            })
        );
    }

    #[test]
    fn transport_auth_rejects_subject_tampering() {
        let auth = TransportAuth::shared_secret("shared-secret").expect("auth config");
        let headers = auth.headers_for("fabricfs.v1.demo.lookup", "_INBOX.123", b"payload");
        let message = nats::Message::new(
            "fabricfs.v1.demo.write",
            Some("_INBOX.123"),
            b"payload",
            Some(headers),
        );

        assert!(matches!(
            auth.authenticate_message(&message),
            TransportAuthStatus::Invalid(message)
                if message.contains("does not match request bytes")
        ));
    }

    #[test]
    fn verified_peer_identity_uses_transport_prefix() {
        let auth = TransportAuth::shared_secret("shared-secret").expect("auth config");
        assert!(is_verified_nats_peer_identity(&auth.peer_identity()));
        assert!(!is_verified_nats_peer_identity("nats"));
    }
}
