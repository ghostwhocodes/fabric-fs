use std::io;
use std::sync::Arc;

use fs_core::{now_unix_nanos, Dispatch, FsError, RpcError, RpcMetadata};
use fs_protocol::{
    decode_request, encode_message, encode_response, validate_invalidation, Errno,
    InvalidationKind, RequestEnvelope, ResponseEnvelope,
};

use crate::auth::{TransportAuth, TransportAuthStatus};
use crate::policy::{
    authenticated_replay_or_expiry_response, deliver_prepared_response,
    response_handles_require_cleanup_after_deadline, AuthenticatedReplayCache, NamespaceGuard,
    NamespaceLocks, TransportResponseDelivery, MAX_AUTHENTICATED_REQUEST_TTL_NANOS,
};
use crate::subjects::{command_subject_parts, invalidation_subject, subscription_subject};

pub struct FileSystemServer {
    dispatcher: Arc<dyn Dispatch>,
    max_frame_bytes: usize,
    invalidation_mount: Option<String>,
    expected_namespace: Option<String>,
    transport_auth: Option<TransportAuth>,
    namespace_locks: NamespaceLocks,
    replay_cache: AuthenticatedReplayCache,
    max_authenticated_request_ttl_nanos: u64,
}

impl FileSystemServer {
    pub fn new(dispatcher: Arc<dyn Dispatch>) -> Self {
        Self {
            dispatcher,
            max_frame_bytes: 4 * 1024 * 1024,
            invalidation_mount: None,
            expected_namespace: None,
            transport_auth: None,
            namespace_locks: NamespaceLocks::default(),
            replay_cache: AuthenticatedReplayCache::default(),
            max_authenticated_request_ttl_nanos: MAX_AUTHENTICATED_REQUEST_TTL_NANOS,
        }
    }

    pub fn with_max_frame_bytes(mut self, max_frame_bytes: usize) -> Self {
        self.max_frame_bytes = max_frame_bytes;
        self
    }

    pub fn with_invalidation_mount(mut self, mount: impl Into<String>) -> Self {
        self.invalidation_mount = Some(mount.into());
        self
    }

    pub fn with_expected_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.expected_namespace = Some(namespace.into());
        self
    }

    pub fn with_transport_auth(mut self, transport_auth: TransportAuth) -> Self {
        self.transport_auth = Some(transport_auth);
        self
    }

    pub fn with_max_authenticated_request_ttl_nanos(mut self, ttl_nanos: u64) -> Self {
        self.max_authenticated_request_ttl_nanos = ttl_nanos.max(1);
        self
    }

    pub fn handle_bytes(&self, request_bytes: &[u8]) -> Result<Vec<u8>, RpcError> {
        self.response_for_bytes(None, request_bytes)
            .map(|(_, response_bytes)| response_bytes)
    }

    pub fn handle_subject_bytes(
        &self,
        subject: &str,
        request_bytes: &[u8],
    ) -> Result<Vec<u8>, RpcError> {
        self.response_for_bytes(Some(subject), request_bytes)
            .map(|(_, response_bytes)| response_bytes)
    }

    fn response_for_bytes(
        &self,
        subject: Option<&str>,
        request_bytes: &[u8],
    ) -> Result<(ResponseEnvelope, Vec<u8>), RpcError> {
        let request = self.decode_request_for_subject(subject, request_bytes)?;
        let _guard = self.namespace_locks.enter(&request.namespace)?;
        let outcome = self.dispatch_request(&request, request_bytes.len(), None, false)?;
        if self.request_expired_after_dispatch(&request, &outcome)? {
            return Err(RpcError::Timeout);
        }
        Ok((outcome.response, outcome.response_bytes))
    }

    pub fn handle_message(
        &self,
        connection: &nats::Connection,
        message: nats::Message,
    ) -> Result<(), RpcError> {
        let _span = tracing::debug_span!(
            "handle_nats_message",
            subject = %message.subject,
            has_reply = message.reply.is_some(),
            payload_len = message.data.len()
        )
        .entered();
        let prepared = self.response_for_message(&message)?;
        self.deliver_prepared_message_response(
            prepared,
            |invalidation| match &self.invalidation_mount {
                Some(mount) => publish_invalidation(connection, mount, invalidation),
                None => Ok(()),
            },
            |reply, response_bytes| {
                connection
                    .publish(reply, response_bytes)
                    .map_err(server_rpc_error_from_io)
            },
        )
    }

    #[cfg(test)]
    fn response_for_command_message(
        &self,
        subject: &str,
        reply: Option<&str>,
        headers: Option<nats::HeaderMap>,
        request_bytes: &[u8],
    ) -> Result<(String, ResponseEnvelope, Vec<u8>), RpcError> {
        let message = nats::Message::new(subject, reply, request_bytes, headers);
        let prepared = self.response_for_message(&message)?;
        let PreparedMessageResponse {
            reply,
            response,
            response_bytes,
            ..
        } = prepared;
        Ok((reply, response, response_bytes))
    }

    fn decode_request_for_subject(
        &self,
        subject: Option<&str>,
        request_bytes: &[u8],
    ) -> Result<RequestEnvelope, RpcError> {
        self.check_frame_len(request_bytes.len())?;
        let request = decode_request(request_bytes)
            .map_err(|error| RpcError::Malformed(error.to_string()))?;
        if let Some(subject) = subject {
            self.validate_subject_route(subject, &request)?;
        } else {
            self.validate_expected_namespace(&request)?;
        }
        Ok(request)
    }

    fn dispatch_request(
        &self,
        request: &RequestEnvelope,
        request_len: usize,
        peer_identity: Option<String>,
        trust_caller: bool,
    ) -> Result<DispatchOutcome, RpcError> {
        let _span = tracing::debug_span!(
            "dispatch_filesystem_request",
            request_id = %request.request_id,
            namespace = %request.namespace,
            operation = request.operation.as_str(),
            payload_len = request_len
        )
        .entered();
        let mut metadata = RpcMetadata::for_request(request, request_len as u64);
        metadata.peer_identity = peer_identity;
        if !trust_caller {
            metadata.caller = None;
        }
        let response = self
            .dispatcher
            .dispatch_request(request.clone(), metadata.clone());
        let response_bytes =
            encode_response(&response).map_err(|error| RpcError::Malformed(error.to_string()))?;
        self.check_frame_len(response_bytes.len())?;
        Ok(DispatchOutcome {
            metadata,
            response,
            response_bytes,
        })
    }

    fn response_for_message(
        &self,
        message: &nats::Message,
    ) -> Result<PreparedMessageResponse<'_>, RpcError> {
        let reply = message.reply.as_deref().ok_or_else(|| {
            RpcError::Malformed("NATS filesystem command is missing a reply subject".into())
        })?;
        let auth_status = self.authenticate_message(message);
        let request = self.decode_request_for_subject(Some(&message.subject), &message.data)?;
        match auth_status {
            TransportAuthStatus::Verified(verified_peer) => {
                if let Some(replay_response) = authenticated_replay_or_expiry_response(
                    &self.replay_cache,
                    self.max_authenticated_request_ttl_nanos,
                    &request,
                    &verified_peer,
                ) {
                    let response_bytes = encode_response(&replay_response)
                        .map_err(|error| RpcError::Malformed(error.to_string()))?;
                    self.check_frame_len(response_bytes.len())?;
                    return Ok(PreparedMessageResponse {
                        reply: reply.to_owned(),
                        request,
                        response: replay_response,
                        response_bytes,
                        dispatch_outcome: None,
                        _namespace_guard: None,
                    });
                }
                let namespace_guard = self.namespace_locks.enter(&request.namespace)?;
                let outcome = self.dispatch_request(
                    &request,
                    message.data.len(),
                    Some(verified_peer.peer_identity),
                    true,
                )?;
                Ok(PreparedMessageResponse {
                    reply: reply.to_owned(),
                    request,
                    response: outcome.response.clone(),
                    response_bytes: outcome.response_bytes.clone(),
                    dispatch_outcome: Some(outcome),
                    _namespace_guard: Some(namespace_guard),
                })
            }
            TransportAuthStatus::Missing | TransportAuthStatus::Invalid(_) => {
                let reason = auth_denial_reason(&auth_status);
                tracing::warn!(
                    subject = %message.subject,
                    request_id = %request.request_id,
                    namespace = %request.namespace,
                    operation = request.operation.as_str(),
                    %reason,
                    "rejecting unauthenticated filesystem request"
                );
                let response =
                    ResponseEnvelope::failure_for(&request, Errno::PermissionDenied, reason);
                let response_bytes = encode_response(&response)
                    .map_err(|error| RpcError::Malformed(error.to_string()))?;
                self.check_frame_len(response_bytes.len())?;
                Ok(PreparedMessageResponse {
                    reply: reply.to_owned(),
                    request,
                    response,
                    response_bytes,
                    dispatch_outcome: None,
                    _namespace_guard: None,
                })
            }
        }
    }

    fn authenticate_message(&self, message: &nats::Message) -> TransportAuthStatus {
        match &self.transport_auth {
            Some(transport_auth) => transport_auth.authenticate_message(message),
            None => TransportAuthStatus::Missing,
        }
    }

    fn request_expired_after_dispatch(
        &self,
        request: &RequestEnvelope,
        outcome: &DispatchOutcome,
    ) -> Result<bool, RpcError> {
        if !response_handles_require_cleanup_after_deadline(
            request,
            &outcome.response,
            now_unix_nanos(),
        ) {
            return Ok(false);
        }
        self.dispatcher
            .abort_response_handles(request, &outcome.metadata, &outcome.response)
            .map_err(dispatch_cleanup_error)?;
        Ok(true)
    }

    fn abort_response_handles(
        &self,
        request: &RequestEnvelope,
        outcome: &DispatchOutcome,
        original: RpcError,
    ) -> RpcError {
        match self
            .dispatcher
            .abort_response_handles(request, &outcome.metadata, &outcome.response)
        {
            Ok(()) => original,
            Err(error) => RpcError::Transport(format!(
                "{original}; failed to release abandoned handle: {error}"
            )),
        }
    }

    fn validate_subject_route(
        &self,
        subject: &str,
        request: &RequestEnvelope,
    ) -> Result<(), RpcError> {
        let subject = command_subject_parts(subject).ok_or_else(|| {
            RpcError::Malformed(format!(
                "NATS subject is not a filesystem command: {subject}"
            ))
        })?;
        if subject.operation != request.operation {
            return Err(RpcError::Malformed(format!(
                "NATS subject operation {:?} does not match envelope operation {:?}",
                subject.operation, request.operation
            )));
        }
        if let Some(expected_namespace) = &self.expected_namespace {
            if subject.mount != *expected_namespace {
                return Err(RpcError::Malformed(format!(
                    "NATS subject mount {} does not match server namespace {}",
                    subject.mount, expected_namespace
                )));
            }
        }
        self.validate_expected_namespace(request)
    }

    fn validate_expected_namespace(&self, request: &RequestEnvelope) -> Result<(), RpcError> {
        if let Some(expected_namespace) = &self.expected_namespace {
            if request.namespace != *expected_namespace {
                return Err(RpcError::Malformed(format!(
                    "request namespace {} does not match server namespace {}",
                    request.namespace, expected_namespace
                )));
            }
        }
        Ok(())
    }

    fn check_frame_len(&self, frame_len: usize) -> Result<(), RpcError> {
        if frame_len <= self.max_frame_bytes {
            Ok(())
        } else {
            Err(RpcError::FrameTooLarge)
        }
    }

    fn deliver_prepared_message_response<PI, PR>(
        &self,
        prepared: PreparedMessageResponse<'_>,
        publish_invalidation: PI,
        publish_reply: PR,
    ) -> Result<(), RpcError>
    where
        PI: FnMut(&fs_protocol::pb::Invalidation) -> Result<(), RpcError>,
        PR: FnMut(&str, &[u8]) -> Result<(), RpcError>,
    {
        deliver_prepared_response(
            TransportResponseDelivery {
                request: &prepared.request,
                response: &prepared.response,
                response_bytes: &prepared.response_bytes,
                reply: &prepared.reply,
            },
            publish_invalidation,
            publish_reply,
            |error| prepared.abort_response_handles(self, error),
            || prepared.cleanup_expired_response_handles(self),
        )
    }
}

struct DispatchOutcome {
    metadata: RpcMetadata,
    response: ResponseEnvelope,
    response_bytes: Vec<u8>,
}

struct PreparedMessageResponse<'a> {
    reply: String,
    request: RequestEnvelope,
    response: ResponseEnvelope,
    response_bytes: Vec<u8>,
    dispatch_outcome: Option<DispatchOutcome>,
    _namespace_guard: Option<NamespaceGuard<'a>>,
}

impl PreparedMessageResponse<'_> {
    fn abort_response_handles(&self, server: &FileSystemServer, original: RpcError) -> RpcError {
        match &self.dispatch_outcome {
            Some(outcome) => server.abort_response_handles(&self.request, outcome, original),
            None => original,
        }
    }

    fn cleanup_expired_response_handles(&self, server: &FileSystemServer) -> Result<(), RpcError> {
        let Some(outcome) = &self.dispatch_outcome else {
            return Ok(());
        };
        server
            .dispatcher
            .abort_response_handles(&self.request, &outcome.metadata, &outcome.response)
            .map_err(dispatch_cleanup_error)
    }
}

pub fn subscribe_requests(
    connection: &nats::Connection,
    mount: &str,
) -> Result<nats::Subscription, RpcError> {
    let subscription = connection
        .subscribe(&subscription_subject(mount))
        .map_err(server_rpc_error_from_io)?;
    connection.flush().map_err(server_rpc_error_from_io)?;
    Ok(subscription)
}

pub fn publish_full_resync(
    connection: &nats::Connection,
    mount: &str,
    namespace: &str,
) -> Result<(), RpcError> {
    let invalidation = fs_protocol::pb::Invalidation {
        namespace: namespace.to_string(),
        sequence: 0,
        kind: InvalidationKind::FullResync.wire_value(),
        path: String::new(),
        old_path: String::new(),
        new_path: String::new(),
        inode: 0,
        handle: 0,
        request_id: "server-start".into(),
    };
    validate_invalidation(&invalidation).map_err(|error| RpcError::Malformed(error.to_string()))?;
    let bytes =
        encode_message(&invalidation).map_err(|error| RpcError::Malformed(error.to_string()))?;
    connection
        .publish(&invalidation_subject(mount), bytes)
        .map_err(server_rpc_error_from_io)?;
    connection.flush().map_err(server_rpc_error_from_io)
}

pub fn publish_invalidation(
    connection: &nats::Connection,
    mount: &str,
    invalidation: &fs_protocol::pb::Invalidation,
) -> Result<(), RpcError> {
    validate_invalidation(invalidation).map_err(|error| RpcError::Malformed(error.to_string()))?;
    let bytes =
        encode_message(invalidation).map_err(|error| RpcError::Malformed(error.to_string()))?;
    connection
        .publish(&invalidation_subject(mount), bytes)
        .map_err(server_rpc_error_from_io)
}

fn auth_denial_reason(status: &TransportAuthStatus) -> String {
    match status {
        TransportAuthStatus::Verified(_) => {
            "transport authentication unexpectedly succeeded".into()
        }
        TransportAuthStatus::Missing => "transport authentication headers are missing".into(),
        TransportAuthStatus::Invalid(reason) => reason.clone(),
    }
}

fn server_rpc_error_from_io(error: io::Error) -> RpcError {
    match error.kind() {
        io::ErrorKind::ConnectionAborted
        | io::ErrorKind::ConnectionRefused
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::NotConnected => RpcError::ConnectionClosed,
        io::ErrorKind::TimedOut => RpcError::Timeout,
        _ => RpcError::Transport(error.to_string()),
    }
}

fn dispatch_cleanup_error(error: FsError) -> RpcError {
    match error.errno {
        fs_protocol::Errno::TimedOut => RpcError::Timeout,
        fs_protocol::Errno::MessageTooLarge => RpcError::FrameTooLarge,
        fs_protocol::Errno::ConnectionReset => RpcError::ConnectionClosed,
        fs_protocol::Errno::InvalidArgument => RpcError::Malformed(error.message),
        _ => RpcError::Transport(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs_core::{Dispatcher, FileSystemService, FsResult, RpcMetadata};
    use fs_protocol::{encode_request, pb, Errno, Operation, ResponsePayload};
    use fs_testkit::{
        file_attr, request_for_operation, request_for_operation_in_namespace, RecordingFs,
    };
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::sync::Mutex;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn no_reply_command_messages_are_rejected_before_dispatch() {
        let service = RecordingFs::default();
        let server = FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
            .with_invalidation_mount("demo");
        let request = request_for_operation(Operation::Write, "no-reply-command");
        let bytes = encode_request(&request).expect("request encodes");

        let error = server
            .response_for_command_message(
                &crate::subjects::command_subject("demo", Operation::Write.as_str()),
                None,
                None,
                &bytes,
            )
            .expect_err("missing reply must be rejected");

        assert!(matches!(error, RpcError::Malformed(message) if message.contains("reply")));
        assert!(
            service.calls().is_empty(),
            "fire-and-forget filesystem commands must not mutate storage"
        );
    }

    #[test]
    fn unauthenticated_command_messages_are_rejected_before_dispatch() {
        let service = RecordingFs::default();
        let auth = TransportAuth::shared_secret("shared-secret").expect("auth config");
        let server = FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
            .with_transport_auth(auth)
            .with_expected_namespace("demo");
        let request = request_for_operation_in_namespace(Operation::Write, "missing-auth", "demo");
        let bytes = encode_request(&request).expect("request encodes");

        let (_reply, response, _bytes) = server
            .response_for_command_message(
                &crate::subjects::command_subject("demo", Operation::Write.as_str()),
                Some("_INBOX.demo"),
                None,
                &bytes,
            )
            .expect("server returns permission denial response");

        assert!(!response.ok);
        assert_eq!(response.errno, Some(Errno::PermissionDenied));
        assert!(response.invalidations.is_empty());
        assert!(
            service.calls().is_empty(),
            "unauthenticated command must not dispatch to storage"
        );
    }

    #[test]
    fn authenticated_command_message_replays_are_rejected_before_dispatch() {
        let service = RecordingFs::default();
        let auth = TransportAuth::shared_secret("shared-secret").expect("auth config");
        let server = FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
            .with_transport_auth(auth.clone())
            .with_expected_namespace("demo");
        let mut request =
            request_for_operation_in_namespace(Operation::Write, "signed-replay", "demo");
        request.deadline_unix_nanos = now_unix_nanos().saturating_add(1_000_000_000);
        let bytes = encode_request(&request).expect("request encodes");
        let subject = crate::subjects::command_subject("demo", Operation::Write.as_str());
        let headers = auth.headers_for(&subject, "_INBOX.demo", &bytes);

        let (_reply, response, _bytes) = server
            .response_for_command_message(
                &subject,
                Some("_INBOX.demo"),
                Some(headers.clone()),
                &bytes,
            )
            .expect("first authenticated request succeeds");
        assert!(response.ok);

        let (_reply, replay_response, _bytes) = server
            .response_for_command_message(&subject, Some("_INBOX.demo"), Some(headers), &bytes)
            .expect("replay is rejected with a concrete response");
        assert!(!replay_response.ok);
        assert_eq!(replay_response.errno, Some(Errno::PermissionDenied));
        assert!(
            replay_response.error_message.contains("replay detected"),
            "expected replay denial, got {replay_response:?}"
        );
        assert_eq!(service.calls(), vec![Operation::Write]);
    }

    #[test]
    fn authenticated_command_messages_without_deadlines_are_rejected_before_dispatch() {
        let service = RecordingFs::default();
        let auth = TransportAuth::shared_secret("shared-secret").expect("auth config");
        let server = FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
            .with_transport_auth(auth.clone())
            .with_expected_namespace("demo");
        let mut request =
            request_for_operation_in_namespace(Operation::Write, "missing-deadline", "demo");
        request.deadline_unix_nanos = 0;
        let bytes = encode_request(&request).expect("request encodes");
        let subject = crate::subjects::command_subject("demo", Operation::Write.as_str());
        let headers = auth.headers_for(&subject, "_INBOX.demo", &bytes);

        let (_reply, response, _bytes) = server
            .response_for_command_message(&subject, Some("_INBOX.demo"), Some(headers), &bytes)
            .expect("missing deadline returns validation error response");

        assert!(!response.ok);
        assert_eq!(response.errno, Some(Errno::InvalidArgument));
        assert!(
            response.error_message.contains("must include a deadline"),
            "expected deadline validation error, got {response:?}"
        );
        assert!(service.calls().is_empty());
    }

    #[test]
    fn authenticated_command_message_allows_client_deadline_above_thirty_seconds() {
        let service = RecordingFs::default();
        let auth = TransportAuth::shared_secret("shared-secret").expect("auth config");
        let server = FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
            .with_transport_auth(auth.clone())
            .with_expected_namespace("demo");
        let mut request =
            request_for_operation_in_namespace(Operation::Write, "long-client-timeout", "demo");
        request.deadline_unix_nanos = now_unix_nanos().saturating_add(60_000_000_000);
        let bytes = encode_request(&request).expect("request encodes");
        let subject = crate::subjects::command_subject("demo", Operation::Write.as_str());
        let headers = auth.headers_for(&subject, "_INBOX.demo", &bytes);

        let (_reply, response, _bytes) = server
            .response_for_command_message(&subject, Some("_INBOX.demo"), Some(headers), &bytes)
            .expect("long authenticated deadline dispatches");

        assert!(
            response.ok,
            "long authenticated deadline failed: {response:?}"
        );
        assert_eq!(service.calls(), vec![Operation::Write]);
    }

    #[test]
    fn authenticated_command_messages_with_excessive_deadlines_are_rejected_without_pinning_replay_cache(
    ) {
        let service = RecordingFs::default();
        let auth = TransportAuth::shared_secret("shared-secret").expect("auth config");
        let server = FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
            .with_transport_auth(auth.clone())
            .with_expected_namespace("demo")
            .with_max_authenticated_request_ttl_nanos(1_000);
        let mut request =
            request_for_operation_in_namespace(Operation::Write, "excessive-deadline", "demo");
        request.deadline_unix_nanos = now_unix_nanos().saturating_add(1_000_000_000);
        let bytes = encode_request(&request).expect("request encodes");
        let subject = crate::subjects::command_subject("demo", Operation::Write.as_str());
        let headers = auth.headers_for(&subject, "_INBOX.demo", &bytes);

        let (_reply, response, _bytes) = server
            .response_for_command_message(
                &subject,
                Some("_INBOX.demo"),
                Some(headers.clone()),
                &bytes,
            )
            .expect("oversized deadline returns validation error response");

        assert!(!response.ok);
        assert_eq!(response.errno, Some(Errno::InvalidArgument));
        assert!(
            response.error_message.contains("maximum TTL"),
            "expected maximum TTL validation error, got {response:?}"
        );
        assert!(service.calls().is_empty());
        assert_eq!(
            server.replay_cache.len(),
            0,
            "rejected deadlines must not retain authenticated replay entries"
        );

        let (_reply, replay_response, _bytes) = server
            .response_for_command_message(&subject, Some("_INBOX.demo"), Some(headers), &bytes)
            .expect("rejected oversized deadlines stay stateless");
        assert!(!replay_response.ok);
        assert_eq!(replay_response.errno, Some(Errno::InvalidArgument));
        assert_eq!(server.replay_cache.len(), 0);
        assert!(service.calls().is_empty());
    }

    #[test]
    fn authenticated_replay_tokens_expire_without_subsequent_claims() {
        let service = RecordingFs::default();
        let auth = TransportAuth::shared_secret("shared-secret").expect("auth config");
        let server = FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
            .with_transport_auth(auth.clone())
            .with_expected_namespace("demo")
            .with_max_authenticated_request_ttl_nanos(50_000_000);
        let mut request =
            request_for_operation_in_namespace(Operation::Write, "replay-expiry", "demo");
        request.deadline_unix_nanos = now_unix_nanos().saturating_add(50_000_000);
        let bytes = encode_request(&request).expect("request encodes");
        let subject = crate::subjects::command_subject("demo", Operation::Write.as_str());
        let headers = auth.headers_for(&subject, "_INBOX.demo", &bytes);

        let (_reply, response, _bytes) = server
            .response_for_command_message(&subject, Some("_INBOX.demo"), Some(headers), &bytes)
            .expect("first authenticated request succeeds");
        assert!(response.ok);
        assert_eq!(server.replay_cache.len(), 1);

        let deadline = Instant::now() + Duration::from_millis(500);
        while Instant::now() < deadline {
            if server.replay_cache.len() == 0 {
                assert_eq!(service.calls(), vec![Operation::Write]);
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }

        panic!("authenticated replay token did not expire while the server was idle");
    }

    #[test]
    fn same_namespace_command_publication_stays_ordered_through_delivery() {
        let service = RecordingFs::default();
        let auth = TransportAuth::shared_secret("shared-secret").expect("auth config");
        let server = Arc::new(
            FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
                .with_invalidation_mount("demo")
                .with_transport_auth(auth.clone())
                .with_expected_namespace("demo"),
        );
        let subject = crate::subjects::command_subject("demo", Operation::Write.as_str());

        let mut first_request =
            request_for_operation_in_namespace(Operation::Write, "ordered-first", "demo");
        first_request.deadline_unix_nanos = now_unix_nanos().saturating_add(1_000_000_000);
        let first_bytes = encode_request(&first_request).expect("first request encodes");
        let first_headers = auth.headers_for(&subject, "_INBOX.first", &first_bytes);
        let first_message = nats::Message::new(
            &subject,
            Some("_INBOX.first"),
            &first_bytes,
            Some(first_headers),
        );
        let prepared = server
            .response_for_message(&first_message)
            .expect("first request dispatches");
        assert_eq!(prepared.response.invalidations.len(), 1);
        assert_eq!(prepared.response.invalidations[0].sequence, 1);

        let mut second_request =
            request_for_operation_in_namespace(Operation::Write, "ordered-second", "demo");
        second_request.deadline_unix_nanos = now_unix_nanos().saturating_add(1_000_000_000);
        let second_bytes = encode_request(&second_request).expect("second request encodes");
        let second_headers = auth.headers_for(&subject, "_INBOX.second", &second_bytes);
        let second_message = nats::Message::new(
            &subject,
            Some("_INBOX.second"),
            &second_bytes,
            Some(second_headers),
        );

        let (second_done_tx, second_done_rx) = mpsc::channel();
        let second_server = Arc::clone(&server);
        let second_call = thread::spawn(move || {
            let prepared = second_server
                .response_for_message(&second_message)
                .expect("second request dispatches after the first delivery finishes");
            second_done_tx
                .send(prepared.response.invalidations[0].sequence)
                .expect("second sequence sends");
        });

        server
            .deliver_prepared_message_response(
                prepared,
                |_invalidation| {
                    assert!(
                        second_done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
                        "same-namespace command publication must stay blocked until the first invalidation finishes publishing"
                    );
                    Ok(())
                },
                |_reply, _bytes| Ok(()),
            )
            .expect("first delivery succeeds");

        assert_eq!(
            second_done_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("second request completes after first delivery"),
            2
        );
        second_call.join().expect("second thread joins");
        assert_eq!(service.calls(), vec![Operation::Write, Operation::Write]);
    }

    #[test]
    fn create_handles_are_released_when_reply_deadline_expires_before_reply_publish() {
        let service = CreateHandleFs::default();
        let auth = TransportAuth::shared_secret("shared-secret").expect("auth config");
        let server = FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
            .with_invalidation_mount("demo")
            .with_transport_auth(auth.clone())
            .with_expected_namespace("demo");
        let mut request =
            request_for_operation_in_namespace(Operation::Create, "delayed-create-publish", "demo");
        request.deadline_unix_nanos = now_unix_nanos().saturating_add(30_000_000);
        let subject = crate::subjects::command_subject("demo", Operation::Create.as_str());
        let bytes = encode_request(&request).expect("request encodes");
        let reply = "_INBOX.demo";
        let headers = auth.headers_for(&subject, reply, &bytes);
        let message = nats::Message::new(&subject, Some(reply), &bytes, Some(headers));
        let prepared = server
            .response_for_message(&message)
            .expect("create dispatch succeeds before publish");
        assert!(matches!(
            prepared.response.payload.as_ref(),
            Some(ResponsePayload::Create(_))
        ));
        thread::sleep(Duration::from_millis(80));

        let reply_published = AtomicBool::new(false);
        let error = server
            .deliver_prepared_message_response(
                prepared,
                |_invalidation| Ok(()),
                |_reply, _bytes| {
                    reply_published.store(true, Ordering::SeqCst);
                    Ok(())
                },
            )
            .expect_err("expired create reply must release its abandoned handle");

        assert_eq!(error, RpcError::Timeout);
        assert!(!reply_published.load(Ordering::SeqCst));
        assert_eq!(service.create_calls(), 1);
        assert_eq!(service.release_calls(), 1);
    }

    #[test]
    fn create_invalidations_are_still_published_when_dispatch_finishes_after_deadline() {
        let service = SlowCreateFs::new(Duration::from_millis(80));
        let auth = TransportAuth::shared_secret("shared-secret").expect("auth config");
        let server = FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
            .with_invalidation_mount("demo")
            .with_transport_auth(auth.clone())
            .with_expected_namespace("demo");
        let mut request = request_for_operation_in_namespace(
            Operation::Create,
            "expired-create-recovery",
            "demo",
        );
        request.deadline_unix_nanos = now_unix_nanos().saturating_add(20_000_000);
        let deadline = request.deadline_unix_nanos;
        let subject = crate::subjects::command_subject("demo", Operation::Create.as_str());
        let bytes = encode_request(&request).expect("request encodes");
        let reply = "_INBOX.demo";
        let headers = auth.headers_for(&subject, reply, &bytes);
        let message = nats::Message::new(&subject, Some(reply), &bytes, Some(headers));

        let prepared = server
            .response_for_message(&message)
            .expect("create dispatch succeeds even if only the reply deadline is missed");
        assert!(
            now_unix_nanos() > deadline,
            "dispatch delay must move beyond the request deadline"
        );

        let published_invalidations = Mutex::new(Vec::new());
        let reply_published = AtomicBool::new(false);
        let error = server
            .deliver_prepared_message_response(
                prepared,
                |invalidation| {
                    published_invalidations
                        .lock()
                        .expect("published invalidations lock")
                        .push(invalidation.clone());
                    Ok(())
                },
                |_reply, _bytes| {
                    reply_published.store(true, Ordering::SeqCst);
                    Ok(())
                },
            )
            .expect_err("expired create reply must still release the abandoned handle");

        let invalidations = published_invalidations
            .lock()
            .expect("published invalidations lock");
        assert_eq!(error, RpcError::Timeout);
        assert_eq!(invalidations.len(), 1);
        assert_eq!(invalidations[0].kind, InvalidationKind::Create.wire_value());
        assert_eq!(invalidations[0].sequence, 1);
        assert!(!reply_published.load(Ordering::SeqCst));
        assert_eq!(service.create_calls(), 1);
        assert_eq!(service.release_calls(), 1);
    }

    #[test]
    fn open_handles_are_released_when_reply_publish_itself_misses_deadline() {
        let service = OpenHandleFs::default();
        let auth = TransportAuth::shared_secret("shared-secret").expect("auth config");
        let server = FileSystemServer::new(Arc::new(Dispatcher::new(service.clone())))
            .with_transport_auth(auth.clone())
            .with_expected_namespace("demo");
        let mut request =
            request_for_operation_in_namespace(Operation::Open, "delayed-open-publish", "demo");
        request.deadline_unix_nanos = now_unix_nanos().saturating_add(30_000_000);
        let deadline = request.deadline_unix_nanos;
        let subject = crate::subjects::command_subject("demo", Operation::Open.as_str());
        let bytes = encode_request(&request).expect("request encodes");
        let reply = "_INBOX.demo";
        let headers = auth.headers_for(&subject, reply, &bytes);
        let message = nats::Message::new(&subject, Some(reply), &bytes, Some(headers));
        let prepared = server
            .response_for_message(&message)
            .expect("open dispatch succeeds before publish");
        assert!(matches!(
            prepared.response.payload.as_ref(),
            Some(ResponsePayload::Open(_))
        ));

        let reply_published = AtomicBool::new(false);
        let error = server
            .deliver_prepared_message_response(
                prepared,
                |_invalidation| Ok(()),
                |_reply, _bytes| {
                    reply_published.store(true, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(80));
                    assert!(
                        now_unix_nanos() > deadline,
                        "reply publish delay must move beyond the request deadline"
                    );
                    Ok(())
                },
            )
            .expect_err("expired open reply must release its abandoned handle");

        assert_eq!(error, RpcError::Timeout);
        assert!(reply_published.load(Ordering::SeqCst));
        assert_eq!(service.open_calls(), 1);
        assert_eq!(service.release_calls(), 1);
    }

    #[derive(Clone, Default)]
    struct OpenHandleFs {
        opens: Arc<AtomicU64>,
        releases: Arc<AtomicU64>,
    }

    impl OpenHandleFs {
        fn open_calls(&self) -> u64 {
            self.opens.load(Ordering::SeqCst)
        }

        fn release_calls(&self) -> u64 {
            self.releases.load(Ordering::SeqCst)
        }
    }

    impl FileSystemService for OpenHandleFs {
        fn open(
            &self,
            _request: &pb::OpenRequest,
            _metadata: &RpcMetadata,
        ) -> FsResult<pb::OpenResponse> {
            let handle = self.opens.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(pb::OpenResponse { handle, flags: 0 })
        }

        fn release(
            &self,
            _request: &pb::ReleaseRequest,
            _metadata: &RpcMetadata,
        ) -> FsResult<pb::EmptyResponse> {
            self.releases.fetch_add(1, Ordering::SeqCst);
            Ok(pb::EmptyResponse {})
        }
    }

    #[derive(Clone, Default)]
    struct CreateHandleFs {
        creates: Arc<AtomicU64>,
        releases: Arc<AtomicU64>,
    }

    impl CreateHandleFs {
        fn create_calls(&self) -> u64 {
            self.creates.load(Ordering::SeqCst)
        }

        fn release_calls(&self) -> u64 {
            self.releases.load(Ordering::SeqCst)
        }
    }

    impl FileSystemService for CreateHandleFs {
        fn create(
            &self,
            _request: &pb::CreateRequest,
            _metadata: &RpcMetadata,
        ) -> FsResult<pb::CreateResponse> {
            let handle = self.creates.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(pb::CreateResponse {
                attr: Some(file_attr(100 + handle)),
                handle,
            })
        }

        fn release(
            &self,
            _request: &pb::ReleaseRequest,
            _metadata: &RpcMetadata,
        ) -> FsResult<pb::EmptyResponse> {
            self.releases.fetch_add(1, Ordering::SeqCst);
            Ok(pb::EmptyResponse {})
        }
    }

    #[derive(Clone)]
    struct SlowCreateFs {
        delay: Duration,
        creates: Arc<AtomicU64>,
        releases: Arc<AtomicU64>,
    }

    impl SlowCreateFs {
        fn new(delay: Duration) -> Self {
            Self {
                delay,
                creates: Arc::new(AtomicU64::new(0)),
                releases: Arc::new(AtomicU64::new(0)),
            }
        }

        fn create_calls(&self) -> u64 {
            self.creates.load(Ordering::SeqCst)
        }

        fn release_calls(&self) -> u64 {
            self.releases.load(Ordering::SeqCst)
        }
    }

    impl FileSystemService for SlowCreateFs {
        fn create(
            &self,
            _request: &pb::CreateRequest,
            _metadata: &RpcMetadata,
        ) -> FsResult<pb::CreateResponse> {
            let handle = self.creates.fetch_add(1, Ordering::SeqCst) + 1;
            thread::sleep(self.delay);
            Ok(pb::CreateResponse {
                attr: Some(file_attr(100 + handle)),
                handle,
            })
        }

        fn release(
            &self,
            _request: &pb::ReleaseRequest,
            _metadata: &RpcMetadata,
        ) -> FsResult<pb::EmptyResponse> {
            self.releases.fetch_add(1, Ordering::SeqCst);
            Ok(pb::EmptyResponse {})
        }
    }
}
