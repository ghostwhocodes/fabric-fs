use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fabricfs_observability::{AtomicHistogram, HistogramSnapshot, LATENCY_BUCKETS_MICROS};
use fs_core::{RpcClient as CommonRpcClient, RpcError};
use fs_protocol::{
    decode_message, decode_request, decode_response, encode_request, encode_response, pb,
    validate_invalidation, RequestEnvelope, ResponseEnvelope,
};
use nats::Connection;

use crate::auth::TransportAuth;
use crate::policy::{
    can_retry_after_publish, can_retry_after_publish_error, effective_transport_deadline,
    OwnInvalidationReplay,
};
use crate::subjects::{command_subject_for_operation, invalidation_subject};

#[derive(Debug, Clone)]
pub struct FileSystemClientConfig {
    pub timeout: Duration,
    pub max_retries: usize,
    pub retry_backoff: Duration,
    pub max_frame_bytes: usize,
    pub transport_auth: Option<TransportAuth>,
}

impl Default for FileSystemClientConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            max_retries: 2,
            retry_backoff: Duration::from_millis(50),
            max_frame_bytes: 4 * 1024 * 1024,
            transport_auth: None,
        }
    }
}

#[derive(Clone)]
pub struct FileSystemClient {
    connection: Connection,
    mount: String,
    config: FileSystemClientConfig,
    request_id_scope: String,
    connected: Arc<AtomicBool>,
    invalidations: nats::Subscription,
    own_request_ids: Arc<Mutex<OwnInvalidationReplay>>,
    metrics: Arc<ClientMetrics>,
}

struct ClientMetrics {
    calls_total: std::sync::atomic::AtomicU64,
    call_failures: std::sync::atomic::AtomicU64,
    retries_total: std::sync::atomic::AtomicU64,
    timeouts_total: std::sync::atomic::AtomicU64,
    invalidation_drains_total: std::sync::atomic::AtomicU64,
    invalidations_delivered_total: std::sync::atomic::AtomicU64,
    round_trip_latency_micros: AtomicHistogram,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientMetricsSnapshot {
    pub calls_total: u64,
    pub call_failures: u64,
    pub retries_total: u64,
    pub timeouts_total: u64,
    pub invalidation_drains_total: u64,
    pub invalidations_delivered_total: u64,
    pub round_trip_latency_micros: HistogramSnapshot,
}

impl Default for ClientMetrics {
    fn default() -> Self {
        Self {
            calls_total: std::sync::atomic::AtomicU64::new(0),
            call_failures: std::sync::atomic::AtomicU64::new(0),
            retries_total: std::sync::atomic::AtomicU64::new(0),
            timeouts_total: std::sync::atomic::AtomicU64::new(0),
            invalidation_drains_total: std::sync::atomic::AtomicU64::new(0),
            invalidations_delivered_total: std::sync::atomic::AtomicU64::new(0),
            round_trip_latency_micros: AtomicHistogram::new(LATENCY_BUCKETS_MICROS),
        }
    }
}

impl ClientMetrics {
    fn snapshot(&self) -> ClientMetricsSnapshot {
        ClientMetricsSnapshot {
            calls_total: self.calls_total.load(Ordering::Relaxed),
            call_failures: self.call_failures.load(Ordering::Relaxed),
            retries_total: self.retries_total.load(Ordering::Relaxed),
            timeouts_total: self.timeouts_total.load(Ordering::Relaxed),
            invalidation_drains_total: self.invalidation_drains_total.load(Ordering::Relaxed),
            invalidations_delivered_total: self
                .invalidations_delivered_total
                .load(Ordering::Relaxed),
            round_trip_latency_micros: self.round_trip_latency_micros.snapshot(),
        }
    }
}

impl FileSystemClient {
    pub fn new(mount: String, connection: Connection) -> Result<Self, RpcError> {
        Self::with_config(mount, connection, FileSystemClientConfig::default())
    }

    pub fn with_config(
        mount: String,
        connection: Connection,
        config: FileSystemClientConfig,
    ) -> Result<Self, RpcError> {
        if mount.is_empty() {
            return Err(RpcError::Malformed("mount name must not be empty".into()));
        }
        let invalidations = connection
            .subscribe(&invalidation_subject(&mount))
            .map_err(rpc_error_from_io)?;
        connection.flush().map_err(rpc_error_from_io)?;

        Ok(Self {
            connection,
            mount,
            config,
            request_id_scope: next_request_id_scope(),
            connected: Arc::new(AtomicBool::new(true)),
            invalidations,
            own_request_ids: Arc::new(Mutex::new(OwnInvalidationReplay::default())),
            metrics: Arc::new(ClientMetrics::default()),
        })
    }

    pub fn metrics(&self) -> ClientMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn disconnect(&self) {
        self.connected.store(false, Ordering::SeqCst);
    }

    pub fn reconnect(&self) {
        self.connected.store(true, Ordering::SeqCst);
    }

    pub fn call_bytes(&self, request_bytes: &[u8]) -> Result<Vec<u8>, RpcError> {
        self.ensure_connected()?;
        self.check_frame_len(request_bytes.len())?;
        let request = decode_request(request_bytes)
            .map_err(|error| RpcError::Malformed(error.to_string()))?;
        let outbound_request = self.request_with_transport_scope(&request);
        let outbound_request_bytes = encode_request(&outbound_request)
            .map_err(|error| RpcError::Malformed(error.to_string()))?;
        self.check_frame_len(outbound_request_bytes.len())?;
        let subject = command_subject_for_operation(&self.mount, outbound_request.operation);
        let response_bytes =
            self.round_trip(&subject, &outbound_request, &outbound_request_bytes)?;
        let response = decode_response(&response_bytes)
            .map_err(|error| RpcError::Malformed(error.to_string()))?;
        response
            .validate_for_request(&outbound_request)
            .map_err(|error| RpcError::Malformed(error.to_string()))?;
        let response = self.restore_caller_request_id(
            response,
            &request.request_id,
            &outbound_request.request_id,
        );
        let response_bytes =
            encode_response(&response).map_err(|error| RpcError::Malformed(error.to_string()))?;
        self.check_frame_len(response_bytes.len())?;
        Ok(response_bytes)
    }

    fn round_trip(
        &self,
        subject: &str,
        request: &RequestEnvelope,
        request_bytes: &[u8],
    ) -> Result<Vec<u8>, RpcError> {
        let _span = tracing::debug_span!(
            "nats_round_trip",
            mount = %self.mount,
            subject,
            request_id = %request.request_id,
            namespace = %request.namespace,
            operation = request.operation.as_str()
        )
        .entered();
        self.check_frame_len(request_bytes.len())?;
        self.ensure_connected()?;
        self.begin_own_request_id(&request.request_id)?;
        let result = self.round_trip_registered(subject, request, request_bytes);
        let finish = if result.is_ok() {
            self.complete_own_request_id(&request.request_id)
        } else {
            self.abandon_own_request_id(&request.request_id)
        };
        finish?;
        result
    }

    fn round_trip_registered(
        &self,
        subject: &str,
        request: &RequestEnvelope,
        request_bytes: &[u8],
    ) -> Result<Vec<u8>, RpcError> {
        let attempts = self.config.max_retries + 1;
        let started = std::time::Instant::now();

        for attempt in 0..attempts {
            self.ensure_connected()?;
            let inbox = self.connection.new_inbox();
            let subscription = self
                .connection
                .subscribe(&inbox)
                .map_err(rpc_error_from_io)?;
            let transport_headers = self
                .config
                .transport_auth
                .as_ref()
                .map(|auth| auth.headers_for(subject, &inbox, request_bytes));

            if let Err(error) = self.connection.publish_with_reply_or_headers(
                subject,
                Some(&inbox),
                transport_headers.as_ref(),
                request_bytes,
            ) {
                tracing::warn!(
                    mount = %self.mount,
                    subject,
                    request_id = %request.request_id,
                    attempt,
                    error = ?error,
                    "NATS publish failed"
                );
                if !can_retry_after_publish_error(request, attempt, attempts) {
                    let latency = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                    self.metrics.round_trip_latency_micros.record(latency);
                    return Err(rpc_error_from_io(error));
                }
                self.metrics.retries_total.fetch_add(1, Ordering::Relaxed);
                backoff(self.config.retry_backoff);
                continue;
            }

            match subscription.next_timeout(self.config.timeout) {
                Ok(message) => {
                    if message.data.is_empty() {
                        return Err(RpcError::ConnectionClosed);
                    }
                    self.check_frame_len(message.data.len())?;
                    let response = decode_response(&message.data)
                        .map_err(|error| RpcError::Malformed(error.to_string()))?;
                    response
                        .validate_for_request(request)
                        .map_err(|error| RpcError::Malformed(error.to_string()))?;
                    let latency = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                    self.metrics.round_trip_latency_micros.record(latency);
                    return Ok(message.data);
                }
                Err(error)
                    if error.kind() == io::ErrorKind::TimedOut
                        && can_retry_after_publish(request)
                        && attempt + 1 < attempts =>
                {
                    self.metrics.retries_total.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        mount = %self.mount,
                        subject,
                        request_id = %request.request_id,
                        attempt,
                        "NATS request timed out before retry"
                    );
                    backoff(self.config.retry_backoff);
                }
                Err(error) if error.kind() == io::ErrorKind::TimedOut => {
                    self.metrics.timeouts_total.fetch_add(1, Ordering::Relaxed);
                    let latency = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                    self.metrics.round_trip_latency_micros.record(latency);
                    tracing::warn!(
                        mount = %self.mount,
                        subject,
                        request_id = %request.request_id,
                        attempt,
                        "NATS request timed out"
                    );
                    return Err(RpcError::Timeout);
                }
                Err(error) if !can_retry_after_publish(request) || attempt + 1 == attempts => {
                    let latency = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                    self.metrics.round_trip_latency_micros.record(latency);
                    return Err(rpc_error_from_io(error));
                }
                Err(_) => {
                    self.metrics.retries_total.fetch_add(1, Ordering::Relaxed);
                    backoff(self.config.retry_backoff)
                }
            }
        }

        self.metrics.timeouts_total.fetch_add(1, Ordering::Relaxed);
        let latency = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.metrics.round_trip_latency_micros.record(latency);
        Err(RpcError::Timeout)
    }

    fn ensure_connected(&self) -> Result<(), RpcError> {
        if self.connected.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(RpcError::ConnectionClosed)
        }
    }

    fn check_frame_len(&self, frame_len: usize) -> Result<(), RpcError> {
        if frame_len <= self.config.max_frame_bytes {
            Ok(())
        } else {
            Err(RpcError::FrameTooLarge)
        }
    }

    fn begin_own_request_id(&self, request_id: &str) -> Result<(), RpcError> {
        self.own_request_ids
            .lock()
            .map_err(|_| RpcError::Transport("own invalidation filter lock poisoned".into()))?
            .begin(request_id.to_owned());
        Ok(())
    }

    fn complete_own_request_id(&self, request_id: &str) -> Result<(), RpcError> {
        self.own_request_ids
            .lock()
            .map_err(|_| RpcError::Transport("own invalidation filter lock poisoned".into()))
            .map(|mut ids| ids.complete(request_id))
    }

    fn abandon_own_request_id(&self, request_id: &str) -> Result<(), RpcError> {
        self.own_request_ids
            .lock()
            .map_err(|_| RpcError::Transport("own invalidation filter lock poisoned".into()))
            .map(|mut ids| ids.abandon(request_id))
    }

    fn take_recovered_own_invalidations(
        &self,
        namespace: &str,
    ) -> Result<Vec<pb::Invalidation>, RpcError> {
        self.own_request_ids
            .lock()
            .map_err(|_| RpcError::Transport("own invalidation filter lock poisoned".into()))
            .map(|mut ids| ids.take_recovered(namespace))
    }

    fn handle_own_request_invalidation(
        &self,
        invalidation: pb::Invalidation,
    ) -> Result<Option<pb::Invalidation>, RpcError> {
        self.own_request_ids
            .lock()
            .map_err(|_| RpcError::Transport("own invalidation filter lock poisoned".into()))
            .map(|mut ids| ids.handle_invalidation(invalidation))
    }

    fn scope_request_id(&self, request_id: &str) -> String {
        format!("{}:{request_id}", self.request_id_scope)
    }

    fn request_with_transport_scope(&self, request: &RequestEnvelope) -> RequestEnvelope {
        let mut scoped = request.clone();
        scoped.request_id = self.scope_request_id(&request.request_id);
        scoped.deadline_unix_nanos =
            effective_transport_deadline(request.deadline_unix_nanos, self.config.timeout);
        scoped
    }

    fn restore_caller_request_id(
        &self,
        mut response: ResponseEnvelope,
        caller_request_id: &str,
        scoped_request_id: &str,
    ) -> ResponseEnvelope {
        if response.request_id == scoped_request_id {
            response.request_id = caller_request_id.to_owned();
        }
        for invalidation in &mut response.invalidations {
            if invalidation.request_id == scoped_request_id {
                invalidation.request_id = caller_request_id.to_owned();
            }
        }
        response
    }
}

impl CommonRpcClient for FileSystemClient {
    fn call(&self, request: RequestEnvelope) -> Result<ResponseEnvelope, RpcError> {
        self.metrics.calls_total.fetch_add(1, Ordering::Relaxed);
        let _span = tracing::debug_span!(
            "filesystem_rpc_call",
            mount = %self.mount,
            request_id = %request.request_id,
            namespace = %request.namespace,
            operation = request.operation.as_str()
        )
        .entered();
        let outbound_request = self.request_with_transport_scope(&request);
        let request_bytes = encode_request(&outbound_request)
            .map_err(|error| RpcError::Malformed(error.to_string()))?;
        let response_bytes = match self.round_trip(
            &command_subject_for_operation(&self.mount, outbound_request.operation),
            &outbound_request,
            &request_bytes,
        ) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.metrics.call_failures.fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        let response = decode_response(&response_bytes)
            .map_err(|error| RpcError::Malformed(error.to_string()))?;
        response
            .validate_for_request(&outbound_request)
            .map_err(|error| RpcError::Malformed(error.to_string()))?;
        Ok(self.restore_caller_request_id(
            response,
            &request.request_id,
            &outbound_request.request_id,
        ))
    }

    fn drain_invalidations(
        &self,
        namespace: &str,
    ) -> Result<Vec<fs_protocol::pb::Invalidation>, RpcError> {
        self.metrics
            .invalidation_drains_total
            .fetch_add(1, Ordering::Relaxed);
        let _span = tracing::debug_span!(
            "drain_invalidations",
            mount = %self.mount,
            namespace
        )
        .entered();
        self.ensure_connected()?;
        let mut invalidations = self.take_recovered_own_invalidations(namespace)?;
        for message in self.invalidations.try_iter() {
            self.check_frame_len(message.data.len())?;
            let invalidation: pb::Invalidation = decode_message(&message.data)
                .map_err(|error| RpcError::Malformed(error.to_string()))?;
            validate_invalidation(&invalidation)
                .map_err(|error| RpcError::Malformed(error.to_string()))?;
            if invalidation.namespace != namespace {
                continue;
            }
            if let Some(invalidation) = self.handle_own_request_invalidation(invalidation)? {
                invalidations.push(invalidation);
            }
        }
        tracing::trace!(
            mount = %self.mount,
            namespace,
            count = invalidations.len(),
            "drained remote invalidations"
        );
        self.metrics
            .invalidations_delivered_total
            .fetch_add(invalidations.len() as u64, Ordering::Relaxed);
        Ok(invalidations)
    }
}

fn next_request_id_scope() -> String {
    format!("nats-{}", nuid::next())
}

fn backoff(duration: Duration) {
    if duration > Duration::ZERO {
        std::thread::sleep(duration);
    }
}

fn rpc_error_from_io(error: io::Error) -> RpcError {
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

#[cfg(test)]
mod tests {
    use super::*;
    use fs_protocol::InvalidationKind;

    #[test]
    fn own_invalidation_tracker_defers_pending_replay_then_drops_after_success() {
        let mut tracker = OwnInvalidationReplay::default();
        tracker.begin("client-1:fuse-1".into());

        assert_eq!(
            tracker.handle_invalidation(invalidation("client-1:fuse-1", 1)),
            None
        );
        assert!(tracker.take_recovered("test-namespace").is_empty());

        tracker.complete("client-1:fuse-1");
        assert!(tracker.take_recovered("test-namespace").is_empty());
        assert_eq!(
            tracker.handle_invalidation(invalidation("client-1:fuse-1", 1)),
            None,
            "completed direct responses suppress later out-of-band duplicates"
        );
    }

    #[test]
    fn own_invalidation_tracker_recovers_deferred_replay_after_timeout() {
        let mut tracker = OwnInvalidationReplay::default();
        tracker.begin("client-1:fuse-2".into());

        assert_eq!(
            tracker.handle_invalidation(invalidation("client-1:fuse-2", 1)),
            None
        );
        tracker.abandon("client-1:fuse-2");

        let recovered = tracker.take_recovered("test-namespace");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].sequence, 1);
        assert_eq!(
            tracker.handle_invalidation(invalidation("client-1:fuse-2", 2)),
            Some(invalidation("client-1:fuse-2", 2)),
            "abandoned requests use later out-of-band frames as recovery"
        );
    }

    #[test]
    fn client_metrics_snapshot_reports_recorded_totals() {
        let metrics = ClientMetrics::default();
        metrics.calls_total.fetch_add(2, Ordering::Relaxed);
        metrics.call_failures.fetch_add(1, Ordering::Relaxed);
        metrics.retries_total.fetch_add(3, Ordering::Relaxed);
        metrics.timeouts_total.fetch_add(1, Ordering::Relaxed);
        metrics
            .invalidation_drains_total
            .fetch_add(4, Ordering::Relaxed);
        metrics
            .invalidations_delivered_total
            .fetch_add(6, Ordering::Relaxed);
        metrics.round_trip_latency_micros.record(42);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.calls_total, 2);
        assert_eq!(snapshot.call_failures, 1);
        assert_eq!(snapshot.retries_total, 3);
        assert_eq!(snapshot.timeouts_total, 1);
        assert_eq!(snapshot.invalidation_drains_total, 4);
        assert_eq!(snapshot.invalidations_delivered_total, 6);
        assert_eq!(snapshot.round_trip_latency_micros.total, 1);
    }

    fn invalidation(request_id: &str, sequence: u64) -> pb::Invalidation {
        pb::Invalidation {
            namespace: "test-namespace".into(),
            sequence,
            kind: InvalidationKind::Modify.wire_value(),
            path: "/file.txt".into(),
            old_path: String::new(),
            new_path: String::new(),
            inode: 0,
            handle: 0,
            request_id: request_id.into(),
        }
    }
}
