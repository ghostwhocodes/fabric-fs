use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use fs_core::{now_unix_nanos, RpcError};
use fs_protocol::{
    pb, Errno, Operation, OperationEffect, RequestEnvelope, ResponseEnvelope, ResponsePayload,
};

use crate::auth::VerifiedTransportPeer;

pub(crate) const MAX_AUTHENTICATED_REQUEST_TTL_NANOS: u64 = 300 * 1_000_000_000;
const MAX_OWN_REQUEST_IDS: usize = 1024;

pub(crate) fn can_retry_after_publish(request: &RequestEnvelope) -> bool {
    !request_has_ambiguous_post_publish_effects(request)
}

pub(crate) fn can_retry_after_publish_error(
    request: &RequestEnvelope,
    attempt: usize,
    attempts: usize,
) -> bool {
    can_retry_after_publish(request) && attempt + 1 < attempts
}

fn request_has_ambiguous_post_publish_effects(request: &RequestEnvelope) -> bool {
    if request.operation == Operation::Release {
        return false;
    }

    match request.operation.spec().effect {
        OperationEffect::ReadOnly => false,
        OperationEffect::HandleLifecycle
        | OperationEffect::ContentMutation
        | OperationEffect::CreateNode
        | OperationEffect::DeleteNode
        | OperationEffect::RenameNode
        | OperationEffect::MetadataMutation
        | OperationEffect::XattrMutation
        | OperationEffect::Durability
        | OperationEffect::LockState
        | OperationEffect::SeekState => true,
    }
}

pub(crate) fn effective_transport_deadline(existing: u64, timeout: Duration) -> u64 {
    let timeout_deadline =
        now_unix_nanos().saturating_add(timeout.as_nanos().min(u128::from(u64::MAX)) as u64);
    if existing == 0 {
        timeout_deadline
    } else {
        existing.min(timeout_deadline)
    }
}

#[derive(Default)]
pub(crate) struct OwnInvalidationReplay {
    completed_order: VecDeque<String>,
    completed: HashSet<String>,
    pending: HashSet<String>,
    deferred: HashMap<String, Vec<pb::Invalidation>>,
    recovered: VecDeque<pb::Invalidation>,
}

impl OwnInvalidationReplay {
    pub(crate) fn begin(&mut self, request_id: String) {
        if request_id.is_empty() {
            return;
        }
        self.completed.remove(&request_id);
        self.completed_order.retain(|value| value != &request_id);
        self.pending.insert(request_id);
    }

    pub(crate) fn complete(&mut self, request_id: &str) {
        self.pending.remove(request_id);
        self.deferred.remove(request_id);
        self.remember_completed(request_id.to_owned());
    }

    pub(crate) fn abandon(&mut self, request_id: &str) {
        self.pending.remove(request_id);
        if let Some(deferred) = self.deferred.remove(request_id) {
            self.recovered.extend(deferred);
        }
    }

    pub(crate) fn handle_invalidation(
        &mut self,
        invalidation: pb::Invalidation,
    ) -> Option<pb::Invalidation> {
        let request_id = invalidation.request_id.clone();
        if request_id.is_empty() {
            return Some(invalidation);
        }
        if self.completed.contains(&request_id) {
            return None;
        }
        if self.pending.contains(&request_id) {
            self.deferred
                .entry(request_id)
                .or_default()
                .push(invalidation);
            return None;
        }
        Some(invalidation)
    }

    pub(crate) fn take_recovered(&mut self, namespace: &str) -> Vec<pb::Invalidation> {
        let mut ready = Vec::new();
        let mut retained = VecDeque::new();
        while let Some(invalidation) = self.recovered.pop_front() {
            if invalidation.namespace == namespace {
                ready.push(invalidation);
            } else {
                retained.push_back(invalidation);
            }
        }
        self.recovered = retained;
        ready
    }

    fn remember_completed(&mut self, request_id: String) {
        if request_id.is_empty() || self.completed.contains(&request_id) {
            return;
        }
        self.completed_order.push_back(request_id.clone());
        self.completed.insert(request_id);
        while self.completed_order.len() > MAX_OWN_REQUEST_IDS {
            if let Some(expired) = self.completed_order.pop_front() {
                self.completed.remove(&expired);
            }
        }
    }
}

pub(crate) struct AuthenticatedReplayCache {
    state: Arc<ReplayCacheState>,
    sweeper: Option<JoinHandle<()>>,
}

impl AuthenticatedReplayCache {
    fn claim(&self, token: &str, ttl: Duration) -> Result<bool, RpcError> {
        let expires_at = Instant::now().checked_add(ttl).unwrap_or_else(Instant::now);
        let mut inner = self
            .state
            .seen
            .lock()
            .map_err(|_| RpcError::Transport("authenticated replay cache poisoned".into()))?;
        inner.discard_expired(Instant::now());
        match inner.seen.get(token).copied() {
            Some(deadline) if deadline > Instant::now() => Ok(false),
            _ => {
                inner.seen.insert(token.to_owned(), expires_at);
                self.state.ready.notify_all();
                Ok(true)
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        let mut inner = self.state.seen.lock().expect("replay cache lock");
        inner.discard_expired(Instant::now());
        inner.seen.len()
    }
}

impl Default for AuthenticatedReplayCache {
    fn default() -> Self {
        let state = Arc::new(ReplayCacheState {
            seen: Mutex::new(ReplayCacheInner::default()),
            ready: Condvar::new(),
        });
        let worker_state = Arc::clone(&state);
        let sweeper = std::thread::spawn(move || sweep_replay_cache(worker_state));
        Self {
            state,
            sweeper: Some(sweeper),
        }
    }
}

impl Drop for AuthenticatedReplayCache {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.state.seen.lock() {
            inner.shutdown = true;
            self.state.ready.notify_all();
        }
        if let Some(handle) = self.sweeper.take() {
            let _ = handle.join();
        }
    }
}

pub(crate) fn authenticated_replay_or_expiry_response(
    replay_cache: &AuthenticatedReplayCache,
    max_authenticated_request_ttl_nanos: u64,
    request: &RequestEnvelope,
    verified_peer: &VerifiedTransportPeer,
) -> Option<ResponseEnvelope> {
    let now = now_unix_nanos();
    if request.deadline_unix_nanos == 0 {
        return Some(ResponseEnvelope::failure_for(
            request,
            Errno::InvalidArgument,
            "transport-authenticated filesystem requests must include a deadline",
        ));
    }
    if request.deadline_unix_nanos <= now {
        return Some(ResponseEnvelope::failure_for(
            request,
            Errno::TimedOut,
            "transport-authenticated filesystem request expired before dispatch",
        ));
    }
    let max_deadline = now.saturating_add(max_authenticated_request_ttl_nanos);
    if request.deadline_unix_nanos > max_deadline {
        return Some(ResponseEnvelope::failure_for(
            request,
            Errno::InvalidArgument,
            format!(
                "transport-authenticated filesystem request deadline exceeds the server maximum TTL of {}ns",
                max_authenticated_request_ttl_nanos
            ),
        ));
    }
    match replay_cache.claim(
        &verified_peer.replay_token,
        Duration::from_nanos(
            request
                .deadline_unix_nanos
                .min(now.saturating_add(max_authenticated_request_ttl_nanos))
                .saturating_sub(now),
        ),
    ) {
        Ok(true) => None,
        Ok(false) => Some(ResponseEnvelope::failure_for(
            request,
            Errno::PermissionDenied,
            "transport-authenticated filesystem request replay detected",
        )),
        Err(error) => Some(ResponseEnvelope::failure_for(
            request,
            Errno::Io,
            format!("transport replay protection failed: {error}"),
        )),
    }
}

struct ReplayCacheState {
    seen: Mutex<ReplayCacheInner>,
    ready: Condvar,
}

#[derive(Default)]
struct ReplayCacheInner {
    seen: HashMap<String, Instant>,
    shutdown: bool,
}

impl ReplayCacheInner {
    fn discard_expired(&mut self, now: Instant) {
        self.seen.retain(|_, deadline| *deadline > now);
    }

    fn next_expiry(&self) -> Option<Instant> {
        self.seen.values().copied().min()
    }
}

fn sweep_replay_cache(state: Arc<ReplayCacheState>) {
    let mut inner = match state.seen.lock() {
        Ok(inner) => inner,
        Err(_) => return,
    };
    loop {
        inner.discard_expired(Instant::now());
        if inner.shutdown {
            return;
        }
        let Some(next_expiry) = inner.next_expiry() else {
            inner = match state.ready.wait(inner) {
                Ok(inner) => inner,
                Err(_) => return,
            };
            continue;
        };
        let now = Instant::now();
        if next_expiry <= now {
            continue;
        }
        let wait = next_expiry.saturating_duration_since(now);
        let (next_inner, _) = match state.ready.wait_timeout(inner, wait) {
            Ok(result) => result,
            Err(_) => return,
        };
        inner = next_inner;
    }
}

#[derive(Default)]
pub(crate) struct NamespaceLocks {
    active: Mutex<HashSet<String>>,
    ready: Condvar,
}

impl NamespaceLocks {
    pub(crate) fn enter(&self, namespace: &str) -> Result<NamespaceGuard<'_>, RpcError> {
        let mut active = self
            .active
            .lock()
            .map_err(|_| RpcError::Transport("namespace ordering lock poisoned".into()))?;
        while active.contains(namespace) {
            active = self
                .ready
                .wait(active)
                .map_err(|_| RpcError::Transport("namespace ordering lock poisoned".into()))?;
        }
        active.insert(namespace.to_owned());
        Ok(NamespaceGuard {
            locks: self,
            namespace: namespace.to_owned(),
        })
    }
}

pub(crate) struct NamespaceGuard<'a> {
    locks: &'a NamespaceLocks,
    namespace: String,
}

impl Drop for NamespaceGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut active) = self.locks.active.lock() {
            active.remove(&self.namespace);
            self.locks.ready.notify_all();
        }
    }
}

pub(crate) struct TransportResponseDelivery<'a> {
    pub request: &'a RequestEnvelope,
    pub response: &'a ResponseEnvelope,
    pub response_bytes: &'a [u8],
    pub reply: &'a str,
}

pub(crate) fn deliver_prepared_response<PI, PR, AH, CH>(
    delivery: TransportResponseDelivery<'_>,
    mut publish_invalidation: PI,
    mut publish_reply: PR,
    mut abort_response_handles: AH,
    mut cleanup_expired_response_handles: CH,
) -> Result<(), RpcError>
where
    PI: FnMut(&pb::Invalidation) -> Result<(), RpcError>,
    PR: FnMut(&str, &[u8]) -> Result<(), RpcError>,
    AH: FnMut(RpcError) -> RpcError,
    CH: FnMut() -> Result<(), RpcError>,
{
    for invalidation in &delivery.response.invalidations {
        publish_invalidation(invalidation).map_err(&mut abort_response_handles)?;
    }
    fail_if_response_deadline_expired(&delivery, &mut cleanup_expired_response_handles)?;
    publish_reply(delivery.reply, delivery.response_bytes).map_err(&mut abort_response_handles)?;
    fail_if_response_deadline_expired(&delivery, &mut cleanup_expired_response_handles)
}

pub(crate) fn response_handles_require_cleanup_after_deadline(
    request: &RequestEnvelope,
    response: &ResponseEnvelope,
    now_unix_nanos: u64,
) -> bool {
    response_returns_backend_handle(response)
        && request.deadline_unix_nanos != 0
        && request.deadline_unix_nanos <= now_unix_nanos
}

fn fail_if_response_deadline_expired(
    delivery: &TransportResponseDelivery<'_>,
    cleanup_expired_response_handles: &mut impl FnMut() -> Result<(), RpcError>,
) -> Result<(), RpcError> {
    if response_handles_require_cleanup_after_deadline(
        delivery.request,
        delivery.response,
        now_unix_nanos(),
    ) {
        cleanup_expired_response_handles()?;
        return Err(RpcError::Timeout);
    }
    Ok(())
}

fn response_returns_backend_handle(response: &ResponseEnvelope) -> bool {
    response.ok
        && matches!(
            response.payload.as_ref(),
            Some(ResponsePayload::Open(_)) | Some(ResponsePayload::Create(_))
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs_protocol::Operation;
    use fs_testkit::request_for_operation;

    #[test]
    fn publish_error_retry_policy_treats_post_publish_effects_as_ambiguous() {
        let attempts = 3;
        let write = request_for_operation(Operation::Write, "ambiguous-write");
        assert!(
            !can_retry_after_publish_error(&write, 0, attempts),
            "a publish error for a mutation may have occurred after bytes reached NATS"
        );

        let open = request_for_operation(Operation::Open, "ambiguous-open");
        assert!(
            !can_retry_after_publish_error(&open, 0, attempts),
            "open allocates backend handle lifecycle state and must not retry after publish errors without server dedupe"
        );

        let release = request_for_operation(Operation::Release, "retryable-release");
        assert!(
            can_retry_after_publish(&release),
            "release response timeouts must be retryable because the FUSE adapter forgets the local handle path after release"
        );
        assert!(
            can_retry_after_publish_error(&release, 0, attempts),
            "release is idempotent cleanup and must be retried after transient transport failures"
        );

        let lookup = request_for_operation(Operation::Lookup, "retryable-lookup");
        assert!(can_retry_after_publish_error(&lookup, 0, attempts));
        assert!(!can_retry_after_publish_error(
            &lookup,
            attempts - 1,
            attempts
        ));
    }

    #[test]
    fn publish_error_retry_policy_is_derived_from_operation_effects() {
        let attempts = 3;

        for operation in Operation::ALL {
            let request = request_for_operation(operation, &format!("retry-{operation:?}"));
            let retryable = can_retry_after_publish_error(&request, 0, attempts);
            let expected_retryable = operation.spec().effect == OperationEffect::ReadOnly
                || operation == Operation::Release;
            assert_eq!(
                retryable, expected_retryable,
                "{operation:?} retry classification must follow OperationSpec effect, except idempotent release cleanup"
            );
        }
    }
}
