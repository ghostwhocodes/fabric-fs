use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use notify::EventKind;

use super::events::self_notify_event_class;
use super::self_notify::{
    normalize_paths, InternalMetadataWrite, SelfNotifyEventClass, TrackedSelfNotifyWrite,
};
use super::SELF_NOTIFY_TTL;

#[derive(Clone, Default)]
pub struct StorageInvalidationGate {
    inner: Arc<StorageInvalidationGateState>,
}

#[derive(Default)]
struct StorageInvalidationGateState {
    state: Mutex<StorageInvalidationGateSnapshot>,
    ready: Condvar,
}

#[derive(Default)]
struct StorageInvalidationGateSnapshot {
    active: bool,
    // Every watch event that requires a full resync becomes owed delivery debt
    // until the publisher reports success. Requests and storage-watch full
    // resync publication share a single execution lane, so queued requests
    // cannot slip between debt creation and debt delivery.
    full_resync_pending: bool,
    lane_owner: Option<StorageExecutionLaneOwner>,
    #[cfg(test)]
    request_waiters: usize,
    // Self-notify suppression tracks only the latest write burst for a hidden
    // path. Replacing the burst is safer than merging heterogeneous event-class
    // credits across writes because stale suppression must never hide a later
    // external edit on the same path.
    self_notify_writes: Vec<TrackedSelfNotifyWrite>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StorageExecutionLaneOwner {
    Request,
    FullResync,
}

impl StorageInvalidationGate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn activate(&self) {
        self.lock_state().active = true;
        self.notify_waiters();
    }

    pub fn start_request(&self) -> StorageRequestGuard {
        let mut state = self.lock_state();
        loop {
            state.prune_expired_self_notify_writes();
            if state.request_may_start() {
                state.lane_owner = Some(StorageExecutionLaneOwner::Request);
                break;
            }
            #[cfg(test)]
            {
                state.request_waiters += 1;
                self.inner.ready.notify_all();
            }
            state = match self.inner.ready.wait(state) {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
            #[cfg(test)]
            {
                state.request_waiters = state.request_waiters.saturating_sub(1);
                self.inner.ready.notify_all();
            }
        }
        StorageRequestGuard {
            gate: self.clone(),
            finished: false,
        }
    }

    pub fn suppress_self_notify_event(&self, kind: &EventKind, paths: &[PathBuf]) -> bool {
        let Some(event_class) = self_notify_event_class(kind) else {
            return false;
        };
        let mut state = self.lock_state();
        state.prune_expired_self_notify_writes();
        state.consume_self_notify_paths(event_class, paths)
    }

    pub(super) fn record_completed_self_notify_write(&self, write: InternalMetadataWrite) {
        let mut state = self.lock_state();
        state.prune_expired_self_notify_writes();
        state.record_completed_write(write);
    }

    fn finish_request(&self) {
        let mut state = self.lock_state();
        if state.lane_owner != Some(StorageExecutionLaneOwner::Request) {
            return;
        }
        state.lane_owner = None;
        state.prune_expired_self_notify_writes();
        drop(state);
        self.notify_waiters();
    }

    pub(super) fn claim_full_resync(&self, stop: &AtomicBool) -> Option<FullResyncClaim> {
        let mut state = self.lock_state();
        loop {
            if stop.load(Ordering::Acquire) {
                return None;
            }
            state.prune_expired_self_notify_writes();
            if state.active && state.claim_full_resync() {
                return Some(FullResyncClaim::new(self.clone()));
            }
            state = match self.inner.ready.wait(state) {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
    }

    pub(super) fn wait_before_retry(&self, stop: &AtomicBool, delay: Duration) -> bool {
        if stop.load(Ordering::Acquire) {
            return false;
        }
        let state = self.lock_state();
        let _state = match self.inner.ready.wait_timeout(state, delay) {
            Ok((state, _)) => state,
            Err(poisoned) => {
                let (state, _) = poisoned.into_inner();
                state
            }
        };
        !stop.load(Ordering::Acquire)
    }

    pub(super) fn record_classified_watch_event(
        &self,
        event_class: SelfNotifyEventClass,
        paths: &[PathBuf],
    ) -> bool {
        let mut state = self.lock_state();
        state.prune_expired_self_notify_writes();
        if !state.active {
            return false;
        }
        if state.consume_self_notify_paths(event_class, paths) {
            return false;
        }
        state.defer_event();
        let should_notify = state.lane_owner.is_none();
        drop(state);
        if should_notify {
            self.notify_waiters();
        }
        true
    }

    pub(super) fn record_unsuppressible_watch_event(&self) -> bool {
        let mut state = self.lock_state();
        state.prune_expired_self_notify_writes();
        if !state.active {
            return false;
        }
        state.defer_event();
        let should_notify = state.lane_owner.is_none();
        drop(state);
        if should_notify {
            self.notify_waiters();
        }
        true
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, StorageInvalidationGateSnapshot> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cfg(test)]
    pub(super) fn wait_for_request_waiters(&self, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut state = self.lock_state();
        loop {
            if state.request_waiters >= expected {
                return;
            }
            let now = Instant::now();
            assert!(
                now < deadline,
                "timed out waiting for {expected} storage request waiter(s); observed {}",
                state.request_waiters
            );
            let timeout = deadline.saturating_duration_since(now);
            state = match self.inner.ready.wait_timeout(state, timeout) {
                Ok((state, _)) => state,
                Err(poisoned) => {
                    let (state, _) = poisoned.into_inner();
                    state
                }
            };
        }
    }

    fn complete_full_resync_claim(&self) {
        self.lock_state().complete_full_resync_claim();
        self.notify_waiters();
    }

    fn restore_full_resync_claim(&self) {
        self.lock_state().restore_full_resync_claim();
        self.notify_waiters();
    }

    pub(super) fn notify_waiters(&self) {
        self.inner.ready.notify_all();
    }
}

impl StorageInvalidationGateSnapshot {
    fn prune_expired_self_notify_writes(&mut self) {
        let now = Instant::now();
        self.self_notify_writes
            .retain(|tracked| tracked.expires_at > now && !tracked.is_exhausted());
    }

    fn record_completed_write(&mut self, write: InternalMetadataWrite) {
        let expires_at = Instant::now() + SELF_NOTIFY_TTL;
        let tracked_write = TrackedSelfNotifyWrite::new(write, expires_at);
        if tracked_write.paths.is_empty() {
            return;
        }
        let tracked_paths: HashSet<PathBuf> = tracked_write.paths.keys().cloned().collect();
        self.self_notify_writes
            .retain(|tracked| !tracked.overlaps_with(&tracked_paths));
        self.self_notify_writes.push(tracked_write);
    }

    fn consume_self_notify_paths(
        &mut self,
        event_class: SelfNotifyEventClass,
        paths: &[PathBuf],
    ) -> bool {
        self.consume_normalized_self_notify_paths(event_class, &normalize_paths(paths))
    }

    fn consume_normalized_self_notify_paths(
        &mut self,
        event_class: SelfNotifyEventClass,
        paths: &[PathBuf],
    ) -> bool {
        if paths.is_empty() {
            return false;
        }
        let mut index = self.self_notify_writes.len();
        while index > 0 {
            index -= 1;
            if !self.self_notify_writes[index].can_consume_paths(event_class, paths) {
                continue;
            }
            if self.self_notify_writes[index].matches_expected_state(paths) {
                let should_remove = {
                    let tracked = &mut self.self_notify_writes[index];
                    tracked.consume(event_class, paths);
                    tracked.is_exhausted()
                };
                if should_remove {
                    self.self_notify_writes.remove(index);
                }
                return true;
            }
            self.self_notify_writes.remove(index);
        }
        false
    }

    fn defer_event(&mut self) {
        self.full_resync_pending = true;
    }

    fn request_may_start(&self) -> bool {
        self.lane_owner.is_none() && !self.full_resync_pending
    }

    pub(super) fn claim_full_resync(&mut self) -> bool {
        if !self.full_resync_pending || self.lane_owner.is_some() {
            return false;
        }
        self.full_resync_pending = false;
        self.lane_owner = Some(StorageExecutionLaneOwner::FullResync);
        true
    }

    fn complete_full_resync_claim(&mut self) {
        if self.lane_owner == Some(StorageExecutionLaneOwner::FullResync) {
            self.lane_owner = None;
        }
    }

    fn restore_full_resync_claim(&mut self) {
        if self.lane_owner == Some(StorageExecutionLaneOwner::FullResync) {
            self.lane_owner = None;
            self.full_resync_pending = true;
        }
    }
}

pub struct StorageRequestGuard {
    gate: StorageInvalidationGate,
    finished: bool,
}

impl StorageRequestGuard {
    pub fn finish(mut self) {
        self.finished = true;
        self.gate.finish_request();
    }
}

impl Drop for StorageRequestGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.gate.finish_request();
        }
    }
}

pub(super) struct FullResyncClaim {
    gate: StorageInvalidationGate,
    delivered: bool,
}

impl FullResyncClaim {
    pub(super) fn new(gate: StorageInvalidationGate) -> Self {
        Self {
            gate,
            delivered: false,
        }
    }

    pub(super) fn mark_delivered(mut self) {
        self.delivered = true;
        self.gate.complete_full_resync_claim();
    }
}

impl Drop for FullResyncClaim {
    fn drop(&mut self) {
        if !self.delivered {
            self.gate.restore_full_resync_claim();
        }
    }
}

pub trait InternalMetadataNotifier: Send + Sync {
    fn record_completed_internal_metadata_write(&self, write: InternalMetadataWrite);
}

impl InternalMetadataNotifier for StorageInvalidationGate {
    fn record_completed_internal_metadata_write(&self, write: InternalMetadataWrite) {
        self.record_completed_self_notify_write(write);
    }
}
