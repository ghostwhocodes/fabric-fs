use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{Context, Result};
use fabricfs_transport::publish_invalidation;
use fs_protocol::pb;

use super::admission::StorageInvalidationGate;
use super::{FULL_RESYNC_RETRY_INITIAL, FULL_RESYNC_RETRY_MAX};

pub(super) struct FullResyncWorker {
    gate: StorageInvalidationGate,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl FullResyncWorker {
    pub(super) fn spawn<P, F>(
        gate: StorageInvalidationGate,
        publisher: P,
        next_full_resync: F,
    ) -> Self
    where
        P: InvalidationPublisher + Send + 'static,
        F: Fn() -> Option<pb::Invalidation> + Send + 'static,
    {
        let stop = Arc::new(AtomicBool::new(false));
        let worker_gate = gate.clone();
        let worker_stop = stop.clone();
        let thread = std::thread::spawn(move || {
            run_full_resync_worker(worker_gate, publisher, next_full_resync, worker_stop);
        });
        Self {
            gate,
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for FullResyncWorker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.gate.notify_waiters();
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                tracing::warn!("storage full-resync worker panicked");
            }
        }
    }
}

fn run_full_resync_worker<P, F>(
    gate: StorageInvalidationGate,
    publisher: P,
    next_full_resync: F,
    stop: Arc<AtomicBool>,
) where
    P: InvalidationPublisher,
    F: Fn() -> Option<pb::Invalidation>,
{
    let mut retry_delay = None;
    loop {
        if let Some(delay) = retry_delay {
            if !gate.wait_before_retry(stop.as_ref(), delay) {
                return;
            }
        }

        let Some(claim) = gate.claim_full_resync(stop.as_ref()) else {
            return;
        };

        match publisher.publish_next(&next_full_resync) {
            Ok(true) => {
                claim.mark_delivered();
                retry_delay = None;
            }
            Ok(false) => {
                let delay = next_full_resync_retry_delay(retry_delay);
                tracing::warn!(
                    retry_delay_ms = delay.as_millis() as u64,
                    "storage full-resync worker had no invalidation to publish; retrying"
                );
                retry_delay = Some(delay);
            }
            Err(error) => {
                let delay = next_full_resync_retry_delay(retry_delay);
                tracing::warn!(
                    error = ?error,
                    retry_delay_ms = delay.as_millis() as u64,
                    "storage full-resync publish failed; retrying"
                );
                retry_delay = Some(delay);
            }
        }
    }
}

fn next_full_resync_retry_delay(previous: Option<Duration>) -> Duration {
    match previous.and_then(|delay| delay.checked_mul(2)) {
        Some(delay) => delay.min(FULL_RESYNC_RETRY_MAX),
        None => FULL_RESYNC_RETRY_INITIAL,
    }
}

pub(super) trait InvalidationPublisher {
    fn publish_next<F>(&self, next_full_resync: F) -> Result<bool>
    where
        F: FnOnce() -> Option<pb::Invalidation>;
}

pub(super) struct NatsInvalidationPublisher {
    pub(super) connection: nats::Connection,
    pub(super) mount: String,
}

impl InvalidationPublisher for NatsInvalidationPublisher {
    fn publish_next<F>(&self, next_full_resync: F) -> Result<bool>
    where
        F: FnOnce() -> Option<pb::Invalidation>,
    {
        let Some(invalidation) = next_full_resync() else {
            return Ok(false);
        };
        publish_invalidation(&self.connection, &self.mount, &invalidation)
            .context("publish storage full-resync invalidation")?;
        self.connection
            .flush()
            .context("flush storage full-resync invalidation")?;
        Ok(true)
    }
}
