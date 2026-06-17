use anyhow::{anyhow, Result};
use fabricfs_observability::{AtomicHistogram, HistogramSnapshot, LATENCY_BUCKETS_MICROS};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Instant;

type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct WorkerPool {
    sender: Option<mpsc::SyncSender<Job>>,
    workers: Vec<JoinHandle<()>>,
    queue_depth: usize,
    metrics: Arc<WorkerPoolMetrics>,
}

struct WorkerPoolMetrics {
    submitted: AtomicU64,
    completed: AtomicU64,
    rejected: AtomicU64,
    panics: AtomicU64,
    queued: AtomicU64,
    max_queued: AtomicU64,
    active: AtomicU64,
    max_active: AtomicU64,
    backpressure_events: AtomicU64,
    submit_wait_micros: AtomicHistogram,
    job_run_micros: AtomicHistogram,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerPoolMetricsSnapshot {
    pub submitted: u64,
    pub completed: u64,
    pub rejected: u64,
    pub panics: u64,
    pub queued: u64,
    pub max_queued: u64,
    pub active: u64,
    pub max_active: u64,
    pub backpressure_events: u64,
    pub submit_wait_micros: HistogramSnapshot,
    pub job_run_micros: HistogramSnapshot,
}

impl Default for WorkerPoolMetrics {
    fn default() -> Self {
        Self {
            submitted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            panics: AtomicU64::new(0),
            queued: AtomicU64::new(0),
            max_queued: AtomicU64::new(0),
            active: AtomicU64::new(0),
            max_active: AtomicU64::new(0),
            backpressure_events: AtomicU64::new(0),
            submit_wait_micros: AtomicHistogram::new(LATENCY_BUCKETS_MICROS),
            job_run_micros: AtomicHistogram::new(LATENCY_BUCKETS_MICROS),
        }
    }
}

impl WorkerPoolMetrics {
    fn snapshot(&self) -> WorkerPoolMetricsSnapshot {
        WorkerPoolMetricsSnapshot {
            submitted: self.submitted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            panics: self.panics.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            max_queued: self.max_queued.load(Ordering::Relaxed),
            active: self.active.load(Ordering::Relaxed),
            max_active: self.max_active.load(Ordering::Relaxed),
            backpressure_events: self.backpressure_events.load(Ordering::Relaxed),
            submit_wait_micros: self.submit_wait_micros.snapshot(),
            job_run_micros: self.job_run_micros.snapshot(),
        }
    }

    fn record_queued(&self, queued: u64) {
        let mut current = self.max_queued.load(Ordering::Relaxed);
        while queued > current {
            match self.max_queued.compare_exchange(
                current,
                queued,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    fn record_active(&self, active: u64) {
        let mut current = self.max_active.load(Ordering::Relaxed);
        while active > current {
            match self.max_active.compare_exchange(
                current,
                active,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

impl WorkerPool {
    pub fn new(worker_count: usize, queue_depth: usize) -> Result<Self> {
        if worker_count == 0 {
            return Err(anyhow!("worker thread count must be at least 1"));
        }
        if queue_depth == 0 {
            return Err(anyhow!("queue depth must be at least 1"));
        }

        let (sender, receiver) = mpsc::sync_channel::<Job>(queue_depth);
        let shared_rx = Arc::new(Mutex::new(receiver));
        let metrics = Arc::new(WorkerPoolMetrics::default());
        let mut workers = Vec::with_capacity(worker_count);

        for worker_index in 0..worker_count {
            let rx = Arc::clone(&shared_rx);
            let metrics = Arc::clone(&metrics);
            workers.push(thread::spawn(move || loop {
                let job = {
                    let guard = match rx.lock() {
                        Ok(guard) => guard,
                        Err(_) => break,
                    };
                    guard.recv()
                };
                let Ok(job) = job else {
                    break;
                };
                metrics.queued.fetch_sub(1, Ordering::Relaxed);
                let active = metrics.active.fetch_add(1, Ordering::Relaxed) + 1;
                metrics.record_active(active);
                let started = Instant::now();
                let outcome = panic::catch_unwind(AssertUnwindSafe(job));
                let run_micros = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                metrics.job_run_micros.record(run_micros);
                metrics.active.fetch_sub(1, Ordering::Relaxed);
                metrics.completed.fetch_add(1, Ordering::Relaxed);
                if outcome.is_err() {
                    metrics.panics.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        component = "worker_pool",
                        worker_index,
                        run_micros,
                        "worker job panicked"
                    );
                }
            }));
        }

        Ok(Self {
            sender: Some(sender),
            workers,
            queue_depth,
            metrics,
        })
    }

    pub fn submit<F>(&self, job: F) -> Result<(), mpsc::SendError<Job>>
    where
        F: FnOnce() + Send + 'static,
    {
        let Some(sender) = &self.sender else {
            self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(mpsc::SendError(Box::new(job)));
        };

        let queued = self.metrics.queued.fetch_add(1, Ordering::Relaxed) + 1;
        self.metrics.record_queued(queued);
        if queued > self.queue_depth as u64 {
            self.metrics
                .backpressure_events
                .fetch_add(1, Ordering::Relaxed);
        }

        let submitted = Instant::now();
        match sender.send(Box::new(job)) {
            Ok(()) => {
                let submit_wait_micros =
                    submitted.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
                if submit_wait_micros > 0 {
                    self.metrics
                        .backpressure_events
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.metrics.submit_wait_micros.record(submit_wait_micros);
                self.metrics.submitted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                self.metrics.queued.fetch_sub(1, Ordering::Relaxed);
                self.metrics.rejected.fetch_add(1, Ordering::Relaxed);
                Err(error)
            }
        }
    }

    pub fn metrics(&self) -> WorkerPoolMetricsSnapshot {
        self.metrics.snapshot()
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.sender.take();
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn worker_pool_tracks_completed_jobs() {
        let pool = WorkerPool::new(1, 2).expect("worker pool");
        let (done_tx, done_rx) = mpsc::channel();
        pool.submit(move || {
            done_tx.send(()).expect("completion signal");
        })
        .expect("submit succeeds");
        done_rx.recv().expect("job completes");

        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        loop {
            let metrics = pool.metrics();
            if metrics.completed >= 1 {
                assert_eq!(metrics.submitted, 1);
                assert_eq!(metrics.completed, 1);
                assert_eq!(metrics.rejected, 0);
                assert_eq!(metrics.panics, 0);
                assert_eq!(metrics.job_run_micros.total, 1);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "worker pool completion metric did not settle in time"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn worker_pool_records_backpressure_and_panics() {
        let pool = WorkerPool::new(1, 1).expect("worker pool");
        let (release_tx, release_rx) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::channel();

        pool.submit(move || {
            started_tx.send(()).expect("start signal");
            release_rx.recv().expect("release signal");
        })
        .expect("first submit succeeds");
        started_rx.recv().expect("first job starts");

        pool.submit(|| panic!("boom"))
            .expect("second submit succeeds");
        std::thread::scope(|scope| {
            let submitter = scope.spawn(|| pool.submit(|| {}));
            let backpressure_deadline = Instant::now() + std::time::Duration::from_secs(1);
            loop {
                if pool.metrics().queued >= 2 {
                    break;
                }
                assert!(
                    Instant::now() < backpressure_deadline,
                    "third submit never reached the full queue before release"
                );
                std::thread::yield_now();
            }
            release_tx.send(()).expect("release worker");
            submitter
                .join()
                .expect("submitter joins")
                .expect("third submit works");
        });

        let deadline = Instant::now() + std::time::Duration::from_secs(1);
        loop {
            let metrics = pool.metrics();
            if metrics.panics >= 1 && metrics.completed >= 3 {
                assert!(metrics.submitted >= 3);
                assert!(metrics.backpressure_events >= 1);
                assert!(metrics.max_queued >= 1);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "worker pool metrics did not settle in time"
            );
            std::thread::yield_now();
        }
    }
}
