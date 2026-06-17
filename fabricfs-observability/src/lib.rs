use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, Registry};

static TRACING_INIT: OnceLock<()> = OnceLock::new();
pub const LATENCY_BUCKETS_MICROS: &[u64] = &[
    50, 100, 250, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
];

pub fn init_tracing(service_name: &'static str) {
    let _ = TRACING_INIT.get_or_init(|| {
        let filter = EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new(default_filter(service_name)))
            .unwrap_or_else(|_| EnvFilter::new("info"));
        let format = fmt::layer()
            .with_target(false)
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_filter(filter);
        Registry::default().with(format).init();
    });
}

fn default_filter(service_name: &str) -> String {
    format!("{service_name}=info,fabricfs=info,fs_core=info,fs_fuse=info,fabricfs_transport=info")
}

pub struct AtomicHistogram {
    buckets: &'static [u64],
    counts: Vec<AtomicU64>,
    overflow: AtomicU64,
    total: AtomicU64,
    sum: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramSnapshot {
    pub buckets: Vec<(u64, u64)>,
    pub overflow: u64,
    pub total: u64,
    pub sum: u64,
}

pub struct MetricsReporter {
    stop: Option<mpsc::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl AtomicHistogram {
    pub fn new(buckets: &'static [u64]) -> Self {
        Self {
            buckets,
            counts: buckets.iter().map(|_| AtomicU64::new(0)).collect(),
            overflow: AtomicU64::new(0),
            total: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }

    pub fn record(&self, value: u64) {
        self.total.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        for (index, upper_bound) in self.buckets.iter().enumerate() {
            if value <= *upper_bound {
                self.counts[index].fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        self.overflow.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            buckets: self
                .buckets
                .iter()
                .enumerate()
                .map(|(index, upper_bound)| {
                    (*upper_bound, self.counts[index].load(Ordering::Relaxed))
                })
                .collect(),
            overflow: self.overflow.load(Ordering::Relaxed),
            total: self.total.load(Ordering::Relaxed),
            sum: self.sum.load(Ordering::Relaxed),
        }
    }
}

pub fn spawn_periodic_metrics_logger<S, F>(
    metrics_component: &'static str,
    interval: Duration,
    snapshot: F,
) -> MetricsReporter
where
    S: std::fmt::Debug + Send + 'static,
    F: Fn() -> S + Send + Sync + 'static,
{
    let (stop_tx, stop_rx) = mpsc::channel();
    let snapshot = Arc::new(snapshot);
    let handle = thread::spawn(move || {
        log_snapshot(metrics_component, &snapshot);
        while matches!(
            stop_rx.recv_timeout(interval),
            Err(mpsc::RecvTimeoutError::Timeout)
        ) {
            log_snapshot(metrics_component, &snapshot);
        }
    });

    MetricsReporter {
        stop: Some(stop_tx),
        handle: Some(handle),
    }
}

fn log_snapshot<S, F>(metrics_component: &str, snapshot: &Arc<F>)
where
    S: std::fmt::Debug + Send + 'static,
    F: Fn() -> S + Send + Sync + 'static,
{
    tracing::info!(
        metrics_component,
        snapshot = ?(snapshot.as_ref())(),
        "runtime metrics snapshot"
    );
}

impl Drop for MetricsReporter {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Write};
    use std::sync::Mutex;
    use std::time::Instant;
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Clone, Default)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    impl SharedBuffer {
        fn clear(&self) {
            self.0.lock().expect("buffer lock").clear();
        }

        fn as_string(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("buffer lock")).into_owned()
        }
    }

    struct SharedBufferWriter(Arc<Mutex<Vec<u8>>>);

    impl<'a> MakeWriter<'a> for SharedBuffer {
        type Writer = SharedBufferWriter;

        fn make_writer(&'a self) -> Self::Writer {
            SharedBufferWriter(self.0.clone())
        }
    }

    impl Write for SharedBufferWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("buffer lock").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn init_test_subscriber() -> SharedBuffer {
        static TEST_BUFFER: OnceLock<SharedBuffer> = OnceLock::new();
        TEST_BUFFER
            .get_or_init(|| {
                let buffer = SharedBuffer::default();
                let subscriber = tracing_subscriber::fmt()
                    .with_ansi(false)
                    .without_time()
                    .with_target(false)
                    .with_max_level(tracing::Level::INFO)
                    .with_writer(buffer.clone())
                    .finish();
                let _ = tracing::subscriber::set_global_default(subscriber);
                buffer
            })
            .clone()
    }

    #[test]
    fn metrics_reporter_emits_runtime_snapshot_logs() {
        let buffer = init_test_subscriber();
        buffer.clear();

        let reporter =
            spawn_periodic_metrics_logger("worker_pool", Duration::from_millis(10), || 7u64);

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let log = buffer.as_string();
            if log.contains("runtime metrics snapshot")
                && log.contains("metrics_component=\"worker_pool\"")
                && log.contains("snapshot=7")
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "metrics reporter did not emit a visible runtime snapshot log: {log}"
            );
            thread::sleep(Duration::from_millis(10));
        }

        drop(reporter);
    }
}
