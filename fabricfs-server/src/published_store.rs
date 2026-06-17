use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use fabricfs_session_protocol::session::{decode_session_message, encode_session_message};
use fabricfs_session_protocol::session_proto as pb;
use nats::header::{self, HeaderMap};
use nats::jetstream::{self, DiscardPolicy, ErrorCode, StorageType, StreamConfig};
use nats::Message;
use thiserror::Error;

const DEFAULT_PUBLISHED_BUCKET: &str = "fabricfs_sessions_published";
const KEY_PREFIX: &str = "checkpoint/";
const KV_OPERATION: &str = "KV-Operation";
const KV_OPERATION_DELETE: &str = "DEL";
const KV_OPERATION_PURGE: &str = "PURGE";

trait PublishedBucket: Send + Sync {
    fn bucket_name(&self) -> &str;
    fn claim(&self, key: &str, value: &[u8]) -> std::io::Result<u64>;
    fn get(&self, key: &str) -> std::io::Result<Option<Vec<u8>>>;
    fn keys(&self) -> std::io::Result<Vec<String>>;
}

#[derive(Clone)]
struct JetStreamPublishedBucket {
    bucket: String,
    stream_name: String,
    prefix: String,
    js: jetstream::JetStream,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BucketOperation {
    Put,
    Delete,
    Purge,
}

impl JetStreamPublishedBucket {
    fn bind(js: jetstream::JetStream, bucket: impl Into<String>) -> io::Result<Self> {
        let bucket = bucket.into();
        let Some(stream_name) = find_stream_name(&js, &bucket)? else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("bucket {bucket} was not found"),
            ));
        };
        let stream_info = js.stream_info(&stream_name)?;
        if stream_info.config.max_msgs_per_subject < 1 {
            return Err(io::Error::other(format!(
                "bucket {bucket} is not a valid key-value store"
            )));
        }

        Ok(Self {
            prefix: format!("$KV.{bucket}."),
            bucket,
            stream_name,
            js,
        })
    }

    fn create(js: jetstream::JetStream, cfg: &PublishedStoreConfig) -> io::Result<Self> {
        if !bucket_name_is_valid(&cfg.bucket) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "bucket {} is not valid for new key-value creation",
                    cfg.bucket
                ),
            ));
        }

        let add_stream = |discard| {
            js.add_stream(&StreamConfig {
                name: format!("KV_{}", cfg.bucket),
                subjects: vec![bucket_subject_pattern(&cfg.bucket)],
                max_msgs_per_subject: cfg.history.max(1),
                storage: cfg.storage,
                allow_rollup: true,
                deny_delete: true,
                num_replicas: cfg.replicas.max(1),
                discard,
                ..Default::default()
            })
        };

        match add_stream(DiscardPolicy::New) {
            Ok(_) => Self::bind(js, cfg.bucket.clone()),
            Err(err) if jetstream_error_code(&err) == Some(ErrorCode::StreamNameExist) => {
                Self::bind(js, cfg.bucket.clone())
            }
            Err(err) if jetstream_error_code(&err) == Some(ErrorCode::StreamInvalidConfigF) => {
                match add_stream(DiscardPolicy::Old) {
                    Ok(_) => Self::bind(js, cfg.bucket.clone()),
                    Err(err) if jetstream_error_code(&err) == Some(ErrorCode::StreamNameExist) => {
                        Self::bind(js, cfg.bucket.clone())
                    }
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        }
    }

    fn subject_for(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }

    fn live_checkpoint_keys(&self) -> io::Result<Vec<String>> {
        let state = self.js.stream_info(&self.stream_name)?.state;
        if state.messages == 0 {
            return Ok(Vec::new());
        }

        let mut latest_operation_by_key = HashMap::new();

        for sequence in state.first_seq..=state.last_seq {
            let message = match self.js.get_message(&self.stream_name, sequence) {
                Ok(message) => message,
                Err(err) if jetstream_error_code(&err) == Some(ErrorCode::SequenceNotFound) => {
                    continue;
                }
                Err(err) => return Err(err),
            };

            let Some(key) = message.subject.strip_prefix(&self.prefix) else {
                continue;
            };
            if !key.starts_with(KEY_PREFIX) {
                continue;
            }

            latest_operation_by_key
                .insert(key.to_string(), bucket_operation(message.headers.as_ref()));
        }

        let mut keys: Vec<_> = latest_operation_by_key
            .into_iter()
            .filter_map(|(key, op)| (op == BucketOperation::Put).then_some(key))
            .collect();
        keys.sort_unstable();
        Ok(keys)
    }
}

impl PublishedBucket for JetStreamPublishedBucket {
    fn bucket_name(&self) -> &str {
        &self.bucket
    }

    fn claim(&self, key: &str, value: &[u8]) -> io::Result<u64> {
        let mut headers = HeaderMap::default();
        headers.insert(header::NATS_EXPECTED_LAST_SUBJECT_SEQUENCE, "0".to_string());
        let message = Message::new(&self.subject_for(key), None, value, Some(headers));
        let publish_ack = self.js.publish_message(&message)?;
        Ok(publish_ack.sequence)
    }

    fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        match self
            .js
            .get_last_message(&self.stream_name, &self.subject_for(key))
        {
            Ok(message) => match bucket_operation(message.headers.as_ref()) {
                BucketOperation::Put => Ok(Some(message.data)),
                BucketOperation::Delete | BucketOperation::Purge => Ok(None),
            },
            Err(err) if jetstream_error_code(&err) == Some(ErrorCode::NoMessageFound) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn keys(&self) -> io::Result<Vec<String>> {
        self.live_checkpoint_keys()
    }
}

fn bucket_operation(headers: Option<&HeaderMap>) -> BucketOperation {
    match headers.and_then(|headers| headers.get(KV_OPERATION)) {
        Some(op) if op == KV_OPERATION_DELETE => BucketOperation::Delete,
        Some(op) if op == KV_OPERATION_PURGE => BucketOperation::Purge,
        _ => BucketOperation::Put,
    }
}

fn jetstream_error_code(err: &io::Error) -> Option<ErrorCode> {
    err.get_ref()
        .and_then(|inner| inner.downcast_ref::<jetstream::Error>())
        .map(|error| error.error_code())
}

fn bucket_name_is_valid(bucket: &str) -> bool {
    !bucket.is_empty()
        && bucket
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

fn bucket_subject_pattern(bucket: &str) -> String {
    format!("$KV.{bucket}.>")
}

fn find_stream_name(js: &jetstream::JetStream, bucket: &str) -> io::Result<Option<String>> {
    if bucket_name_is_valid(bucket) {
        let direct_stream = format!("KV_{bucket}");
        match js.stream_info(&direct_stream) {
            Ok(_) => return Ok(Some(direct_stream)),
            Err(err) if jetstream_error_code(&err) == Some(ErrorCode::StreamNotFound) => {}
            Err(err) => return Err(err),
        }
    }

    let subject = bucket_subject_pattern(bucket);
    let mut matches = Vec::new();
    for item in js.list_streams() {
        let info = item?;
        if info
            .config
            .subjects
            .iter()
            .any(|candidate| candidate == &subject)
        {
            matches.push(info.config.name.clone());
        }
    }

    match matches.len() {
        0 => Ok(None),
        1 => Ok(matches.pop()),
        _ => Err(io::Error::other(format!(
            "multiple streams advertise published bucket {bucket}: {}",
            matches.join(", ")
        ))),
    }
}

#[derive(Clone)]
pub struct PublishedStore {
    bucket: Arc<dyn PublishedBucket>,
    retry: RetryPolicy,
    metrics: Arc<PublishedMetrics>,
}

pub trait PublishedCheckpointStore {
    fn publish(
        &self,
        remote_id: &str,
        checkpoint: &pb::PublishedCheckpoint,
    ) -> Result<PublishedOutcome, PublishedError>;

    fn list(&self) -> Result<Vec<pb::PublishedCheckpoint>, PublishedError>;

    fn fetch(&self, remote_id: &str) -> Result<pb::PublishedCheckpoint, PublishedError>;
}

#[derive(Debug, Clone)]
pub struct PublishedStoreConfig {
    pub bucket: String,
    pub storage: StorageType,
    pub history: i64,
    pub replicas: usize,
    pub timeout: Duration,
    pub max_attempts: usize,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
}

impl Default for PublishedStoreConfig {
    fn default() -> Self {
        Self {
            bucket: DEFAULT_PUBLISHED_BUCKET.into(),
            storage: StorageType::File,
            history: 1,
            replicas: 1,
            timeout: Duration::from_secs(2),
            max_attempts: 5,
            backoff_base: Duration::from_millis(50),
            backoff_max: Duration::from_millis(500),
        }
    }
}

#[derive(Debug, Clone)]
struct RetryPolicy {
    timeout: Duration,
    max_attempts: usize,
    backoff_base: Duration,
    backoff_max: Duration,
}

impl From<PublishedStoreConfig> for RetryPolicy {
    fn from(cfg: PublishedStoreConfig) -> Self {
        RetryPolicy {
            timeout: cfg.timeout,
            max_attempts: cfg.max_attempts.max(1),
            backoff_base: cfg.backoff_base,
            backoff_max: cfg.backoff_max,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishedOutcome {
    Stored,
    Unchanged,
}

#[derive(Debug, Error)]
pub enum PublishedError {
    #[error("invalid remote checkpoint id")]
    InvalidKey,
    #[error("jetstream error: {0}")]
    JetStream(#[from] std::io::Error),
    #[error("checkpoint {0} not found")]
    NotFound(String),
    #[error("checkpoint {remote_id} conflicts with existing payload")]
    IdempotencyConflict { remote_id: String },
    #[error("checkpoint {remote_id} payload is corrupt")]
    Corrupt { remote_id: String },
    #[error("encode/decode error: {0}")]
    Codec(#[from] fabricfs_session_protocol::session::SessionCodecError),
    #[error("operation {op} timed out after retries: {message}")]
    Timeout { op: String, message: String },
}

impl PublishedError {
    pub fn status(&self) -> pb::OperationStatus {
        pb::OperationStatus {
            ok: false,
            message: self.to_string(),
        }
    }

    fn retryable(&self) -> bool {
        matches!(self, PublishedError::JetStream(_))
    }
}

#[derive(Default)]
pub struct PublishedMetrics {
    publish_total: AtomicU64,
    publish_failures: AtomicU64,
    publish_retries: AtomicU64,
    list_total: AtomicU64,
    list_failures: AtomicU64,
    list_retries: AtomicU64,
    fetch_total: AtomicU64,
    fetch_failures: AtomicU64,
    fetch_retries: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedMetricsSnapshot {
    pub publish_total: u64,
    pub publish_failures: u64,
    pub publish_retries: u64,
    pub list_total: u64,
    pub list_failures: u64,
    pub list_retries: u64,
    pub fetch_total: u64,
    pub fetch_failures: u64,
    pub fetch_retries: u64,
}

#[derive(Clone, Copy)]
enum MetricOp {
    Publish,
    List,
    Fetch,
}

impl MetricOp {
    fn name(&self) -> &'static str {
        match self {
            MetricOp::Publish => "publish",
            MetricOp::List => "list",
            MetricOp::Fetch => "fetch",
        }
    }
}

impl PublishedMetrics {
    fn snapshot(&self) -> PublishedMetricsSnapshot {
        PublishedMetricsSnapshot {
            publish_total: self.publish_total.load(Ordering::Relaxed),
            publish_failures: self.publish_failures.load(Ordering::Relaxed),
            publish_retries: self.publish_retries.load(Ordering::Relaxed),
            list_total: self.list_total.load(Ordering::Relaxed),
            list_failures: self.list_failures.load(Ordering::Relaxed),
            list_retries: self.list_retries.load(Ordering::Relaxed),
            fetch_total: self.fetch_total.load(Ordering::Relaxed),
            fetch_failures: self.fetch_failures.load(Ordering::Relaxed),
            fetch_retries: self.fetch_retries.load(Ordering::Relaxed),
        }
    }

    fn record_attempt(&self, op: MetricOp) {
        match op {
            MetricOp::Publish => {
                self.publish_total.fetch_add(1, Ordering::Relaxed);
            }
            MetricOp::List => {
                self.list_total.fetch_add(1, Ordering::Relaxed);
            }
            MetricOp::Fetch => {
                self.fetch_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn record_failure(&self, op: MetricOp) {
        match op {
            MetricOp::Publish => {
                self.publish_failures.fetch_add(1, Ordering::Relaxed);
            }
            MetricOp::List => {
                self.list_failures.fetch_add(1, Ordering::Relaxed);
            }
            MetricOp::Fetch => {
                self.fetch_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn record_retry(&self, op: MetricOp) {
        match op {
            MetricOp::Publish => {
                self.publish_retries.fetch_add(1, Ordering::Relaxed);
            }
            MetricOp::List => {
                self.list_retries.fetch_add(1, Ordering::Relaxed);
            }
            MetricOp::Fetch => {
                self.fetch_retries.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl PublishedStore {
    pub fn new(js: jetstream::JetStream) -> Result<Self, PublishedError> {
        Self::with_config(js, PublishedStoreConfig::default())
    }

    pub fn with_config(
        js: jetstream::JetStream,
        cfg: PublishedStoreConfig,
    ) -> Result<Self, PublishedError> {
        let retry = RetryPolicy::from(cfg.clone());
        let bucket = Self::resolve_bucket(js, &cfg)?;

        Ok(PublishedStore {
            bucket,
            retry,
            metrics: Arc::new(PublishedMetrics::default()),
        })
    }

    pub fn publish(
        &self,
        remote_id: &str,
        checkpoint: &pb::PublishedCheckpoint,
    ) -> Result<PublishedOutcome, PublishedError> {
        let key = Self::key_for(remote_id)?;
        let payload = Arc::new(encode_session_message(checkpoint)?);

        self.with_retry(MetricOp::Publish, || {
            Self::publish_claim(self.bucket.as_ref(), &key, remote_id, payload.as_ref())
        })
    }

    pub fn list(&self) -> Result<Vec<pb::PublishedCheckpoint>, PublishedError> {
        self.with_retry(MetricOp::List, || {
            let mut out = Vec::new();
            let keys = self.bucket.keys().map_err(PublishedError::JetStream)?;
            for key in keys {
                if !key.starts_with(KEY_PREFIX) {
                    continue;
                }
                if let Some(bytes) = self.bucket.get(&key)? {
                    let checkpoint = decode_session_message::<pb::PublishedCheckpoint>(&bytes)
                        .map_err(|_| PublishedError::Corrupt {
                            remote_id: key.trim_start_matches(KEY_PREFIX).to_string(),
                        })?;
                    out.push(checkpoint);
                }
            }
            Ok(out)
        })
    }

    pub fn fetch(&self, remote_id: &str) -> Result<pb::PublishedCheckpoint, PublishedError> {
        let key = Self::key_for(remote_id)?;
        self.with_retry(MetricOp::Fetch, || {
            let Some(bytes) = self.bucket.get(&key)? else {
                return Err(PublishedError::NotFound(remote_id.to_string()));
            };
            decode_session_message::<pb::PublishedCheckpoint>(&bytes).map_err(|_| {
                PublishedError::Corrupt {
                    remote_id: remote_id.to_string(),
                }
            })
        })
    }

    pub fn metrics(&self) -> PublishedMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn bucket_name(&self) -> &str {
        self.bucket.bucket_name()
    }

    fn resolve_bucket(
        js: jetstream::JetStream,
        cfg: &PublishedStoreConfig,
    ) -> Result<Arc<dyn PublishedBucket>, PublishedError> {
        match JetStreamPublishedBucket::bind(js.clone(), cfg.bucket.clone()) {
            Ok(store) => return Ok(Arc::new(store)),
            Err(err)
                if err.kind() == io::ErrorKind::NotFound
                    || jetstream_error_code(&err) == Some(ErrorCode::StreamNotFound) => {}
            Err(err) => return Err(PublishedError::JetStream(err)),
        }

        Ok(Arc::new(JetStreamPublishedBucket::create(js, cfg)?))
    }

    fn publish_claim<C>(
        bucket: &C,
        key: &str,
        remote_id: &str,
        payload: &[u8],
    ) -> Result<PublishedOutcome, PublishedError>
    where
        C: PublishedBucket + ?Sized,
    {
        // Claim the remote ID atomically, then classify the winner's payload.
        match bucket.claim(key, payload) {
            Ok(_) => Ok(PublishedOutcome::Stored),
            Err(claim_err) => match bucket.get(key) {
                Ok(Some(existing)) if existing == payload => Ok(PublishedOutcome::Unchanged),
                Ok(Some(_)) => Err(PublishedError::IdempotencyConflict {
                    remote_id: remote_id.to_string(),
                }),
                Ok(None) | Err(_) => Err(PublishedError::JetStream(claim_err)),
            },
        }
    }

    fn with_retry<T, F>(&self, op: MetricOp, mut f: F) -> Result<T, PublishedError>
    where
        F: FnMut() -> Result<T, PublishedError>,
    {
        self.metrics.record_attempt(op);

        let start = Instant::now();
        let mut delay = self.retry.backoff_base;
        let mut last_error = None;

        for attempt in 1..=self.retry.max_attempts {
            match f() {
                Ok(value) => return Ok(value),
                Err(err) if !err.retryable() => {
                    self.metrics.record_failure(op);
                    return Err(err);
                }
                Err(err) => {
                    last_error = Some(err);
                    if attempt == self.retry.max_attempts || start.elapsed() >= self.retry.timeout {
                        let message = last_error
                            .as_ref()
                            .map(|e| e.to_string())
                            .unwrap_or_else(|| "unknown failure".into());
                        let timeout = PublishedError::Timeout {
                            op: op.name().into(),
                            message,
                        };
                        self.metrics.record_failure(op);
                        return Err(timeout);
                    }
                    self.metrics.record_retry(op);
                    thread::sleep(delay);
                    delay = std::cmp::min(self.retry.backoff_max, delay.saturating_mul(2));
                }
            }
        }

        let message = last_error
            .as_ref()
            .map(|e| e.to_string())
            .unwrap_or_else(|| "exhausted retries".into());
        let timeout = PublishedError::Timeout {
            op: op.name().into(),
            message,
        };
        self.metrics.record_failure(op);
        Err(timeout)
    }

    pub(crate) fn key_for(remote_id: &str) -> Result<String, PublishedError> {
        let trimmed = remote_id.trim();
        if trimmed.is_empty()
            || trimmed.contains(char::is_whitespace)
            || trimmed.contains('/')
            || trimmed.contains('\\')
        {
            return Err(PublishedError::InvalidKey);
        }
        Ok(format!("{KEY_PREFIX}{trimmed}"))
    }
}

impl PublishedCheckpointStore for PublishedStore {
    fn publish(
        &self,
        remote_id: &str,
        checkpoint: &pb::PublishedCheckpoint,
    ) -> Result<PublishedOutcome, PublishedError> {
        PublishedStore::publish(self, remote_id, checkpoint)
    }

    fn list(&self) -> Result<Vec<pb::PublishedCheckpoint>, PublishedError> {
        PublishedStore::list(self)
    }

    fn fetch(&self, remote_id: &str) -> Result<pb::PublishedCheckpoint, PublishedError> {
        PublishedStore::fetch(self, remote_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;

    #[derive(Default)]
    struct FakeClaimStore {
        entries: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl PublishedBucket for FakeClaimStore {
        fn bucket_name(&self) -> &str {
            "fake"
        }

        fn claim(&self, key: &str, value: &[u8]) -> io::Result<u64> {
            let mut entries = self.entries.lock().expect("claim store lock");
            if entries.contains_key(key) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "remote checkpoint already claimed",
                ));
            }
            entries.insert(key.to_string(), value.to_vec());
            Ok(entries.len() as u64)
        }

        fn get(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
            Ok(self
                .entries
                .lock()
                .expect("claim store lock")
                .get(key)
                .cloned())
        }

        fn keys(&self) -> io::Result<Vec<String>> {
            Ok(self
                .entries
                .lock()
                .expect("claim store lock")
                .keys()
                .cloned()
                .collect())
        }
    }

    #[test]
    fn rejects_bad_remote_ids() {
        assert!(PublishedStore::key_for("").is_err());
        assert!(PublishedStore::key_for("with space").is_err());
        assert!(PublishedStore::key_for("with/slash").is_err());
        assert!(PublishedStore::key_for("with\\backslash").is_err());
        assert_eq!(PublishedStore::key_for("abc").unwrap(), "checkpoint/abc");
    }

    #[test]
    fn error_status_is_populated() {
        let err = PublishedError::NotFound("missing".into());
        let status = err.status();
        assert!(!status.ok);
        assert!(status.message.contains("missing"));
    }

    #[test]
    fn published_metrics_snapshot_tracks_attempts_failures_and_retries() {
        let metrics = PublishedMetrics::default();
        metrics.record_attempt(MetricOp::Publish);
        metrics.record_attempt(MetricOp::List);
        metrics.record_failure(MetricOp::List);
        metrics.record_retry(MetricOp::Fetch);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.publish_total, 1);
        assert_eq!(snapshot.list_total, 1);
        assert_eq!(snapshot.list_failures, 1);
        assert_eq!(snapshot.fetch_retries, 1);
    }

    #[test]
    fn default_bucket_name_uses_nats_kv_charset() {
        let cfg = PublishedStoreConfig::default();
        assert!(!cfg.bucket.is_empty());
        assert!(bucket_name_is_valid(&cfg.bucket));
        assert_eq!(cfg.bucket, DEFAULT_PUBLISHED_BUCKET);
    }

    #[test]
    fn publish_claim_returns_unchanged_for_identical_existing_payload() {
        let key = PublishedStore::key_for("remote").expect("key");
        let store = FakeClaimStore::default();
        store.claim(&key, b"same-bytes").expect("initial claim");

        let outcome = PublishedStore::publish_claim(&store, &key, "remote", b"same-bytes")
            .expect("idempotent publish");
        assert_eq!(outcome, PublishedOutcome::Unchanged);
    }

    #[test]
    fn publish_claim_allows_only_one_concurrent_winner_per_remote_id() {
        let key = PublishedStore::key_for("remote").expect("key");
        let store = Arc::new(FakeClaimStore::default());
        let barrier = Arc::new(Barrier::new(3));

        let first_store = Arc::clone(&store);
        let first_barrier = Arc::clone(&barrier);
        let first = thread::spawn(move || {
            first_barrier.wait();
            PublishedStore::publish_claim(first_store.as_ref(), &key, "remote", b"first")
        });

        let key = PublishedStore::key_for("remote").expect("key");
        let second_store = Arc::clone(&store);
        let second_barrier = Arc::clone(&barrier);
        let second = thread::spawn(move || {
            second_barrier.wait();
            PublishedStore::publish_claim(second_store.as_ref(), &key, "remote", b"second")
        });

        barrier.wait();

        let first_result = first.join().expect("first publish thread");
        let second_result = second.join().expect("second publish thread");
        let outcomes = [&first_result, &second_result];

        assert_eq!(
            outcomes
                .iter()
                .filter(|result| matches!(result, Ok(PublishedOutcome::Stored)))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| {
                    matches!(
                        result,
                        Err(PublishedError::IdempotencyConflict { remote_id })
                            if remote_id == "remote"
                    )
                })
                .count(),
            1
        );

        let stored = store
            .get("checkpoint/remote")
            .expect("stored payload")
            .expect("winner payload");
        let winner = if matches!(first_result, Ok(PublishedOutcome::Stored)) {
            b"first".to_vec()
        } else {
            b"second".to_vec()
        };
        assert_eq!(stored, winner);
    }
}
