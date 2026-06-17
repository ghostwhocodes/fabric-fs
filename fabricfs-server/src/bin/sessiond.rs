use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use fabricfs_observability::{init_tracing, spawn_periodic_metrics_logger};
use fabricfs_server::published_store::PublishedStore;
use fabricfs_server::session_service::SessionService;
use fabricfs_server::session_storage::SessionStore;
use fabricfs_transport::connect_nats;

#[derive(Parser, Debug)]
#[command(name = "fabricfs-sessiond")]
#[command(about = "SessionControl server for FabricFs (NATS-based)")]
struct Args {
    #[arg(long, default_value = "nats://127.0.0.1:4222")]
    nats_url: String,

    /// Path to NATS credentials file (overrides embedded credentials in URL).
    /// Can also be set via NATS_CREDS_FILE environment variable.
    #[arg(long, env = "NATS_CREDS_FILE")]
    nats_creds_file: Option<PathBuf>,

    #[arg(long, value_name = "PATH")]
    cow_root: PathBuf,

    #[arg(long, env = "FABRICFS_METRICS_INTERVAL_SECS", default_value_t = 30)]
    metrics_interval_secs: u64,
}

fn main() -> Result<()> {
    init_tracing("fabricfs_sessiond");
    let args = Args::parse();
    let cow_root = args
        .cow_root
        .canonicalize()
        .with_context(|| format!("cow_root {} does not exist", args.cow_root.display()))?;

    let store = SessionStore::load(cow_root.clone()).context("init session store")?;
    let nats = connect_nats(
        &args.nats_url,
        args.nats_creds_file
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned())
            .as_deref(),
    )?;
    let js = nats::jetstream::new(nats.clone());
    let published = PublishedStore::new(js).context("init published store")?;
    let published_metrics = published.clone();

    tracing::info!(
        subject_prefix = fabricfs_session_protocol::session::SESSION_SUBJECT_PREFIX,
        cow_root = %cow_root.display(),
        "fabricfs-sessiond ready"
    );

    let service = SessionService::new(nats, store, published);
    let service_metrics = service.metrics_handle();
    let _metrics_reporters = if args.metrics_interval_secs == 0 {
        Vec::new()
    } else {
        tracing::info!(
            interval_secs = args.metrics_interval_secs,
            "runtime metrics logging enabled"
        );
        vec![
            spawn_periodic_metrics_logger(
                "session_service",
                Duration::from_secs(args.metrics_interval_secs),
                {
                    let metrics = service_metrics.clone();
                    move || metrics.snapshot()
                },
            ),
            spawn_periodic_metrics_logger(
                "published_store",
                Duration::from_secs(args.metrics_interval_secs),
                {
                    let published = published_metrics.clone();
                    move || published.metrics()
                },
            ),
        ]
    };
    service.run()
}
