use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use fabricfs_fuse::{cli, fs::FabricFsFuse, startup};
use fabricfs_observability::{init_tracing, spawn_periodic_metrics_logger};
use fabricfs_transport::{
    connect_nats, redact_nats_url, FileSystemClient, FileSystemClientConfig, TransportAuth,
};
use fs_fuse::FuseAdapter;
use fs_protocol::PROTOCOL_VERSION;
use fuser::MountOption;

fn main() -> Result<()> {
    init_tracing("fabricfs_fuse");
    let args = cli::parse_args();
    let mount_name = args.resolved_mount_name();
    let debug = std::env::var("FABRICFS_DEBUG").is_ok();
    let creds_file = args
        .nats_creds_file
        .as_ref()
        .map(|path| path.to_string_lossy().into_owned());
    let transport_auth = std::env::var("FABRICFS_TRANSPORT_AUTH_TOKEN").map_err(|_| {
        anyhow!("FABRICFS_TRANSPORT_AUTH_TOKEN must be set for filesystem transport")
    })?;
    let transport_auth = TransportAuth::shared_secret(&transport_auth)
        .map_err(|error| anyhow!("invalid transport auth token: {error}"))?;
    let metrics_interval_secs = std::env::var("FABRICFS_METRICS_INTERVAL_SECS")
        .ok()
        .map_or(30, |value| value.parse::<u64>().unwrap_or(30));
    let client_config = startup::filesystem_client_config(
        transport_auth.clone(),
        Duration::from_secs(args.timeout_secs),
    );

    let nats = connect_nats(&args.nats_url, creds_file.as_deref()).with_context(|| {
        format!(
            "failed to connect to NATS at {}",
            redact_nats_url(&args.nats_url)
        )
    })?;

    startup::require_backend_ready(&mount_name, &nats, &client_config)
        .context("backend unavailable; refusing to mount")?;

    let client = FileSystemClient::with_config(
        mount_name.clone(),
        nats,
        FileSystemClientConfig {
            max_retries: client_config.max_retries,
            retry_backoff: client_config.retry_backoff,
            max_frame_bytes: client_config.max_frame_bytes,
            timeout: client_config.timeout,
            transport_auth: Some(transport_auth),
        },
    )
    .map_err(|error| anyhow!("invalid filesystem client config: {error}"))?;
    let client_metrics = client.clone();
    let adapter = FuseAdapter::new(client, mount_name.clone());
    let adapter_metrics = adapter.metrics_handle();
    let filesystem = FabricFsFuse::new(adapter, debug);
    let _metrics_reporters = if metrics_interval_secs == 0 {
        Vec::new()
    } else {
        tracing::info!(
            interval_secs = metrics_interval_secs,
            "runtime metrics logging enabled"
        );
        vec![
            spawn_periodic_metrics_logger(
                "fuse_adapter",
                Duration::from_secs(metrics_interval_secs),
                {
                    let metrics = adapter_metrics.clone();
                    move || metrics.snapshot()
                },
            ),
            spawn_periodic_metrics_logger(
                "transport_client",
                Duration::from_secs(metrics_interval_secs),
                move || client_metrics.metrics(),
            ),
        ]
    };

    let options = vec![MountOption::FSName("fabricfs".to_string())];

    tracing::info!(
        protocol_version = PROTOCOL_VERSION,
        mount_name,
        mount_point = %args.mount_point.display(),
        ?options,
        "mounting fabricfs FUSE filesystem"
    );

    fuser::mount2(filesystem, &args.mount_point, &options)
        .context("failed to mount FUSE filesystem")?;

    tracing::info!("fabricfs-fuse mount succeeded");
    Ok(())
}
