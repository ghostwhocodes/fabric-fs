use anyhow::{Context, Result};
use clap::Parser;
use fabricfs_observability::{init_tracing, spawn_periodic_metrics_logger};
use fs_core::Dispatcher;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fabricfs_server::{
    auth::FabricFsAuthorizer,
    overlay::OverlayFs,
    passthrough::PassthroughFs,
    server::{current_process_umask, DynStorage, FsLimits, FsOptions},
    service::FabricFsFileSystemService,
    watch::{start_storage_invalidation_watcher, StorageInvalidationGate},
    worker_pool::WorkerPool,
};
use fabricfs_transport::{
    connect_nats, publish_invalidation, subscribe_requests, subscription_subject, FileSystemServer,
    TransportAuth,
};
use fs_protocol::{pb, InvalidationKind, PROTOCOL_VERSION};

#[derive(Parser, Debug)]
#[command(name = "fabricfs-server")]
#[command(about = "NATS-backed fabricfs server with overlay filesystem", long_about = None)]
struct Args {
    #[arg(long, default_value = "nats://127.0.0.1:4222")]
    nats_url: String,
    #[arg(long, env = "FABRICFS_TRANSPORT_AUTH_TOKEN")]
    transport_auth_token: String,
    /// Path to NATS credentials file (overrides embedded credentials in URL).
    /// Can also be set via NATS_CREDS_FILE environment variable.
    #[arg(long, env = "NATS_CREDS_FILE")]
    nats_creds_file: Option<PathBuf>,
    #[arg(long, default_value = "fabricfs")]
    mount_name: String,
    #[arg(long)]
    backing_root: Option<PathBuf>,
    #[arg(long)]
    alias_path: Option<PathBuf>,
    #[arg(long)]
    cow_path: Option<PathBuf>,
    #[arg(long)]
    worker_threads: Option<usize>,
    #[arg(long)]
    max_queued: Option<usize>,
    #[arg(long, default_value_t = default_io_chunk_bytes())]
    io_chunk_bytes: usize,
    #[arg(long, default_value_t = default_max_read_bytes())]
    max_read_bytes: usize,
    #[arg(long, default_value_t = default_umask(), value_parser = parse_octal_umask)]
    umask: u32,
    #[arg(long, default_value_t = false)]
    propagate_acls: bool,
    #[arg(long, default_value_t = false)]
    update_permissions: bool,
    #[arg(long, default_value_t = false)]
    update_xattrs: bool,
    #[arg(long, default_value_t = false)]
    update_backingtree: bool,
    #[arg(long, default_value_t = true)]
    enable_reflinks: bool,
    #[arg(long, default_value_t = true)]
    preserve_sparse_files: bool,
    #[arg(long, env = "FABRICFS_METRICS_INTERVAL_SECS", default_value_t = 30)]
    metrics_interval_secs: u64,
    /// Maximum future deadline accepted on authenticated filesystem requests.
    #[arg(
        long,
        env = "FABRICFS_AUTHENTICATED_REQUEST_TTL_SECS",
        default_value_t = 300
    )]
    authenticated_request_ttl_secs: u64,
}

fn default_worker_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

fn default_io_chunk_bytes() -> usize {
    FsLimits::default().io_chunk_bytes
}

fn default_max_read_bytes() -> usize {
    FsLimits::default().max_read_bytes
}

fn default_umask() -> u32 {
    current_process_umask()
}

fn duration_secs_to_nanos(secs: u64) -> u64 {
    secs.saturating_mul(1_000_000_000).max(1)
}

fn parse_octal_umask(src: &str) -> Result<u32, String> {
    u32::from_str_radix(src, 8)
        .map_err(|e| format!("invalid umask '{src}': {e}"))
        .and_then(|v| {
            if v > 0o777 {
                Err("umask must be <= 0777".to_string())
            } else {
                Ok(v)
            }
        })
}

fn ensure_existing_dir(path: &Path, label: &str) -> Result<()> {
    if !path.is_dir() {
        anyhow::bail!("{label} must exist and be a directory: {}", path.display());
    }
    Ok(())
}

fn main() -> Result<()> {
    init_tracing("fabricfs_server");
    let args = Args::parse();
    let creds_file = args
        .nats_creds_file
        .as_ref()
        .map(|path| path.to_string_lossy().into_owned());
    let transport_auth = TransportAuth::shared_secret(&args.transport_auth_token)
        .map_err(|error| anyhow::anyhow!("invalid transport auth token: {error}"))?;
    let nats = connect_nats(&args.nats_url, creds_file.as_deref())?;
    tracing::info!(
        protocol_version = PROTOCOL_VERSION,
        "fabricfs-server protocol ready"
    );

    // Validate directories
    if let Some(root) = &args.backing_root {
        ensure_existing_dir(root, "backing_root")?;
    }
    if let Some(cow) = &args.cow_path {
        ensure_existing_dir(cow, "cow_path")?;
    }
    if let Some(alias) = &args.alias_path {
        ensure_existing_dir(alias, "alias_path")?;
    }
    if args.io_chunk_bytes == 0 {
        anyhow::bail!("io_chunk_bytes must be greater than zero");
    }
    if args.max_read_bytes == 0 {
        anyhow::bail!("max_read_bytes must be greater than zero");
    }

    // Build filesystem
    let limits = FsLimits::new(args.io_chunk_bytes, args.max_read_bytes);
    let mut options = FsOptions::with_limits(limits.clone());
    options.umask = args.umask & 0o777;
    options.propagate_acls = args.propagate_acls;
    options.allow_backing_permission_updates = args.update_permissions;
    options.allow_xattr_updates = args.update_xattrs;
    options.allow_direct_backing_updates = args.update_backingtree;

    let overlay_mode =
        args.backing_root.is_some() || args.alias_path.is_some() || args.cow_path.is_some();
    let storage_watch_roots: Vec<PathBuf> = if overlay_mode {
        vec![
            args.backing_root.clone(),
            args.alias_path.clone(),
            args.cow_path.clone(),
        ]
        .into_iter()
        .flatten()
        .collect()
    } else {
        vec![PathBuf::from("/tmp/fabricfs_default")]
    };
    let storage_watch_gate = StorageInvalidationGate::new();

    // Choose filesystem implementation based on overlay configuration
    let fs: DynStorage = if overlay_mode {
        // Use OverlayFs for overlay mode
        Arc::new(OverlayFs::new_with_internal_metadata_notifier(
            args.backing_root.clone(),
            args.alias_path.clone(),
            args.cow_path.clone(),
            limits,
            options.umask,
            options.propagate_acls,
            options.allow_backing_permission_updates,
            options.allow_xattr_updates,
            options.allow_direct_backing_updates,
            args.enable_reflinks,
            args.preserve_sparse_files,
            Some(Arc::new(storage_watch_gate.clone())),
        )?)
    } else {
        // Use PassthroughFs for simple passthrough mode
        let passthrough_root = storage_watch_roots[0].clone();
        ensure_existing_dir(&passthrough_root, "passthrough_root")?;
        Arc::new(PassthroughFs::new(passthrough_root, options)?)
    };

    let dispatcher = Arc::new(Dispatcher::with_authorizer(
        FabricFsFileSystemService::new(fs.clone()),
        FabricFsAuthorizer::for_namespace(args.mount_name.clone()),
    ));
    let rpc = Arc::new(
        FileSystemServer::new(dispatcher.clone())
            .with_invalidation_mount(args.mount_name.clone())
            .with_expected_namespace(args.mount_name.clone())
            .with_transport_auth(transport_auth)
            .with_max_authenticated_request_ttl_nanos(duration_secs_to_nanos(
                args.authenticated_request_ttl_secs,
            )),
    );

    let worker_threads = args
        .worker_threads
        .unwrap_or_else(default_worker_threads)
        .max(1);
    let queue_depth = args
        .max_queued
        .unwrap_or(worker_threads * 4)
        .max(worker_threads);
    let pool = Arc::new(WorkerPool::new(worker_threads, queue_depth)?);
    let _metrics_reporters = if args.metrics_interval_secs == 0 {
        Vec::new()
    } else {
        tracing::info!(
            interval_secs = args.metrics_interval_secs,
            "runtime metrics logging enabled"
        );
        vec![spawn_periodic_metrics_logger(
            "worker_pool",
            Duration::from_secs(args.metrics_interval_secs),
            {
                let pool = Arc::clone(&pool);
                move || pool.metrics()
            },
        )]
    };

    let subject = subscription_subject(&args.mount_name);
    let sub = subscribe_requests(&nats, &args.mount_name)?;
    let watch_namespace = args.mount_name.clone();
    let watch_dispatcher_for_watcher = dispatcher.clone();
    let watch_request_id = Arc::new(AtomicU64::new(1));
    let watch_request_id_for_closure = watch_request_id.clone();
    let _storage_watcher =
        start_storage_invalidation_watcher(
            nats.clone(),
            args.mount_name.clone(),
            storage_watch_roots,
            storage_watch_gate.clone(),
            move || {
                let request_id = watch_request_id_for_closure.fetch_add(1, Ordering::SeqCst);
                Some(watch_dispatcher_for_watcher.full_resync_invalidation(
                    &watch_namespace,
                    format!("storage-watch-{request_id}"),
                ))
            },
        )?;
    let startup_invalidation = pb::Invalidation {
        namespace: args.mount_name.clone(),
        sequence: 0,
        kind: InvalidationKind::FullResync.wire_value(),
        path: String::new(),
        old_path: String::new(),
        new_path: String::new(),
        inode: 0,
        handle: 0,
        request_id: "server-start".into(),
    };
    publish_invalidation(&nats, &args.mount_name, &startup_invalidation)?;
    storage_watch_gate.activate();
    nats.flush()
        .context("flush startup full-resync invalidation")?;
    tracing::info!(
        subject,
        worker_threads,
        queue_depth,
        "fabricfs-server subscribed to filesystem requests"
    );
    if args.backing_root.is_some() || args.alias_path.is_some() || args.cow_path.is_some() {
        tracing::info!("overlay mode enabled");
        if let Some(b) = &args.backing_root {
            tracing::info!(backing = %b.display(), "overlay backing root");
        }
        if let Some(a) = &args.alias_path {
            tracing::info!(alias = %a.display(), "overlay alias root");
        }
        if let Some(c) = &args.cow_path {
            tracing::info!(cow = %c.display(), "overlay cow root");
        }
    } else {
        tracing::info!("passthrough mode enabled");
    }

    for msg in sub.messages() {
        let rpc = rpc.clone();
        let nats = nats.clone();
        let storage_watch_gate = storage_watch_gate.clone();
        pool.submit(move || {
            let request_guard = storage_watch_gate.start_request();
            let result = rpc.handle_message(&nats, msg);
            request_guard.finish();
            if let Err(e) = result {
                tracing::error!(error = ?e, "filesystem request handler failed");
            }
        })
        .map_err(|e| anyhow::anyhow!("failed to submit job: {:?}", e))?;
    }

    Ok(())
}
