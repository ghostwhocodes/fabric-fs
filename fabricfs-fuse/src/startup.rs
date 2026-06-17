use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use fabricfs_transport::{FileSystemClient, FileSystemClientConfig, TransportAuth};
use fs_core::RpcClient;
use fs_protocol::{path, pb, Errno, RequestEnvelope, RequestPayload};

pub fn filesystem_client_config(
    transport_auth: TransportAuth,
    timeout: Duration,
) -> FileSystemClientConfig {
    FileSystemClientConfig {
        timeout,
        transport_auth: Some(transport_auth),
        ..FileSystemClientConfig::default()
    }
}

fn readiness_client_config(
    runtime_client_config: &FileSystemClientConfig,
) -> FileSystemClientConfig {
    let mut config = runtime_client_config.clone();
    config.max_retries = 0;
    config
}

pub fn require_backend_ready(
    mount_name: &str,
    nats: &nats::Connection,
    runtime_client_config: &FileSystemClientConfig,
) -> Result<()> {
    let client = FileSystemClient::with_config(
        mount_name.to_string(),
        nats.clone(),
        readiness_client_config(runtime_client_config),
    )
    .map_err(|error| anyhow!("invalid filesystem client config: {error}"))?;

    backend_ready_via_client(mount_name, &client)
}

fn backend_ready_via_client<C>(mount_name: &str, client: &C) -> Result<()>
where
    C: RpcClient,
{
    let request = RequestEnvelope::new(
        "fabricfs-fuse-readiness",
        mount_name,
        0,
        pb::TraceContext::default(),
        RequestPayload::Getattr(pb::GetattrRequest {
            path: Some(path("/").context("root path DTO must be valid")?),
        }),
    )
    .context("failed to build readiness request")?
    .with_caller(current_caller_context());

    let response = client.call(request).map_err(|error| {
        anyhow!(
            "backend getattr probe failed: {error}; errno={:?}",
            error.errno()
        )
    })?;

    if response.ok {
        return Ok(());
    }

    let errno = response.errno.unwrap_or(Errno::Io).wire_value();
    Err(anyhow!(
        "backend unavailable (errno={}): {}",
        errno,
        readiness_hint(errno)
    ))
}

fn current_caller_context() -> pb::CallerContext {
    pb::CallerContext {
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
        pid: unsafe { libc::getpid() as u32 },
    }
}

pub fn readiness_hint(errno: i32) -> &'static str {
    match errno {
        code if code == libc::ETIMEDOUT => {
            "backend did not respond before the probe deadline; ensure fabricfs-server is running or increase --timeout-secs"
        }
        code if code == libc::EAGAIN || code == libc::EWOULDBLOCK => {
            "publish to NATS failed; verify the NATS URL and connectivity"
        }
        code if code == libc::ENOSYS => "client/server protocol mismatch; rebuild both sides",
        code
            if code == libc::ECONNREFUSED
                || code == libc::ENOTCONN
                || code == libc::EHOSTUNREACH =>
        {
            "connection refused by NATS; check the broker status and credentials"
        }
        _ => "backend reported an error during readiness probe",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    use fabricfs_transport::{subscribe_requests, FileSystemServer};
    use fs_core::{Dispatcher, FileSystemService, FsResult, RpcMetadata};
    use fs_protocol::directory_attr;

    #[test]
    fn readiness_probe_reuses_runtime_timeout_and_disables_retries() {
        let runtime = filesystem_client_config(
            TransportAuth::shared_secret("test-secret").expect("transport auth"),
            Duration::from_secs(9),
        );

        let readiness = readiness_client_config(&runtime);

        assert_eq!(readiness.timeout, runtime.timeout);
        assert_eq!(readiness.max_retries, 0);
        assert_eq!(readiness.retry_backoff, runtime.retry_backoff);
        assert_eq!(readiness.max_frame_bytes, runtime.max_frame_bytes);
        assert_eq!(
            readiness.transport_auth.is_some(),
            runtime.transport_auth.is_some()
        );
    }

    #[test]
    fn readiness_probe_defaults_to_transport_client_timeout() {
        let config = filesystem_client_config(
            TransportAuth::shared_secret("test-secret").expect("transport auth"),
            FileSystemClientConfig::default().timeout,
        );

        assert_eq!(config.timeout, FileSystemClientConfig::default().timeout);
    }

    #[test]
    fn delayed_healthy_backend_passes_startup_probe_when_timeout_allows_it() {
        let Some(broker) = NatsBroker::start() else {
            eprintln!("nats-server is not available; skipping live FUSE readiness test");
            return;
        };
        let mount = "fabricfs-fuse-readiness-test";
        let server = with_test_transport_auth(
            FileSystemServer::new(Arc::new(Dispatcher::new(SlowGetattrFs {
                delay: Duration::from_millis(700),
            })))
            .with_expected_namespace(mount),
        );
        let _task =
            ServerTask::spawn(broker.url.clone(), mount.to_string(), server).expect("server task");
        let client_connection = nats::connect(&broker.url).expect("client connects");
        let runtime_client_config =
            filesystem_client_config(test_transport_auth(), Duration::from_secs(2));

        require_backend_ready(mount, &client_connection, &runtime_client_config)
            .expect("a healthy backend within the configured timeout must pass startup");
    }

    #[test]
    fn timeout_hint_points_to_real_cli_surface() {
        assert!(readiness_hint(libc::ETIMEDOUT).contains("--timeout-secs"));
    }

    #[derive(Clone)]
    struct SlowGetattrFs {
        delay: Duration,
    }

    impl FileSystemService for SlowGetattrFs {
        fn getattr(
            &self,
            _request: &pb::GetattrRequest,
            _metadata: &RpcMetadata,
        ) -> FsResult<pb::GetattrResponse> {
            thread::sleep(self.delay);
            Ok(pb::GetattrResponse {
                attr: Some(directory_attr(1)),
            })
        }
    }

    fn test_transport_auth() -> TransportAuth {
        TransportAuth::shared_secret("test-secret").expect("transport auth")
    }

    fn with_test_transport_auth(server: FileSystemServer) -> FileSystemServer {
        server.with_transport_auth(test_transport_auth())
    }

    struct ServerTask {
        stop: Arc<AtomicBool>,
        thread: Option<thread::JoinHandle<()>>,
    }

    impl ServerTask {
        fn spawn(url: String, mount: String, server: FileSystemServer) -> Result<Self, String> {
            let stop = Arc::new(AtomicBool::new(false));
            let thread_stop = stop.clone();
            let (ready_tx, ready_rx) = mpsc::channel();

            let handle = thread::spawn(move || {
                let connection = match nats::connect(&url) {
                    Ok(connection) => connection,
                    Err(error) => {
                        let _ = ready_tx.send(Err(format!("server connect failed: {error}")));
                        return;
                    }
                };
                let subscription = match subscribe_requests(&connection, &mount) {
                    Ok(subscription) => subscription,
                    Err(error) => {
                        let _ = ready_tx.send(Err(format!("server subscribe failed: {error}")));
                        return;
                    }
                };
                let _ = ready_tx.send(Ok(()));

                while !thread_stop.load(Ordering::SeqCst) {
                    match subscription.next_timeout(Duration::from_millis(25)) {
                        Ok(message) => {
                            let _ = server.handle_message(&connection, message);
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::TimedOut => {}
                        Err(_) => break,
                    }
                }
            });

            match ready_rx.recv_timeout(Duration::from_secs(2)) {
                Ok(Ok(())) => Ok(Self {
                    stop,
                    thread: Some(handle),
                }),
                Ok(Err(error)) => {
                    stop.store(true, Ordering::SeqCst);
                    let _ = handle.join();
                    Err(error)
                }
                Err(error) => {
                    stop.store(true, Ordering::SeqCst);
                    let _ = handle.join();
                    Err(format!("server task did not become ready: {error}"))
                }
            }
        }
    }

    impl Drop for ServerTask {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            if let Some(handle) = self.thread.take() {
                let _ = handle.join();
            }
        }
    }

    struct NatsBroker {
        url: String,
        child: Option<Child>,
    }

    impl NatsBroker {
        fn start() -> Option<Self> {
            if let Ok(url) = std::env::var("NATS_TEST_URL") {
                return wait_for_broker(url).map(|url| Self { url, child: None });
            }

            let port = free_tcp_port()?;
            let url = format!("nats://127.0.0.1:{port}");
            let mut child = Command::new("nats-server")
                .arg("-a")
                .arg("127.0.0.1")
                .arg("-p")
                .arg(port.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .ok()?;

            if let Some(url) = wait_for_broker(url.clone()) {
                Some(Self {
                    url,
                    child: Some(child),
                })
            } else {
                let _ = child.kill();
                let _ = child.wait();
                None
            }
        }
    }

    impl Drop for NatsBroker {
        fn drop(&mut self) {
            if let Some(child) = &mut self.child {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    fn free_tcp_port() -> Option<u16> {
        TcpListener::bind("127.0.0.1:0")
            .ok()
            .and_then(|listener| listener.local_addr().ok())
            .map(|addr| addr.port())
    }

    fn wait_for_broker(url: String) -> Option<String> {
        let started = Instant::now();
        while started.elapsed() < Duration::from_secs(3) {
            if nats::connect(&url).is_ok() {
                return Some(url);
            }
            thread::sleep(Duration::from_millis(50));
        }
        None
    }
}
