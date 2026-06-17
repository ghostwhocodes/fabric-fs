use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "fabricfs-fuse")]
#[command(about = "FUSE3 → NATS bridge for fabricfs", long_about = None)]
pub struct Args {
    /// Mount point path (e.g. /tmp/fabricfs-mnt)
    pub mount_point: PathBuf,

    /// NATS server URL (e.g. nats://127.0.0.1:4222)
    pub nats_url: String,

    /// Path to NATS credentials file (overrides embedded credentials in URL).
    /// Can also be set via NATS_CREDS_FILE environment variable.
    #[arg(long, env = "NATS_CREDS_FILE")]
    pub nats_creds_file: Option<PathBuf>,

    /// Logical mount name (defaults to mount point string)
    #[arg(long)]
    pub mount_name: Option<String>,

    /// Filesystem RPC timeout in seconds for startup readiness and mounted calls.
    #[arg(long, default_value_t = 5)]
    pub timeout_secs: u64,
}

impl Args {
    pub fn resolved_mount_name(&self) -> String {
        self.mount_name
            .clone()
            .unwrap_or_else(|| self.mount_point.to_string_lossy().into_owned())
    }
}

pub fn parse_args() -> Args {
    Args::parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_secs_defaults_to_five() {
        let args = Args::parse_from(["fabricfs-fuse", "/tmp/mount", "nats://127.0.0.1:4222"]);

        assert_eq!(args.timeout_secs, 5);
    }

    #[test]
    fn timeout_secs_accepts_explicit_override() {
        let args = Args::parse_from([
            "fabricfs-fuse",
            "/tmp/mount",
            "nats://127.0.0.1:4222",
            "--timeout-secs",
            "12",
        ]);

        assert_eq!(args.timeout_secs, 12);
    }
}
