#!/usr/bin/env bash
set -euo pipefail

NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CREDS_FILE="${NATS_CREDS_FILE:-}"
MOUNT_NAME="${MOUNT_NAME:-fabricfs-smoke}"
FABRICFS_TRANSPORT_AUTH_TOKEN="${FABRICFS_TRANSPORT_AUTH_TOKEN:-$(python3 - <<'PY'
import secrets
print(secrets.token_hex(16))
PY
)}"
MOUNTPOINT="$(mktemp -d)"
BACKING_ROOT="$(mktemp -d)"
COW_ROOT="$(mktemp -d)"
ALIAS_ROOT="${ALIAS_ROOT:-$(mktemp -d)}"
SERVER_LOG="$(mktemp)"
FUSE_LOG="$(mktemp)"
FUSE_RUST_LOG="${FUSE_RUST_LOG:-debug}"

redact_url() {
  echo "$1" | sed -E 's#//[^/@]+@#//***:***@#'
}

# Strip userinfo from URL if using credential file
get_clean_url() {
  if [[ -n "$NATS_CREDS_FILE" ]]; then
    echo "$1" | sed -E 's#(nats://)[^/@]+@#\1#'
  else
    echo "$1"
  fi
}

cleanup() {
  if mountpoint -q "$MOUNTPOINT" 2>/dev/null; then
    if command -v fusermount >/dev/null 2>&1; then
      fusermount -u "$MOUNTPOINT" || true
    else
      umount "$MOUNTPOINT" || true
    fi
  fi
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  [[ -n "${FUSE_PID:-}" ]] && kill "$FUSE_PID" 2>/dev/null || true
  rm -rf "$MOUNTPOINT" "$BACKING_ROOT" "$COW_ROOT" "$ALIAS_ROOT" "$SERVER_LOG" "$FUSE_LOG"
}
trap cleanup EXIT

dump_logs() {
  echo "[smoke] server log" >&2
  tail -120 "$SERVER_LOG" >&2 || true
  echo "[smoke] fuse log" >&2
  tail -120 "$FUSE_LOG" >&2 || true
}
trap dump_logs ERR

wait_for_log() {
  local file="$1"
  local pattern="$2"
  local pid="$3"
  local label="$4"

  for _ in $(seq 1 100); do
    if grep -q "$pattern" "$file"; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "[smoke] $label exited before readiness" >&2
      tail -80 "$file" >&2 || true
      return 1
    fi
    sleep 0.1
  done

  echo "[smoke] timed out waiting for $label readiness" >&2
  tail -80 "$file" >&2 || true
  return 1
}

wait_for_mount() {
  local pid="$1"

  for _ in $(seq 1 100); do
    if mountpoint -q "$MOUNTPOINT" 2>/dev/null; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "[smoke] fuse exited before mount readiness" >&2
      tail -80 "$FUSE_LOG" >&2 || true
      return 1
    fi
    sleep 0.1
  done

  echo "[smoke] timed out waiting for fuse mount readiness" >&2
  tail -80 "$FUSE_LOG" >&2 || true
  return 1
}

echo "[smoke] mount=$MOUNTPOINT backing_root=$BACKING_ROOT cow=$COW_ROOT alias=$ALIAS_ROOT"
echo "[smoke] starting server @ $(redact_url "$NATS_URL")"
if [[ -n "$NATS_CREDS_FILE" ]]; then
  echo "[smoke] using NATS credentials file: $NATS_CREDS_FILE"
  NATS_CREDS_FILE="$NATS_CREDS_FILE" FABRICFS_TRANSPORT_AUTH_TOKEN="$FABRICFS_TRANSPORT_AUTH_TOKEN" cargo run -p fabricfs-server --bin fabricfs-server -- --nats-url "$(get_clean_url "$NATS_URL")" --mount-name "$MOUNT_NAME" --backing-root "$BACKING_ROOT" --alias-path "$ALIAS_ROOT" --cow-path "$COW_ROOT" >"$SERVER_LOG" 2>&1 &
else
  FABRICFS_TRANSPORT_AUTH_TOKEN="$FABRICFS_TRANSPORT_AUTH_TOKEN" cargo run -p fabricfs-server --bin fabricfs-server -- --nats-url "$(get_clean_url "$NATS_URL")" --mount-name "$MOUNT_NAME" --backing-root "$BACKING_ROOT" --alias-path "$ALIAS_ROOT" --cow-path "$COW_ROOT" >"$SERVER_LOG" 2>&1 &
fi
SERVER_PID=$!
wait_for_log "$SERVER_LOG" "subscribed to" "$SERVER_PID" "server"

echo "[smoke] starting fuse"
if [[ -n "$NATS_CREDS_FILE" ]]; then
  NATS_CREDS_FILE="$NATS_CREDS_FILE" FABRICFS_TRANSPORT_AUTH_TOKEN="$FABRICFS_TRANSPORT_AUTH_TOKEN" FABRICFS_DEBUG=1 RUST_LOG="$FUSE_RUST_LOG" cargo run -p fabricfs-fuse --bin fabricfs-fuse -- "$MOUNTPOINT" "$(get_clean_url "$NATS_URL")" --mount-name "$MOUNT_NAME" >"$FUSE_LOG" 2>&1 &
else
  FABRICFS_TRANSPORT_AUTH_TOKEN="$FABRICFS_TRANSPORT_AUTH_TOKEN" FABRICFS_DEBUG=1 RUST_LOG="$FUSE_RUST_LOG" cargo run -p fabricfs-fuse --bin fabricfs-fuse -- "$MOUNTPOINT" "$(get_clean_url "$NATS_URL")" --mount-name "$MOUNT_NAME" >"$FUSE_LOG" 2>&1 &
fi
FUSE_PID=$!
wait_for_mount "$FUSE_PID"

echo "[smoke] basic file ops"
echo "hello world" >"$MOUNTPOINT/hello.txt"
cat "$MOUNTPOINT/hello.txt"
mkdir "$MOUNTPOINT/dir"
cp "$MOUNTPOINT/hello.txt" "$MOUNTPOINT/dir/copy.txt"
mv "$MOUNTPOINT/dir/copy.txt" "$MOUNTPOINT/dir/moved.txt"
ls -l "$MOUNTPOINT"
ls -l "$MOUNTPOINT/dir"
rm "$MOUNTPOINT/dir/moved.txt"
rm "$MOUNTPOINT/hello.txt"
rmdir "$MOUNTPOINT/dir"

echo "[smoke] mounted common surface"
printf "abcdefghi\n" >"$MOUNTPOINT/posix.txt"

MOUNTPOINT="$MOUNTPOINT" python3 - <<'PY'
import errno
import fcntl
import os
import stat
import subprocess
import sys
import textwrap

root = os.environ["MOUNTPOINT"]
path = os.path.join(root, "posix.txt")
link_path = os.path.join(root, "posix.link")
hard_path = os.path.join(root, "posix.hard")
copy_path = os.path.join(root, "posix.copy")

os.setxattr(path, b"user.fabricfs-smoke", b"value")
if os.getxattr(path, b"user.fabricfs-smoke") != b"value":
    raise RuntimeError("getxattr did not round-trip the written value")
if "user.fabricfs-smoke" not in os.listxattr(path):
    raise RuntimeError("listxattr did not include the written name")
os.removexattr(path, b"user.fabricfs-smoke")
if "user.fabricfs-smoke" in os.listxattr(path):
    raise RuntimeError("removexattr did not remove the written name")

os.symlink("posix.txt", link_path)
if os.readlink(link_path) != "posix.txt":
    raise RuntimeError("readlink did not return the created symlink target")

os.link(path, hard_path)
file_stat = os.lstat(path)
hard_stat = os.lstat(hard_path)
if file_stat.st_ino != hard_stat.st_ino or hard_stat.st_nlink < 2:
    raise RuntimeError("hardlink did not preserve inode identity and link count")

os.chmod(path, 0o600)
if stat.S_IMODE(os.lstat(path).st_mode) != 0o600:
    raise RuntimeError("chmod did not persist through mounted setattr")

os.truncate(path, 3)
with open(path, "rb") as handle:
    if handle.read() != b"abc":
        raise RuntimeError("truncate did not shrink the mounted file")

with open(path, "r+b", buffering=0) as source:
    source_fd = source.fileno()
    os.fsync(source_fd)
    os.fdatasync(source_fd)

    fcntl.lockf(source_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    contender = subprocess.run(
        [
            sys.executable,
            "-c",
            textwrap.dedent(
                """
                import fcntl
                import os
                import sys

                fd = os.open(sys.argv[1], os.O_RDWR)
                try:
                    fcntl.lockf(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as exc:
                    print(exc.errno)
                else:
                    print("ok")
                """
            ),
            path,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    if contender.stdout.strip() not in {str(errno.EACCES), str(errno.EAGAIN)}:
        raise RuntimeError(
            f"separate-process POSIX lock contention failed: {contender.stdout.strip()!r}"
        )
    fcntl.lockf(source_fd, fcntl.LOCK_UN)

    with open(copy_path, "w+b", buffering=0) as dest:
        dest_fd = dest.fileno()
        os.lseek(source_fd, 0, os.SEEK_SET)
        copied = os.copy_file_range(source_fd, dest_fd, 3)
        if copied != 3:
            raise RuntimeError(f"copy_file_range copied {copied} bytes instead of 3")
        os.fsync(dest_fd)
        with open(copy_path, "rb") as copied_file:
            if copied_file.read() != b"abc":
                raise RuntimeError("copy_file_range did not preserve copied bytes")
        # Generic SEEK_END on a mounted fd resolves through inode metadata on
        # Linux; use a path relookup so smoke does not depend on inode-only
        # open-handle fallback semantics.
        if os.stat(copy_path).st_size != 3:
            raise RuntimeError("copy_file_range did not advance the destination size")
        os.posix_fallocate(dest_fd, 4096, 8192)
        os.fsync(dest_fd)
        if os.stat(copy_path).st_size < 12288:
            raise RuntimeError("posix_fallocate did not extend the mounted file")

stats = os.statvfs(root)
if stats.f_bsize <= 0 or stats.f_frsize <= 0:
    raise RuntimeError(f"statvfs returned invalid block sizes: {stats}")
PY

rm "$MOUNTPOINT/posix.link" "$MOUNTPOINT/posix.hard" "$MOUNTPOINT/posix.copy" "$MOUNTPOINT/posix.txt"

echo "[smoke] done; unmounting"
