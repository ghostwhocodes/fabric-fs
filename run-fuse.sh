#!/usr/bin/env bash
set -euo pipefail

# Start the FUSE bridge. Requires a mountpoint directory to exist.
# Usage: ./run-fuse.sh <mountpoint> [NATS_URL] [--mount-name <name>] [--timeout-secs <seconds>]
# Note: the server must be started separately and needs --alias-path for writes.
# Set NATS_CREDS_FILE env var to use a NATS credentials file instead of embedded credentials.
# Set FABRICFS_TRANSPORT_AUTH_TOKEN to the same shared secret passed to fabricfs-server.

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <mountpoint> [NATS_URL] [--mount-name <name>]" >&2
  exit 1
fi

MOUNTPOINT="$1"
NATS_URL="${2:-nats://127.0.0.1:4222}"
NATS_CREDS_FILE="${NATS_CREDS_FILE:-}"
FABRICFS_TRANSPORT_AUTH_TOKEN="${FABRICFS_TRANSPORT_AUTH_TOKEN:-}"

if [[ -z "$FABRICFS_TRANSPORT_AUTH_TOKEN" ]]; then
  echo "[run-fuse] FABRICFS_TRANSPORT_AUTH_TOKEN must be set" >&2
  exit 1
fi

# Strip userinfo from URL if using credential file
get_clean_url() {
  if [[ -n "$NATS_CREDS_FILE" ]]; then
    echo "$1" | sed -E 's#(nats://)[^/@]+@#\1#'
  else
    echo "$1"
  fi
}

CLEAN_URL="$(get_clean_url "$NATS_URL")"

if [[ -n "$NATS_CREDS_FILE" ]]; then
  echo "[run-fuse] using NATS credentials file: $NATS_CREDS_FILE" >&2
  NATS_CREDS_FILE="$NATS_CREDS_FILE" FABRICFS_TRANSPORT_AUTH_TOKEN="$FABRICFS_TRANSPORT_AUTH_TOKEN" cargo run -p fabricfs-fuse -- "${MOUNTPOINT}" "${CLEAN_URL}" "${@:3}"
else
  FABRICFS_TRANSPORT_AUTH_TOKEN="$FABRICFS_TRANSPORT_AUTH_TOKEN" cargo run -p fabricfs-fuse -- "${MOUNTPOINT}" "${CLEAN_URL}" "${@:3}"
fi
