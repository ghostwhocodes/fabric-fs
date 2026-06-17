#!/usr/bin/env bash
set -euo pipefail

# Start the fabricfs server against local NATS.
# Usage: ./run-server.sh [BACKING_ROOT] [NATS_URL] [--alias-path <DIR>] [--cow-path <DIR>] [--io-chunk-bytes N] [--max-read-bytes N]
# - BACKING_ROOT: path used for passthrough reads.
# - --alias-path enables mutations and persists tombstones/aliases in <alias-path>/.fabricfs_tombstones.
#   Without it, the server runs read-only.
# - --cow-path enables copy-on-write; backing data stays untouched.
# - --io-chunk-bytes caps per-chunk IO buffering (defaults to 1MiB).
# - --max-read-bytes caps a single read RPC payload (defaults to 4MiB).
# You can also set ALIAS_PATH env var to inject --alias-path automatically.
# Set NATS_CREDS_FILE env var to use a NATS credentials file instead of embedded credentials.
# Set FABRICFS_TRANSPORT_AUTH_TOKEN to a shared secret used by fabricfs-server and fabricfs-fuse.

BACKING_ROOT="${1:-}"
NATS_URL="${2:-nats://127.0.0.1:4222}"
NATS_CREDS_FILE="${NATS_CREDS_FILE:-}"
EXTRA_ARGS=("${@:3}")
ALIAS_PATH="${ALIAS_PATH:-}"
FABRICFS_TRANSPORT_AUTH_TOKEN="${FABRICFS_TRANSPORT_AUTH_TOKEN:-}"

if [[ -z "$FABRICFS_TRANSPORT_AUTH_TOKEN" ]]; then
  echo "[run-server] FABRICFS_TRANSPORT_AUTH_TOKEN must be set" >&2
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

has_alias_flag=false
for arg in "${EXTRA_ARGS[@]}"; do
  if [[ "$arg" == "--alias-path" ]]; then
    has_alias_flag=true
    break
  fi
done

if [[ -n "$ALIAS_PATH" && "$has_alias_flag" == false ]]; then
  EXTRA_ARGS+=(--alias-path "$ALIAS_PATH")
  has_alias_flag=true
fi

if [[ "$has_alias_flag" == false ]]; then
  echo "[run-server] no --alias-path provided; server will run read-only" >&2
fi

CLEAN_URL="$(get_clean_url "$NATS_URL")"
cmd=(cargo run -p fabricfs-server --bin fabricfs-server -- --nats-url "${CLEAN_URL}" "${EXTRA_ARGS[@]}")
if [[ -n "${BACKING_ROOT}" ]]; then
  cmd+=(--backing-root "${BACKING_ROOT}")
fi

if [[ -n "$NATS_CREDS_FILE" ]]; then
  echo "[run-server] using NATS credentials file: $NATS_CREDS_FILE" >&2
  NATS_CREDS_FILE="$NATS_CREDS_FILE" FABRICFS_TRANSPORT_AUTH_TOKEN="$FABRICFS_TRANSPORT_AUTH_TOKEN" "${cmd[@]}"
else
  FABRICFS_TRANSPORT_AUTH_TOKEN="$FABRICFS_TRANSPORT_AUTH_TOKEN" "${cmd[@]}"
fi
