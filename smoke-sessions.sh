#!/usr/bin/env bash
set -euo pipefail

NATS_URL="${NATS_URL:-nats://127.0.0.1:4222}"
NATS_CREDS_FILE="${NATS_CREDS_FILE:-}"
COW_ROOT="$(mktemp -d)"
SESSIOND_LOG="${SESSIOND_LOG:-/tmp/fabricfs-sessiond.log}"

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
  if [[ -n "${SESSIOND_PID:-}" ]]; then
    kill "${SESSIOND_PID}" 2>/dev/null || true
  fi
  rm -rf "$COW_ROOT"
}
trap cleanup EXIT

ctl() {
  if [[ -n "$NATS_CREDS_FILE" ]]; then
    NATS_CREDS_FILE="$NATS_CREDS_FILE" cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$(get_clean_url "$NATS_URL")" "$@"
  else
    cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$(get_clean_url "$NATS_URL")" "$@"
  fi
}

echo "[sessions-smoke] cow_root=$COW_ROOT nats=$(redact_url "$NATS_URL")"
if [[ -n "$NATS_CREDS_FILE" ]]; then
  echo "[sessions-smoke] using NATS credentials file: $NATS_CREDS_FILE"
fi
echo "[sessions-smoke] starting sessiond (logs -> $SESSIOND_LOG)"
if [[ -n "$NATS_CREDS_FILE" ]]; then
  NATS_CREDS_FILE="$NATS_CREDS_FILE" cargo run -p fabricfs-server --bin fabricfs-sessiond -- --nats-url "$(get_clean_url "$NATS_URL")" --cow-root "$COW_ROOT" >"$SESSIOND_LOG" 2>&1 &
else
  cargo run -p fabricfs-server --bin fabricfs-sessiond -- --nats-url "$(get_clean_url "$NATS_URL")" --cow-root "$COW_ROOT" >"$SESSIOND_LOG" 2>&1 &
fi
SESSIOND_PID=$!
sleep 1

echo "[sessions-smoke] note: data server is not started here; run fabricfs-server separately with --alias-path for mutations."

echo "[sessions-smoke] create session"
SESSION_ID="$(ctl sessions create smoke "$COW_ROOT")"
echo "[sessions-smoke] session_id=$SESSION_ID"

echo "[sessions-smoke] list sessions (json)"
ctl sessions list --json

echo "[sessions-smoke] add alias and list overlay (json)"
ctl overlay alias-add "$SESSION_ID" /foo /bar
ctl overlay list "$SESSION_ID" --json

echo "[sessions-smoke] checkpoint and publish"
CHECKPOINT_ID="$(ctl checkpoints commit "$SESSION_ID" --label smoke)"
REMOTE_ID="smoke-${CHECKPOINT_ID}"
ctl published push "$SESSION_ID" "$CHECKPOINT_ID" --remote-id "$REMOTE_ID"
ctl published list --json

echo "[sessions-smoke] pull into new session and show overlay"
NEW_SESSION="$(ctl published pull "$REMOTE_ID" --new-session-name pulled-smoke)"
echo "[sessions-smoke] new_session_id=$NEW_SESSION"
ctl overlay list "$NEW_SESSION" --json

echo "[sessions-smoke] snapshot for verification"
ctl sessions show "$NEW_SESSION" --json

echo "[sessions-smoke] done"
