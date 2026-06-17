# fabricfsctl usage guide

Practical command sequences for driving the SessionControl API over NATS. All examples assume a running `fabricfs-sessiond` connected to `NATS_URL` (defaults to `nats://127.0.0.1:4222`).

## Setup

```bash
export NATS_URL=${NATS_URL:-nats://127.0.0.1:4222}
cow_root=$(mktemp -d)
cargo run -p fabricfs-server --bin fabricfs-sessiond -- --nats-url "$NATS_URL" --cow-root "$cow_root"
```

## Session lifecycle

```bash
# Create a session; stdout is the session id
SESSION_ID=$(cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" sessions create demo "$cow_root")

# List sessions as a table or JSON
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" sessions list
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" sessions list --json

# Inspect the snapshot (overlay entries + overlay_version)
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" sessions show "$SESSION_ID" --json

# Attach returns a single tab-delimited line: id name workspace cow_root
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" sessions attach "$SESSION_ID"
```

## Overlay edits and filtering

```bash
# Add alias and tombstone entries
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" overlay alias-add "$SESSION_ID" /docs /var/data/docs
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" overlay tomb-add "$SESSION_ID" /tmp/old-file

# List overlay entries; filter by prefix if needed
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" overlay list "$SESSION_ID"
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" overlay list "$SESSION_ID" --dir /docs --json
```

## Checkpoint and publish

```bash
# Commit a checkpoint (returns checkpoint id)
CHECKPOINT_ID=$(cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" checkpoints commit "$SESSION_ID" --label initial-overlay)

# List checkpoints
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" checkpoints list "$SESSION_ID" --json

# Publish to JetStream; omit --remote-id to reuse the checkpoint id
REMOTE_ID="demo-${CHECKPOINT_ID}"
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" published push "$SESSION_ID" "$CHECKPOINT_ID" --remote-id "$REMOTE_ID"

# Inspect the published catalog
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" published list
```

## Import flows

```bash
# Create a new session from the published checkpoint
IMPORTED_SESSION=$(cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" published pull "$REMOTE_ID" --new-session-name imported-demo)

# Merge the published overlay into an existing session with conflict handling and optimistic version guard
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" published pull "$REMOTE_ID" \
  --into "$SESSION_ID" \
  --mode merge \
  --conflict-policy overwrite-remote \
  --expect-overlay-version 1
```

## Password-protected runs

```bash
# Protect a session; password is read from stdin and never logged
SECURE_SESSION=$(cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" sessions create secure "$cow_root" --password prompt)

# Mutations and imports must include --password prompt for protected sessions
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" overlay alias-add "$SECURE_SESSION" /secret /srv/secret --password prompt
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" published pull "$REMOTE_ID" --into "$SECURE_SESSION" --password prompt
```

## Persistence spot-check

```bash
# Restart sessiond against the same cow_root to confirm overlay persistence
killall fabricfs-sessiond || true
cargo run -p fabricfs-server --bin fabricfs-sessiond -- --nats-url "$NATS_URL" --cow-root "$cow_root"
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url "$NATS_URL" overlay list "$SESSION_ID" --json
```

## Acceptance notes (Tickets 1–4)

- Ticket 1: Session protocol and subject helpers live in `session.proto` and `fabricfs-session-protocol/src/session.rs`; helper tests cover subject mapping and protobuf encode/decode.
- Ticket 2: Durable layout, overlay versioning, and checkpoint writers are implemented and tested in `fabricfs-server/src/session_storage.rs` (creation, checkpoints, password enforcement, overlay reload, and import merge/replace cases).
- Ticket 3: JetStream KV handling, key validation, and retry/backoff live in `fabricfs-server/src/published_store.rs` with unit tests for key rules and error/status mapping; import equivalence checks are validated in `fabricfs-server/src/session_service.rs` tests.
- Ticket 4: CLI wiring is in `fabricfs-server/src/bin/fabricfsctl.rs`; the flows above and `smoke-sessions.sh` provide the exercised surfaces for manual verification.
