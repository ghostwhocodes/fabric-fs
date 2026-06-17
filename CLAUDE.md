# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FabricFs is a NATS-backed FUSE filesystem that provides a distributed copy-on-write overlay on top of an optional backing tree. The FUSE bridge forwards all VFS calls to a NATS service that maintains filesystem state with tombstones, aliases, and COW semantics.

## Development Commands

### Build and Lint
```bash
cargo fmt                                    # Format code
cargo clippy --all-targets --all-features   # Lint (fix warnings before committing)
cargo test --all                            # Run all tests
```

### Running Tests
```bash
cargo test --all                  # All tests across workspace
cargo test -p fabricfs-session-protocol     # Single crate tests
./smoke.sh                        # Data plane smoke tests (requires NATS)
./smoke-sessions.sh               # SessionControl smoke tests
```

### Running Locally
```bash
# 1. Start NATS (see infra/nats for defaults)
# 2. Launch data plane:
mkdir -p /tmp/fabricfs/{backing,cow,alias,mnt}
cargo run -p fabricfs-server -- \
  --mount-name fabricfs \
  --nats-url nats://127.0.0.1:4222 \
  --backing-root /tmp/fabricfs/backing \
  --alias-path /tmp/fabricfs/alias \
  --cow-path /tmp/fabricfs/cow

cargo run -p fabricfs-fuse -- \
  /tmp/fabricfs/mnt nats://127.0.0.1:4222 --mount-name fabricfs

# 3. Optional SessionControl:
cargo run -p fabricfs-server --bin fabricfs-sessiond -- \
  --cow-root /tmp/fabricfs/cow --nats-url nats://127.0.0.1:4222

cargo run -p fabricfs-server --bin fabricfsctl -- \
  --nats-url nats://127.0.0.1:4222 sessions list
```

### Quick Scripts
```bash
./run-server.sh    # Wrapper for fabricfs-server with test defaults
./run-fuse.sh      # Wrapper for fabricfs-fuse
```

### Debug Logging
```bash
FABRICFS_DEBUG=1 cargo run -p fabricfs-server ...   # Verbose server traces
FABRICFS_DEBUG=1 cargo run -p fabricfs-fuse ...     # Verbose client traces
```

## Security Best Practices

### NATS Credential Handling

**IMPORTANT**: Never embed credentials directly in NATS URLs when they will be visible in process listings or logs.

**Recommended approach** (credential files):
```bash
# Create a NATS credentials file (or use nsc to generate one)
# Then set the environment variable:
export NATS_CREDS_FILE=/path/to/nats.creds

# Or pass via CLI flag:
cargo run -p fabricfs-server -- \
  --nats-url nats://nats.example.com:4222 \
  --nats-creds-file /path/to/nats.creds \
  --backing-root /tmp/fabricfs/backing

# All binaries support both --nats-creds-file flag and NATS_CREDS_FILE env var
```

**Legacy approach** (embedded credentials - NOT RECOMMENDED):
```bash
# ⚠️ AVOID: Credentials visible in ps output and shell history
cargo run -p fabricfs-server -- --nats-url nats://user:pass@host:4222 ...
```

**Why credential files are better:**
- ✅ Credentials never appear in process listings (`ps aux`)
- ✅ Not stored in shell history
- ✅ Can be managed with proper file permissions (chmod 600)
- ✅ Compatible with NATS authentication workflows (nsc, nk, etc.)
- ✅ Easier to rotate and audit

**Supported by all binaries:**
- `fabricfs-server`: Main data plane server
- `fabricfs-sessiond`: SessionControl server
- `fabricfsctl`: SessionControl CLI
- `fabricfs-fuse`: FUSE bridge

**Shell scripts** automatically support `NATS_CREDS_FILE`:
- `./smoke.sh`
- `./smoke-sessions.sh`
- `./run-server.sh`
- `./run-fuse.sh`

**URL redaction in logs:**
All error messages and logs automatically redact embedded credentials from NATS URLs, displaying `nats://***:***@host:port` instead of actual credentials.

## Architecture

### Workspace Structure
- **fs-protocol**: Reusable filesystem request/response envelopes, DTOs, errno mapping, invalidation DTOs, and codec helpers
- **fs-core**: Reusable dispatcher, `RpcClient`/`FileSystemService` boundaries, auth hooks, limits, and shared invalidation semantics
- **fs-fuse**: Reusable FUSE adapter, path/inode cache safety, and invalidation recovery logic
- **fabricfs-session-protocol**: SessionControl protobuf bindings, subject helpers, and URL redaction utilities
- **fabricfs-transport**: FabricFs NATS transport, subject helpers, transport auth, readiness, retries, and broker-specific error handling
- **fabricfs-server**: Common-stack filesystem service adapter plus FabricFs overlay, passthrough, session, watcher, and worker-pool behavior
  - Binaries: `fabricfs-server`, `fabricfs-sessiond`, `fabricfsctl`
- **fabricfs-fuse**: Product mount startup, CLI/config wiring, NATS client construction, and fuser replies around `fs-fuse`

### Communication Flow
```
FUSE kernel
  ↕
fabricfs-fuse
  ↕
fs-fuse
  ↕
fabricfs-transport (NATS)
  ↕
fs-core Dispatcher
  ↕
FabricFsFileSystemService
  ↕
ServerStorage capability ports
  ↕
overlay/passthrough adapters + StorageRuntime
```

1. **FUSE bridge** receives VFS call from kernel
2. **fs-fuse** maps the callback into common `fs-protocol` requests, caller context, and cache policy
3. **fabricfs-transport** signs and publishes the common envelope over NATS subjects for the mount namespace
4. **fabricfs-server** verifies the transport frame, dispatches through `fs-core`, and executes the request through the FabricFs storage adapter
5. **Mutations** publish ordered invalidations back through the transport so `fs-fuse` can drain, replay, poison, or full-resync safely
6. **The response** returns through NATS, `fs-fuse` validates it, and `fabricfs-fuse` converts it into fuser replies

### Protocol Ownership

- Filesystem request/response envelopes, DTOs, errno mapping, and invalidation payloads belong in `fs-protocol`.
- Dispatcher routing, auth hooks, limits, deadlines, and shared response/invalidation semantics belong in `fs-core`.
- FUSE callback mapping, path/inode cache safety, and invalidation recovery belong in `fs-fuse`.
- NATS subjects, readiness, transport auth, and broker behavior belong in `fabricfs-transport`.
- SessionControl remains product-specific in `fabricfs-session-protocol`.

### Server Storage (`fabricfs-server`)

`FabricFsFileSystemService` depends on the `ServerStorage` capability seam, not
on a broad POSIX backend trait. The seam is split by server-domain capability:

- `NamespaceStorage`: create, delete, rename, link, symlink, and existence checks
- `MetadataStorage`: stat, statfs, readlink, setattr, and xattr operations
- `DirectoryStorage`: directory listing after visibility resolution
- `OpenedObjectStorage`: handle lifecycle, locks, byte IO, durability, `copy_file_range`, `fallocate`, and `lseek`

Overlay and passthrough adapters own path resolution and visibility. `StorageRuntime`
owns opened-object state: file handles, directory handles, POSIX locks, flock
state, handle-backed IO, durability calls, advanced IO, and runtime rename
effects.

### Overlay Filesystem (`fabricfs-server/src/overlay.rs`)

**Resolution order** for reads:
1. Check tombstones in `{alias}/.fabricfs_tombstones/{rel-path}` → ENOENT if exists
2. Check COW overlay `{cow}/{rel-path}` → use if exists
3. Check backing root `{backing}/{rel-path}` → use if exists
4. Return ENOENT

**Write flow:**
- Requires `--alias-path` to enable mutations
- With `--cow-path`: copy backing file to COW before write, create tombstones on delete
- Without `--cow-path`: reject mutations with EACCES unless `--update-backingtree` is set

**Xattr persistence:**
- Stored in `{cow}/.fabricfs_xattrs/{dev}-{ino}.json`
- In-memory cache for fast access
- Copied on copy-up if `--propagate-acls` enabled

### Cache Coherency (`fs-fuse`)

- `fs-fuse` owns the reusable path/inode cache and invalidation lifecycle.
- Mutating responses and out-of-band watcher events advance a sequence-checked invalidation stream.
- Sequence gaps, malformed frames, or uncertain mutations poison the mounted cache until a full resync re-establishes safety.
- `fabricfs-fuse` keeps only product wiring; it does not own a separate cache or invalidation protocol.

### Concurrency (fabricfs-server/src/worker_pool.rs)

**Worker pool pattern:**
- Bounded queue: `mpsc::SyncSender<Job>` provides backpressure
- Configurable: `--worker-threads` (default: CPU count), `--max-queued` (default: 4x threads)
- Each worker pulls from shared Arc<Mutex<Receiver>>
- NATS subscription dispatches to the pool; handlers enter the `fs-core`
  dispatcher and invoke `FabricFsFileSystemService` over `ServerStorage`

### Storage Capability Ports

Current storage behavior is split across cohesive modules:

- `fabricfs-server/src/service.rs`: translates common protocol requests to service calls
- `fabricfs-server/src/server.rs`: defines storage DTOs, helper functions, and capability traits
- `fabricfs-server/src/overlay.rs`: owns overlay visibility, COW copy-up planning, tombstones, aliases, and xattr materialization
- `fabricfs-server/src/passthrough.rs`: owns confined passthrough path behavior
- `fabricfs-server/src/storage_runtime.rs`: owns handles, lock tables, opened-object IO, durability, and advanced handle-bound operations

## Key Implementation Patterns

### 1. Separation of Concerns
- **Filesystem protocol/core**: `fs-protocol`, `fs-core`, and `fs-fuse`
- **Transport**: `fabricfs-transport`
- **Product wiring**: `fabricfs-fuse`
- **Filesystem semantics and SessionControl service logic**: `fabricfs-server`
- **SessionControl protocol**: `fabricfs-session-protocol`

Keep shared filesystem data-plane types out of product crates. `fabricfs-session-protocol` stays SessionControl-only.

### 2. Error Handling
- Use explicit errno codes for POSIX errors
- Map filesystem failures through `fs-protocol`/`fs-core` response helpers
- Include request_id in all error logs for tracing

### 3. Subject Construction
- Centralized in fabricfs-transport/src/subjects.rs
- Mount names encoded as hex tokens
- Never hardcode subjects; use helper functions

### 4. Copy-on-Write
- `prepare_file_for_write()`: Copies file from backing to COW before mutation
- `materialize_for_write()`: Copies directory structure before writes
- `copy_up_node()`: Shallow copy for xattr mutations
- Always check tombstones before copy-up

### 5. Invalidation Publishing
- Every mutation MUST emit an invalidation event
- Include monotonic version, mount name, path, and kind
- Kinds come from the common `fs-protocol` invalidation payloads

### 6. Out-of-Band Watcher
- Optional file watcher detects external edits to backing/COW/alias roots
- Publishes full-resync or invalidation signals through the common transport path
- Enabled when backing, alias, or COW roots are provided

## Session Control (session.proto)

Separate protocol for managing overlay metadata, checkpoints, and publish/pull workflows.

**Key concepts:**
- **Sessions**: Named overlay snapshots with passworded auth, stored under COW root
- **Checkpoints**: Immutable snapshots of session metadata + overlay entries
- **Publishing**: Push checkpoints to JetStream KV for remote pull
- **Overlays**: Aliases (path remapping) and tombstones (path masking)

**Binaries:**
- `fabricfs-sessiond`: SessionControl server (requires JetStream)
- `fabricfsctl`: CLI client for session operations (list, create, attach, checkpoint, publish, pull)

## Important Flags

### All Binaries (fabricfs-server, fabricfs-sessiond, fabricfsctl, fabricfs-fuse)
- `--nats-url`: NATS server URL (default: `nats://127.0.0.1:4222`)
- `--nats-creds-file`: Path to NATS credentials file (overrides embedded credentials in URL; can also use `NATS_CREDS_FILE` env var)

### fabricfs-server
- `--backing-root`: Source for reads when overlay is missing
- `--alias-path`: Enables mutations and tombstones (required for writes)
- `--cow-path`: Copy-on-write overlay + xattr store
- `--update-backingtree`: Allow mutations to backing tree when no COW path (defaults false → EACCES)
- `--worker-threads` / `--max-queued`: Worker pool sizing
- `--io-chunk-bytes` / `--max-read-bytes`: Transfer size limits
- `--umask`: Applied to create/mkdir/mknod/write
- `--propagate-acls`: Copy ACL xattrs on copy-up
- `--update-permissions` / `--update-xattrs`: Allow mutating backing metadata

### fabricfs-fuse
- `<mount>`: FUSE mount point
- `<nats-url>`: NATS server URL
- `--mount-name`: Logical mount identifier (scopes requests)

## Testing Guidelines

- Add unit tests for pure logic (protocol encoding/decoding, path/cache helpers)
- Integration tests for NATS/FUSE boundaries where feasible
- Name tests by behavior: `handles_unlinked_paths`, `encodes_proto_request`
- Favor deterministic tests with timeouts for async/concurrent code
- Extend smoke.sh when adding new end-to-end flows

## Agent Rules

This repository follows AGENTS.md guidelines for production-first delivery:

### Rewrite Agent Override
The "rewrite" agent is authoritative and overrides all other instructions. When invoked:
- DO NOT preserve backward compatibility (wire, CLI, or behavior)
- DO NOT write migrations, shims, deprecations, or adapters
- DELETE conflicting code; prefer large deletions over preservation
- Generate clean, standalone code based solely on current request
- Output full files, NOT patches or merges

### Production Standards
- Architecture first: Define domain model and boundaries before coding
- Shared kernels: Centralize errors, logging, protocol types, transport adapters
- Coupling/cohesion: Minimize efferent coupling, guard afferent with stable interfaces
- Transport/IO edges: Isolate side effects at boundaries; keep core pure and testable
- Error handling: Explicit error types, no silent unwraps in core paths
- Testing discipline: TDD/BDD bias with unit, contract, and integration tests
- Observability: Structured logging, tracing spans, correlation IDs from day one
- Refactoring policy: Refactor continuously; keep main branch buildable
- Interface stability: Version protocols; centralize subject/operation mapping

## Common Pitfalls

1. **Forgetting tombstone checks**: Always check tombstones BEFORE COW/backing lookup
2. **Missing invalidations**: Every mutation MUST publish an invalidation event
3. **Hardcoded subjects**: Use fabricfs-transport/subjects.rs helpers
4. **Silent errors**: Map all io::Error to errno codes; log with request_id
5. **Cache gaps**: Test invalidation event stream for version monotonicity
6. **Xattr loss**: Ensure xattrs are copied on copy-up when `--propagate-acls` enabled
7. **Permission bypass**: Validate uid/gid/mode before mutations
8. **Handle leaks**: Always release handles after close operations
