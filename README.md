FabricFs — FUSE over NATS
=======================

FabricFs is a NATS-backed filesystem: a FUSE3 bridge forwards the common mounted
filesystem operation set to a NATS service that maintains a copy-on-write
overlay on top of an optional backing tree. A companion session control service
manages overlay metadata, checkpoints, and publish/pull workflows.

Components
----------
- `fs-protocol`, `fs-core`, and `fs-fuse`: reusable filesystem protobuf envelopes, neutral `OperationSpec`, dispatcher semantics, errno/invalidation rules, and the FUSE cache kernel.
- `fabricfs-session-protocol`: SessionControl protocol (`session.proto`) and URL redaction helpers.
- `fabricfs-transport`: NATS subject helpers, connection setup, common filesystem envelope request/reply carrier, and transport policy for retries, deadlines, replay, delivery, and namespace ordering.
- `fabricfs-server`: NATS worker with `ServerStorage` capability ports that adapt common filesystem requests to passthrough or overlay storage semantics. Binaries: `fabricfs-server`, `fabricfs-sessiond`, and the `fabricfsctl` CLI.
- `fabricfs-fuse`: mount startup, NATS client construction, readiness checks, and fuser replies around the reusable `fs-fuse` adapter.
- Helpers: `run-server.sh`, `run-fuse.sh`, `smoke.sh`, `smoke-sessions.sh`, and `infra/nats` for local broker defaults.

Highlights
----------
- NATS transport carries common filesystem envelopes with request id, namespace, deadline, caller context, errno, observations, response invalidations, and mount-scoped invalidation broadcasts for other clients. Broker-facing request IDs are scoped per client and registered as pending before publish so invalidation replay filtering is safe across multiple mounts and in-flight replies. Pending own broadcasts are deferred, direct-response success drops duplicates, and timed-out/lost replies release deferred broadcasts to later drains as recovery invalidations. Mutations, opens, and `lseek` cursor updates are not retried after a publish attempt without server-side request dedupe. Client timeouts are projected into the outbound envelope deadline; if an open/create response cannot be delivered before that deadline or a post-dispatch publish fails, the server releases the returned backend handle before reporting the transport failure. Filesystem command frames are authenticated with `FABRICFS_TRANSPORT_AUTH_TOKEN`; the server only trusts caller `uid`/`gid`/`pid` after verifying that transport signature, binds delivered command subjects to the expected mount namespace, validates delivered operation subjects against decoded envelopes, rejects no-reply or unauthenticated filesystem commands before dispatch, and flushes startup subscription/full-resync work before logging readiness.
- Overlay/backing layout: `--alias-path` gates mutations; optional `--cow-path` copies backing data before writes and stores tombstones under `<alias>/.fabricfs_tombstones`; resolution order is tombstone → COW → backing. Without an alias path the server runs read-only; without a COW path, mutations are blocked unless `--update-backingtree` is set.
- **Copy-on-write optimizations**: reflink support for instant copies on Btrfs/XFS/ZFS (100-1000x faster), sparse file preservation with SEEK_DATA/SEEK_HOLE (50-90% space savings for VM images and databases), and automatic fallback to standard copy on other filesystems. Configurable via `--enable-reflinks` and `--preserve-sparse-files`.
- IO and POSIX controls: `--io-chunk-bytes` and `--max-read-bytes` bound transfer sizes, `--umask` applies to creates, `--propagate-acls` copies ACL xattrs on copy-up, and `--update-permissions` / `--update-xattrs` opt into mutating backing metadata.
- Mounted POSIX surface: `fabricfs-fuse` mounts lookup, getattr, readdir, open, read, write, create, rename, unlink, mkdir, rmdir, readlink, symlink, hardlink, setattr (mode/uid/gid/size only), flush, fsync, fsyncdir, POSIX `getlk`/`setlk`, `copy_file_range`, `fallocate`, `lseek`, `statfs`, `getxattr`/`setxattr`/`listxattr`/`removexattr`, and release through the common stack. The common protocol also defines BSD-style `flock`, but the current fuser 0.14 product API does not expose a distinct mounted flock-acquisition callback, so flock ownership remains a backend capability rather than a separately mounted product callback.
- Extended attributes persist under `<cow>/.fabricfs_xattrs` (JSON keyed by dev/inode) so xattrs survive restarts and copy-up.
- Cache coherency: mutating responses include common invalidations, the server broadcasts those invalidations on a mount-scoped NATS subject for other clients, server startup publishes a sequence-reset full-resync invalidation, and watched storage-root changes publish dispatcher-sequenced full-resync invalidations. Same-namespace dispatch, invalidation publication, and storage-watch full resyncs pass through an ordered namespace section so later mutation sequences cannot be broadcast before earlier ones. Watcher notifications observed during server RPC handling are coalesced and published after active requests finish, so ambiguous external mutations are not dropped. Common attrs preserve access, modify, change, and creation timestamps, and common statfs preserves free versus available blocks plus block versus fragment size through the service and FUSE mappings. The `fs-fuse` cache kernel drains invalidations before resolving cached parent, inode, or handle paths; rename and product readdir resolve all cached request values from one post-drain snapshot, and scoped caller guards keep kernel caller identity request-local. Malformed or uncertain invalidation drains fail closed, and decoded drain batches still apply later full-resync recovery messages after earlier sequence gaps. Empty late-joining adapters can baseline on their first drained mutation sequence, while adapters with cached state or open handles poison on gaps. Open-handle lifecycle state is retained until release so backend handles are not leaked when the path cache is poisoned or reset, committed rename responses rewrite retained handle paths before sequence-gap poison, and kernel forget messages decrement lookup reference counts instead of dropping live inode mappings early.
- Concurrency and backpressure: worker pool with tunable thread count and queue depth per server instance.
- Runtime observability: `fabricfs-server`, `fabricfs-fuse`, and `fabricfs-sessiond` emit periodic structured metrics snapshots for worker-pool saturation, request latency, invalidation health, session activity, and published-store health. Configure the cadence with `--metrics-interval-secs` or `FABRICFS_METRICS_INTERVAL_SECS`; set it to `0` to disable periodic emission.
- Session control: `fabricfs-sessiond` stores overlay metadata (aliases/tombstones) under the COW root, supports passworded sessions, checkpoints, publishing to JetStream, and importing remote checkpoints. Import retries are idempotent across restarts because the imported session ID is derived from the remote checkpoint ID; `fabricfsctl` drives the API over NATS (sessions list/create/show/delete/attach, overlay add/rm alias/tombstone, checkpoint commit/list, published push/list/pull).

Path semantics
--------------
- Reads check for tombstones first, then the COW overlay, then the backing root.
- Writes require `--alias-path` and either a COW or backing root; with a COW path, data is copied up before mutation and deletions create tombstones; without a COW path, mutations are rejected with `EACCES` unless `--update-backingtree` is enabled, in which case writes go directly to the backing tree.
- The mounted common FUSE surface supports lookup, getattr, readdir, open, read, write, create, rename, unlink, mkdir, rmdir, readlink, symlink, hardlink, setattr (mode/uid/gid/size only), flush, fsync, fsyncdir, POSIX `getlk`/`setlk`, `copy_file_range`, `fallocate`, `lseek`, statfs, xattr operations, and release. BSD-style `flock` remains backend-only until the current fuser 0.14 product surface exposes a dedicated callback.
- Xattr updates against backing entries are blocked unless `--update-xattrs` is set.
- Xattrs and ACLs are copied during copy-up when enabled; ACL propagation is opt-in via `--propagate-acls`.

Performance optimizations
-------------------------
FabricFs implements several copy-on-write optimizations to minimize copy overhead for large files:

### Reflink support (Linux 4.5+)
When `--enable-reflinks` is enabled (default: true), the server attempts to use the FICLONE ioctl for instant copy-on-write:
- **Supported filesystems**: Btrfs (4.5+), XFS (4.20+), ZFS (2.2.0+)
- **Behavior**: Creates a new file that shares data blocks with the source; only modified blocks consume additional space
- **Performance**: Near-instant copies regardless of file size (100-1000x faster than regular copy)
- **Fallback**: Automatically falls back to regular copy on filesystems that don't support reflinks

### Sparse file preservation (Linux 3.1+)
When `--preserve-sparse-files` is enabled (default: true), the server detects and preserves holes in sparse files:
- **Detection**: Files where allocated blocks < apparent size are treated as sparse
- **Implementation**: Uses SEEK_DATA/SEEK_HOLE to identify data regions and only copies actual data
- **Benefits**: 50-90% space savings for VM disk images, database files, and large preallocated files
- **Performance**: 2-5x faster copy-up for sparse files compared to full copy

### Copy strategy
The copy-up process follows this decision tree:
1. Try reflink (FICLONE ioctl) if enabled and source/dest on same filesystem
2. If reflink fails or disabled, check if file is sparse
3. For sparse files: use SEEK_DATA/SEEK_HOLE to preserve holes
4. Otherwise: use standard filesystem copy

**Example usage:**
```sh
# Use Btrfs for backing and COW to get instant reflinks
fabricfs-server --backing-root /mnt/btrfs/backing --cow-path /mnt/btrfs/cow --alias-path /tmp/alias

# Disable optimizations if needed
fabricfs-server --backing-root /backing --cow-path /cow --alias-path /alias \
  --enable-reflinks=false --preserve-sparse-files=false
```

Running locally
---------------
1) Start NATS (see `infra/nats` for defaults).
2) Launch the data plane:
```sh
mkdir -p /tmp/fabricfs/{backing,cow,alias,mnt}
export FABRICFS_TRANSPORT_AUTH_TOKEN=dev-shared-secret
cargo run -p fabricfs-server -- \
  --mount-name fabricfs \
  --nats-url nats://127.0.0.1:4222 \
  --backing-root /tmp/fabricfs/backing \
  --alias-path /tmp/fabricfs/alias \
  --cow-path /tmp/fabricfs/cow
cargo run -p fabricfs-fuse -- \
  /tmp/fabricfs/mnt nats://127.0.0.1:4222 --mount-name fabricfs
```
3) Optional SessionControl (shares the same COW root):
```sh
cargo run -p fabricfs-server --bin fabricfs-sessiond -- \
  --cow-root /tmp/fabricfs/cow --nats-url nats://127.0.0.1:4222
cargo run -p fabricfs-server --bin fabricfsctl -- --nats-url nats://127.0.0.1:4222 sessions list
# examples:
# fabricfsctl sessions create demo /tmp/fabricfs/cow
# fabricfsctl overlay alias-add <session> /virtual/path /backing/path
# fabricfsctl checkpoints commit <session> --label initial
# fabricfsctl published push <session> <checkpoint> --remote-id demo-initial
# fabricfsctl published pull <remote-id> --new-session-name restored
```
4) Scripts: `./run-server.sh` and `./run-fuse.sh` wrap the commands above and require `FABRICFS_TRANSPORT_AUTH_TOKEN`; `./smoke.sh` auto-generates a throwaway token when one is not set and exercises baseline mounted file, directory, xattr, and statfs paths; `./smoke-sessions.sh` drives the session API.

CLI flags
---------
- `fabricfs-fuse <mount> <nats-url> [--mount-name <name>] [--timeout-secs <seconds>]` — mount the bridge (honors `FABRICFS_DEBUG`, requires `FABRICFS_TRANSPORT_AUTH_TOKEN`, uses the same timeout for the startup readiness probe and mounted RPC calls, and reads `FABRICFS_METRICS_INTERVAL_SECS`, default `30`, `0` disables periodic metrics logs).
- `fabricfs-server`:
  - `--nats-url <URL>` and `--mount-name <NAME>`
  - `--transport-auth-token <TOKEN>` or `FABRICFS_TRANSPORT_AUTH_TOKEN` (shared secret required by filesystem transport clients)
  - `--backing-root <DIR>` (source for reads when overlay is missing)
  - `--alias-path <DIR>` (enables mutations and tombstones; required for writes)
  - `--cow-path <DIR>` (copy-on-write overlay plus xattr store)
  - `--update-backingtree` (allow mutations to land on the backing tree when no COW path is provided; defaults to false and otherwise yields `EACCES`)
  - `--worker-threads <N>` / `--max-queued <N>` (worker pool sizing)
  - `--metrics-interval-secs <N>` or `FABRICFS_METRICS_INTERVAL_SECS` (periodic structured metrics logs; default `30`, `0` disables)
  - `--authenticated-request-ttl-secs <N>` or `FABRICFS_AUTHENTICATED_REQUEST_TTL_SECS` (maximum accepted future deadline for signed filesystem requests; default `300`)
  - `--io-chunk-bytes <N>` / `--max-read-bytes <N>`
  - `--umask <OCTAL>` (applied to create/mkdir/mknod/write)
  - `--propagate-acls`, `--update-permissions`, `--update-xattrs`
  - `--enable-reflinks` (enable reflink-based copy optimization; default: true)
  - `--preserve-sparse-files` (preserve holes during copy-up of sparse files; default: true)
- `fabricfs-sessiond --cow-root <DIR> [--nats-url <URL>]` — SessionControl server (requires JetStream for published checkpoints; reads `--metrics-interval-secs <N>` / `FABRICFS_METRICS_INTERVAL_SECS`, default `30`, `0` disables periodic metrics logs).
- `fabricfsctl` — NATS client for SessionControl (see `fabricfsctl --help` for subcommands).

Development
-----------
- List recipes: `just`
- Format: `just fmt`
- Lint: `just lint`
- Tests: `just test`
- Full local gate: `just check`
- CI coverage gate: `just ci`
- Smoke: `./smoke.sh` for data plane, `./smoke-sessions.sh` for SessionControl
- Debug logging: set `FABRICFS_DEBUG=1` for verbose server/client traces

See `DEVELOPMENT.md`, `TESTING.md`, `COMMANDS.md`, and
`docs/ARCHITECTURE.md` for the release-track workflow, coverage expectations,
command overview, and crate boundary rules.

License
-------
FabricFs is licensed under the GNU Affero General Public License v3.0 only
(`AGPL-3.0-only`). See [LICENSE](LICENSE) for the full text and
[COPYRIGHT](COPYRIGHT) for the repository notice. This program is distributed
without any warranty.
