# FabricFs Architecture

FabricFs is a NATS-backed FUSE filesystem. The local FUSE process translates
kernel callbacks into typed protocol requests, NATS carries those requests, and
the server applies filesystem semantics against a backing tree plus optional
copy-on-write and alias roots.

## Workspace Boundaries

| Crate | Responsibility | Boundary Rule |
|-------|----------------|---------------|
| `fs-protocol` | Reusable protobuf filesystem DTOs, neutral `OperationSpec`, envelope codec, errno mapping, invalidation DTOs, and golden fixtures | No concrete transport, FUSE callback, or product storage dependency |
| `fs-core` | Reusable dispatcher, `RpcClient`, `FileSystemService`, metadata, auth hooks, limits, deadline checks, and spec-derived mutation invalidations | No concrete transport, FUSE callback, or FabricFs overlay behavior |
| `fs-testkit` | Shared fixtures, fake service, and reusable direct/serialized conformance runners | No product-specific storage semantics |
| `fs-transport-local` | Direct and serialized in-process transport for tests and embedded use | No filesystem operation semantics |
| `fs-fuse` | Reusable FUSE-facing adapter surface plus the cache kernel for path/inode mappings, lookup refs, retained handle paths, invalidation application, and poison/full-resync recovery | Depends on `RpcClient`, not a concrete transport |
| `fabricfs-session-protocol` | SessionControl protobuf bindings, subject helpers, and URL redaction utilities | No filesystem data-plane DTOs, envelopes, errno mapping, NATS filesystem routing, or FUSE types |
| `fabricfs-transport` | NATS subject construction, connection helpers, common-envelope filesystem request/reply carrier, and transport policy for retry, deadline, replay, delivery, handle cleanup, and namespace command ordering | No filesystem DTO ownership or storage semantics |
| `fabricfs-server` | Server storage capability ports, path resolution, backing/COW/alias behavior, tombstones, xattrs, storage runtime, session storage, JetStream publishing | Owns filesystem behavior and persistence |
| `fabricfs-fuse` | Mount startup, CLI configuration, NATS client construction, readiness probing, and product reply presentation around `fs-fuse` | No product-owned filesystem DTOs, cache-safety logic, or server-side storage decisions |
| `infra/` and scripts | Local NATS and smoke-test orchestration | No product logic |

## Dataflow

```text
kernel VFS
   |
   v
fabricfs-fuse
   |
   v
fs-fuse
   |  fs_protocol::RequestEnvelope through fs_core::RpcClient
   v
fabricfs-transport
   |  common envelope over NATS request/reply
   |  mount-scoped invalidation broadcasts
   |
   v
fabricfs-server
   |
   v
fs-core Dispatcher
   |
   v
FabricFsFileSystemService
   |
   +--> overlay/passthrough path adapter
          |
          v
       resolved storage object
          |
          v
       storage runtime (handles, locks, opened-object IO)
          |
          +--> backing root (read source)
          +--> COW root (copy-up data, xattrs, session metadata)
          +--> alias root (mutation gate and tombstones)
```

Mutation invalidations are carried in common response envelopes and published on
`fabricfs.invalidate.v1.<hex_mount>` for other clients. Server startup publishes a
full-resync invalidation, and the server watches configured storage roots for
external changes. Watch events publish dispatcher-sequenced full-resync
invalidations on the same subject. `fs-fuse` drains out-of-band invalidations
before resolving cached parent, inode, or handle paths, updates or poisons
local cache state before the product fuser reply returns, and retains
open-handle lifecycle state until release even when path/inode cache state is
reset or poisoned. Multi-path requests such as rename and product readdir
derive every cached path and parent value from one post-drain snapshot, so an
out-of-band invalidation cannot be applied midway through request payload
construction. `fabricfs-fuse` binds the kernel caller identity with a scoped
guard for each callback, and `fs-fuse` copies that request-local caller context
into the common envelope when the RPC is built.
`fabricfs-fuse::reply::ProductFuseReplyPresenter` owns the fuser reply surface:
callback argument rejection, file/statfs conversion, xattr/listxattr shaping,
readdir entry shaping, lock replies, copy-file-range reply-write sizing, and
errno presentation. The product callback module remains an adapter that binds
caller context, calls `fs-fuse`, and delegates concrete reply emission to the
presenter.

Filesystem command frames are also authenticated at the transport boundary.
`fabricfs-fuse` signs the delivered subject, reply subject, and payload bytes
with a shared secret, and `fabricfs-transport` verifies that signature before the
server authorizer trusts the caller context carried in the envelope.

Inside `fabricfs-server`, overlay and passthrough path adapters resolve virtual
paths into resolved storage objects. The storage runtime consumes those objects
for handle lifecycle, POSIX lock lifecycle, opened-object reads and writes,
durability operations, advanced handle-bound IO, and runtime state rewrites
after rename. Overlay-specific visibility, tombstones, copy-up planning, alias
layout, and COW layout stay in the overlay path adapter; passthrough root
confinement stays in the passthrough path adapter.

The service boundary uses the `ServerStorage` capability seam. Namespace,
metadata, directory, and opened-object runtime capabilities expose cohesive
server-domain behavior to `FabricFsFileSystemService`; the service does not
depend on a broad POSIX-shaped backend facade. `StorageRuntime` remains behind
the opened-object capability for handle lifecycle, lock lifecycle, byte IO,
durability, `copy_file_range`, `fallocate`, `lseek`, and runtime rename
effects.

The current deep module ownership is:

- `fabricfs-server/src/session_storage/durability.rs`: recovery journal
  replay, live-boundary settlement, prepared atomic writes, password
  durability, delete quarantine, restart replay, and idempotent recovery.
- `fabricfs-server/src/watch/`: request admission, full-resync debt,
  self-notify suppression, watcher event classification, and NATS publication
  retry.
- `fabricfs-server/src/server/`: path helpers, permission policy, byte IO,
  advanced IO math, metadata conversion, errno helpers, xattrs, and lock/flock
  tables used by storage adapters and the runtime.
- `fabricfs-server/src/overlay/`: overlay layout, visibility, tombstones,
  copy-up, xattr manifests, resolved-object production, and operation groups
  for namespace, directory, metadata, runtime, and full storage trait wiring.
- `fs-protocol/src/`: envelope, payload, codec, errno, operation conversion,
  path validation, generic field validation, attributes, and invalidation
  contract checks; `operation_spec` remains the single operation-fact source.
- `fabricfs-fuse/src/reply.rs`: product FUSE reply presentation.

## Core Invariants

- The reusable common core owns the shared filesystem protobuf contract,
  dispatcher semantics, local transport conformance surface, and FUSE adapter
  cache-safety behavior without absorbing FabricFs overlay/session behavior.
- Successful common-core responses are validated against their requests before
  dispatcher invalidation allocation or FUSE cache side effects. Oversized read
  data, impossible write counts, excessive readdir entries, oversized
  nonzero-buffer xattr/listxattr results, and create/mkdir attribute-kind
  mismatches fail closed as invalid protocol responses; zero-size
  xattr/listxattr probes may carry full results so callers can compute the
  required buffer length.
- Common-core failure responses are protocol-valid even when direct typed
  callers manually construct malformed request envelopes. Invalid request IDs
  or namespaces are replaced with deterministic non-empty fallback identity
  fields before a dispatcher rejection is returned.
- Common-core invalidation sequence numbers are namespace-scoped. FUSE-facing
  consumers validate response request ID, namespace, and operation correlation
  before applying response-borne invalidations, and malformed same-namespace or
  unclassifiable invalidations poison cache state until full resync. Cache
  sequence state advances only after an invalidation has been applied,
  conservatively removed from the path cache, or handled as a safe
  metadata-only event. Response invalidations are consumed exactly once; local
  transport drains do not replay invalidations already returned by a unary
  response. FabricFs NATS clients receive other clients' mutation invalidations
  through a mount-scoped invalidation subject, and server startup publishes a
  sequence-reset full-resync invalidation to recover existing clients after
  dispatcher sequence reset without advancing the new dispatcher's mutation
  sequence. Server instances also watch configured storage roots
  (backing, alias, and COW roots in overlay mode; the passthrough root in
  passthrough mode) and publish dispatcher-sequenced full-resync invalidations
  for out-of-band storage changes, so the next dispatcher mutation remains
  contiguous after the resync. Watcher notifications observed while server
  RPCs are active are coalesced and published after active requests finish;
  notifications observed after requests finish publish immediately. This keeps
  ambiguous external mutations from being dropped while preventing watcher
  full resyncs from racing ahead of the direct response path. The NATS client
  scopes outbound request IDs per client
  and registers the scoped ID as pending in its own-replay filter before
  publish. In-flight own broadcasts are deferred, successful direct responses
  mark the scoped ID complete and drop deferred duplicates, and timed-out or
  otherwise abandoned requests release deferred broadcasts back to later drains
  as recovery invalidations. It restores the original ID on direct responses,
  so own-replay filtering cannot suppress another mount that used the same
  local request ID, cannot apply the originating client's broadcast while the
  direct response is still in flight, and does not lose committed invalidations
  after a direct reply is lost.
- Out-of-band invalidation drains fail closed. If the transport consumes a
  malformed, oversized, disconnected, or otherwise uncertain invalidation
  frame, the reusable FUSE adapter poisons path/inode cache state before
  returning the transport error. Subsequent cache-backed calls require a full
  resync instead of continuing with stale cached state. A decoded drain batch
  is processed through the end after an apply error, so a later full-resync
  recovery message in the same consumed batch is not dropped after an earlier
  sequence gap. Existing open handles keep their handle path until release;
  handle-bound I/O remains governed by the server handle table, and release
  bypasses invalidation draining so it can best-effort close backend handles
  even when the cache is unhealthy. Committed rename responses rewrite retained
  handle paths before any sequence-gap poison is surfaced, keeping handle I/O
  aligned with the server's renamed handle table while cache-backed lookups wait
  for full resync.
- A FUSE adapter with no cached non-root paths, no lookup references, no open
  handles, and no poison state may accept the first drained non-full-resync
  invalidation as its sequence baseline. This lets late-joining or freshly
  reconnected empty clients converge after missing the one-shot startup
  full-resync publish. Once the adapter has cached state or open handles,
  noncontiguous drained invalidations poison cache state until a full resync;
  direct response invalidation gaps still poison even if the cache is empty,
  because the answered request has already crossed the mutation boundary.
- The NATS server validates that the decoded command subject and common request
  envelope bind to the same product namespace before dispatch. Product
  filesystem servers configure the expected namespace to the mount name, reject
  subject mount or envelope namespace mismatches without touching storage, and
  reject operation-token mismatches the same way. This preserves
  namespace-scoped cache invalidation, operation-scoped broker routing, and ACL
  assumptions. Command messages without a reply subject are also rejected
  before decode or dispatch, so filesystem mutations cannot bypass
  request/response correlation.
- Caller context is not transport-trusted by default. `fabricfs-transport`
  preserves `uid`/`gid`/`pid` for authorization only after transport auth
  verification succeeds and tags the request with a transport-controlled
  `nats-auth:<key-id>` peer identity. Missing or invalid auth returns
  `PermissionDenied` before storage dispatch or invalidation-sequence
  advancement.
- Server-side invalidation publication is ordered per namespace. Command
  handling holds the namespace section from dispatch through response
  invalidation publication and reply publication, and storage-watch full
  resyncs use the same section. A later same-namespace mutation sequence cannot
  be broadcast before an earlier one.
- Server startup readiness is a broker-confirmed lifecycle point: the request
  subscription is flushed before startup full resync is published, and the
  full-resync publication is flushed before readiness is logged.
- Common-core mutating FUSE callbacks fail closed unless the successful
  response includes same-namespace, request-correlated invalidation evidence
  when the mutation can change cached path, data, metadata, or xattr state.
  Create-like mutations (`create`, `mkdir`, `symlink`, `hardlink`) require the
  covering invalidation to carry the created inode from the validated success
  payload. Remove/rename mutations require path coverage. Data and metadata
  mutations (`write`, truncating `open`, `setattr`, `setxattr`,
  `removexattr`, `copy_file_range`, and `fallocate`) require modify,
  metadata, or xattr coverage for the requested path. A same-request
  full-resync invalidation is also valid coverage because it clears cache
  state instead of applying a path-specific update. Missing, unrelated, or
  inode-less covering invalidations poison the adapter cache so stale mappings
  cannot survive a server-side mutation.
- Common-core mutating FUSE callbacks also poison cache state when the
  transport fails or the response protocol cannot be trusted after dispatch.
  The adapter treats those outcomes as unknown because the server may have
  committed before the caller observed the failure. Handle release bypasses
  invalidation draining so backend handles can still be closed.
- The NATS transport retries only failures that happen before publish, plus
  post-publish failures for read-only requests. NATS publish-call errors are
  treated as ambiguous for mutations because the client cannot prove bytes were
  not handed to the socket or flusher. Writes, create, mkdir, symlink,
  hardlink, rename, unlink, rmdir, setattr, xattr mutations, durability calls,
  lock mutations, copy_file_range, fallocate, and all opens are not retried
  after any publish attempt unless server-side request dedupe exists. Opens are
  included because they allocate backend handle lifecycle state, and writable
  overlay opens can copy up storage before the client observes a response.
- The NATS client narrows outbound request deadlines to its own response
  timeout. The NATS server accepts authenticated deadlines up to its configured
  authenticated request TTL, which defaults to 300 seconds. After dispatch, the
  NATS server treats an expired deadline or a failed invalidation/reply
  publication as a lost handle-returning response. For successful open/create
  payloads it calls the dispatcher abort hook, which releases the returned
  backend handle through the service before the transport error is reported.
- Common metadata DTOs preserve the mounted fields FabricFs reports to the
  kernel. `FileAttr` carries distinct access, modify, change, and creation
  timestamps; `StatFs` carries both free and available block counts and both
  block and fragment sizes. Product FUSE reply code maps those fields directly
  instead of substituting mtime or block size for missing DTO fields.
- The reusable FUSE adapter accounts for kernel lookup references. Each
  successful lookup/create/mkdir entry increments the inode lookup count, and
  forget or batch-forget removes the path mapping only after the kernel-provided
  `nlookup` count releases all outstanding references.
- Common-core readdir adapters preserve the caller's pagination offset in the
  protobuf request; the adapter must not restart directory iteration by
  forcing offset zero. Product FUSE replies emit `.` and `..` with stable
  offsets before server entries.
- Common-core open invalidation is request-aware. Successful opens with
  `OPEN_FLAG_TRUNCATE` emit modify invalidations because the handler may have
  changed file size or contents, while successful opens without truncation
  do not consume invalidation sequence numbers.
- Common-core handle-bound FUSE callbacks do not depend on live path-cache
  entries after open or create. The adapter retains an `(inode, handle)` path
  table so read, write, handle-backed setattr, durability operations, advanced
  I/O, and release can still send RPCs after unlink invalidations, full resync,
  cache poison, or forget remove the inode path. Release drops the retained
  handle path after the release attempt and does not drain invalidations before
  contacting the server.
- Filesystem protocol version, operation naming, envelope fields, errno
  conversion, and invalidation semantics have a single source of truth in
  `fs-protocol` and `fs-core`.
- The mounted common data plane exposed by the current `fabricfs-fuse` product is
  lookup, getattr, readdir, open, read, write, create, rename, unlink, mkdir,
  rmdir, readlink, symlink, hardlink, setattr, flush, fsync, fsyncdir, getlk,
  setlk, copy_file_range, fallocate, lseek, statfs, xattr operations, and
  release. `Setattr` is mounted only for the current common fields
  (mode/uid/gid/size plus optional open handle binding). The common protocol
  also defines `flock`; the current fuser 0.14 product API exposes POSIX locks
  and release-time flock ownership, not a distinct callback method for BSD
  flock acquisition.
  Blocking `setlk`, conflicting blocking flock acquisition, nonzero
  `copy_file_range` flags, nonzero `fallocate` modes, and sparse
  `SEEK_DATA`/`SEEK_HOLE` return explicit unsupported errno responses in the
  shared adapter/service path; conflicting flock requests with `LOCK_NB`
  return `EAGAIN`.
- Server-mounted inode identity is keyed by backend `(dev, ino)` when metadata
  is available, so hardlink aliases report the same mounted inode and link
  count through lookup, getattr, readdir, and hardlink replies. The reusable
  FUSE path cache stores every known path alias for an inode and removes only
  the affected alias on path invalidation, so unlinking one hardlink does not
  make another cached alias stale. Symlink existence checks use
  `symlink_metadata` so dangling symlink objects remain visible to `stat`,
  `readdir`, and `readlink`.
- Backend POSIX lock state is a shared byte-range table for overlay and
  passthrough. It rejects incompatible overlapping locks from different
  owners, reports conflicts through `getlk`, splits partially unlocked ranges,
  and removes handle-owned locks on release. Backend flock state is a separate
  whole-file table: it permits exclusive flock on read-only descriptors,
  conflicts only with other flock owners, returns `EOPNOTSUPP` rather than
  blocking on conflicting blocking acquisition, and is released with the
  handle.
- POSIX `O_*` request flags cross the protocol only as request inputs.
  Open/create replies to the kernel use FUSE open-reply flags, which default
  to zero. Product code must not echo backend request flags into fuser replies.
- SessionControl message encoding has a single source of truth in
  `fabricfs-session-protocol`.
- Remote-checkpoint imports are deterministically idempotent. `fabricfs-sessiond`
  derives the imported session ID from the remote checkpoint ID and persists it
  through the same session-storage path as other sessions, so retries or
  restarts converge on one imported session instead of relying on a separate
  side registry.
- NATS subject names and subscription scopes have a single source of truth in
  `fabricfs-transport`.
- A mount without `--alias-path` is read-only. Mutations require an alias path
  and either a COW root or explicit `--update-backingtree`.
- Backing data is never modified by default. COW copy-up handles regular files,
  hard links, symlinks, xattrs, sparse files, and reflinks where supported.
- Tombstones hide backing entries after delete or rename and persist under the
  alias root.
- FUSE inode/path cache safety belongs to `fs-fuse`; product FUSE code only
  owns mount process wiring and product reply presentation.
- Transport and server failures must map to explicit errno values. Silent
  success on partial or ambiguous failure is not acceptable.
- Production binaries surface runtime health through structured metrics logs:
  `fabricfs-server` reports worker-pool saturation and latency, `fabricfs-fuse`
  reports adapter and transport-client state, and `fabricfs-sessiond` reports
  session-service and published-store activity. Operators control cadence with
  `--metrics-interval-secs` / `FABRICFS_METRICS_INTERVAL_SECS`.
- Long-running validations use repo-local locks through
  `scripts/run_managed_command.sh` or release-task wrappers.

## Lifecycle

1. Start or provide a NATS server.
2. Start `fabricfs-server` with a mount name and storage roots.
3. Mount `fabricfs-fuse` with the same mount name and NATS URL.
4. Optionally start `fabricfs-sessiond` against the COW root for session,
   checkpoint, publish, and pull workflows.
5. Run smoke checks through `./smoke.sh` and `./smoke-sessions.sh`.

## Observability

- Request IDs, namespace, trace context, caller context, and invalidations are
  carried in common filesystem envelopes.
- Transport errors are mapped to explicit common `RpcError` variants.
- Server mutation paths return response invalidations so FUSE cache behavior is
  diagnosable at the request boundary.
- Release-track tasks must preserve structured errors and avoid ad hoc string
  protocols across crate boundaries.

## Quality Gates

- `just check`: formatting, clippy with `-D warnings`, and the
  workspace test suite.
- `just ci`: formatting, clippy, and the llvm-cov line
  gate over the current release-measured surface. The gate includes migrated
  common protocol/core/FUSE/transport/service runtime libraries and excludes
  product process entrypoints plus session-control utilities that are covered
  by command-path tests.
- `just coverage-full-summary`: unfiltered coverage snapshot for tracking
  surfaces outside the release-measured gate.
- Migrated runtime adapters and storage engines also require targeted cargo
  suites and live NATS/FUSE smoke evidence because their command paths depend
  on broker and mount execution.
- `scripts/release-check.sh`: workspace hygiene, coverage, and smoke checks
  when `nats-server` is locally available.
