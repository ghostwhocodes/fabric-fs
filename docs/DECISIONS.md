# Decision Log

## 2026-05-19: Adopt Repo-Local Task Workflow

FabricFs tracks large work packages in `ai/tasks/<task-slug>/` with task state,
validation events, closeout evidence, and review readiness so multi-run work does
not depend on chat history.

Validation commands for task completion are run through the repo's standard
validation flow so evidence is captured for each gate.

## 2026-05-19: Use `just` As The Developer Gate

Developer and CI workflows are normalized through `justfile` recipes. The
baseline closeout gate is `just check`; CI uses `just ci` to add coverage.

The managed command wrapper serializes long-running cargo and coverage work in
one checkout and terminates child process groups on interruption.

## 2026-05-19: Make Coverage Debt Visible

FabricFs has two coverage views:

- `just ci` enforces the configured line gate over the migrated runtime
  libraries and service adapters. The gate includes common protocol/core/FUSE
  code, NATS filesystem transport, FabricFs service adapters, product FUSE reply
  wiring, storage libraries, and retained SessionControl helpers.
- `just coverage-full-summary` reports unfiltered workspace coverage.

Product process entrypoints and session-control process utilities are excluded
from line coverage and validated through command-path tests. The coverage
threshold follows the actual measured release surface instead of re-excluding
migrated runtime files to preserve the old percentage.

## 2026-05-19: Keep Crate Ownership Explicit

FabricFs keeps protocol, transport, server semantics, and FUSE adaptation in
separate crates. New shared wire or subject helpers belong in
`fs-protocol` or `fabricfs-transport`; filesystem behavior belongs in
`fabricfs-server`; reusable kernel callback and path-cache behavior belongs in
`fs-fuse`; product mount wiring belongs in `fabricfs-fuse`.

## 2026-05-22: Keep SessionControl v1 During Unreleased Iteration

FabricFs has not shipped a SessionControl compatibility boundary. Session server
and CLI binaries are rebuilt together during this phase, so unreleased
SessionControl protobuf changes do not require a durable migration layer.

## 2026-05-24: Specify A Transport-Neutral FUSE/Protobuf Core

Future filesystem work should extract a reusable core where protobuf defines
the filesystem protocol and transports only carry protobuf envelopes. NATS
becomes one adapter beside local in-process, Unix socket, and HTTP adapters.

The first build lives in `fs-protocol`, `fs-core`, `fs-testkit`,
`fs-transport-local`, and `fs-fuse`.

## 2026-05-27: Complete The Common Filesystem Data Plane

FabricFs filesystem traffic now uses the common stack end-to-end. `fabricfs-fuse`
wraps `fs_fuse::FuseAdapter`, `fabricfs-transport` carries common filesystem
envelopes over NATS, and `fabricfs-server` dispatches through
`fs_core::Dispatcher` into `FabricFsFileSystemService`.

`fabricfs-session-protocol` no longer owns filesystem operation DTOs, filesystem
envelopes, errno mapping, invalidation events, or FUSE callback types. It owns
SessionControl and URL redaction helpers only. Product-specific overlay,
passthrough, checkpoint, JetStream, CLI, and mount-process behavior remain in
the FabricFs crates.

Old product filesystem handler, transport, and FUSE cache modules were removed
instead of kept as compatibility paths because the old data-plane wire format
is out of scope.

## 2026-05-24: Consume Response Invalidations Once

Common-core mutation responses may carry cache invalidations. A consumer that
applies those response-borne invalidations must not receive the same sequence
again through an out-of-band drain. The local transport returns mutation
invalidations in the unary response and leaves `drain_invalidations` empty
because it has no separate invalidation stream.

The FabricFs NATS transport has an out-of-band invalidation stream on
`fabricfs.invalidate.v1.<hex_mount>`. Servers publish successful response
invalidations for other clients, clients filter their own request-correlated
broadcast copy, and server startup publishes a sequence-reset full-resync
invalidation so existing mounts clear cache and handle-path state after
dispatcher sequence reset without consuming the new dispatcher's mutation
sequence.

Servers also watch configured storage roots: backing, alias, and COW roots in
overlay mode, or the passthrough root in passthrough mode. Any mutating or
ambiguous notify event on those roots publishes a full-resync invalidation
through the common invalidation stream. The full resync is allocated through
the common dispatcher sequence state, not a separate watcher counter, so
clients that receive it remain contiguous with subsequent response
invalidations. Notify events observed during active server RPC handling are
coalesced and published after active requests finish; notify events observed
after requests finish publish immediately. This preserves external
out-of-band mutation visibility instead of dropping ambiguous watcher events.

NATS clients scope outbound request IDs with a per-client prefix before
publishing and restore the caller's request ID on direct responses. The scoped
ID remains on broker invalidations, which makes own-replay filtering safe even
when two FUSE mounts both generate local IDs such as `fuse-1`.

FUSE cache consumers treat malformed invalidations with an empty namespace as
unclassifiable. They poison cache state before returning the protocol error so
stale mappings cannot survive an unapplyable successful mutation response.
The same fail-closed rule applies to out-of-band drain errors: a malformed,
oversized, disconnected, or otherwise uncertain drained frame means the
transport may already have consumed state the adapter cannot classify, so the
adapter poisons path/inode cache state before returning the error.

Open-handle path state is not path-cache state. The FUSE adapter retains it
until release so read, write, and release can still reach the server after
unlink, full resync, sequence gaps, cache poison, or forget remove inode/path
cache entries. Release also bypasses invalidation draining before sending its
RPC so malformed pending invalidations cannot leak backend file handles.

Path-mutating FUSE callback successes also require a same-namespace,
request-correlated invalidation that covers the requested create, mkdir,
unlink, rmdir, or rename. Missing or unrelated response invalidations poison
the cache and fail the call because the adapter has no safe proof that its
path/inode mappings still match the server state.

Create and mkdir successes require that covering invalidation to carry the
created inode from the validated success payload. A path-only create
invalidation is safe for out-of-band conservative cache removal, but it is not
enough proof for a successful create or mkdir callback to return a live
FUSE-facing inode.

Path-mutating callback transport failures and response-protocol failures also
poison the adapter cache. Those failures may happen after a serialized or
future external transport has dispatched the request and the server has
committed, so continuing to trust cached path/inode mappings would be unsafe.

For the same reason, the NATS request/reply client does not retry mutating
requests or opens after publish unless the server grows request-ID dedupe.
Publish failures that the client cannot prove were pre-send are treated as
ambiguous: writes, creates, mkdirs, renames, deletes, xattr mutations, and all
opens return timeout or transport failure as an uncertain operation after any
publish attempt. Opens are included even without `O_TRUNC` because the server
allocates persistent backend handle lifecycle state and writable overlay opens
can copy up storage before a response is observed.
Release is the handle-lifecycle exception: it removes backend handle and lock
state if present and returns success if the handle was already gone, so the
transport may retry release cleanup after transient publish or reply failures.
The client also narrows outbound request deadlines to its own response timeout.
If an open/create response is no longer deliverable because that deadline
expired after dispatch, or because invalidation/reply publication failed, the
NATS server aborts the handle-returning response through the dispatcher so the
service releases the backend handle before the timeout or transport failure is
reported.

Common metadata DTOs preserve mounted metadata fidelity rather than forcing the
FUSE product layer to synthesize missing fields. `FileAttr` carries distinct
atime, mtime, ctime, and crtime values, and `StatFs` carries both `bfree` and
`bavail` plus both block size and fragment size. Backend birth time remains
zero when the storage layer cannot report it.

The NATS server treats operation subjects as part of the transport contract.
Messages delivered on `fabricfs.v1.<hex_mount>.<operation>` are decoded only
after the delivered subject token can be parsed, and the decoded envelope
operation must match that token before storage dispatch. This keeps
operation-scoped broker routing and ACLs aligned with server execution.
Filesystem command messages must also include a reply subject. No-reply NATS
publications are rejected before decode or dispatch so mutating operations
cannot execute outside caller-visible request/response correlation.

Data-plane server readiness is logged only after the broker has acknowledged
the request subscription and startup full-resync publication through `flush`.
The readiness log is therefore evidence that the responder subject is installed
and startup recovery invalidation is publishable.

FUSE inode cache lifetime follows kernel lookup accounting. The reusable
adapter increments lookup references for entry-returning callbacks and removes
inode/path mappings on forget only after the kernel-provided `nlookup` count
releases all outstanding references; batch forget applies the same accounting.

If dispatcher invalidation sequence state is poisoned, the dispatcher recovers
without panicking and returns a same-request full-resync invalidation for the
successful mutation. The FUSE adapter accepts that full resync as covering path
mutation evidence because it clears path/inode cache state.

The transport conformance suite lives in `fs-testkit` as reusable direct and
serialized runners. Concrete transports provide factories and narrow
transport-specific hooks, while the shared suite owns assertions for malformed
bytes, deadline rejection, frame limits, namespace ordering,
request-contradictory responses, connection loss, and invalidation replay
prevention.

## 2026-05-24: Sanitize Direct Failure Response Identity

Direct typed clients can construct request envelopes without passing through
the protobuf decoder first. The dispatcher still validates those envelopes
before metadata checks, authorization, limits, or service callbacks, but a
malformed request ID or namespace cannot be echoed into a failure response.

`fs-protocol::ResponseEnvelope::failure_for` therefore uses the current
protocol version and deterministic non-empty fallback request identity fields
when the direct request identity is unusable. This keeps direct-mode rejection
artifacts protocol-valid while still preserving normal request correlation for
valid request IDs and namespaces.

## 2026-05-24: Keep Request-Specific Semantics At The Boundary

Some common-core behavior depends on request fields rather than only the
operation enum. The reusable FUSE adapter therefore accepts the readdir
callback offset in its public API and forwards it unchanged in
`ReaddirRequest.offset`, leaving pagination semantics to the filesystem
handler.

The dispatcher classifies successful opens with `OPEN_FLAG_TRUNCATE` as
modify-producing requests because the handler may truncate file contents during
open. Successful opens without that flag remain read-only and do not allocate
invalidation sequences.

## 2026-05-31: Trust Caller Context Only After Transport Authentication

Filesystem caller `uid`/`gid`/`pid` arrives in the common request envelope, but
NATS publishers can otherwise forge those fields. FabricFs therefore binds
authorization to a transport-controlled proof: the client signs the delivered
subject, reply subject, and payload bytes with a shared secret, and the server
only preserves caller context for authorization after that signature verifies.

Verified requests receive a transport-owned peer identity of the form
`nats-auth:<key-id>`. Missing or invalid auth fails closed with
`PermissionDenied` before dispatch, storage mutation, or invalidation-sequence
advancement.

## 2026-05-31: Make Remote Checkpoint Imports Deterministically Idempotent

Remote-checkpoint import state must not depend on a second best-effort registry
that can diverge from durable session storage. FabricFs now derives the imported
session ID from the remote checkpoint ID and persists that session through the
same storage path as every other session.

Retries or restarts for the same remote checkpoint therefore converge on one
session ID. If the stored snapshot matches, the retry returns the existing
session; if it differs, the import fails as a real conflict instead of
creating a second imported session.

## 2026-05-31: Emit Runtime Metrics From Production Binaries

Snapshot getters alone are not an operator surface. FabricFs now emits periodic
structured metrics logs from `fabricfs-server`, `fabricfs-fuse`, and
`fabricfs-sessiond` so queue saturation, request latency, invalidation health,
session activity, and published-store health are visible in production runs.

Operators control the cadence with `--metrics-interval-secs` or
`FABRICFS_METRICS_INTERVAL_SECS`; `0` disables periodic emission.

## 2026-06-13: Resolve Paths Before Entering The Storage Runtime

FabricFs server storage behavior is split between path adapters and the storage
runtime. Overlay and passthrough adapters resolve virtual paths into resolved
storage objects. The storage runtime consumes those objects for handle
lifecycle, POSIX lock lifecycle, handle-backed IO, durability operations,
advanced IO, and runtime state rewrites after rename.

Overlay remains the owner of tombstone visibility, COW layout, alias layout,
xattr materialization, and copy-up planning. The runtime may execute a
resolved copy-up or opened-object plan, but it does not decide overlay
visibility or policy.

This keeps handle and lock semantics local to one module while preserving
overlay and passthrough as concrete path-resolution adapters.

## 2026-06-16: Keep Runtime Architecture Seams Deep

Filesystem operation facts live in the neutral `fs-protocol` operation
specification. Consumer crates may derive policy from that specification, but
they must not keep independent operation inventories for retry, invalidation,
or cache-coverage behavior.

Server storage crosses the service boundary through `ServerStorage`
capability ports for namespace, metadata, directory, and opened-object runtime
behavior. The storage runtime remains the locality point for handles, locks,
opened-object IO, durability, advanced IO, and runtime rename effects.

FUSE cache safety lives in the `fs-fuse` cache kernel. Product FUSE code and
callback translation code use kernel decisions and snapshots instead of owning
path-cache, lookup-reference, retained-handle, invalidation, poison, or
full-resync policy themselves.

NATS-facing retry, deadline, replay, response-delivery, abandoned-handle
cleanup, and namespace command-ordering policy lives in
`fabricfs-transport::policy`. Client and server modules remain NATS adapters
around that policy. Storage-watch full-resync admission stays in
`fabricfs-server`.

Session durability lives behind `fabricfs-server/src/session_storage/durability.rs`.
SessionStore owns SessionControl-facing behavior and delegates recovery
journals, live-boundary settlement, prepared atomic writes, password
durability, delete quarantine, restart replay, and idempotent recovery to that
module.

Storage-watch behavior lives behind `fabricfs-server/src/watch/`: admission
and full-resync debt in `admission.rs`, self-notify expected-state matching in
`self_notify.rs`, notify classification in `events.rs`, and NATS invalidation
publication/retry in `publication.rs`.

Server storage primitives live behind `fabricfs-server/src/server/` instead of
one broad helper shelf. Path confinement, permission policy, byte IO, advanced
IO math, metadata conversion, errno helpers, xattr helpers, and lock/flock
tables remain server-domain implementation modules used by path adapters and
the storage runtime.

Overlay path adapter policy lives behind `fabricfs-server/src/overlay/`.
Overlay layout, visibility, tombstones, copy-up, xattr manifests,
resolved-object production, and cohesive operation groups stay there instead
of leaking into the storage runtime.

Protocol envelope behavior lives in named `fs-protocol` modules:
`envelope`, `payload`, `codec`, `errno`, `operation`, `path`, `validation`,
`attributes`, and `invalidation`. `fs-protocol/src/lib.rs` is an export surface
and `operation_spec` remains the single source for operation facts.

Product FUSE reply presentation lives in
`fabricfs-fuse::reply::ProductFuseReplyPresenter`. Fuser callback code owns
caller binding and `fs-fuse` adapter calls; the presenter owns callback
argument rejection, reply shaping, fuser type conversion, lock replies,
copy-file-range reply sizing, and errno presentation.
