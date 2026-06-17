FabricFs Protocol
================

FabricFs uses two protocol surfaces:

- Filesystem data-plane traffic uses the reusable common stack in
  `fs-protocol`, `fs-core`, `fs-fuse`, and `fabricfs-transport`.
- SessionControl traffic remains product-specific in `fabricfs-session-protocol` and
  `session.proto`.

Filesystem Data Plane
---------------------
FUSE callbacks are converted by `fs-fuse` into `fs_protocol::RequestEnvelope`
values. `fabricfs-fuse` supplies caller context and sends those envelopes through
`fabricfs_transport::FileSystemClient`. NATS carries the encoded common envelope
to `fabricfs-server`, where `fabricfs_transport::FileSystemServer` decodes it and
dispatches through `fs_core::Dispatcher` into
`fabricfs_server::service::FabricFsFileSystemService`.

`fs-protocol` owns the neutral operation specification for filesystem
commands: wire value, subject token, payload and response shape, path roles,
broad effect category, response limits, and handle-returning response facts.
`fs-core`, `fs-fuse`, and `fabricfs-transport` derive invalidation allocation,
cache-coverage checks, subject parsing, and retry policy from those facts while
keeping concrete dispatcher, FUSE, and NATS decisions in their own crates.

`fs-protocol/src/lib.rs` is the protocol export surface. The current protocol
implementation is split by responsibility:

- `operation_spec.rs`: the single operation-fact table.
- `operation.rs`: operation enum conversion from wire values and subject
  tokens.
- `envelope.rs`: request/response identity, version, namespace, operation, and
  correlation validation.
- `payload.rs`: typed payload registry, payload encode/decode dispatch,
  request-aware response validation, and response limits.
- `codec.rs`: protobuf message and envelope encode/decode helpers.
- `errno.rs` and `error.rs`: errno and protocol error semantics.
- `path.rs` and `validation.rs`: path DTO and generic protocol field
  validation.
- `attributes.rs`: common attribute constructors used by tests and fixtures.
- `invalidation.rs`: invalidation kind conversion and invalidation contract
  checks.

The mounted filesystem surface exposed by the current `fabricfs-fuse` product is
exactly:
lookup, getattr, readdir, open, read, write, create, rename, unlink, mkdir,
rmdir, readlink, symlink, hardlink, setattr, flush, fsync, fsyncdir, getlk,
setlk, copy_file_range, fallocate, lseek, statfs, getxattr, setxattr,
listxattr, removexattr, and release.

Mounted-callback caveats:

- Metadata: `Setattr` with optional mode, uid, gid, size, and handle fields
  for chmod/chown/truncate behavior. When FUSE supplies an open file handle,
  FabricFs carries that handle so fchmod/fchown/ftruncate-style updates can
  target an open file after its path has been unlinked or invalidated.
  Timestamp, birth-time, and inode-flag updates are not part of the common
  mounted `Setattr` contract and fail closed at the product edge.
- Locking: `Getlk`, `Setlk`, and `Flock`. POSIX byte-range locks and BSD-style
  whole-file flock state are separate backend tables. POSIX locks reject
  incompatible overlapping ranges, report conflicts through `Getlk`, split
  partial unlocks, and drop handle-owned ranges during release. Flock permits
  exclusive whole-file locks on read-only descriptors, conflicts only with
  other flock owners, and is released with the file handle. A conflicting
  flock request with `LOCK_NB` returns `WouldBlock`; a conflicting blocking
  flock acquisition returns `NotSupported` instead of blocking a worker. The
  current product fuser 0.14 API exposes POSIX lock callbacks and release-time
  flock ownership, but does not expose a separate BSD-flock callback method for
  acquisition.
- Advanced I/O: `CopyFileRange`, `Fallocate`, and `Lseek`. `CopyFileRange`
  supports only zero flags. `Fallocate` supports mode 0 only and never shrinks
  a file when the requested allocation range ends before EOF.

Unsupported host or backend cases are explicit protocol failures. The current
server returns `NotSupported` for blocking `setlk`, conflicting blocking
flock acquisition, nonzero `copy_file_range` flags, nonzero `fallocate` modes,
and sparse `SEEK_DATA`/`SEEK_HOLE`. Those cases remain covered through common
adapter and service tests, and the mounted product surface forwards the
supported non-blocking/common forms through the reusable adapter.

Request envelopes carry:

- `protocol_version`
- `request_id`
- `operation`
- `namespace`
- `deadline_unix_nanos`
- `trace`
- `caller { uid, gid, pid }`
- operation-specific protobuf payload

NATS transport also carries authentication headers outside the protobuf
envelope. The filesystem client signs the delivered subject, reply subject,
and payload bytes with a shared secret. The server verifies those headers
before it treats the envelope caller context as trustworthy.

Responses echo the request identity and carry:

- `ok`
- `errno`
- `error_message`
- operation-specific protobuf payload
- ordered invalidations
- observations

Common file attributes carry inode, size, kind, permissions, uid, gid, nlink,
and distinct access, modify, change, and creation timestamps in Unix
nanoseconds. FabricFs storage currently supplies access, modify, and change
times from the backend stat result; creation time is zero when the backend does
not expose a birth time. Common statfs responses carry total blocks, free
blocks, available blocks, total files, free files, block size, fragment size,
and maximum name length so mounted `getattr` and `statfs` replies do not
collapse backend metadata fields at the FUSE edge.

The mounted product reply boundary is outside the protocol crate.
`fabricfs-fuse::reply::ProductFuseReplyPresenter` converts validated reusable
adapter outputs into fuser replies and rejects callback-only arguments before
RPC. Cache safety, invalidation coverage, and request construction remain in
`fs-fuse`; NATS subjects and retry/delivery policy remain in
`fabricfs-transport`.

The client validates response request ID, namespace, operation, payload shape,
and invalidations before the FUSE adapter applies cache side effects.
Request IDs are opaque correlation values. The NATS client scopes outbound
request IDs with a per-client transport prefix before publishing to the broker,
then restores the caller's original request ID on the direct response. Broker
invalidations retain the scoped ID so a client can filter only its own
out-of-band replay without dropping another mount's colliding local ID. The
scoped request ID is registered as pending in the client's own-replay filter
before the request is published, so a concurrent invalidation drain cannot
apply the client's own broadcast while the direct response is still in flight.
Pending own broadcasts are deferred rather than discarded. A successful direct
response completes the scoped ID and drops deferred duplicates; a timeout,
malformed direct response, or lost reply abandons the pending ID and makes any
deferred own broadcasts available to later drains as recovery invalidations.
The NATS client also projects its transport timeout into the outbound envelope
deadline when the caller did not provide an earlier deadline. If an open/create
handler returns a backend handle after that deadline has expired, or if
post-dispatch invalidation or reply publication fails, the transport server
calls the common dispatcher abort hook to release that handle before returning
the timeout or transport error. Authenticated request deadlines are accepted up
to the server's configured authenticated request TTL, which defaults to 300
seconds and can be raised for deployments that intentionally use longer client
timeouts.

NATS Subjects
-------------
Filesystem requests use:

```text
fabricfs.v1.<hex_mount>.<operation>
```

The server subscribes to:

```text
fabricfs.v1.<hex_mount>.>
```

Out-of-band invalidations use:

```text
fabricfs.invalidate.v1.<hex_mount>
```

`<hex_mount>` is the hex-encoded mount name. Subject construction is centralized
in `fabricfs-transport`; neither `fs-protocol` nor `fs-core` know about NATS.
When a request arrives from NATS, `fabricfs-transport` parses the delivered
mount and operation tokens. Product filesystem servers configure the expected
namespace to the mount name and reject the message before dispatch if either
the subject mount or encoded envelope namespace does not match that expected
namespace. The server also rejects a delivered operation token that does not
match the encoded envelope operation. Broker ACLs and operation-scoped subjects
therefore bind to the namespace and operation the server will execute.
Filesystem command messages must include a NATS reply subject; no-reply
command publications are rejected before envelope decode or storage dispatch
because filesystem operations are request/response contracts.

The transport auth boundary is part of the NATS contract. Missing or invalid
transport-auth headers return `PermissionDenied` before dispatch, storage
mutation, or invalidation-sequence advancement. Verified requests are tagged
with a transport-controlled peer identity of the form `nats-auth:<key-id>`,
and only that verified identity allows the server authorizer to trust caller
`uid`/`gid`/`pid`.

Error Semantics
---------------
`fs-protocol` owns the errno enum and wire conversion. Filesystem failures
return protocol-valid failure envelopes with positive POSIX errno values.
Transport failures are mapped at the FUSE adapter boundary to explicit kernel
errors such as `EIO`, `ETIMEDOUT`, `EPROTO`, or connection-closed errors.
The NATS client may retry only failures it can classify as safe for the
request. A publish call error is ambiguous for mutations because bytes may have
reached the socket or flusher before the error surfaced. Writes, creates,
mkdirs, symlinks, hardlinks, renames, deletes, setattr, xattr mutations,
durability calls, lock mutations, copy_file_range, fallocate, and truncating
opens are therefore not retried after a publish attempt without server-side
request dedupe. Non-truncating opens are also not retried after publish because
they allocate backend handle lifecycle state and writable overlay opens can
perform copy-up before the client observes a response.
Release requests are retryable after publish because server release is
idempotent cleanup: a duplicate release removes the same backend handle and
locks if still present and otherwise succeeds.

`OpenRequest.flags` carries POSIX `O_*` request flags to storage. It is not a
FUSE reply field. `OpenRequest.kind` carries the caller intent: file opens send
`OPEN_KIND_FILE`, and directory opens send `OPEN_KIND_DIRECTORY`. Services pass
that kind to storage instead of inferring it from metadata, so symlink traversal
and file-vs-directory mismatch errors remain backend-defined. `OpenResponse.flags`
and the create/open handle returned to the product FUSE layer carry FUSE
open-reply flags only and default to zero; echoing POSIX bits such as `O_EXCL`
can make the kernel reject an otherwise successful create/open reply with `EIO`.

Invalidations
-------------
`fs-core` allocates response-borne invalidations for mutating operations.
`fabricfs-transport` publishes successful response invalidations on the
mount-scoped invalidation subject so other clients can drain them before their
next cache-backed operation. The originating client filters its own
request-correlated broadcast copy so response invalidations are not replayed to
the caller that already consumed them. That filter is armed before publish,
because the server broadcasts invalidations before it sends the direct reply.
The server holds a namespace ordering section from request dispatch through
response invalidation publication and reply publication. This prevents a later
sequence from reaching NATS before an earlier same-namespace mutation
invalidation. Storage-watch full-resync invalidations are generated and
published through the same ordered namespace section.
`fabricfs-server` publishes a sequence-reset full-resync invalidation on startup
so existing clients clear path/inode cache state after server restart. Startup
full resync does not advance the new dispatcher's mutation sequence, so clients
that miss startup can still accept the first real mutation sequence. The server
installs the storage watcher before queuing startup full resync, flushes the
request subscription before publishing startup full resync, and flushes that
full-resync publish before logging readiness.

The FUSE adapter requires successful mutating responses to include a
same-namespace, request-correlated invalidation covering any cache state the
mutation can change. Create-like operations (`create`, `mkdir`, `symlink`,
`hardlink`) require invalidations carrying the created inode from the validated
success payload. Remove and rename operations require path coverage. Data,
metadata, and xattr operations (`write`, truncating `open`, `setattr`,
`setxattr`, `removexattr`, `copy_file_range`, and `fallocate`) require modify,
metadata, or xattr coverage for the requested path. A same-request full-resync
invalidation also covers those mutations because it clears cache state instead
of applying a narrower path update.

Malformed invalidations, sequence gaps, missing mutation coverage, or
transport/protocol failures after a path mutation poison the adapter cache.
This prevents stale inode/path mappings from surviving uncertain server state.
The adapter drains out-of-band invalidations before resolving cached parent,
inode, or handle paths for a request, so queued renames and full resyncs are
reflected in the request payload instead of being applied after a stale path is
captured. Multi-path operations resolve all cached paths and parent values from
one post-drain snapshot; rename does not drain separately for old and new
parents, and product readdir receives its `..` inode and `ReaddirRequest.path`
from the same snapshot. The adapter still processes an already consumed
out-of-band drain batch through the end, so a later full-resync invalidation in
the same batch can recover from an earlier sequence gap instead of being
discarded. A drained non-full-resync invalidation with a noncontiguous sequence
may be accepted as a baseline only when the adapter has no cached non-root
paths, lookup references, open handles, or poison state. This covers late joins
and empty reconnects that missed the one-shot startup full resync. Once local
cache or handle state exists, sequence gaps poison cache state until full
resync; direct response invalidation gaps also poison because the response is
tied to a mutation whose ordering must already be known. For committed rename
responses, the adapter rewrites retained open-handle paths before reporting
sequence-gap poison so later handle-backed I/O uses the server's renamed path
while path-cache lookups remain blocked until full resync.
`fabricfs-fuse` captures the kernel caller for each callback and binds it with a
scoped request guard. The caller context copied into a common request envelope
is therefore request-local and cannot be overwritten by another concurrent FUSE
callback.
Open-handle path state is retained until release and is not part of the
path/inode cache. Handle-backed reads, writes, setattr, durability operations,
advanced I/O, and release can continue after the cached path is removed because
the adapter resolves them through the retained `(inode, handle)` table.
Release does not drain invalidations before sending its RPC, so backend file
handles can be closed best-effort even when the cache is poisoned or a
malformed invalidation is pending.
FUSE forget handling tracks kernel lookup reference counts; partial forgets
decrement the count and keep inode/path mappings live until all lookup
references are released. The path cache stores all known hard-link aliases for
an inode, so removing or forgetting one alias does not evict another live path.

Overlay Layout
--------------
- Reads resolve tombstones first, then the COW root, then the backing root.
- Mutations require `--alias-path` plus either `--cow-path` or
  `--update-backingtree`.
- With `--cow-path`, writes and creates land in the COW root and deletions
  create tombstones under `<alias-path>/.fabricfs_tombstones`.
- A child tombstone directory is not itself a tombstone. When a path tombstone
  collides with descendant tombstones, FabricFs records an explicit
  `.fabricfs_tombstone` marker.
- Extended attributes for COW entries persist under `<cow-path>/.fabricfs_xattrs`.
- Servers watch configured storage roots. External create, modify, remove,
  rename, write-close, or ambiguous storage events publish a
  dispatcher-sequenced full-resync invalidation on the mount-scoped NATS
  invalidation subject. Notify events observed during active server RPCs are
  coalesced and published after active requests finish. Notify events observed
  after requests finish publish immediately, so ambiguous out-of-band storage
  mutations are not dropped.

SessionControl
--------------
SessionControl remains in `fabricfs-session-protocol`:

- subject helpers and operation names live in `fabricfs_session_protocol::session`
- protobuf bindings are generated from `session.proto`
- `fabricfs-sessiond` serves session, overlay metadata, checkpoint, publish, and
  import operations
- `fabricfsctl` is the CLI client for that API

SessionControl does not define filesystem operation DTOs or the FUSE/NATS
filesystem envelope.
