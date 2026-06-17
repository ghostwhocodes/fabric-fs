# FabricFs Context

## Storage runtime

The module that owns behavior for resolved filesystem objects after path
resolution has selected the concrete storage target. It owns handle lifecycle,
lock lifecycle, byte IO, durability operations, opened-file permission checks,
resource limits, and rename effects on live runtime state.

Important invariants:
- It does not decide overlay visibility, tombstone visibility, copy-up policy,
  alias layout, or COW layout.
- It operates on resolved storage objects supplied by path adapters.
- It is the locality point for bugs in handles, locks, and opened-object IO.

Related terms: Resolved storage object, Overlay path adapter, Passthrough path
adapter.

## Resolved storage object

A typed description of what a FabricFs virtual path means at the storage edge.
It carries enough information for the storage runtime to perform an operation
without re-resolving overlay or passthrough layout.

Important invariants:
- It preserves whether the target is backing, COW, alias metadata, tombstone
  metadata, or a copy-up plan.
- It includes the host path and policy facts needed for permission and handle
  behavior; the storage runtime derives backend identity from opened files when
  lock behavior needs it.
- It is produced before runtime operations and consumed by the storage runtime.

Related terms: Storage runtime, Overlay path adapter, Passthrough path adapter.

## Overlay path adapter

The adapter that resolves FabricFs virtual paths through backing, COW, alias,
tombstone, and `/.fabricfs` layout rules. It produces resolved storage objects
for the storage runtime.

Important invariants:
- It owns overlay visibility and copy-up planning.
- It does not own generic handle, lock, or byte IO behavior.

Related terms: Resolved storage object, Storage runtime.

## Server storage capability seam

A small server-domain interface that groups cohesive storage behavior behind a
deep module instead of exposing one method per POSIX operation. It replaces the
old broad backend shape with a smaller set of capability ports around path
resolution, namespace mutation, metadata/xattr behavior, directory views, and
opened-object runtime behavior.

Important invariants:
- It is expressed in FabricFs server-domain terms, not protobuf DTOs.
- It must not be a giant request executor that re-matches every filesystem
  operation internally.
- It must not split into one shallow trait per operation.
- It composes with the storage runtime for handles, locks, byte IO,
  durability, and opened-object state.

Related terms: Storage runtime, Resolved storage object, Overlay path adapter,
Passthrough path adapter.

## FUSE cache kernel

The deep module inside `fs-fuse` that owns kernel-facing cache safety. It owns
path/inode mappings, lookup references, retained open-handle paths,
invalidation snapshots, invalidation application, full-resync recovery,
poison state, and mutation coverage proof.

Important invariants:
- It is independent of FabricFs product wiring and concrete transport adapters.
- It is the locality point for cache poisoning, sequence gaps, full resync, and
  retained handle-path behavior.
- FUSE callback code consumes snapshots and decisions from this module instead
  of reimplementing cache policy inline.

Related terms: Operation specification, Transport policy.

## Transport policy

The deep module inside `fabricfs-transport` that owns NATS-facing policy:
publish retry classification, deadline projection, own-invalidation replay,
authenticated replay TTL, response deliverability, abandoned-handle cleanup,
and namespace command ordering through invalidation and reply publication.

Important invariants:
- It does not own filesystem storage semantics.
- It does not own storage-watch full-resync debt or request admission around
  storage-watch events; those remain in `fabricfs-server`.
- NATS client/server code remains an adapter for publish, subscribe, message
  decode, and reply mechanics.

Related terms: Operation specification, FUSE cache kernel.

## Operation specification

The neutral operation-facts module in `fs-protocol`. It names each filesystem
operation once and describes protocol-level facts such as wire value, subject
token, payload and response shape, path roles, broad mutation/effect category,
and neutral response limits.

Important invariants:
- It does not depend on FUSE, NATS, or FabricFs server storage.
- It does not own concrete FUSE cache coverage policy or NATS retry policy.
- `fs-core`, `fs-fuse`, and `fabricfs-transport` may define thin policy modules
  that consume the operation specification, but they must not maintain
  independent operation inventories.

Related terms: FUSE cache kernel, Transport policy.

## Session durability

The deep module behind SessionStore that owns durable SessionControl state
transitions. It owns recovery journals, live-boundary persistence, prepared
atomic writes, password durability, delete quarantine, restart replay, and
idempotent recovery.

Important invariants:
- SessionStore exposes SessionControl-facing behavior; disk recovery mechanics
  stay behind Session durability.
- A write that has not crossed the live boundary must be discardable on
  restart without reanimating partial state.
- A write that has crossed the live boundary must either settle durably or
  recover deterministically on restart.
- Delete quarantine must prevent partially deleted or migrated legacy session
  directories from becoming visible again.

Related terms: Storage watch module.

## Storage watch module

The deep module in `fabricfs-server` that owns storage-watch request admission,
full-resync debt, self-notify suppression, watcher event classification, and
full-resync publication retry.

Important invariants:
- It keeps storage-watch full-resync debt and request admission in
  `fabricfs-server`, not in `fabricfs-transport`.
- Ambiguous external storage changes fail closed by owing a full-resync
  invalidation before later requests can pass the storage execution lane.
- Internal metadata writes may suppress only matching self-notify events whose
  expected path state still matches the completed write.
- Publication failure preserves full-resync debt until delivery succeeds.

Related terms: Overlay path adapter, Transport policy.

## Server storage primitives

The storage-kernel modules inside `fabricfs-server` that own low-level
filesystem facts shared by path adapters and the storage runtime, such as path
confinement, permission policy, byte IO helpers, lock and flock tables, and
advanced IO math.

Important invariants:
- They do not form a new broad POSIX operation interface.
- They remain server-domain implementation modules, not protobuf, FUSE, or
  NATS policy modules.
- They support the Server storage capability seam and Storage runtime instead
  of replacing those seams.

Related terms: Server storage capability seam, Storage runtime, Overlay path
adapter, Passthrough path adapter.

## Protocol envelope contract

The protocol modules in `fs-protocol` that own request and response envelope
identity, payload correlation, path DTO validation, errno mapping,
encoding/decoding helpers, and invalidation contract checks.

Important invariants:
- It remains neutral protocol behavior and does not own concrete FUSE cache
  coverage policy or NATS retry/delivery policy.
- Operation facts remain centralized in the Operation specification.
- `fs-protocol/src/lib.rs` should be an intentional export surface, not the
  locality point for every protocol invariant.

Related terms: Operation specification, FUSE cache kernel, Transport policy.

## Product FUSE reply presenter

The module in `fabricfs-fuse` that owns kernel reply presentation for the
product mount. It maps reusable `fs-fuse` outputs into fuser replies and owns
callback argument rejection, `FileAttr` and `StatFs` conversion, xattr reply
shaping, readdir reply shaping, lock reply mapping, copy-file-range reply
sizing, and errno presentation.

Important invariants:
- It does not own cache safety; cache safety remains in the FUSE cache kernel.
- It does not own server storage semantics or protocol DTO definitions.
- Product FUSE callbacks remain adapters around `fs-fuse` and the reply
  presenter.

Related terms: FUSE cache kernel, Operation specification.

## Passthrough path adapter

The adapter that resolves FabricFs virtual paths directly under one host root. It
produces resolved storage objects for the storage runtime.

Important invariants:
- It owns root confinement and path normalization for passthrough storage.
- It does not own generic handle, lock, or byte IO behavior.

Related terms: Resolved storage object, Storage runtime.
