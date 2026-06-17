# FabricFs Command Overview

FabricFs currently ships three binaries from the workspace.

Filesystem data-plane commands require transport authentication. Set
`FABRICFS_TRANSPORT_AUTH_TOKEN` for `fabricfs-fuse`, `run-fuse.sh`, and local
server/fuse command paths; `fabricfs-server` can also receive the shared
secret through `--transport-auth-token`.

```text
fabricfs-server
├── --mount-name <NAME>              Mount namespace used in NATS subjects.
├── --nats-url <URL>                 NATS server URL.
├── --transport-auth-token <TOKEN>   Shared secret for filesystem transport auth.
├── --authenticated-request-ttl-secs <N>
│                                     Maximum future deadline for signed filesystem requests.
├── --backing-root <DIR>             Read source when COW has no entry.
├── --alias-path <DIR>               Mutation gate and tombstone root.
├── --cow-path <DIR>                 Copy-on-write data, xattrs, and session metadata root.
├── --update-backingtree             Allow direct backing-tree mutation when no COW root is set.
├── --worker-threads <N>             Server worker count.
├── --max-queued <N>                 Backpressure queue depth.
├── --io-chunk-bytes <N>             Chunk size for read/write transfer.
├── --max-read-bytes <N>             Maximum read size.
├── --metrics-interval-secs <N>      Structured metrics cadence; 0 disables periodic logs.
├── --umask <OCTAL>                  Creation umask.
├── --propagate-acls                 Copy ACL xattrs during copy-up.
├── --update-permissions             Allow backing permission updates.
├── --update-xattrs                  Allow backing xattr updates.
├── --enable-reflinks <BOOL>         Use reflink copy-up when supported.
└── --preserve-sparse-files <BOOL>   Preserve holes during sparse copy-up.

fabricfs-fuse <MOUNT> <NATS_URL>
├── --mount-name <NAME>              Must match the server mount name.
├── --timeout-secs <N>               Startup probe and mounted RPC timeout.
└── --metrics-interval-secs <N>      Structured metrics cadence; 0 disables periodic logs.

fabricfs-sessiond
├── --cow-root <DIR>                 Session metadata root.
├── --nats-url <URL>                 NATS server URL.
└── --metrics-interval-secs <N>      Structured metrics cadence; 0 disables periodic logs.

fabricfsctl
├── sessions list|create|show|delete|attach
├── overlay alias-add|alias-rm|tombstone-add|tombstone-rm
├── checkpoints commit|list
└── published push|list|pull
```

Run each binary with `--help` for the exact current arguments and subcommands.
