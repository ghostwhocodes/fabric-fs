# Development

FabricFs is a Rust workspace. Use `just` for repeatable developer commands and
plain `cargo` for targeted crate work.

## Build

- List recipes: `just`
- Build debug: `just build`
- Build release: `just build-release`
- Run server: `just run-server --help`
- Run FUSE bridge: `just run-fuse --help`

## Standard Validation

- Format: `just fmt`
- Format check: `just fmt-check`
- Lint: `just lint`
- Workspace tests: `just test`
- Full local gate: `just check`
- CI-style gate with coverage: `just ci`
- Logged gate: `just check-logged <label>`

Use targeted commands such as `cargo test -p fs-protocol` or
`cargo test -p fabricfs-server --test service_adapter` while developing narrow
changes, but close non-trivial work with `just check`.

The structural guards enforce the current crate boundaries: protocol-neutral
operation facts stay in `fs-protocol`, FUSE cache state stays in `fs-fuse`,
NATS retry/deadline/replay/delivery policy stays in `fabricfs-transport`, and
server storage behavior crosses the service boundary through `ServerStorage`
capability ports. They also guard the deep runtime modules: Session durability,
storage-watch admission/publication, server storage primitives, Overlay path
adapter internals, protocol envelope modules, and product FUSE reply
presentation.

## Local Runtime

Start NATS first. The repository includes `infra/nats/docker-compose.yml` for a
local broker.

```bash
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

`run-server.sh` and `run-fuse.sh` require the same
`FABRICFS_TRANSPORT_AUTH_TOKEN`. `./smoke.sh` auto-generates a throwaway token
when one is not set and exercises basic data-plane file operations.
`./smoke-sessions.sh` exercises session-control flows.

The product FUSE process keeps cache safety in `fs-fuse` and reply conversion
in `fabricfs-fuse::reply::ProductFuseReplyPresenter`; runtime debugging should
start from those ownership boundaries before changing callback code.

## Release Check

Run:

```bash
just release-check
```

The release check runs the workspace gate, coverage gate, and smoke checks when
`nats-server` is installed.
