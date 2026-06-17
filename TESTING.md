# Testing Guide

FabricFs uses layered testing: fast pure logic tests, workspace integration tests,
coverage gates, and NATS-backed smoke checks.

## Standard Checks

```bash
just check
```

This runs:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace`

## Coverage

```bash
just ci
```

`just ci` runs the CI coverage gate with `cargo llvm-cov` and the configured
line threshold from `justfile`. The measured surface includes migrated runtime
libraries and service adapters: `fs-protocol`, `fs-core`, `fs-fuse`,
`fs-transport-local`, `fabricfs-transport`, `fabricfs-server` library/service
code, `fabricfs-fuse` product reply wiring, and retained `fabricfs-session-protocol`
SessionControl helpers. Product process entrypoints and session-control
process utilities are excluded from line coverage and covered by command-path
tests.

Useful coverage commands:

- `just coverage-summary`: print the gated coverage summary.
- `just coverage`: generate HTML coverage under `target/llvm-cov/`.
- `just coverage-open`: generate and open HTML coverage.
- `just coverage-lcov`: write `coverage.lcov`.
- `just coverage-json`: write `coverage.json`.
- `just coverage-full-summary`: print the unfiltered workspace snapshot.
- `just coverage-clean`: remove coverage artifacts.

The unfiltered summary is intentionally separate for tracking code outside the
gate. Product runtime adapters and storage engines are still validated by
targeted cargo tests and live NATS/FUSE smoke because their important behavior
depends on broker and mount command paths.

## Targeted Tests

Use targeted cargo commands while working on a narrow surface:

```bash
cargo test -p fs-protocol -p fs-core -p fs-fuse -p fs-transport-local
cargo test -p fabricfs-session-protocol
cargo test -p fabricfs-transport
cargo test -p fabricfs-server --test ops
cargo test -p fabricfs-server --test service_adapter
cargo test -p fabricfs-fuse -p fs-fuse
cargo test -p fs-protocol -p fs-core -p fs-transport-local -p fs-fuse -p fabricfs-transport
```

## NATS And FUSE Smoke Tests

These require a reachable NATS server and local mount permissions:

```bash
./smoke.sh
./smoke-sessions.sh
```

The smoke scripts are intentionally end-to-end and should not replace unit
tests for protocol encoding, path-cache behavior, errno mapping, session
storage, or overlay semantics.

`run-server.sh` and `run-fuse.sh` require `FABRICFS_TRANSPORT_AUTH_TOKEN` for the
filesystem data plane. `smoke.sh` auto-generates a throwaway token when one is
not already set so local command-path checks keep the real transport trust
boundary enabled. The mounted filesystem smoke now probes links, chmod/truncate
setattr behavior, fsync/fdatasync, POSIX byte-range locks, `copy_file_range`,
`posix_fallocate`, xattrs, and `statvfs` on the repaired common-stack surface.
Mounted `lseek`, `flush`, `fsyncdir`, and `getlk` remain covered by focused
unit and product-wiring tests rather than the live smoke harness.

## Validation Discipline

- Prefer deterministic tests with explicit timeouts for async or concurrent
  behavior.
- Avoid sleeps in tests when a synchronization primitive or observable event is
  available.
- Record command output through `just check-logged <label>`
  wrappers when validation evidence needs to survive across runs.
- After a failed or interrupted validation, inspect for stale same-checkout
  `just`, `cargo`, `cargo llvm-cov`, wrapper, or broker processes before
  rerunning so lock contention or overlapping test harnesses do not contaminate
  the next result.
- Do not close release-track tasks while `just check` or relevant smoke checks
  are failing.
