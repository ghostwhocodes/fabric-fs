<!--
Project note: this file is intentionally kept as a dedicated guidance surface for codex tests.
`test_common_core_layering.py` reads this file and fails if it drifts from the active
architecture contract, so treat it as synchronized with `AGENTS.md` and keep it updated
whenever module ownership or boundary rules change.
-->

# Repository Guidance for Repository Rewriters

## Project Structure & Module Organization
- Workspace crates live at the root: `fs-protocol`, `fs-core`, and `fs-fuse` own reusable filesystem protocol, dispatcher, and FUSE adapter behavior; `fabricfs-session-protocol` owns SessionControl only; `fabricfs-fuse` keeps product mount wiring in `src/main.rs`; and `fabricfs-server` keeps NATS-backed filesystem/storage and SessionControl service logic.
- Keep new code modular: shared filesystem protocol/rpc helpers belong in `fs-protocol` and `fs-core`; `fabricfs-session-protocol` stays SessionControl-only; transport or cache utilities shared by binaries should live in their own modules to avoid duplication.

## Build, Test, and Development Commands
- `cargo fmt`
- `cargo clippy --all-targets --all-features`
- `cargo test --all`
- `./run-server.sh` and `./run-fuse.sh <mount> <nats-url>`
- `./smoke.sh`

## Coding Style & Naming
- Use Rust `rustfmt` defaults and explicit enums over stringly-typed operation identifiers.
- Keep modules focused and cohesive; prefer single-level-of-abstraction functions.
- Keep files small and intentionally structured by domain.

## Testing Practices
- Add unit tests for protocol encoding/decoding and path/cache helpers.
- Keep integration tests for NATS/FUSE boundaries where feasible.
- Prefer deterministic async tests with timeouts; avoid sleeps in protocol/fuse contracts.
- Name tests by behavior, e.g. `handles_unlinked_paths`, `encodes_proto_request`.

## Protocol and Responsibility Contracts
- `fabricfs-session-protocol` owns SessionControl-only contracts.
- Filesystem data-plane protocol, operations, envelopes, errors, and mappings belong in `fs-protocol`.
- Shared cross-cutting transport helpers should be factored in core transport crates, not in SessionControl protocol.
- `fs-fuse`, `fabricfs-fuse`, and `fabricfs-server` must consume the protocol contracts without redefining filesystem protocol types.

## Key Boundary Rules
- Do not place filesystem operation DTOs, operation enums, envelope codecs, or errno mapping in `fabricfs-session-protocol`.
- Do not use `fs-protocol` for SessionControl command subjects and protobuf business wiring.
- Keep transport/IO effects at boundaries and maintain pure core logic in protocol/dispatch modules.
