# Session protocol codegen and tooling

The `fabricfs-session-protocol` crate owns the SessionControl protobuf contract used by
`fabricfsctl` and the server. Code generation is handled via `prost-build` in
`fabricfs-session-protocol/build.rs`.

- Source schema: `session.proto` at the workspace root.
- Generated Rust: emitted to `OUT_DIR` at build time and exposed as
  `fabricfs_session_protocol::session_proto::*`.
- Subject helpers and codec utilities live in `fabricfs_session_protocol::session`.

Workflows:
- Regenerate bindings: `cargo build -p fabricfs-session-protocol` (build.rs runs `prost-build`).
- Lint/format: `cargo fmt` and `cargo clippy --all-targets --all-features`.
- Tests: `cargo test -p fabricfs-session-protocol`.

When updating `session.proto`, commit the schema alongside any code that relies
on the new fields. The generated code is not checked in; builds perform codegen
automatically.
