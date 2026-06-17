# FabricFs development tasks

# The line gate measures migrated runtime libraries, service adapters, and
# SessionControl services. Product process entrypoints are validated by
# command-path tests instead of line coverage.
coverage_exclude := '(fabricfs-fuse/src/(cli|main)\.rs|fabricfs-server/src/main\.rs|fabricfs-server/src/bin/.*)'
coverage_min_lines := '70'
test_threads := env_var_or_default("RUST_TEST_THREADS", "4")

default:
    @just --list

build:
    cargo build --workspace

build-release:
    cargo build --workspace --release

test:
    bash scripts/run_managed_command.sh env RUST_TEST_THREADS={{test_threads}} cargo test --workspace

test-one NAME:
    bash scripts/run_managed_command.sh env RUST_TEST_THREADS={{test_threads}} cargo test --workspace {{NAME}}

lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

check: fmt-check lint test

check-logged LABEL='just-check':
    bash scripts/run_logged_command.sh {{LABEL}} just check

coverage:
    bash scripts/run_managed_command.sh cargo llvm-cov --workspace --all-features --html --ignore-filename-regex '{{coverage_exclude}}'

coverage-open:
    bash scripts/run_managed_command.sh cargo llvm-cov --workspace --all-features --open --ignore-filename-regex '{{coverage_exclude}}'

coverage-summary:
    bash scripts/run_managed_command.sh cargo llvm-cov --workspace --all-features --no-report
    mkdir -p target/llvm-cov
    cargo llvm-cov report --json --summary-only --skip-functions --output-path target/llvm-cov/coverage-summary.json --ignore-filename-regex '{{coverage_exclude}}'
    python3 -c "import json, pathlib; data=json.loads(pathlib.Path('target/llvm-cov/coverage-summary.json').read_text()); totals=data['data'][0]['totals']; print('TOTAL'); print(f\"  regions : {totals['regions']['percent']:.2f}%\"); print(f\"  lines   : {totals['lines']['percent']:.2f}%\"); print(f\"  funcs   : {totals['functions']['percent']:.2f}%\")"

coverage-full-summary:
    bash scripts/run_managed_command.sh cargo llvm-cov --workspace --all-features --summary-only --skip-functions

coverage-lcov:
    bash scripts/run_managed_command.sh cargo llvm-cov --workspace --all-features --lcov --output-path coverage.lcov --ignore-filename-regex '{{coverage_exclude}}'

coverage-json:
    bash scripts/run_managed_command.sh cargo llvm-cov --workspace --all-features --json --output-path coverage.json --ignore-filename-regex '{{coverage_exclude}}'

coverage-clean:
    cargo llvm-cov --workspace clean

ci: fmt-check lint
    bash scripts/run_managed_command.sh cargo llvm-cov --workspace --all-features --fail-under-lines {{coverage_min_lines}} --ignore-filename-regex '{{coverage_exclude}}'

smoke:
    ./smoke.sh

smoke-sessions:
    ./smoke-sessions.sh

run-server *ARGS:
    cargo run -p fabricfs-server -- {{ARGS}}

run-fuse *ARGS:
    cargo run -p fabricfs-fuse -- {{ARGS}}

release-check:
    bash scripts/release-check.sh

clean:
    cargo clean
