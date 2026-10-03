#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

cargo fmt --all --check

cargo +1.89.0 check --workspace --all-features --all-targets
cargo +1.89.0 check -p aktor --no-default-features --lib
cargo +1.89.0 check -p aktor --no-default-features --features wasm_browser_workers --lib

cargo +1.89.0 test --workspace --all-features --all-targets
cargo +1.89.0 test -p aktor --no-default-features
cargo +1.89.0 test -p aktor --no-default-features --features tokio --all-targets
cargo test --workspace --all-features --all-targets
cargo test -p aktor --no-default-features
cargo test -p aktor --no-default-features --features tokio --all-targets

cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo clippy -p aktor --lib --no-default-features -- -D warnings
cargo clippy -p aktor --lib --no-default-features --features wasm_browser_workers -- -D warnings
cargo clippy -p aktor --no-default-features --features tokio --all-targets -- -D warnings

RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
RUSTDOCFLAGS="-D warnings" cargo doc -p aktor --no-default-features --no-deps
