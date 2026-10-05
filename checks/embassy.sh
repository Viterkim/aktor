#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

cargo +1.89.0 test --manifest-path integrations/Cargo.toml -p aktor-embassy-proof
cargo test --manifest-path integrations/Cargo.toml -p aktor-embassy-proof
cargo clippy --manifest-path integrations/Cargo.toml -p aktor-embassy-proof --all-targets -- -D warnings

for target in thumbv6m-none-eabi thumbv7em-none-eabihf; do
    cargo +1.89.0 check -p aktor --target "$target"
    cargo check -p aktor --target "$target"
    cargo +1.89.0 check -p aktor --no-default-features --features macros,local --target "$target"
    cargo +1.89.0 check --manifest-path integrations/Cargo.toml -p aktor-embassy-proof --target "$target"
    cargo check --manifest-path integrations/Cargo.toml -p aktor-embassy-proof --target "$target"
done
