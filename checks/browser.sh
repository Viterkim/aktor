#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

extras_dir=${AKTOR_EXTRAS:-"$PWD/../aktor-extras"}

cargo test --locked --manifest-path "$extras_dir/checks/Cargo.toml" --lib --test harness

cargo +1.89.0 check -p aktor --no-default-features --features macros,bevy --target wasm32-unknown-unknown
cargo clippy -p aktor --no-default-features --features macros,bevy --target wasm32-unknown-unknown -- -D warnings

cargo +1.89.0 check -p aktor --no-default-features --features macros,browser_local --target wasm32-unknown-unknown
cargo clippy -p aktor --no-default-features --features macros,browser_local --target wasm32-unknown-unknown -- -D warnings
cargo clippy -p aktor --no-default-features --features macros,browser_local,std_thread,bevy --target wasm32-unknown-unknown -- -D warnings
cargo +1.89.0 check -p aktor --no-default-features --features macros,wasm_browser_workers --target wasm32-unknown-unknown
cargo clippy -p aktor --no-default-features --features macros,wasm_browser_workers --target wasm32-unknown-unknown -- -D warnings
cargo +1.89.0 check --manifest-path integrations/Cargo.toml -p aktor-worker-proof --target wasm32-unknown-unknown
cargo clippy --manifest-path integrations/Cargo.toml -p aktor-worker-proof --target wasm32-unknown-unknown -- -D warnings
bash integrations/worker/build.sh
cargo run --quiet --locked --manifest-path "$extras_dir/checks/Cargo.toml" \
    --bin browser -- "$PWD"

role_output=$(mktemp)
trap 'rm -f "$role_output"' EXIT
if cargo check --manifest-path integrations/Cargo.toml -p aktor-worker-proof \
    --target wasm32-unknown-unknown --features wrong-role > "$role_output" 2>&1; then
    cat "$role_output"
    exit 1
fi
rg -q 'trait bound.*Worker<u32, Session>.*Database' "$role_output"
