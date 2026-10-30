#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

extras_dir=${AKTOR_EXTRAS:-"$PWD/../aktor-extras"}

cargo test --locked --manifest-path "$extras_dir/checks/Cargo.toml" --lib --test harness

cargo +1.95.0 check -p aktor --no-default-features --features macros,transport --target wasm32-unknown-unknown
cargo clippy -p aktor --no-default-features --features macros,transport --target wasm32-unknown-unknown -- -D warnings

cargo +1.95.0 check -p aktor --no-default-features --features macros,bevy --target wasm32-unknown-unknown
cargo clippy -p aktor --no-default-features --features macros,bevy --target wasm32-unknown-unknown -- -D warnings

cargo +1.95.0 check -p aktor --no-default-features --features macros,browser_local --target wasm32-unknown-unknown
cargo clippy -p aktor --no-default-features --features macros,browser_local --target wasm32-unknown-unknown -- -D warnings
cargo +1.95.0 check -p aktor --no-default-features --features std_thread,local --target wasm32-unknown-unknown --lib
cargo clippy -p aktor --no-default-features --features macros,browser_local,std_thread,bevy --target wasm32-unknown-unknown -- -D warnings
cargo +1.95.0 check -p aktor --no-default-features --features macros,wasm_browser_workers --target wasm32-unknown-unknown
cargo clippy -p aktor --no-default-features --features macros,wasm_browser_workers --target wasm32-unknown-unknown -- -D warnings
cargo +1.95.0 check --manifest-path integrations/Cargo.toml -p aktor-worker-proof --target wasm32-unknown-unknown
cargo clippy --manifest-path integrations/Cargo.toml -p aktor-worker-proof --target wasm32-unknown-unknown -- -D warnings
for profile in debug release; do
    bash integrations/worker/build.sh "$profile"
    cargo run --quiet --locked --manifest-path "$extras_dir/checks/Cargo.toml" \
        --bin browser -- "$PWD"
done

role_output=$(mktemp)
trap 'rm -f "$role_output"' EXIT
if cargo check --manifest-path integrations/Cargo.toml -p aktor-worker-proof \
    --target wasm32-unknown-unknown --features wrong-role > "$role_output" 2>&1; then
    cat "$role_output"
    exit 1
fi
rg -q 'trait bound.*Worker<u32, Session>.*Database' "$role_output"

for profile in debug release; do
    bash integrations/threads/build.sh "$profile"
    cargo run --quiet --locked --manifest-path "$extras_dir/checks/Cargo.toml" \
        --bin threads -- "$PWD"
done

if bash integrations/threads/build.sh release --features non-send > "$role_output" 2>&1; then
    cat "$role_output"
    exit 1
fi
rg -q 'Rc<u32>.*cannot be sent between threads safely' "$role_output"
