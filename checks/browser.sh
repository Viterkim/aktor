#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s checks -p browser_test.py

cargo +1.89.0 check -p aktor --no-default-features --features macros,wasm_browser_workers --target wasm32-unknown-unknown
cargo clippy -p aktor --no-default-features --features macros,wasm_browser_workers --target wasm32-unknown-unknown -- -D warnings
cargo +1.89.0 check --manifest-path integrations/Cargo.toml -p aktor-worker-proof --target wasm32-unknown-unknown
cargo clippy --manifest-path integrations/Cargo.toml -p aktor-worker-proof --target wasm32-unknown-unknown -- -D warnings
bash integrations/worker/build.sh
python3 checks/browser.py

role_output=$(mktemp)
trap 'rm -f "$role_output"' EXIT
if cargo check --manifest-path integrations/Cargo.toml -p aktor-worker-proof --target wasm32-unknown-unknown --features wrong-role > "$role_output" 2>&1; then
    cat "$role_output"
    exit 1
fi
rg -q 'trait bound.*Worker<u32, Session>.*Database' "$role_output"
