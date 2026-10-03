#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

cargo +1.89.0 check -p aktor --no-default-features --features macros,wasm_browser_workers --target wasm32-unknown-unknown
cargo clippy -p aktor --no-default-features --features macros,wasm_browser_workers --target wasm32-unknown-unknown -- -D warnings
cargo +1.89.0 check --manifest-path integrations/Cargo.toml -p aktor-worker-proof --target wasm32-unknown-unknown
cargo clippy --manifest-path integrations/Cargo.toml -p aktor-worker-proof --target wasm32-unknown-unknown -- -D warnings
bash integrations/worker/build.sh
python3 -m http.server 8766 --bind 127.0.0.1 --directory integrations/worker/web > /tmp/aktor-browser-http.log 2>&1 &
server=$!
trap 'kill "$server" 2>/dev/null || true' EXIT
export AKTOR_PROOF_URL=http://127.0.0.1:8766
node integrations/worker/web/check.cjs

role_output=$(mktemp)
trap 'rm -f "$role_output"; kill "$server" 2>/dev/null || true' EXIT
if cargo check --manifest-path integrations/Cargo.toml -p aktor-worker-proof --target wasm32-unknown-unknown --features wrong-role > "$role_output" 2>&1; then
    cat "$role_output"
    exit 1
fi
rg -q 'trait bound.*Worker<u32, Session>.*Database' "$role_output"
