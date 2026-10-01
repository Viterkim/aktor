#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
cargo build --manifest-path ../Cargo.toml -p aktor-worker-proof --target wasm32-unknown-unknown
wasm-bindgen ../target/wasm32-unknown-unknown/debug/aktor_worker_proof.wasm --target web --out-dir web/pkg
