#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

profile=${1:-debug}
build_flags=()
case "$profile" in
    debug) ;;
    release) build_flags+=(--release) ;;
    *) printf 'Unknown worker build profile: %s\n' "$profile" >&2; exit 1 ;;
esac

cargo build --manifest-path ../Cargo.toml -p aktor-worker-proof --target wasm32-unknown-unknown "${build_flags[@]}"

wasm-bindgen "../target/wasm32-unknown-unknown/$profile/aktor_worker_proof.wasm" --target web --out-dir web/pkg
