#!/usr/bin/env bash
set -euo pipefail
worker_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

profile=${1:-debug}
target_dir=${CARGO_TARGET_DIR:-"$worker_dir/../target"}
build_flags=()
case "$profile" in
    debug) ;;
    release) build_flags+=(--release) ;;
    *) printf 'Unknown worker build profile: %s\n' "$profile" >&2; exit 1 ;;
esac

cargo build --manifest-path "$worker_dir/Cargo.toml" -p aktor-worker-proof --target wasm32-unknown-unknown \
    --target-dir "$target_dir" "${build_flags[@]}"

wasm-bindgen "$target_dir/wasm32-unknown-unknown/$profile/aktor_worker_proof.wasm" --target web --out-dir "$worker_dir/web/pkg"
