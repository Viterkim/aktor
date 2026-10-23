#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"

profile=${1:-release}
target_dir=${CARGO_TARGET_DIR:-../target/threads}
flags=()
case "$profile" in
    debug) ;;
    release) flags+=(--release) ;;
    *) printf 'Unknown thread build profile: %s\n' "$profile" >&2; exit 1 ;;
esac

RUSTFLAGS='-C target-feature=+atomics,+bulk-memory,+mutable-globals -C link-arg=--shared-memory -C link-arg=--import-memory -C link-arg=--max-memory=1073741824 -C link-arg=--export=__wasm_init_tls -C link-arg=--export=__tls_size -C link-arg=--export=__tls_align -C link-arg=--export=__tls_base' \
    cargo +nightly-2026-05-18 build -Zbuild-std=std,panic_abort \
    --manifest-path ../Cargo.toml -p aktor-thread-proof --target wasm32-unknown-unknown \
    --target-dir "$target_dir" "${flags[@]}" "${@:2}"

wasm-bindgen "$target_dir/wasm32-unknown-unknown/$profile/aktor_thread_proof.wasm" \
    --target web --out-dir web/pkg
