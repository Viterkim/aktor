#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

extras_dir=${AKTOR_EXTRAS:-"$PWD/../aktor-extras"}
artifact=$(mktemp)
trap 'rm -f "$artifact"' EXIT
host=$(rustc ${1:+"+$1"} -vV | sed -n 's/^host: //p')

cargo ${1:+"+$1"} build -p aktor --features tokio,embassy_cross_core,wasm_browser_workers \
    --target "$host" --message-format=json > "$artifact"

cargo run --quiet --locked --manifest-path "$extras_dir/checks/Cargo.toml" \
    --bin macros -- "$artifact" "${1:-}"
