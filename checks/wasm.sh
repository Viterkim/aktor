#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

extras_dir=${AKTOR_EXTRAS:-"$PWD/../aktor-extras"}
current_toolchain=$(rustup show active-toolchain | cut -d ' ' -f 1)

for toolchain in 1.89.0 "$current_toolchain"; do
    cargo +"$toolchain" test --manifest-path integrations/Cargo.toml -p aktor-wasm-proof
    cargo +"$toolchain" test --manifest-path integrations/Cargo.toml -p aktor-wasm-proof --release

    for clock in clock-1m clock-32k; do
        cargo +"$toolchain" build --manifest-path integrations/Cargo.toml -p aktor-wasm-proof \
            --target wasm32-unknown-unknown --release --no-default-features --features "$clock"

        wasm_module=integrations/target/wasm32-unknown-unknown/release/aktor_wasm_proof.wasm

        cargo run --quiet --locked --manifest-path "$extras_dir/checks/Cargo.toml" \
            --bin wasm -- "$wasm_module"
    done
done

cargo clippy --manifest-path integrations/Cargo.toml -p aktor-wasm-proof --all-targets -- -D warnings
