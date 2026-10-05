#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

wasm_python=${AKTOR_WASMTIME_PYTHON:-target/wasm-host-env/bin/python}
if [[ -z ${AKTOR_WASMTIME_PYTHON:-} && -f target/wasm-host-env/Scripts/python.exe ]]; then
    wasm_python=target/wasm-host-env/Scripts/python.exe
fi

current_toolchain=$(rustup show active-toolchain | cut -d ' ' -f 1)
for toolchain in 1.89.0 "$current_toolchain"; do
    cargo +"$toolchain" test --manifest-path integrations/Cargo.toml -p aktor-wasm-proof
    cargo +"$toolchain" test --manifest-path integrations/Cargo.toml -p aktor-wasm-proof --release
    for clock in clock-1m clock-32k; do
        cargo +"$toolchain" build --manifest-path integrations/Cargo.toml -p aktor-wasm-proof \
            --target wasm32-unknown-unknown --release --no-default-features --features "$clock"

        wasm_module=integrations/target/wasm32-unknown-unknown/release/aktor_wasm_proof.wasm

        node integrations/wasm/check.cjs "$wasm_module"
        "$wasm_python" integrations/wasm/check.py "$wasm_module"
    done
done

cargo clippy --manifest-path integrations/Cargo.toml -p aktor-wasm-proof --all-targets -- -D warnings
