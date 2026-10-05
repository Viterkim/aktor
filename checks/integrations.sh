#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

cargo +1.89.0 test --manifest-path integrations/Cargo.toml
cargo test --manifest-path integrations/Cargo.toml

for backend in standard bevy; do
    feature=bevy
    if [[ $backend == standard ]]; then
        feature=std_thread
    fi
    cargo +1.89.0 run --manifest-path integrations/Cargo.toml -p aktor-native-proof \
        --no-default-features --features "$feature" --bin "$backend"
    cargo run --manifest-path integrations/Cargo.toml -p aktor-native-proof \
        --no-default-features --features "$feature" --bin "$backend"

    tree=$(cargo tree --manifest-path integrations/Cargo.toml -p aktor-native-proof \
        --no-default-features --features "$feature" -e normal,build,features)
    if rg -q 'tokio feature "(rt|rt-multi-thread|time|net)"' <<< "$tree"; then
        printf '%s\n' "$tree"
        exit 1
    fi
done
