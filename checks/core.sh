#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

cargo fmt --all --check

bash checks/macros.sh 1.89.0
bash checks/macros.sh

cargo +1.89.0 check --workspace --all-features --all-targets
cargo +1.89.0 check -p aktor --no-default-features --lib
cargo +1.89.0 check -p aktor --no-default-features --features macros,local --lib
cargo +1.89.0 check -p aktor --no-default-features --features macros,embassy_cross_core --lib
cargo +1.89.0 check -p aktor --no-default-features --features macros,std_thread --lib
cargo +1.89.0 check -p aktor --no-default-features --features macros,bevy --lib
cargo +1.89.0 check -p aktor --no-default-features --features macros,browser_local --lib
cargo +1.89.0 check -p aktor --no-default-features --features wasm_browser_workers --lib

cargo +1.89.0 test --workspace --all-features --all-targets
cargo +1.89.0 test -p aktor --no-default-features
cargo +1.89.0 test -p aktor --no-default-features --features local,std --lib
cargo +1.89.0 test -p aktor --no-default-features --features std,embassy_cross_core --lib
cargo +1.89.0 test -p aktor --no-default-features --features tokio --all-targets
cargo test --workspace --all-features --all-targets
cargo test -p aktor --no-default-features
cargo test -p aktor --no-default-features --features local,std --lib
cargo test -p aktor --no-default-features --features std,embassy_cross_core --lib
cargo test -p aktor --no-default-features --features tokio --all-targets

cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo clippy -p aktor --lib --no-default-features -- -D warnings
cargo clippy -p aktor --lib --no-default-features --features local -- -D warnings
cargo clippy -p aktor --lib --no-default-features --features local,std -- -D warnings
cargo clippy -p aktor --lib --no-default-features --features embassy_cross_core -- -D warnings
cargo clippy -p aktor --lib --no-default-features --features std_thread -- -D warnings
cargo clippy -p aktor --lib --no-default-features --features bevy -- -D warnings
cargo clippy -p aktor --lib --no-default-features --features browser_local -- -D warnings
cargo clippy -p aktor --lib --no-default-features --features wasm_browser_workers -- -D warnings
cargo clippy -p aktor --no-default-features --features tokio --all-targets -- -D warnings

RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
RUSTDOCFLAGS="-D warnings" cargo doc -p aktor --no-default-features --no-deps
