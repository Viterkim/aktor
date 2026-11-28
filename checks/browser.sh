#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

extras_dir=${AKTOR_EXTRAS:-"$PWD/../aktor-extras"}

cargo test --locked --manifest-path "$extras_dir/checks/Cargo.toml" --lib --test harness

cargo +1.95.0 check -p aktor --no-default-features --features macros,transport --target wasm32-unknown-unknown
cargo +1.95.0 check -p aktor --no-default-features --features tokio,wasm_browser_workers --target wasm32-unknown-unknown
cargo clippy -p aktor --no-default-features --features macros,transport --target wasm32-unknown-unknown -- -D warnings

cargo +1.95.0 check -p aktor --no-default-features --features macros,bevy --target wasm32-unknown-unknown
cargo clippy -p aktor --no-default-features --features macros,bevy --target wasm32-unknown-unknown -- -D warnings

cargo +1.95.0 check -p aktor --no-default-features --features macros,browser_local --target wasm32-unknown-unknown
cargo clippy -p aktor --no-default-features --features macros,browser_local --target wasm32-unknown-unknown -- -D warnings
cargo +1.95.0 check -p aktor --no-default-features --features std_thread,local --target wasm32-unknown-unknown --lib
cargo clippy -p aktor --no-default-features --features macros,browser_local,std_thread,bevy --target wasm32-unknown-unknown -- -D warnings
cargo +1.95.0 check -p aktor --no-default-features --features macros,wasm_browser_workers --target wasm32-unknown-unknown
cargo clippy -p aktor --no-default-features --features macros,wasm_browser_workers --target wasm32-unknown-unknown -- -D warnings
cargo +1.95.0 check -p aktor --no-default-features --features macros,wasm_browser_worker_server --target wasm32-unknown-unknown
cargo clippy -p aktor --no-default-features --features macros,wasm_browser_worker_server --target wasm32-unknown-unknown -- -D warnings
cargo +1.95.0 check --manifest-path integrations/Cargo.toml -p renamed --target wasm32-unknown-unknown
cargo +1.95.0 check --manifest-path integrations/Cargo.toml -p aktor-worker-proof --target wasm32-unknown-unknown
cargo clippy --manifest-path integrations/Cargo.toml -p aktor-worker-proof --target wasm32-unknown-unknown -- -D warnings
for profile in debug release; do
    bash integrations/worker/build.sh "$profile"
    browser_root=$PWD
    if [[ $profile == debug ]]; then
        counter_dir=${CARGO_TARGET_DIR:-"$PWD/integrations/target"}/counter
        RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }--cfg aktor_serve=\"Counter\"" \
            cargo build --manifest-path integrations/Cargo.toml -p aktor-worker-proof \
            --target wasm32-unknown-unknown --target-dir "$counter_dir"

        browser_root="$counter_dir/browser"
        web="$browser_root/integrations/worker/web"
        mkdir -p "$web/all"
        cp -R integrations/worker/web/pkg/. "$web/all/"
        wasm-bindgen "$counter_dir/wasm32-unknown-unknown/debug/aktor_worker_proof.wasm" \
            --target web --out-dir "$web/pkg"

        # The selected module serves Counter and uses the SQLite signatures to call the other worker.
        for entry in integrations/worker/web/*.js; do
            sed 's|\./pkg/|./all/|g' "$entry" > "$web/${entry##*/}"
        done
        cp integrations/worker/web/{setup_worker.js,index.html} "$web/"

        rg -aFq 'INSERT INTO preferences' "$web/all/aktor_worker_proof_bg.wasm"
        if rg -aFq 'INSERT INTO preferences' "$web/pkg/aktor_worker_proof_bg.wasm"; then
            echo "The Counter worker linked the SQLite write body" >&2
            exit 1
        fi
    fi

    cargo run --quiet --locked --manifest-path "$extras_dir/checks/Cargo.toml" \
        --bin browser -- "$browser_root"
done

role_output=$(mktemp)
trap 'rm -f "$role_output"' EXIT
if cargo check --manifest-path integrations/Cargo.toml -p aktor-worker-proof \
    --target wasm32-unknown-unknown --features wrong-role > "$role_output" 2>&1; then
    cat "$role_output"
    exit 1
fi
rg -q 'Session.*Database|Database.*Session' "$role_output"

for profile in debug release; do
    bash integrations/threads/build.sh "$profile"
    cargo run --quiet --locked --manifest-path "$extras_dir/checks/Cargo.toml" \
        --bin threads -- "$PWD"
done

if bash integrations/threads/build.sh release --features non-send > "$role_output" 2>&1; then
    cat "$role_output"
    exit 1
fi
rg -Fq "An actor on another thread needs Send + 'static arguments and results." "$role_output"
