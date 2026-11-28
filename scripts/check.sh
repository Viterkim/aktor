#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

# rustc 1.95 hits an incremental ICE on these tests, a full build every time is the cheap fix.
export CARGO_INCREMENTAL=0

if [[ "${1:-}" == browser ]]; then
    exec bash checks/browser.sh
fi

if [[ "${1:-}" == embassy ]]; then
    exec bash checks/embassy.sh
fi

if [[ "${1:-}" == wasm ]]; then
    exec bash checks/wasm.sh
fi

bash checks/core.sh
bash checks/integrations.sh

cargo run -p aktor --features tokio --example tokio_connection
cargo run -p aktor --features tokio --example sqlite

cargo +1.95.0 run -p aktor --features tokio --example guide
cargo run -p aktor --features tokio --example guide
