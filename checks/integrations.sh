#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

cargo +1.89.0 test --manifest-path integrations/Cargo.toml
cargo test --manifest-path integrations/Cargo.toml
