#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

package_dir=$(mktemp -d "${TMPDIR:-/tmp}/aktor-package.XXXXXX")
trap 'rm -rf -- "$package_dir"' EXIT
version=$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml)

cargo package --workspace --no-verify --all-features --target-dir "$package_dir/target" "$@"

mkdir -p "$package_dir/aktor" "$package_dir/aktor-macros" "$package_dir/.cargo"
for package in aktor aktor-macros; do
    tar -xzf "$package_dir/target/package/$package-$version.crate" \
        --strip-components=1 -C "$package_dir/$package"
done
test -f "$package_dir/aktor/src/dispatch/call.rs"
test -f "$package_dir/aktor/src/data/decode.rs"
test -f "$package_dir/aktor-macros/src/data/generate.rs"
tar --exclude=target --exclude=pkg -cf - integrations \
    | tar -xf - -C "$package_dir"
cp rust-toolchain.toml "$package_dir/"
cat > "$package_dir/.cargo/config.toml" <<EOF
[patch.crates-io]
aktor-macros = { path = "$package_dir/aktor-macros" }
EOF

cd -- "$package_dir"
export CARGO_TARGET_DIR="$package_dir/target"

cargo +1.89.0 test --manifest-path aktor/Cargo.toml --features tokio --all-targets
RUSTDOCFLAGS="-D warnings" cargo +1.89.0 doc --manifest-path aktor/Cargo.toml --features tokio --no-deps
cargo +1.89.0 test --manifest-path aktor/Cargo.toml --no-default-features --features tokio --all-targets
cargo +1.89.0 check --manifest-path aktor/Cargo.toml --no-default-features \
    --features macros,wasm_browser_workers --target wasm32-unknown-unknown
cargo +1.89.0 test --manifest-path integrations/Cargo.toml
cargo +1.89.0 run --manifest-path integrations/Cargo.toml -p renamed
cargo +1.89.0 check --manifest-path integrations/Cargo.toml -p aktor-worker-proof \
    --target wasm32-unknown-unknown
for target in thumbv6m-none-eabi thumbv7em-none-eabihf; do
    cargo +1.89.0 check --manifest-path integrations/Cargo.toml -p aktor-embassy-proof \
        --target "$target"
done
