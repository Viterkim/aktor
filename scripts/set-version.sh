#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

if [[ $# != 1 || ! $1 =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
    printf 'Usage: %s 0.0.1\n' "$0" >&2
    exit 1
fi

version=$1
sed -i -E "s/^version = \"[^\"]+\"$/version = \"$version\"/" Cargo.toml
sed -i -E "s/^(aktor-macros = \{ version = \"=)[^\"]+/\1$version/" aktor/Cargo.toml

shopt -s globstar nullglob
for doc in README.md aktor-macros/README.md aktor/docs/**/*.md; do
    sed -i -E \
        -e "s/^((aktor|aktor-macros) = \")[^\"]+/\1$version/" \
        -e "s/^((aktor|aktor-macros) = \{ version = \")[^\"]+/\1$version/" \
        "$doc"
done

cargo update --offline -p aktor -p aktor-macros
cargo update --offline --manifest-path integrations/Cargo.toml -p aktor -p aktor-macros

printf 'aktor + aktor-macros are now %s\n' "$version"
