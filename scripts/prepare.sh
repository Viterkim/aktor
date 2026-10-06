#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

if [[ -n "$(git status --porcelain)" ]]; then
    printf 'Commit first man\n' >&2
    exit 1
fi

bash scripts/check.sh
bash scripts/check.sh browser
bash scripts/check.sh embassy
bash scripts/check.sh wasm

# !Checks can update lockfiles! DONT export an older committed version by accident.
if [[ -n "$(git status --porcelain)" ]]; then
    printf 'Checks changed files. Review and commit them first man\n' >&2
    exit 1
fi

release_dir=$(mktemp -d "${TMPDIR:-/tmp}/aktor-release.XXXXXX")
git archive HEAD | tar -xf - -C "$release_dir"
release_url="https://github.com/Viterkim/aktor/blob/$(git rev-parse HEAD)"

# Only the published copy gets full links.
sed -i -E \
    -e "s@\]\((\./)?aktor/@](${release_url}/aktor/@g" \
    -e "s@\]\((\./)?integrations/@](${release_url}/integrations/@g" \
    "$release_dir/README.md"
sed -i -E \
    -e "s@\]\((\./)?\.\./\.\./README\.md@](${release_url}/README.md@g" \
    -e "s@\]\((\./)?examples\.md@](${release_url}/aktor/docs/examples.md@g" \
    -e "s@\]\((\./)?runtime\.md@](${release_url}/aktor/docs/runtime.md@g" \
    "$release_dir/aktor/docs/functions.md"
sed -i -E \
    -e "s@\]\((\./)?\.\./README\.md@](${release_url}/README.md@g" \
    -e "s@\]\((\./)?\.\./aktor/@](${release_url}/aktor/@g" \
    "$release_dir/aktor-macros/README.md"

cd -- "$release_dir"
bash checks/package.sh

printf '\nDry run bingo! You are in %s\n' "$release_dir"
printf 'Bash:'
printf 'cargo publish -p aktor-macros && cargo publish -p aktor\n'
printf 'Nushell:'
printf 'cargo publish -p aktor-macros; cargo publish -p aktor\n'
printf 'exit this shell when you are done.\n\n'
exec "${SHELL:-bash}" -i
