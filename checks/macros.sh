#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

artifact=$(mktemp)
trap 'rm -f "$artifact"' EXIT
cargo ${1:+"+$1"} build -p aktor --features tokio --message-format=json > "$artifact"

python3 - "$artifact" "${1:-}" <<'PY'
import json
import pathlib
import subprocess
import sys
import tempfile

messages = (json.loads(line) for line in pathlib.Path(sys.argv[1]).read_text().splitlines())
artifact = next(
    filename
    for message in messages
    if message.get('reason') == 'compiler-artifact' and message['target']['name'] == 'aktor'
    for filename in message['filenames']
    if filename.endswith('.rlib')
)
compiler = ['rustc'] if not sys.argv[2] else ['rustup', 'run', sys.argv[2], 'rustc']

with tempfile.TemporaryDirectory(prefix='aktor-macros-') as directory:
    for fixture in sorted(pathlib.Path('aktor/tests/ui').glob('*.rs')):
        result = subprocess.run(
            compiler + ['--edition=2024', '--crate-type=lib', '--extern', f'aktor={artifact}',
                        '-L', f'dependency={pathlib.Path(artifact).parent / "deps"}', '--out-dir', directory, str(fixture)],
            capture_output=True, text=True, timeout=30,
        )
        expected = fixture.with_suffix('.stderr').read_text().strip()
        if result.returncode == 0 or expected not in result.stderr:
            raise SystemExit(f'{fixture}: expected {expected}\n{result.stderr}')
        print(f'{fixture.name}: rejected as expected')
PY
