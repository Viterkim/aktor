#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

artifact=$(mktemp)
trap 'rm -f "$artifact"' EXIT
host=$(rustc ${1:+"+$1"} -vV | sed -n 's/^host: //p')
cargo ${1:+"+$1"} build -p aktor --features tokio,embassy_cross_core --target "$host" --message-format=json > "$artifact"

python3 - "$artifact" "${1:-}" <<'PY'
import json
import pathlib
import subprocess
import sys
import tempfile

messages = [json.loads(line) for line in pathlib.Path(sys.argv[1]).read_text().splitlines()]
artifact = next(
    filename
    for message in messages
    if message.get('reason') == 'compiler-artifact' and message['target']['name'] == 'aktor'
    for filename in message['filenames']
    if filename.endswith('.rlib')
)
compiler = ['rustc'] if not sys.argv[2] else ['rustup', 'run', sys.argv[2], 'rustc']
dependencies = sorted(
    {
        str(pathlib.Path(filename).parent)
        for message in messages
        if message.get('reason') == 'compiler-artifact'
        for filename in message['filenames']
    }
)
search = [argument for path in dependencies for argument in ['-L', f'dependency={path}']]

with tempfile.TemporaryDirectory(prefix='aktor-macros-') as directory:

    def filename(name, kind, source):
        return subprocess.check_output(
            compiler
            + ['--crate-name', name, '--crate-type', kind, '--print', 'file-names', source],
            text=True,
        ).strip()

    probe_source = 'aktor/tests/attributes/probe.rs'
    composition_source = 'aktor/tests/attributes/calls.rs'
    probe = pathlib.Path(directory) / filename('probe', 'proc-macro', probe_source)

    subprocess.run(
        compiler
        + [
            '--edition=2024',
            '--crate-type=proc-macro',
            '--crate-name=probe',
            probe_source,
            '-o',
            str(probe),
        ],
        check=True,
        timeout=30,
    )

    composition = pathlib.Path(directory) / filename('composition', 'bin', composition_source)

    subprocess.run(
        compiler
        + [
            '--edition=2024',
            '--test',
            '--extern',
            f'aktor={artifact}',
            '--extern',
            f'probe={probe}',
            *search,
            composition_source,
            '-o',
            str(composition),
        ],
        check=True,
        timeout=30,
    )
    subprocess.run([str(composition)], check=True, timeout=30)

    for fixture in sorted(pathlib.Path('aktor/tests/ui').glob('*.rs')):
        result = subprocess.run(
            compiler
            + [
                '--edition=2024',
                '--crate-type=lib',
                '--extern',
                f'aktor={artifact}',
                *search,
                '--out-dir',
                directory,
                str(fixture),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        expected = fixture.with_suffix('.stderr').read_text().strip()

        if result.returncode == 0 or expected not in result.stderr:
            raise SystemExit(f'{fixture}: expected {expected}\n{result.stderr}')

        print(f'{fixture.name}: rejected as expected')
PY
