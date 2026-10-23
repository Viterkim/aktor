import json
from pathlib import Path
import selectors
import signal
import subprocess
import tempfile


root = Path(__file__).resolve().parents[2]
manifest = root / 'integrations/Cargo.toml'
subprocess.run([
    'cargo', 'build', '--locked', '--manifest-path', str(manifest),
    '-p', 'aktor-remote-proof', '--bins',
], check=True)
metadata = json.loads(subprocess.check_output([
    'cargo', 'metadata', '--no-deps', '--format-version=1',
    '--manifest-path', str(manifest),
]))
binary = Path(metadata['target_directory']) / 'debug'
children = []


def start(name, socket):
    process = subprocess.Popen(
        [str(binary / name), str(socket)], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    children.append(process)
    return process


def line(process):
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        if not selector.select(10):
            raise TimeoutError('remote process did not reach its next step')

    result = process.stdout.readline().strip()
    if not result:
        raise RuntimeError(f'remote process exited early: {process.poll()}')
    return result


try:
    with tempfile.TemporaryDirectory(prefix='aktor-remote-') as directory:
        socket = Path(directory) / 'power.sock'
        original = start('server', socket)
        assert 'listening' in line(original)
        client = start('reconnect', socket)
        assert line(client) == 'ready'

        original.kill()
        original.wait(timeout=10)
        client.stdin.write('continue\n')
        client.stdin.flush()
        assert line(client) == 'updating'
        client.stdin.write('continue\n')
        client.stdin.flush()
        assert line(client) == 'updated'
        replacement = start('server', socket)
        assert 'listening' in line(replacement)
        output, _ = client.communicate(timeout=10)
        assert client.returncode == 0, output
        assert 'reconnected to replacement owner' in output

        replacement.send_signal(signal.SIGTERM)
        output, _ = replacement.communicate(timeout=10)
        assert replacement.returncode == 0, output
        assert output.count('power manager closed after 1 cycles') == 1, output
        assert not socket.exists()
        print('Remote: retained handle and latest sender, replacement owner and SIGTERM cleanup passed.')
finally:
    for process in children:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)
