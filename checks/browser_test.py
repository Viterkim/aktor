from concurrent.futures import ThreadPoolExecutor
import errno
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

import browser

PROBE = """
import json, os, sys, urllib.request
base = os.environ['AKTOR_PROOF_URL']
token = os.environ['AKTOR_PROOF_TOKEN']
assert not base.endswith(':8766')
with urllib.request.urlopen(base + '/__aktor_check', timeout=5) as response:
    assert response.read().decode() == token
with urllib.request.urlopen(base, timeout=5) as response:
    assert b'aktor worker proof' in response.read()
if len(sys.argv) > 1:
    with open(sys.argv[1], 'w') as output:
        json.dump([base, token], output)
"""


class BrowserServer(unittest.TestCase):
    def tearDown(self):
        self.assertFalse(any('serve_forever' in thread.name for thread in threading.enumerate()))

    def test_occupied_port(self):
        with socket.socket() as occupied:
            try:
                occupied.bind(('127.0.0.1', 8766))
                occupied.listen()
            except OSError as error:
                if error.errno != errno.EADDRINUSE:
                    raise
            browser.run([sys.executable, '-c', PROBE])

    def test_concurrent(self):
        with tempfile.TemporaryDirectory() as folder, ThreadPoolExecutor(2) as pool:
            paths = [Path(folder) / str(index) for index in range(2)]
            jobs = [pool.submit(browser.run, [sys.executable, '-c', PROBE, str(path)]) for path in paths]
            for job in jobs:
                job.result(timeout=10)
            first, second = [json.loads(path.read_text()) for path in paths]
            self.assertNotEqual(first[0], second[0])
            self.assertNotEqual(first[1], second[1])

    def test_readiness(self):
        for failure in ['connection', 'identity']:
            with self.subTest(failure=failure), patch.object(browser.urllib.request, 'urlopen') as request:
                if failure == 'connection':
                    request.side_effect = OSError('server failed')
                else:
                    request.return_value.__enter__.return_value.read.return_value = b'wrong server'
                with patch.object(browser.subprocess, 'run') as child:
                    with self.assertRaises((OSError, RuntimeError)):
                        browser.run([sys.executable, '-c', PROBE])
                    child.assert_not_called()

    def test_child_failure(self):
        with self.assertRaises(subprocess.CalledProcessError) as error:
            browser.run([sys.executable, '-c', 'raise SystemExit(17)'])
        self.assertEqual(error.exception.returncode, 17)


if __name__ == '__main__':
    unittest.main()
