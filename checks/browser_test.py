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
            jobs = [
                pool.submit(browser.run, [sys.executable, '-c', PROBE, str(path)]) for path in paths
            ]

            for job in jobs:
                job.result(timeout=10)

            first, second = [json.loads(path.read_text()) for path in paths]

            self.assertNotEqual(first[0], second[0])
            self.assertNotEqual(first[1], second[1])

    def test_readiness(self):
        for failure in ['connection', 'identity']:
            with self.subTest(failure=failure), patch.object(
                browser.urllib.request, 'urlopen'
            ) as request:
                if failure == 'connection':
                    request.side_effect = OSError('server failed')
                else:
                    request.return_value.__enter__.return_value.read.return_value = b'wrong server'

                with patch.object(browser.subprocess, 'Popen') as child:
                    with self.assertRaises((OSError, RuntimeError)):
                        browser.run([sys.executable, '-c', PROBE])

                    child.assert_not_called()

    def test_child_failure(self):
        with self.assertRaises(subprocess.CalledProcessError) as error:
            browser.run([sys.executable, '-c', 'raise SystemExit(17)'])

        self.assertEqual(error.exception.returncode, 17)

    def test_child_timeout(self):
        with tempfile.TemporaryDirectory() as folder:
            marker = Path(folder) / 'survived'
            descendant = """
import pathlib
import sys
import time

time.sleep(1)
pathlib.Path(sys.argv[1]).touch()
"""
            child = """
import subprocess, sys, time

subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2]])
time.sleep(60)
"""

            with self.assertRaises(subprocess.TimeoutExpired):
                browser.run([sys.executable, '-c', child, descendant, str(marker)], timeout=0.5)

            threading.Event().wait(1)
            self.assertFalse(marker.exists(), 'browser child survived harness timeout')

    def test_descendant_cleanup(self):
        for result in [0, 17]:
            with self.subTest(result=result), tempfile.TemporaryDirectory() as folder:
                entered = Path(folder) / 'entered'
                survived = Path(folder) / 'survived'
                descendant = """
import pathlib, sys, time

pathlib.Path(sys.argv[1]).touch()
time.sleep(1)
pathlib.Path(sys.argv[2]).touch()
"""
                child = """
import pathlib, subprocess, sys, time

subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2], sys.argv[3]])

while not pathlib.Path(sys.argv[2]).exists():
    time.sleep(0.01)

raise SystemExit(int(sys.argv[4]))
"""
                command = [
                    sys.executable,
                    '-c',
                    child,
                    descendant,
                    str(entered),
                    str(survived),
                    str(result),
                ]

                if result:
                    with self.assertRaises(subprocess.CalledProcessError) as error:
                        browser.run(command, timeout=5)

                    self.assertEqual(error.exception.returncode, result)
                else:
                    browser.run(command, timeout=5)

                threading.Event().wait(1)
                self.assertFalse(survived.exists(), 'descendant survived its parent exit')


if __name__ == '__main__':
    unittest.main()
