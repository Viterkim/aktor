import functools
import http.server
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import tempfile
import threading
import urllib.request


def windows_job():
    import ctypes

    class Limits(ctypes.Structure):
        _fields_ = [
            ('process_time', ctypes.c_int64),
            ('job_time', ctypes.c_int64),
            ('flags', ctypes.c_uint32),
            ('minimum', ctypes.c_size_t),
            ('maximum', ctypes.c_size_t),
            ('processes', ctypes.c_uint32),
            ('affinity', ctypes.c_size_t),
            ('priority', ctypes.c_uint32),
            ('scheduling', ctypes.c_uint32),
        ]

    class ExtendedLimits(ctypes.Structure):
        _fields_ = [
            ('basic', Limits),
            ('io', ctypes.c_uint64 * 6),
            ('process_memory', ctypes.c_size_t),
            ('job_memory', ctypes.c_size_t),
            ('peak_process', ctypes.c_size_t),
            ('peak_job', ctypes.c_size_t),
        ]

    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p]
    kernel.CreateJobObjectW.restype = ctypes.c_void_p
    kernel.GetCurrentProcess.restype = ctypes.c_void_p
    kernel.SetInformationJobObject.argtypes = [
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_uint32,
    ]
    kernel.AssignProcessToJobObject.argtypes = [ctypes.c_void_p, ctypes.c_void_p]

    job = kernel.CreateJobObjectW(None, None)

    if not job:
        raise ctypes.WinError(ctypes.get_last_error())

    limits = ExtendedLimits()
    limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE

    if not kernel.SetInformationJobObject(job, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
        raise ctypes.WinError(ctypes.get_last_error())

    if not kernel.AssignProcessToJobObject(job, kernel.GetCurrentProcess()):
        raise ctypes.WinError(ctypes.get_last_error())

    # The launcher holds the only job handle until it exits, including when killed.
    return job


def run(command, timeout=120):
    root = Path(__file__).resolve().parent.parent
    token = secrets.token_hex(16)
    log = tempfile.NamedTemporaryFile(prefix="aktor-browser-http-", suffix=".log", delete=False)

    class Handler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/__aktor_check":
                body = token.encode()

                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                super().do_GET()

        def log_message(self, format, *args):
            log.write((format % args + "\n").encode())
            log.flush()

    handler = functools.partial(Handler, directory=root / "integrations/worker/web")

    with http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler) as server:
        serving = threading.Thread(target=server.serve_forever, daemon=True)

        serving.start()

        address = f"http://127.0.0.1:{server.server_port}"

        try:
            with urllib.request.urlopen(address + "/__aktor_check", timeout=5) as response:
                if response.read().decode() != token:
                    raise RuntimeError("browser check server identity mismatch")

            environment = dict(os.environ, AKTOR_PROOF_URL=address, AKTOR_PROOF_TOKEN=token)
            launch = command

            if os.name == 'nt':
                launch = [sys.executable, str(Path(__file__).resolve()), '--child', *command]

            with subprocess.Popen(
                launch, cwd=root, env=environment, start_new_session=os.name != 'nt'
            ) as child:
                try:
                    result = child.wait(timeout=timeout)

                    if result:
                        raise subprocess.CalledProcessError(result, command)
                finally:
                    if os.name == 'nt':
                        if child.poll() is None:
                            child.kill()
                    else:
                        try:
                            os.killpg(child.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass

                    child.wait()
        finally:
            server.shutdown()
            serving.join()
            log.close()
            print(f"HTTP log: {log.name}")


if __name__ == "__main__":
    if sys.argv[1:2] == ['--child']:
        job = windows_job()

        raise SystemExit(subprocess.call(sys.argv[2:]))

    run(["node", "integrations/worker/web/check.cjs"])
