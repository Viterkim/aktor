import functools
import http.server
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import threading
import urllib.request


def run(command):
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
            subprocess.run(command, cwd=root, env=environment, check=True)
        finally:
            server.shutdown()
            serving.join()
            log.close()
            print(f"HTTP log: {log.name}")


if __name__ == "__main__":
    run(["node", "integrations/worker/web/check.cjs"])
