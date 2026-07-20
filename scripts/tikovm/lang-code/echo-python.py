#!/usr/bin/env python3
"""Minimal Python HTTP echo handler for the tikovm lang-rootfs.

This file is NOT baked into the rootfs — it is deployed to the remote_slow
volume and loaded at cold start by /usr/local/bin/lang-bootstrap. This
compute-vs-storage separation is the Lambda model: the VM image is the
ephemeral runtime layer; this file is the durable function-code layer.

  GET /         -> 200 "hello world from python <version>\\n"
  GET /health   -> 200 {"ok": true}

No external deps; uses the stdlib http.server so the python3 in the image
suffices. Mirrors echo-node.js 1:1 so the two runtimes are interchangeable
via the .runtime marker on the volume.
"""
from __future__ import annotations

import json
import os
import platform
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def _parse_port(argv: list[str], default: int = 8080) -> int:
    if "--port" in argv:
        i = argv.index("--port")
        if i + 1 < len(argv):
            return int(argv[i + 1])
    env = os.environ.get("PORT")
    return int(env) if env else default


PORT = _parse_port(sys.argv)
PY_VERSION = platform.python_version()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, code: int, body: bytes | str, ctype: str = "text/plain") -> None:
        body = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("content-type", ctype)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send(200, json.dumps({"ok": True}), "application/json")
            return
        self._send(200, f"hello world from python {PY_VERSION}\n")

    def log_message(self, format: str, *args) -> None:  # noqa: A002
        sys.stderr.write(f"{self.address_string()} - {format % args}\n")


if __name__ == "__main__":
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(
        f"tikovm lang-echo (python {PY_VERSION}) listening on :{PORT}",
        file=sys.stderr,
    )
    srv.serve_forever()
