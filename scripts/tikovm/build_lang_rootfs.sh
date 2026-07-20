#!/bin/bash
# =============================================================================
# Build a language-runtime rootfs (Node.js 22 LTS + Python 3.12) as a
# derivative of the tikovm base rootfs. Purpose: demonstrate the 2nd kind
# of tikovm workload (after the Rust echo binary) — a lambda-like
# language-runtime serverless worker. Same generic supervisor + vsock
# scale-to-zero machinery as the echo rootfs; the workload process is an
# interpreted language runtime instead of a compiled binary.
#
# The image bakes in BOTH runtimes AND a "hello world" echo HTTP server per
# runtime (one Node.js, one Python). The manifest defaults to Node.js; swap
# to Python by editing /etc/tikovm/workload.toml [process] (two-line change,
# documented in the manifest comment).
#
# SSH access is baked into the base (see build_base_rootfs.sh); this script
# only adds the language-runtime payload.
#
# Output: tikod/assets/lang-rootfs.ext4
# =============================================================================
set -euo pipefail

REPO=/home/ubuntu/tiko
# tikovm-family base (scripts/tikovm/build_base_rootfs.sh).
BASE=$REPO/tikod/assets/tikovm-base-rootfs.ext4
OUT=$REPO/tikod/assets/lang-rootfs.ext4
GUESTD=$REPO/target/debug/tikovm-guestd

# Node.js 22 LTS ("Jod"; active LTS Oct 2024 → maintenance through Apr 2027).
# Override NODE_VERSION to pick a different patch from https://nodejs.org/dist/
# 22.11.0 is the canonical first 22 LTS release; known to exist.
NODE_VERSION="${NODE_VERSION:-22.11.0}"
NODE_TARBALL="node-v${NODE_VERSION}-linux-x64.tar.xz"

[ -f "$GUESTD" ] || { echo "build guestd first: cargo build -p tikovm-guest"; exit 1; }
[ -f "$BASE" ]   || { echo "build the tikovm base first: bash scripts/tikovm/build_base_rootfs.sh"; exit 1; }

if [ ! -f "$OUT" ]; then
  echo "sparse-copying base rootfs -> $OUT (one-time)"
  cp --sparse=always "$BASE" "$OUT"
fi

MNT=$(mktemp -d)
cleanup() {
  # Best-effort: some of these may not be mounted depending on where we failed.
  sudo umount "$MNT/dev"  2>/dev/null || true
  sudo umount "$MNT/sys"  2>/dev/null || true
  sudo umount "$MNT/proc" 2>/dev/null || true
  sudo umount "$MNT"      2>/dev/null || true
  rmdir "$MNT" 2>/dev/null || true
}
trap cleanup EXIT

echo "mounting $OUT at $MNT"
sudo mount -o loop "$OUT" "$MNT"

echo "injecting tikovm-guestd"
sudo install -m755 "$GUESTD" "$MNT/usr/local/bin/tikovm-guestd"

# ---- Node.js 22 LTS ----------------------------------------------------------
echo "installing Node.js v${NODE_VERSION} -> /usr/local"
curl -fsSL -o "/tmp/${NODE_TARBALL}" \
    "https://nodejs.org/dist/v${NODE_VERSION}/${NODE_TARBALL}"
# strip-components=1 lands bin/node, lib/node_modules, include/, etc. directly
# under /usr/local (the layout upstream recommends for /usr/local installs).
sudo tar -xJ -C "$MNT/usr/local" --strip-components=1 \
    --exclude='*.md' --exclude='LICENSE' \
    -f "/tmp/${NODE_TARBALL}"
rm -f "/tmp/${NODE_TARBALL}"

# ---- Python 3.12 (apt inside chroot; Ubuntu 24.04 Noble ships 3.12) ----------
echo "installing Python 3.12 via apt (chroot)"
# Bind-mount /proc /sys /dev so apt/dpkg work inside the chroot. /etc/resolv.conf
# is already baked in the base rootfs; the chroot inherits the host's network
# (we don't unshare the netns).
sudo mount --bind /proc "$MNT/proc"
sudo mount --bind /sys  "$MNT/sys"
sudo mount --bind /dev  "$MNT/dev"
sudo chroot "$MNT" /bin/bash <<'EOF'
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
# python3 in Noble = 3.12. pip + venv so a lambda-style workload can install
# deps at runtime if needed; --no-install-recommends keeps the image lean.
apt-get install -y --no-install-recommends python3 python3-pip python3-venv >/dev/null
apt-get clean
rm -rf /var/lib/apt/lists/*
echo "  python: $(python3 --version)"
EOF
sudo umount "$MNT/dev"
sudo umount "$MNT/sys"
sudo umount "$MNT/proc"

# ---- "hello world" echo servers ---------------------------------------------
echo "injecting echo servers (node + python)"
sudo install -d -m755 "$MNT/usr/local/lib/tikovm"

# Node.js echo server — invoked as: node /usr/local/lib/tikovm/echo-node.js --port 8080
sudo tee "$MNT/usr/local/lib/tikovm/echo-node.js" >/dev/null <<'JS'
'use strict';
// Minimal Node.js HTTP echo server for tikovm lang-rootfs.
//   GET /         -> 200 "hello world from node v<version>\n"
//   GET /health   -> 200 {"ok":true}
// No external deps; uses the built-in http module so the freshly-baked
// Node.js runtime in the image suffices. Mirrors echo-python.py 1:1 so the
// two runtimes are interchangeable from the manifest's [process] block.
const http = require('http');

const argv = process.argv;
let port = 8080;
for (let i = 0; i < argv.length; i++) {
  if (argv[i] === '--port' && i + 1 < argv.length) port = parseInt(argv[i + 1], 10);
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
    return;
  }
  res.writeHead(200, { 'content-type': 'text/plain' });
  res.end('hello world from node ' + process.version + '\n');
});

server.listen(port, '0.0.0.0', () => {
  console.error('tikovm lang-echo (node ' + process.version + ') listening on :' + port);
});
JS

# Python echo server — invoked as: /usr/bin/python3 /usr/local/lib/tikovm/echo-python.py --port 8080
sudo tee "$MNT/usr/local/lib/tikovm/echo-python.py" >/dev/null <<'PY'
#!/usr/bin/env python3
"""Minimal Python HTTP echo server for tikovm lang-rootfs.

  GET /         -> 200 "hello world from python <version>\\n"
  GET /health   -> 200 {"ok": true}

No external deps; uses the stdlib http.server so the apt-installed python3
in the image suffices. Mirrors echo-node.js 1:1.
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

    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write(f"{self.address_string()} - {fmt % args}\n")


if __name__ == "__main__":
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(
        f"tikovm lang-echo (python {PY_VERSION}) listening on :{PORT}",
        file=sys.stderr,
    )
    srv.serve_forever()
PY
sudo chmod 644 "$MNT/usr/local/lib/tikovm/echo-node.js"
sudo chmod 755 "$MNT/usr/local/lib/tikovm/echo-python.py"

# ---- workload manifest -------------------------------------------------------
echo "injecting workload manifest (default runtime: node)"
sudo mkdir -p "$MNT/etc/tikovm"
sudo tee "$MNT/etc/tikovm/workload.toml" >/dev/null <<'TOML'
version = 1
workload = "lang-echo"

# Lambda-like language-runtime workload. Default runtime: Node.js 22 LTS.
# Swap to Python by editing [process] to:
#   cmd = "/usr/bin/python3"
#   args = ["/usr/local/lib/tikovm/echo-python.py", "--port", "8080"]
[process]
cmd = "/usr/local/bin/node"
args = ["/usr/local/lib/tikovm/echo-node.js", "--port", "8080"]

[health]
kind = "http"
path = "/health"
port = 8080
interval_secs = 5

[expose]
http_port = 8080

# a local_fast volume: the host creates an ext4 image (labeled "data") and
# attaches it; the guest mounts it by label at /mnt/data.
[[volumes]]
name = "data"
tier = "local_fast"
mount_path = "/mnt/data"
size_mb = 64

# a remote_slow volume: the host places the image on a mounted remote FS
# (source set at provision time); persists across destroy.
[[volumes]]
name = "archive"
tier = "remote_slow"
mount_path = "/mnt/archive"
size_mb = 64

# scale-to-zero: after 15s with no connections to :8080, guestd asks the host
# to suspend this VM. Same setting as the echo rootfs so the lambda-style
# scale-to-zero behavior is directly comparable.
[idle]
tick_secs = 2
idle_secs = 15
[[idle.probes]]
kind = "host_network"

# lifecycle hooks: marker echoes hit the console so a build/test run can
# confirm PreSuspend/PostRestore fired inside the language runtime VM.
[suspend]
pre_suspend_cmd = "echo tikovm: pre-suspend hook ran (lang-echo)"
post_restore_cmd = "echo tikovm: post-restore hook ran (lang-echo)"
TOML

# ---- systemd unit ------------------------------------------------------------
echo "injecting systemd unit for tikovm-guestd"
sudo tee "$MNT/etc/systemd/system/tikovm-guestd.service" >/dev/null <<'UNIT'
[Unit]
Description=tikovm guest agent (language-runtime workload)
After=network-online.target systemd-networkd.service

[Service]
ExecStart=/usr/local/bin/tikovm-guestd
Restart=on-failure
RestartSec=2
StandardOutput=journal+console
StandardError=journal+console

[Install]
WantedBy=multi-user.target
UNIT

sudo mkdir -p "$MNT/etc/systemd/system/multi-user.target.wants"
sudo ln -sf /etc/systemd/system/tikovm-guestd.service \
            "$MNT/etc/systemd/system/multi-user.target.wants/tikovm-guestd.service"

# Defensive: mask the legacy Tiko agent from the tikod platform. The tikovm
# base already does this, but keep it so a future base swap can't resurrect it.
sudo ln -sf /dev/null "$MNT/etc/systemd/system/tikoguest.service"

sync
sudo umount "$MNT"
echo "built $OUT"
echo "ssh access (from host): ssh root@172.16.<n>.2  (n = vm index, e.g. vm-0 -> 172.16.0.2)"
echo "swap runtime in guest: edit /etc/tikovm/workload.toml [process] (node <-> python)"
