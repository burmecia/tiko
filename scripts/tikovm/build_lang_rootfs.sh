#!/bin/bash
# =============================================================================
# Build a language-runtime rootfs (Node.js 22 LTS + Python 3.12) as a
# derivative of the tikovm base rootfs. Purpose: demonstrate the 2nd kind
# of tikovm workload (after the Rust echo binary) — a lambda-like
# language-runtime serverless worker.
#
# LAMBDA MODEL — COMPUTE AND CODE ARE SEPARATED:
# The rootfs contains ONLY the language RUNTIMES (Node.js, Python) and a
# generic bootstrap loader. The application CODE (the actual handler source)
# is NOT baked in — it lives on the remote_slow volume and is loaded at cold
# start. This mirrors AWS Lambda's architecture:
#
#   - The VM image = the ephemeral "runtime layer" (this rootfs).
#   - remote_slow  = the durable "function-code layer" (mounted at /mnt/archive).
#   - The bootstrap loader reads /mnt/archive/code/, selects the runtime
#     via a .runtime marker file, and exec's the handler.
#
# The same rootfs serves both Node and Python — switching runtimes is a
# storage-side operation (change the .runtime marker on the volume), not a
# rootfs rebuild. See scripts/tikovm/lang-code/ for the handler source and
# run_lang_e2e.sh for the deploy→cold-start→serve→destroy flow.
#
# SSH access is baked into the base (see build_base_rootfs.sh).
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
sudo tar -xJ -C "$MNT/usr/local" --strip-components=1 \
    --exclude='*.md' --exclude='LICENSE' \
    -f "/tmp/${NODE_TARBALL}"
rm -f "/tmp/${NODE_TARBALL}"

# ---- Python 3.12 (apt inside chroot; Ubuntu 24.04 Noble ships 3.12) ----------
echo "installing Python 3.12 via apt (chroot)"
sudo mount --bind /proc "$MNT/proc"
sudo mount --bind /sys  "$MNT/sys"
sudo mount --bind /dev  "$MNT/dev"
sudo chroot "$MNT" /bin/bash <<'EOF'
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y --no-install-recommends python3 python3-pip python3-venv >/dev/null
apt-get clean
rm -rf /var/lib/apt/lists/*
echo "  python: $(python3 --version)"
EOF
sudo umount "$MNT/dev"
sudo umount "$MNT/sys"
sudo umount "$MNT/proc"

# ---- Lambda-style bootstrap loader -------------------------------------------
# The rootfs does NOT contain application code. This generic loader reads the
# handler source from the remote_slow volume (/mnt/archive/code/) at cold
# start, selects the runtime via a .runtime marker file, and exec's it.
# See scripts/tikovm/lang-code/ for the handler source deployed to the volume.
echo "injecting lang-bootstrap loader"
sudo tee "$MNT/usr/local/bin/lang-bootstrap" >/dev/null <<'BOOTSTRAP'
#!/bin/sh
# Lambda-style code loader for the tikovm lang-rootfs.
#
# The VM image contains only the language RUNTIMES (Node.js, Python). The
# application CODE lives on the remote_slow volume, persisted independently
# of the ephemeral compute VM. This script loads the code from the mounted
# remote_slow volume and launches the appropriate runtime — the same
# separation AWS Lambda enforces between the runtime layer and the function
# code layer.
#
# Code layout on the remote_slow volume (/mnt/archive):
#   /mnt/archive/code/echo-node.js     (Node.js handler)
#   /mnt/archive/code/echo-python.py   (Python handler)
#   /mnt/archive/code/.runtime         (marker: "node" or "python")
#
# Arguments (e.g., --port 8080) are forwarded to the handler.
set -eu

CODE_DIR=/mnt/archive/code
RUNTIME_FILE="$CODE_DIR/.runtime"

# Wait briefly for the remote_slow volume to be mounted. guestd's fs module
# mounts volumes before starting the workload, but a short retry covers any
# race with the mount-by-label udev event.
for i in 1 2 3 4 5; do
  [ -d "$CODE_DIR" ] && break
  echo "lang-bootstrap: waiting for $CODE_DIR (attempt $i)..." >&2
  sleep 1
done

if [ ! -d "$CODE_DIR" ]; then
  echo "FATAL: $CODE_DIR not found — remote_slow volume not mounted" >&2
  echo "       ensure the provision request declares an 'archive' volume" >&2
  exit 1
fi

# Select runtime from the marker file, default to node.
if [ -f "$RUNTIME_FILE" ]; then
  RUNTIME=$(tr -dc 'a-z' < "$RUNTIME_FILE")
else
  RUNTIME=node
fi

echo "lang-bootstrap: runtime=$RUNTIME, code_dir=$CODE_DIR" >&2

case "$RUNTIME" in
  node)
    CODE="$CODE_DIR/echo-node.js"
    if [ ! -f "$CODE" ]; then
      echo "FATAL: $CODE not found on remote_slow volume" >&2
      exit 1
    fi
    exec /usr/local/bin/node "$CODE" "$@"
    ;;
  python)
    CODE="$CODE_DIR/echo-python.py"
    if [ ! -f "$CODE" ]; then
      echo "FATAL: $CODE not found on remote_slow volume" >&2
      exit 1
    fi
    exec /usr/bin/python3 "$CODE" "$@"
    ;;
  *)
    echo "FATAL: unknown runtime '$RUNTIME' in $RUNTIME_FILE" >&2
    echo "       expected 'node' or 'python'" >&2
    exit 1
    ;;
esac
BOOTSTRAP
sudo chmod 755 "$MNT/usr/local/bin/lang-bootstrap"

# ---- workload manifest -------------------------------------------------------
echo "injecting workload manifest (Lambda-style: bootstrap loads code from remote_slow)"
sudo mkdir -p "$MNT/etc/tikovm"
sudo tee "$MNT/etc/tikovm/workload.toml" >/dev/null <<'TOML'
version = 1
workload = "lang-echo"

# Lambda-style bootstrap: the generic loader at /usr/local/bin/lang-bootstrap
# reads the handler source from /mnt/archive/code/ (remote_slow volume) and
# launches the runtime selected by the .runtime marker. The rootfs contains
# ONLY the runtimes; the application code is deployed separately to the
# remote_slow volume (see run_lang_e2e.sh's seed_volume step).
[process]
cmd = "/usr/local/bin/lang-bootstrap"
args = ["--port", "8080"]

[health]
kind = "http"
path = "/health"
port = 8080
interval_secs = 5

[expose]
http_port = 8080

# local_fast: ephemeral scratch space (per-VM, destroyed on VM destroy).
[[volumes]]
name = "data"
tier = "local_fast"
mount_path = "/mnt/data"
size_mb = 64

# remote_slow: the "function code" layer. The application source lives here
# (persisted across VM destroy/provision cycles). The bootstrap loader reads
# from /mnt/archive/code/. This is the compute-vs-storage separation that
# defines Lambda-style serverless: the VM is a pure ephemeral compute unit,
# the code is durable on remote storage.
[[volumes]]
name = "archive"
tier = "remote_slow"
mount_path = "/mnt/archive"
size_mb = 64

# scale-to-zero: after 15s with no connections to :8080, guestd asks the host
# to suspend this VM.
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

# Defensive: mask the legacy Tiko agent from the tikod platform.
sudo ln -sf /dev/null "$MNT/etc/systemd/system/tikoguest.service"

sync
sudo umount "$MNT"
echo "built $OUT"
echo "Lambda model: rootfs = runtimes only; code deployed to remote_slow at /mnt/archive/code/"
echo "deploy code:  see run_lang_e2e.sh (seed_volume step)"
