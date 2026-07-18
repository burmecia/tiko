#!/bin/bash
# Build an echo-workload rootfs: copy the base ubuntu rootfs and inject the
# tikovm-guestd agent, a tiny echo HTTP server, the workload manifest, and a
# systemd unit that runs guestd at boot. Also bakes in SSH access (root
# authorized_keys) for dev/debug. Output: tikod/assets/echo-rootfs.ext4
set -euo pipefail

REPO=/home/ubuntu/tiko
BASE=$REPO/tikod/assets/ubuntu-24.04-rootfs.ext4
OUT=$REPO/tikod/assets/echo-rootfs.ext4
GUESTD=$REPO/target/debug/tikovm-guestd
ECHO=$REPO/target/debug/examples/echo-server

[ -f "$GUESTD" ] || { echo "build guestd first: cargo build -p tikovm-guest"; exit 1; }
[ -f "$ECHO" ]   || { echo "build echo-server first: cargo build -p tikovm-guest --example echo-server"; exit 1; }

if [ ! -f "$OUT" ]; then
  echo "sparse-copying base rootfs -> $OUT (one-time)"
  cp --sparse=always "$BASE" "$OUT"
fi

MNT=$(mktemp -d)
cleanup() { sudo umount "$MNT" 2>/dev/null || true; rmdir "$MNT" 2>/dev/null || true; }
trap cleanup EXIT

echo "mounting $OUT at $MNT"
sudo mount -o loop "$OUT" "$MNT"

echo "injecting guestd + echo-server + manifest + systemd unit"
sudo install -m755 "$GUESTD" "$MNT/usr/local/bin/tikovm-guestd"
sudo install -m755 "$ECHO"   "$MNT/usr/local/bin/echo-server"
sudo mkdir -p "$MNT/etc/tikovm"
sudo tee "$MNT/etc/tikovm/workload.toml" >/dev/null <<'TOML'
version = 1
workload = "echo"

[process]
cmd = "/usr/local/bin/echo-server"
args = ["--port", "8080"]

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
# (source is set at provision time); persists across destroy.
[[volumes]]
name = "archive"
tier = "remote_slow"
mount_path = "/mnt/archive"
size_mb = 64

# scale-to-zero: after 15s with no connections to :8080, guestd asks the host
# to suspend this VM. The host proxy wakes it on the next connection.
[idle]
tick_secs = 2
idle_secs = 15
[[idle.probes]]
kind = "host_network"

# lifecycle hooks: the host sends PreSuspend/PostRestore over vsock; these run
# inside the guest while it can still execute (marker echoes hit the console).
[suspend]
pre_suspend_cmd = "echo tikovm: pre-suspend hook ran"
post_restore_cmd = "echo tikovm: post-restore hook ran"
TOML

sudo tee "$MNT/etc/systemd/system/tikovm-guestd.service" >/dev/null <<'UNIT'
[Unit]
Description=tikovm guest agent (generic in-VM supervisor)
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
# Avoid noise from the legacy Tiko agent (its host is absent in tikovm).
sudo ln -sf /dev/null "$MNT/etc/systemd/system/tikoguest.service"

# Bake in SSH access for dev/debug. openssh-server is already installed in the
# base rootfs with PermitRootLogin yes; what's missing is an authorized key.
# Defaults to the current user's pubkey; override with TIKOVM_SSH_PUBKEY
# (key contents) or TIKOVM_SSH_PUBKEY_FILE (path).
echo "baking ssh access (root authorized_keys)"
sudo ln -sf /usr/lib/systemd/system/ssh.service \
            "$MNT/etc/systemd/system/multi-user.target.wants/ssh.service"
PUBKEY=${TIKOVM_SSH_PUBKEY:-}
if [ -z "$PUBKEY" ]; then
  for f in ${TIKOVM_SSH_PUBKEY_FILE:-} "$HOME/.ssh/id_ed25519.pub" "$HOME/.ssh/id_rsa.pub"; do
    if [ -n "$f" ] && [ -f "$f" ]; then PUBKEY=$(cat "$f"); break; fi
  done
fi
if [ -z "$PUBKEY" ]; then
  echo "WARNING: no SSH pubkey found; skipping authorized_keys"
  echo "         (set TIKOVM_SSH_PUBKEY or TIKOVM_SSH_PUBKEY_FILE to enable ssh)"
else
  sudo install -d -m700 "$MNT/root/.ssh"
  echo "$PUBKEY" | sudo tee "$MNT/root/.ssh/authorized_keys" >/dev/null
  sudo chmod 600 "$MNT/root/.ssh/authorized_keys"
fi

sync
sudo umount "$MNT"
echo "built $OUT"
echo "ssh access (from host): ssh root@172.16.<n>.2  (n = vm index, e.g. vm-0 -> 172.16.0.2)"
