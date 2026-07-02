#!/bin/bash
#
# Resume a Firecracker VM from a snapshot created by snapshot_vm.sh.
#
# Starts a fresh firecracker process (no boot — state is restored from the
# snapshot), loads the microVM-state + guest-memory files, and resumes. The
# original VM must be stopped first (snapshot/resume onto the same RW rootfs
# while the original still writes would corrupt it).
#
# The snapshot already encodes the device config (rootfs path, tap name, MAC,
# machine-config), so we only ensure the host-side prerequisites the snapshot
# references still exist: the rootfs-<id>.ext4 file and the tap<id> device.
#
# Usage:
#   ./resume_vm.sh [VM_ID]
#
# Env: SNAPSHOT_DIR (default $ASSETS_DIR/snapshots/vm-<VM_ID>)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ASSETS_DIR="$SCRIPT_DIR/../assets"
FC_DIR="$SCRIPT_DIR/../../firecracker/build/cargo_target/x86_64-unknown-linux-musl/debug"

VM_ID="${VM_ID:-0}"
[ $# -ge 1 ] && [ "$1" != "--" ] && VM_ID="$1"
if ! [[ "$VM_ID" =~ ^[0-9]+$ ]]; then
    echo "VM_ID must be a non-negative integer (got '$VM_ID')" >&2
    exit 1
fi

API_SOCKET="/tmp/fc-${VM_ID}.socket"
TAP_DEV="tap${VM_ID}"
SUBNET="172.16.${VM_ID}"
TAP_IP="${SUBNET}.1/24"
GUEST_IP="${SUBNET}.2"
ROOTFS_COPY="$ASSETS_DIR/rootfs-${VM_ID}.ext4"
SNAPSHOT_DIR="${SNAPSHOT_DIR:-$ASSETS_DIR/snapshots/vm-${VM_ID}}"
STATE_FILE="$SNAPSHOT_DIR/snap.bin"
MEM_FILE="$SNAPSHOT_DIR/mem.bin"

# ── Prechecks ────────────────────────────────────────────────────────────────
if [ ! -f "$STATE_FILE" ] || [ ! -f "$MEM_FILE" ]; then
    echo "snapshot not found in $SNAPSHOT_DIR (run snapshot_vm.sh ${VM_ID} first)" >&2
    exit 1
fi
if [ ! -f "$ROOTFS_COPY" ]; then
    echo "rootfs $ROOTFS_COPY missing — the snapshot references it; cannot resume." >&2
    echo "  (start_vm.sh creates it; don't delete rootfs-<id>.ext4 before resuming)" >&2
    exit 1
fi
if pgrep -f -- "--api-sock $API_SOCKET" >/dev/null; then
    echo "a firecracker is still bound to $API_SOCKET — stop VM ${VM_ID} first (shutdown_vm.sh ${VM_ID})." >&2
    echo "  resuming while the original runs would corrupt the shared RW rootfs." >&2
    exit 1
fi

STATE_ABS="$(cd "$SNAPSHOT_DIR" && pwd)/snap.bin"
MEM_ABS="$(cd "$SNAPSHOT_DIR" && pwd)/mem.bin"

# ── Host networking (mirror start_vm.sh — the snapshot references tap<id>) ───
echo ">>> VM ${VM_ID}: ensuring tap ${TAP_DEV} (${TAP_IP})..."
if ! ip link show "$TAP_DEV" &>/dev/null; then
    sudo ip tuntap add "$TAP_DEV" mode tap
    sudo ip addr add "$TAP_IP" dev "$TAP_DEV"
    sudo ip link set "$TAP_DEV" up
fi
DEFAULT_IFACE="$(ip route show default | awk '/default/ {print $5; exit}')"
sudo sysctl -w net.ipv4.ip_forward=1 >/dev/null
sudo iptables -t nat -C POSTROUTING -o "$DEFAULT_IFACE" -j MASQUERADE 2>/dev/null \
    || sudo iptables -t nat -A POSTROUTING -o "$DEFAULT_IFACE" -j MASQUERADE
sudo iptables -C FORWARD -i "$TAP_DEV" -o "$DEFAULT_IFACE" -j ACCEPT 2>/dev/null \
    || sudo iptables -A FORWARD -i "$TAP_DEV" -o "$DEFAULT_IFACE" -j ACCEPT
sudo iptables -C FORWARD -i "$DEFAULT_IFACE" -o "$TAP_DEV" -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null \
    || sudo iptables -A FORWARD -i "$DEFAULT_IFACE" -o "$TAP_DEV" -m state --state RELATED,ESTABLISHED -j ACCEPT

# ── Start firecracker (no config-file; snapshot encodes device config) ───────
sudo rm -f "$API_SOCKET"
mkdir -p "$SNAPSHOT_DIR"
echo ">>> VM ${VM_ID}: starting firecracker (restore mode)..."
sudo "$FC_DIR/firecracker" --api-sock "$API_SOCKET" \
    >"$SNAPSHOT_DIR/resume.log" 2>&1 &
FC_PID=$!
cleanup() { sudo kill "$FC_PID" 2>/dev/null || true; }
trap cleanup EXIT

# Wait for the API socket to come up.
for _ in $(seq 1 50); do [ -S "$API_SOCKET" ] && break; sleep 0.1; done
if [ ! -S "$API_SOCKET" ]; then
    echo "firecracker did not create $API_SOCKET; log:" >&2
    cat "$SNAPSHOT_DIR/resume.log" >&2
    exit 1
fi

# ── Load snapshot + resume ───────────────────────────────────────────────────
echo ">>> VM ${VM_ID}: loading snapshot + resuming..."
body_file="$(mktemp)"
# Firecracker's API socket is root-owned (sudo); connect() needs write perms.
# `|| true`: curl occasionally exits non-zero on unix-socket teardown; rely on code.
code=$(sudo curl -s -o "$body_file" --unix-socket "$API_SOCKET" \
    -X PUT 'http://localhost/snapshot/load' \
    -H 'Accept: application/json' -H 'Content-Type: application/json' \
    -d "{\"snapshot_path\":\"$STATE_ABS\",\"mem_backend\":{\"backend_path\":\"$MEM_ABS\",\"backend_type\":\"File\"},\"resume_vm\":true}" \
    -w "%{http_code}" || true)
if [ "$code" != "204" ] && [ "$code" != "200" ]; then
    echo "snapshot load failed (HTTP $code): $(cat "$body_file")" >&2
    rm -f "$body_file"
    exit 1
fi
rm -f "$body_file"

# ── Reachability check ───────────────────────────────────────────────────────
echo ">>> VM ${VM_ID}: waiting for guest ${GUEST_IP} to respond..."
ok=0
for _ in $(seq 1 30); do
    if ping -c1 -W1 "$GUEST_IP" >/dev/null 2>&1; then ok=1; break; fi
    sleep 1
done
if [ "$ok" = 1 ]; then
    echo ">>> guest reachable. connect with: psql -h $GUEST_IP -U postgres"
else
    echo ">>> guest did not respond to ping within 30s; check $SNAPSHOT_DIR/resume.log" >&2
fi

wait "$FC_PID"
