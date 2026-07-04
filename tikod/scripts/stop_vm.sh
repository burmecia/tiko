#!/bin/bash
#
# Stop a single running Firecracker VM without deleting it.
#
# Only kills the firecracker process for the given VM_ID. The overlay image
# (assets/overlay-<id>.ext4), the host tap device (tap<id>), and the per-VM
# iptables rules are all left in place, so a subsequent
#   ./start_vm.sh <VM_ID>           # (without --fresh)
# relaunches firecracker against the existing tap + overlay — a fast restart
# with no overlay rebuild, no tap creation, and no iptables reconfiguration.
#
# For a full teardown (remove tap + iptables too), use shutdown_vm.sh instead.
#
# Usage:
#   ./stop_vm.sh [VM_ID]

set -euo pipefail

VM_ID="${VM_ID:-0}"
[ $# -ge 1 ] && VM_ID="$1"
if ! [[ "$VM_ID" =~ ^[0-9]+$ ]]; then
    echo "VM_ID must be a non-negative integer (got '$VM_ID')" >&2
    exit 1
fi

API_SOCKET="/tmp/fc-${VM_ID}.socket"
OVERLAY_IMAGE="$(cd "$(dirname "$0")" && pwd)/../assets/overlay-${VM_ID}.ext4"

if [ ! -S "$API_SOCKET" ] && ! pgrep -f -- "--api-sock $API_SOCKET" >/dev/null; then
    echo "no firecracker running for VM ${VM_ID} (socket $API_SOCKET absent, no matching process)" >&2
    exit 0
fi

echo ">>> VM ${VM_ID}: stopping firecracker..."
PIDS="$(pgrep -f -- "--api-sock $API_SOCKET" || true)"
if [ -n "$PIDS" ]; then
    sudo kill $PIDS 2>/dev/null || true
    # Wait briefly for the process to exit so the API socket is released.
    for pid in $PIDS; do
        for _ in $(seq 1 50); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.1
        done
    done
else
    echo "    no firecracker process matched $API_SOCKET (stale socket?)" >&2
fi
sudo rm -f "$API_SOCKET"

echo ">>> VM ${VM_ID}: stopped. Kept for restart:"
echo "    overlay: $OVERLAY_IMAGE"
echo "    tap     : tap${VM_ID} (still up)"
echo "    restart : ./start_vm.sh ${VM_ID}   # without --fresh"
