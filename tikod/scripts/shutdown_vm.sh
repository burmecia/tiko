#!/bin/bash
#
# Shut down a single Firecracker VM and tear down its host-side resources.
# Tears down only the VM identified by VM_ID (positional arg or env, default 0),
# leaving all other running VMs untouched.
#
# Usage:
#   ./shutdown_vm.sh [VM_ID]
#   ./shutdown_vm.sh --all      # shut down every running VM and clean up all taps
#
# The shared MASQUERADE rule is removed only when no tapN device remains, so
# other still-running VMs keep their outbound NAT.

set -euo pipefail

ALL=0
VM_ID="${VM_ID:-0}"
while [ $# -gt 0 ]; do
    case "$1" in
        --all) ALL=1; shift ;;
        *) VM_ID="$1"; shift ;;
    esac
done

if [ "$ALL" -eq 1 ]; then
    # Kill every firecracker, then remove every tapN and the shared NAT.
    echo ">>> shutting down ALL VMs..."
    sudo pkill firecracker 2>/dev/null || true
    for tap in $(ip -o link show | awk -F: '/tap[0-9]+/ {gsub(/ /,"",$2); print $2}'); do
        sudo ip link del "$tap" 2>/dev/null || true
    done
    DEFAULT_IFACE="$(ip route show default | awk '/default/ {print $5; exit}')"
    sudo iptables -t nat -D POSTROUTING -o "$DEFAULT_IFACE" -j MASQUERADE 2>/dev/null || true
    sudo iptables -F FORWARD 2>/dev/null || true
    exit 0
fi

if ! [[ "$VM_ID" =~ ^[0-9]+$ ]]; then
    echo "VM_ID must be a non-negative integer (got '$VM_ID')" >&2
    exit 1
fi

TAP_DEV="tap${VM_ID}"
API_SOCKET="/tmp/fc-${VM_ID}.socket"

echo ">>> VM ${VM_ID}: stopping firecracker..."
# Match the specific firecracker bound to this VM's API socket.
PIDS="$(pgrep -f -- "--api-sock $API_SOCKET" || true)"
if [ -n "$PIDS" ]; then
    sudo kill $PIDS 2>/dev/null || true
else
    echo "    no firecracker process for socket $API_SOCKET" >&2
fi

DEFAULT_IFACE="$(ip route show default | awk '/default/ {print $5; exit}')"
if ip link show "$TAP_DEV" &>/dev/null; then
    echo ">>> VM ${VM_ID}: removing tap $TAP_DEV..."
    # Per-VM FORWARD rules (tap-specific).
    sudo iptables -D FORWARD -i "$TAP_DEV" -o "$DEFAULT_IFACE" -j ACCEPT 2>/dev/null || true
    sudo iptables -D FORWARD -i "$DEFAULT_IFACE" -o "$TAP_DEV" -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null || true
    sudo ip link del "$TAP_DEV" 2>/dev/null || true

    # Remove the shared MASQUERADE only when no other tapN remains.
    if ! ip -o link show | awk -F': ' '{print $2}' | grep -qE '^tap[0-9]+$'; then
        sudo iptables -t nat -D POSTROUTING -o "$DEFAULT_IFACE" -j MASQUERADE 2>/dev/null || true
    fi
else
    echo "    tap $TAP_DEV not present" >&2
fi
