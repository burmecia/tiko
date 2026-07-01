#!/bin/bash

set -euo pipefail

TAP_DEV=tap0

sudo pkill firecracker || true

# Tear down the host tap device and NAT/forwarding rules set up by start_vm.sh
if ip link show "$TAP_DEV" &>/dev/null; then
    DEFAULT_IFACE=$(ip route show default | awk '/default/ {print $5; exit}')
    sudo iptables -t nat -D POSTROUTING -o "$DEFAULT_IFACE" -j MASQUERADE 2>/dev/null || true
    sudo iptables -D FORWARD -i "$TAP_DEV" -o "$DEFAULT_IFACE" -j ACCEPT 2>/dev/null || true
    sudo iptables -D FORWARD -i "$DEFAULT_IFACE" -o "$TAP_DEV" -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null || true
    sudo ip link del "$TAP_DEV"
fi

