#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ASSETS_DIR="$SCRIPT_DIR/../assets"
FC_DIR="$SCRIPT_DIR/../../firecracker/build/cargo_target/x86_64-unknown-linux-musl/debug"
API_SOCKET=/tmp/firecracker.socket
TAP_DEV=tap0
TAP_IP=172.16.0.1/24

# Set up the host tap device (see vm_config.json network-interfaces / guest's
# /etc/systemd/network/20-eth0.network for the matching guest-side config).
if ! ip link show "$TAP_DEV" &>/dev/null; then
    sudo ip tuntap add "$TAP_DEV" mode tap
    sudo ip addr add "$TAP_IP" dev "$TAP_DEV"
    sudo ip link set "$TAP_DEV" up
fi

DEFAULT_IFACE=$(ip route show default | awk '/default/ {print $5; exit}')
sudo sysctl -w net.ipv4.ip_forward=1 >/dev/null
sudo iptables -t nat -C POSTROUTING -o "$DEFAULT_IFACE" -j MASQUERADE 2>/dev/null \
    || sudo iptables -t nat -A POSTROUTING -o "$DEFAULT_IFACE" -j MASQUERADE
sudo iptables -C FORWARD -i "$TAP_DEV" -o "$DEFAULT_IFACE" -j ACCEPT 2>/dev/null \
    || sudo iptables -A FORWARD -i "$TAP_DEV" -o "$DEFAULT_IFACE" -j ACCEPT
sudo iptables -C FORWARD -i "$DEFAULT_IFACE" -o "$TAP_DEV" -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null \
    || sudo iptables -A FORWARD -i "$DEFAULT_IFACE" -o "$TAP_DEV" -m state --state RELATED,ESTABLISHED -j ACCEPT

sudo rm -f $API_SOCKET
sudo $FC_DIR/firecracker --api-sock $API_SOCKET --config-file $SCRIPT_DIR/vm_config.json
