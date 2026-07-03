#!/bin/bash
#
# Start a single Firecracker VM. Multiple VMs run in parallel by giving each a
# unique VM_ID (positional arg or env). Every per-instance resource is derived
# from VM_ID so two VMs never collide:
#
#   VM_ID  rootfs copy                 api socket          tap    subnet        guest mac
#   -----  --------------------------  -----------------  -----  ------------   ----------------
#   0      assets/rootfs-0.ext4        /tmp/fc-0.socket   tap0   172.16.0.0/24   AA:FC:00:00:00:02
#   1      assets/rootfs-1.ext4        /tmp/fc-1.socket   tap1   172.16.1.0/24   AA:FC:00:00:00:03
#
# Usage:
#   ./start_vm.sh [VM_ID] [--fresh]
#
#   VM_ID       non-negative integer (default 0, max 250). Env: VM_ID.
#   --fresh     discard any existing per-VM rootfs copy and rebuild from the
#               base image. Without it, an existing copy is reused (fast restart)
#               and only the network/identity files are re-injected.
#
# Per-VM Tiko identity (org/db/project) can be overridden via env vars. DB_ID
# distinguishes each Tiko database; default db == VM_ID so each VM is its own
# database under org 12:
#   TIKO_ORG_ID / TIKO_DB_ID / TIKO_PROJECT_ID

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ASSETS_DIR="$SCRIPT_DIR/../assets"
FC_DIR="$SCRIPT_DIR/../../firecracker/build/cargo_target/x86_64-unknown-linux-musl/debug"
BASE_IMAGE="$ASSETS_DIR/ubuntu-24.04-rootfs.ext4"

# ── Args ────────────────────────────────────────────────────────────────────
VM_ID="${VM_ID:-0}"
FRESH=0
while [ $# -gt 0 ]; do
    case "$1" in
        --fresh) FRESH=1; shift ;;
        *) VM_ID="$1"; shift ;;
    esac
done

if ! [[ "$VM_ID" =~ ^[0-9]+$ ]]; then
    echo "VM_ID must be a non-negative integer (got '$VM_ID')" >&2
    exit 1
fi
if [ "$VM_ID" -gt 250 ]; then
    echo "VM_ID must be <= 250 (MAC/subnet octet space)" >&2
    exit 1
fi

# ── Per-VM derived values ────────────────────────────────────────────────────
ROOTFS_COPY="$ASSETS_DIR/rootfs-${VM_ID}.ext4"
API_SOCKET="/tmp/fc-${VM_ID}.socket"
TAP_DEV="tap${VM_ID}"
SUBNET="172.16.${VM_ID}"
TAP_IP="${SUBNET}.1/24"
GUEST_IP="${SUBNET}.2"
GUEST_GW="${SUBNET}.1"
GUEST_MAC="AA:FC:00:00:00:$(printf '%02x' $((VM_ID + 2)))"
VM_CONFIG="$SCRIPT_DIR/vm_config-${VM_ID}.json"

# Per-VM Tiko identity (env-overridable). DB_ID distinguishes each Tiko
# database; default db == VM_ID so each VM is its own database under org 12.
export TIKO_ORG_ID="${TIKO_ORG_ID:-12}"
export TIKO_DB_ID="${TIKO_DB_ID:-$VM_ID}"
export TIKO_PROJECT_ID="${TIKO_PROJECT_ID:-56}"

# ── Rootfs copy (ext4 is single-writer; sharing one image corrupts it) ────────
if [ "$FRESH" -eq 1 ]; then
    rm -f "$ROOTFS_COPY"
fi
if [ ! -f "$ROOTFS_COPY" ]; then
    echo ">>> VM ${VM_ID}: copying base image -> $(basename "$ROOTFS_COPY")..."
    [ -f "$BASE_IMAGE" ] || { echo "base image not found: $BASE_IMAGE (run create_rootfs.sh first)" >&2; exit 1; }
    cp --sparse=always "$BASE_IMAGE" "$ROOTFS_COPY"
fi

# ── Inject per-VM network config + Tiko identity into the copy ────────────────
echo ">>> VM ${VM_ID}: injecting network + Tiko identity..."
ROOTFS_MNT="$(mktemp -d)"
sudo mount "$ROOTFS_COPY" "$ROOTFS_MNT"

# Guest static networking on this VM's /24.
sudo tee "$ROOTFS_MNT/etc/systemd/network/20-eth0.network" >/dev/null <<NETWORK
[Match]
Name=eth0

[Network]
Address=${GUEST_IP}/24
Gateway=${GUEST_GW}
DNS=1.1.1.1
NETWORK

# Per-VM hostname.
echo "tiko-vm-${VM_ID}" | sudo tee "$ROOTFS_MNT/etc/hostname" >/dev/null

# Per-VM Tiko identity (single source of truth, sourced by init_pg.sh /
# start_pg.sh via tiko_env.sh, and by .bash_profile).
sudo tee "$ROOTFS_MNT/var/lib/postgresql/tiko.env" >/dev/null <<TIKO_ENV
TIKO_ORG_ID=${TIKO_ORG_ID}
TIKO_DB_ID=${TIKO_DB_ID}
TIKO_PROJECT_ID=${TIKO_PROJECT_ID}
TIKO_STORAGE_ROOT=/mnt/s3files/tiko_root
TIKO_LOCAL_PATH=/var/lib/postgresql/tiko_local
TIKO_ENV

sudo umount "$ROOTFS_MNT"
rmdir "$ROOTFS_MNT"

# ── Host networking: per-VM tap + NAT ────────────────────────────────────────
echo ">>> VM ${VM_ID}: host tap ${TAP_DEV} (${TAP_IP})..."
if ! ip link show "$TAP_DEV" &>/dev/null; then
    sudo ip tuntap add "$TAP_DEV" mode tap
    sudo ip addr add "$TAP_IP" dev "$TAP_DEV"
    sudo ip link set "$TAP_DEV" up
fi

DEFAULT_IFACE="$(ip route show default | awk '/default/ {print $5; exit}')"
sudo sysctl -w net.ipv4.ip_forward=1 >/dev/null
# MASQUERADE on the default iface is shared across all VMs; idempotent.
sudo iptables -t nat -C POSTROUTING -o "$DEFAULT_IFACE" -j MASQUERADE 2>/dev/null \
    || sudo iptables -t nat -A POSTROUTING -o "$DEFAULT_IFACE" -j MASQUERADE
# Per-VM FORWARD rules (tap-specific).
sudo iptables -C FORWARD -i "$TAP_DEV" -o "$DEFAULT_IFACE" -j ACCEPT 2>/dev/null \
    || sudo iptables -A FORWARD -i "$TAP_DEV" -o "$DEFAULT_IFACE" -j ACCEPT
sudo iptables -C FORWARD -i "$DEFAULT_IFACE" -o "$TAP_DEV" -m state --state RELATED,ESTABLISHED -j ACCEPT 2>/dev/null \
    || sudo iptables -A FORWARD -i "$DEFAULT_IFACE" -o "$TAP_DEV" -m state --state RELATED,ESTABLISHED -j ACCEPT

# ── Firecracker config (per-VM) ──────────────────────────────────────────────
echo ">>> VM ${VM_ID}: writing firecracker config..."
cat > "$VM_CONFIG" <<CONFIG
{
    "boot-source": {
        "kernel_image_path": "$ASSETS_DIR/vmlinux-6.1",
        "boot_args": "root=/dev/vda rw console=ttyS0 reboot=k panic=1 pci=off systemd.unified_cgroup_hierarchy=0",
        "initrd_path": null
    },
    "drives": [
        {
            "drive_id": "rootfs",
            "partuuid": null,
            "is_root_device": true,
            "cache_type": "Unsafe",
            "is_read_only": false,
            "path_on_host": "$ROOTFS_COPY",
            "io_engine": "Sync",
            "rate_limiter": null,
            "socket": null
        }
    ],
    "machine-config": {
        "vcpu_count": 2,
        "mem_size_mib": 512,
        "smt": false,
        "track_dirty_pages": false,
        "huge_pages": "None"
    },
    "cpu-config": null,
    "balloon": null,
    "network-interfaces": [
        {
            "iface_id": "eth0",
            "guest_mac": "$GUEST_MAC",
            "host_dev_name": "$TAP_DEV"
        }
    ],
    "vsock": null,
    "logger": null,
    "metrics": null,
    "mmds-config": null,
    "entropy": null,
    "pmem": [],
    "memory-hotplug": null
}
CONFIG

echo ">>> VM ${VM_ID}: launching firecracker (socket $API_SOCKET)..."
sudo rm -f "$API_SOCKET"
sudo "$FC_DIR/firecracker" --api-sock "$API_SOCKET" --config-file "$VM_CONFIG"
