#!/bin/bash
#
# Start a single Firecracker VM using the two-drive overlay model:
#
#   /dev/vda  RO  assets/ubuntu-24.04-rootfs.ext4   SHARED immutable base
#   /dev/vdb  RW  assets/overlay-<id>.ext4          per-VM mutable overlay
#
# The base image is attached read-only and shared by ALL VMs (a root-fs upgrade
# is just swapping this one file). The overlay image is small, sparse, and
# per-VM; it holds the overlayfs upper/work layers plus the per-VM network +
# Tiko-identity files (seeded under upper/ so they shadow the base). An
# initramfs (assets/tiko-initramfs.cpio.gz) glues the two into a writable root
# via overlayfs, then boots systemd.
#
# Multiple VMs run in parallel by giving each a unique VM_ID (positional arg or
# env). Every per-instance resource is derived from VM_ID so two VMs never
# collide:
#
#   VM_ID  overlay image            api socket          tap     subnet        guest mac
#   -----  -----------------------  -----------------   ------  ------------   ----------------
#   0      assets/overlay-0.ext4    /tmp/fc-0.socket    tap0    172.16.0.0/24   AA:FC:00:00:00:02
#   1      assets/overlay-1.ext4    /tmp/fc-1.socket    tap1    172.16.1.0/24   AA:FC:00:00:00:03
#
# Usage:
#   ./start_vm.sh [VM_ID] [--fresh]
#
#   VM_ID     non-negative integer (default 0, max 250). Env: VM_ID.
#   --fresh   discard any existing per-VM overlay image and rebuild it from
#             scratch (a clean guest /var, /etc, etc.). Without it, an existing
#             overlay is reused (fast restart) and only the network/identity
#             files are re-seeded.
#
# Per-VM Tiko identity (org/db/project) can be overridden via env vars. DB_ID
# distinguishes each Tiko database; default db == VM_ID so each VM is its own
# database under org 12:
#   TIKO_ORG_ID / TIKO_DB_ID / TIKO_PROJECT_ID
#
# Build the base + initramfs first:
#   ./create_rootfs.sh && ./build_initramfs.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ASSETS_DIR="$SCRIPT_DIR/../assets"
FC_DIR="$SCRIPT_DIR/../../firecracker/build/cargo_target/x86_64-unknown-linux-musl/debug"
BASE_IMAGE="$ASSETS_DIR/ubuntu-24.04-rootfs.ext4"
INITRAMFS="$ASSETS_DIR/tiko-initramfs.cpio.gz"

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
OVERLAY_IMAGE="$ASSETS_DIR/overlay-${VM_ID}.ext4"
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

# ── Prechecks ────────────────────────────────────────────────────────────────
[ -f "$BASE_IMAGE" ] || { echo "base image not found: $BASE_IMAGE (run create_rootfs.sh first)" >&2; exit 1; }
[ -f "$INITRAMFS" ]  || { echo "initramfs not found: $INITRAMFS (run build_initramfs.sh first)" >&2; exit 1; }

# ── Per-VM overlay image (the only per-VM storage) ───────────────────────────
# Sparse: only blocks the guest actually writes consume host disk. Default
# 2 GB; override with OVERLAY_SIZE_MB.
OVERLAY_SIZE_MB="${OVERLAY_SIZE_MB:-2048}"

if [ "$FRESH" -eq 1 ]; then
    rm -f "$OVERLAY_IMAGE"
fi
if [ ! -f "$OVERLAY_IMAGE" ]; then
    echo ">>> VM ${VM_ID}: creating overlay image ($(basename "$OVERLAY_IMAGE"), ${OVERLAY_SIZE_MB} MB sparse)..."
    truncate -s "${OVERLAY_SIZE_MB}M" "$OVERLAY_IMAGE"
    mkfs.ext4 -q "$OVERLAY_IMAGE"
fi

# Seed the per-VM files into the overlay's `upper/` tree. overlayfs merges the
# upper directory with the RO base: a file present in upper shadows the same
# path in the base, so this is how each VM gets its own network config,
# hostname, and Tiko identity without touching the shared base image.
#
# IMPORTANT: overlayfs takes a *merged* directory's ownership/mode from the
# UPPER entry. So any directory we pre-create under upper/ as root would
# otherwise shadow the base's ownership — e.g. /var/lib/postgresql is
# postgres-owned in the base but would become root-owned. We therefore mount
# the base RO and replicate ownership+mode (--reference) for every dir/file we
# place in upper/, keeping the overlay's metadata identical to the base.
echo ">>> VM ${VM_ID}: seeding overlay (network + Tiko identity)..."
OV_MNT="$(mktemp -d)"
BASE_RO="$(mktemp -d)"
sudo mount "$OVERLAY_IMAGE" "$OV_MNT"
sudo mount -o ro,loop "$BASE_IMAGE" "$BASE_RO"
cleanup_overlay() {
    sudo umount "$OV_MNT" 2>/dev/null || true
    sudo umount "$BASE_RO" 2>/dev/null || true
    rmdir "$OV_MNT" "$BASE_RO" 2>/dev/null || true
}
trap cleanup_overlay EXIT

# Replicate a directory from the base into upper/, preserving ownership + mode.
mirror_dir() {
    local rel="$1"
    local src="$BASE_RO/$rel" dst="$OV_MNT/upper/$rel"
    sudo mkdir -p "$dst"
    [ -e "$src" ] && sudo chown --reference="$src" "$dst" && sudo chmod --reference="$src" "$dst" || true
}

for d in etc etc/systemd etc/systemd/network var var/lib var/lib/postgresql; do
    mirror_dir "$d"
done
sudo mkdir -p "$OV_MNT/work"

# Guest static networking on this VM's /24.
sudo tee "$OV_MNT/upper/etc/systemd/network/20-eth0.network" >/dev/null <<NETWORK
[Match]
Name=eth0

[Network]
Address=${GUEST_IP}/24
Gateway=${GUEST_GW}
DNS=1.1.1.1
NETWORK
sudo chown --reference="$BASE_RO/etc/systemd/network/20-eth0.network" \
    "$OV_MNT/upper/etc/systemd/network/20-eth0.network" 2>/dev/null || true

# Per-VM hostname.
echo "tiko-vm-${VM_ID}" | sudo tee "$OV_MNT/upper/etc/hostname" >/dev/null
sudo chown --reference="$BASE_RO/etc/hostname" "$OV_MNT/upper/etc/hostname" 2>/dev/null || true

# Per-VM Tiko identity (single source of truth, sourced by init_pg.sh /
# start_pg.sh via tiko_env.sh, and by .bash_profile).
sudo tee "$OV_MNT/upper/var/lib/postgresql/tiko.env" >/dev/null <<TIKO_ENV
TIKO_ORG_ID=${TIKO_ORG_ID}
TIKO_DB_ID=${TIKO_DB_ID}
TIKO_PROJECT_ID=${TIKO_PROJECT_ID}
TIKO_STORAGE_ROOT=/mnt/s3files/tiko_root
TIKO_LOCAL_PATH=/var/lib/postgresql/tiko_local
TIKO_ENV
sudo chown --reference="$BASE_RO/var/lib/postgresql/tiko.env" \
    "$OV_MNT/upper/var/lib/postgresql/tiko.env" 2>/dev/null || true

sudo umount "$OV_MNT"
sudo umount "$BASE_RO"
rmdir "$OV_MNT" "$BASE_RO" 2>/dev/null || true
trap - EXIT

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
# Two drives: the shared RO base (vda) + the per-VM RW overlay (vdb). The
# initramfs assembles an overlayfs root from them, so boot_args no longer names
# a root device — /init handles that.
echo ">>> VM ${VM_ID}: writing firecracker config..."
cat > "$VM_CONFIG" <<CONFIG
{
    "boot-source": {
        "kernel_image_path": "$ASSETS_DIR/vmlinux-6.1",
        "initrd_path": "$INITRAMFS",
        "boot_args": "console=ttyS0 reboot=k panic=1 pci=off systemd.unified_cgroup_hierarchy=0"
    },
    "drives": [
        {
            "drive_id": "rootfs",
            "partuuid": null,
            "is_root_device": true,
            "cache_type": "Unsafe",
            "is_read_only": true,
            "path_on_host": "$BASE_IMAGE",
            "io_engine": "Sync",
            "rate_limiter": null,
            "socket": null
        },
        {
            "drive_id": "overlay",
            "partuuid": null,
            "is_root_device": false,
            "cache_type": "Unsafe",
            "is_read_only": false,
            "path_on_host": "$OVERLAY_IMAGE",
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
