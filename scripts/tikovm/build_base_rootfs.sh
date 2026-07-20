#!/bin/bash
# =============================================================================
# Build the tikovm-family BASE rootfs (a from-scratch Ubuntu 24.04 image).
#
# This is to the tikovm platform what scripts/create_rootfs.sh is to the tikod
# platform: a SHARED READ-ONLY base image, attached as /dev/vda to every VM
# and used as the overlayfs lower layer (see scripts/initramfs_init.sh).
# Per-VM mutable state lives on a separate small RW overlay image (/dev/vdb),
# so this base is stored exactly once regardless of VM count.
#
# Differences vs create_rootfs.sh:
#   * Lean minbase (no python/vim/wget, no Postgres, no PostgREST).
#   * No S3 Files mount, no efs-utils, no AWS creds, no postgres user.
#   * No legacy tikoguest agent (masked to /dev/null defensively).
#   * Pre-creates conventional local_fast (/mnt/data) and remote_slow
#     (/mnt/archive) mount-point placeholders. The actual mount happens at
#     boot: tikovm-guestd mounts each manifest volume by ext4 LABEL at its
#     declared mount_path (tikovm-guest/src/fs.rs), creating the dir if
#     missing — these dirs are just conventional seeds.
#   * Bakes an SSH authorized_keys (dev/debug) via TIKOVM_SSH_PUBKEY /
#     TIKOVM_SSH_PUBKEY_FILE, so derivative rootfs inherit SSH access.
#
# Derivative rootfs scripts (e.g. build_echo_rootfs.sh) copy this image and
# inject their own payload (tikovm-guestd, workload binaries, manifest).
#
# Output: tikod/assets/tikovm-base-rootfs.ext4
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ASSETS_DIR="$SCRIPT_DIR/../../tikod/assets"
IMAGE="$ASSETS_DIR/tikovm-base-rootfs.ext4"
ROOTFS=/tmp/tikovm-base-rootfs
# Default 1536 MB: comfortably fits the minbase install + headroom for
# derivative rootfs payloads. Override with ROOTFS_SIZE_MB at build time.
ROOTFS_SIZE_MB="${ROOTFS_SIZE_MB:-1536}"

echo ">>> Install debootstrap..."
sudo apt update -qq
sudo apt install debootstrap -y >/dev/null 2>&1

echo ">>> Create and mount the image ($ROOTFS_SIZE_MB MB)..."
mkdir -p "$ASSETS_DIR"
rm -f "$IMAGE"
truncate -s "${ROOTFS_SIZE_MB}M" "$IMAGE"
mkfs.ext4 -q "$IMAGE"
mkdir -p "$ROOTFS"
sudo umount "$ROOTFS" >/dev/null 2>&1 || true
sudo mount -o loop "$IMAGE" "$ROOTFS"

echo ">>> Bootstrap Ubuntu 24.04 (Noble, minbase)..."
sudo debootstrap \
    --arch=amd64 \
    --variant=minbase \
    --components=main,universe \
    --include=systemd,systemd-sysv,udev,sudo,iproute2,iputils-ping,curl,openssh-server,ca-certificates \
    noble \
    "$ROOTFS" \
    http://archive.ubuntu.com/ubuntu >/dev/null 2>&1

echo ">>> Configure rootfs..."

# Bind-mount before chrooting
sudo mount --bind /proc "$ROOTFS/proc"
sudo mount --bind /sys  "$ROOTFS/sys"
sudo mount --bind /dev  "$ROOTFS/dev"
sudo mount --bind /dev/pts "$ROOTFS/dev/pts"

sudo chroot "$ROOTFS" /bin/bash << 'EOF'
# Set hostname
echo "tiko-vm" > /etc/hostname

# Set up /etc/hosts
cat > /etc/hosts << 'HOSTS'
127.0.0.1   localhost
127.0.1.1   tiko-vm
HOSTS

# Set root password (dev/debug; SSH pubkey is the primary access path)
echo "root:root" | chpasswd

# Enable serial console for Firecracker (ttyS0)
systemctl enable serial-getty@ttyS0.service

# Set up sshd to allow root login
sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
systemctl enable ssh

# Configure static networking for the Firecracker tap interface.
# (Per-VM IP is rewritten at provision time by tikovm-hostd; this is the
# vm-0 default.)
mkdir -p /etc/systemd/network
cat > /etc/systemd/network/20-eth0.network << 'NETWORK'
[Match]
Name=eth0

[Network]
Address=172.16.0.2/24
Gateway=172.16.0.1
DNS=1.1.1.1
NETWORK
systemctl enable systemd-networkd

# minbase doesn't include systemd-resolved, so DNS= above isn't consumed by
# anything - point resolv.conf at the same DNS server directly.
cat > /etc/resolv.conf << 'RESOLV'
nameserver 1.1.1.1
RESOLV

# Set up fstab. The root is an overlayfs assembled by the initramfs
# (lowerdir=/dev/vda = this RO base, upperdir=/dev/vdb = per-VM RW overlay),
# so do NOT list /dev/vda as the root here — that would make systemd try to
# remount it over the overlay. Only the kernel VFSes + /tmp need an entry
# (/run is auto-tmpfs by systemd).
cat > /etc/fstab << 'FSTAB'
proc      /proc proc  defaults                0 0
sysfs     /sys  sysfs defaults                0 0
tmpfs     /tmp  tmpfs defaults,nosuid,nodev   0 0
FSTAB

# Configure apt sources (derivative rootfs scripts may apt-install payloads)
cat > /etc/apt/sources.list << 'SOURCES'
deb http://archive.ubuntu.com/ubuntu noble main restricted universe multiverse
deb http://archive.ubuntu.com/ubuntu noble-updates main restricted universe multiverse
deb http://security.ubuntu.com/ubuntu noble-security main restricted universe multiverse
SOURCES

# Conventional mount-point placeholders for tikovm volumes. The actual mount
# happens at boot: tikovm-guestd mounts each manifest volume by ext4 LABEL
# at its declared mount_path (tikovm-guest/src/fs.rs). These dirs match the
# echo manifest convention (local_fast=data, remote_slow=archive); workloads
# are free to declare other paths — guestd creates them on demand.
mkdir -p /mnt/data /mnt/archive

# Set timezone
echo "UTC" > /etc/timezone
ln -sf /usr/share/zoneinfo/UTC /etc/localtime

# Remove artifact of usr-merge
find / -maxdepth 1 -name "*.usr-is-merged" -type d -delete

# Defensively mask the legacy Tiko agent from the tikod platform. This base
# never installs it, but derivative rootfs built from a mix of bases should
# never accidentally start it inside a tikovm VM.
ln -sf /dev/null /etc/systemd/system/tikoguest.service
EOF

# Unmount in reverse order after chroot exits
sudo umount "$ROOTFS/dev/pts"
sudo umount "$ROOTFS/dev"
sudo umount "$ROOTFS/sys"
sudo umount "$ROOTFS/proc"

echo ">>> Baking SSH access (root authorized_keys)..."
# Same resolution order as build_echo_rootfs.sh: env var > file env var >
# current user's default pubkey. Warn + skip if none found.
sudo mkdir -p "$ROOTFS/etc/systemd/system/multi-user.target.wants"
sudo ln -sf /usr/lib/systemd/system/ssh.service \
            "$ROOTFS/etc/systemd/system/multi-user.target.wants/ssh.service"
PUBKEY=${TIKOVM_SSH_PUBKEY:-}
if [ -z "$PUBKEY" ]; then
  for f in ${TIKOVM_SSH_PUBKEY_FILE:-} "$HOME/.ssh/id_ed25519.pub" "$HOME/.ssh/id_rsa.pub"; do
    if [ -n "$f" ] && [ -f "$f" ]; then PUBKEY=$(cat "$f"); break; fi
  done
fi
if [ -z "$PUBKEY" ]; then
  echo "    WARNING: no SSH pubkey found; skipping authorized_keys" >&2
  echo "             (set TIKOVM_SSH_PUBKEY or TIKOVM_SSH_PUBKEY_FILE to enable ssh)" >&2
else
  sudo install -d -m700 "$ROOTFS/root/.ssh"
  echo "$PUBKEY" | sudo tee "$ROOTFS/root/.ssh/authorized_keys" >/dev/null
  sudo chmod 600 "$ROOTFS/root/.ssh/authorized_keys"
  echo "    baked SSH pubkey for root"
fi

echo ">>> Verifying image..."
sudo umount "$ROOTFS"
# -y: auto-answer yes so non-interactive builds don't abort on minor dirt.
e2fsck -fy "$IMAGE"

echo ">>> Done: $IMAGE"
echo "    Derivative rootfs scripts should copy this image and inject their payload."
