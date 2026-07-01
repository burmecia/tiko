#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ASSETS_DIR="$SCRIPT_DIR/../assets"
IMAGE="$ASSETS_DIR/ubuntu-24.04-rootfs.ext4"
ROOTFS=/tmp/rootfs
PG_INSTALL_DIR="$SCRIPT_DIR/../../target/pg-install"
PG_TGT_DIR="$ROOTFS/usr/local"
PG_HOME_DIR="$ROOTFS/var/lib/postgresql"

echo ">>> Install debootstrap..."
sudo apt update -qq
sudo apt install debootstrap -y >/dev/null 2>&1

echo ">>> Create and mount the image..."
dd if=/dev/zero of="$IMAGE" bs=1M count=1024
mkfs.ext4 "$IMAGE"
mkdir -p "$ROOTFS"
sudo mount "$IMAGE" "$ROOTFS"

echo ">>> Bootstrap Ubuntu 24.04 (Noble)..."
sudo debootstrap \
    --arch=amd64 \
    --variant=minbase \
    --include=systemd,systemd-sysv,udev,sudo,iproute2,iputils-ping,curl,vim,openssh-server \
    noble \
    "$ROOTFS" \
    http://archive.ubuntu.com/ubuntu >/dev/null 2>&1

echo ">>> Configure rootfs..."

# Bind-mount before chrooting
sudo mount --bind /proc "$ROOTFS/proc"
sudo mount --bind /sys "$ROOTFS/sys"
sudo mount --bind /dev "$ROOTFS/dev"
sudo mount --bind /dev/pts "$ROOTFS/dev/pts"

sudo chroot "$ROOTFS" /bin/bash << 'EOF'
# Set hostname
echo "tiko-vm" > /etc/hostname

# Set up /etc/hosts
cat > /etc/hosts << 'HOSTS'
127.0.0.1   localhost
127.0.1.1   tiko-vm
HOSTS

# Set root password
echo "root:root" | chpasswd

# Enable serial console for Firecracker (ttyS0)
systemctl enable serial-getty@ttyS0.service

# Set up sshd to allow root login
sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
systemctl enable ssh

# Configure static networking for the Firecracker tap interface (see start_vm.sh)
mkdir -p /etc/systemd/network
cat > /etc/systemd/network/20-eth0.network << 'NETWORK'
[Match]
Name=eth0

[Network]
Address=172.16.0.2/24
Gateway=172.16.0.1
DNS=8.8.8.8
NETWORK
systemctl enable systemd-networkd

# Set up fstab
cat > /etc/fstab << 'FSTAB'
/dev/vda  /     ext4  defaults,noatime  0 1
proc      /proc proc  defaults          0 0
sysfs     /sys  sysfs defaults          0 0
FSTAB

# Configure apt sources
cat > /etc/apt/sources.list << 'SOURCES'
deb http://archive.ubuntu.com/ubuntu noble main restricted universe multiverse
deb http://archive.ubuntu.com/ubuntu noble-updates main restricted universe multiverse
deb http://security.ubuntu.com/ubuntu noble-security main restricted universe multiverse
SOURCES

# Set timezone
echo "UTC" > /etc/timezone
ln -sf /usr/share/zoneinfo/UTC /etc/localtime

# Remove artifact of usr-merge
find / -maxdepth 1 -name "*.usr-is-merged" -type d -delete

# Create postgres user
useradd --system --create-home --home-dir /var/lib/postgresql --shell /bin/bash postgres

# Set up Tiko env vars
cat >> /var/lib/postgresql/.bash_profile << 'BASH_PROFILE'
export TIKO_ORG_ID="12"
export TIKO_DB_ID="34"
export TIKO_PROJECT_ID="56"
export TIKO_STORAGE_ROOT=/var/lib/postgresql/tiko_root
export TIKO_LOCAL_PATH=/var/lib/postgresql/tiko_local
BASH_PROFILE

chown postgres:postgres /var/lib/postgresql/.bash_profile

EOF

# Unmount in reverse order after chroot exits
sudo umount "$ROOTFS/dev/pts"
sudo umount "$ROOTFS/dev"
sudo umount "$ROOTFS/sys"
sudo umount "$ROOTFS/proc"

echo ">>> Installing Postgres..."
sudo cp -r $PG_INSTALL_DIR/* "$PG_TGT_DIR/"
sudo cp "$SCRIPT_DIR/start_pg.sh" "$PG_HOME_DIR"
sudo cp "$SCRIPT_DIR/../../postgresql.tiko.conf" "$PG_HOME_DIR"

echo ">>> Verifying image..."
sudo umount "$ROOTFS"
e2fsck -f "$IMAGE"

echo ">>> Done"
