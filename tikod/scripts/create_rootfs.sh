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
# This is the SHARED READ-ONLY base image: immutable OS + pre-baked files,
# attached as /dev/vda to every VM and used as the overlayfs lower layer (see
# scripts/initramfs_init.sh). Per-VM mutable state lives on a separate small
# RW overlay image, so this 5 GB base is stored exactly once regardless of VM
# count. Default 5 GB; override with ROOTFS_SIZE_MB at build time.
ROOTFS_SIZE_MB="${ROOTFS_SIZE_MB:-5120}"
rm -f "$IMAGE"
truncate -s "${ROOTFS_SIZE_MB}M" "$IMAGE"
mkfs.ext4 "$IMAGE"
mkdir -p "$ROOTFS"
sudo umount "$ROOTFS" >/dev/null 2>&1 || true
sudo mount "$IMAGE" "$ROOTFS"

echo ">>> Bootstrap Ubuntu 24.04 (Noble)..."
sudo debootstrap \
    --arch=amd64 \
    --variant=minbase \
    --components=main,universe \
    --include=systemd,systemd-sysv,udev,sudo,iproute2,iputils-ping,curl,vim,openssh-server,ca-certificates,wget,python3,python3-pip,python3-venv \
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
# remount it over the overlay. Only the kernel VFSes + /tmp need an entry; the
# S3 Files network mount is appended below (and /run is auto-tmpfs by systemd).
cat > /etc/fstab << 'FSTAB'
proc      /proc proc  defaults                0 0
sysfs     /sys  sysfs defaults                0 0
tmpfs     /tmp  tmpfs defaults,nosuid,nodev   0 0
FSTAB

# Configure apt sources
cat > /etc/apt/sources.list << 'SOURCES'
deb http://archive.ubuntu.com/ubuntu noble main restricted universe multiverse
deb http://archive.ubuntu.com/ubuntu noble-updates main restricted universe multiverse
deb http://security.ubuntu.com/ubuntu noble-security main restricted universe multiverse
SOURCES

# Install amazon-efs-utils (adds its own apt repo, then installs the package)
export DEBIAN_FRONTEND=noninteractive
curl -fsSL https://amazon-efs-utils.aws.com/efs-utils-installer.sh | sh -s -- --install

# Install botocore
pip3 install --target /usr/lib/python3/dist-packages botocore >/dev/null 2>&1

# Create s3 files mount point
mkdir -p /mnt/s3files

# S3 Files: AWS client config. The guest has no IMDS, so the efs-utils mount
# helper reads credentials from /root/.aws/credentials (written after the
# chroot, from env/creds-file, so the secret isn't in this committed script).
mkdir -p /root/.aws
cat > /root/.aws/config << 'AWS_CFG'
[default]
region = ap-southeast-2
AWS_CFG
: > /root/.aws/credentials
chmod 600 /root/.aws/credentials

# Monitor TLS mount health at boot.
systemctl enable amazon-efs-mount-watchdog 2>/dev/null || true

# Set timezone
echo "UTC" > /etc/timezone
ln -sf /usr/share/zoneinfo/UTC /etc/localtime

# Remove artifact of usr-merge
find / -maxdepth 1 -name "*.usr-is-merged" -type d -delete

# Create postgres user
useradd --system --create-home --home-dir /var/lib/postgresql --shell /bin/bash postgres
usermod -aG sudo postgres
echo "postgres ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/postgres
chmod 0440 /etc/sudoers.d/postgres

# /mnt/s3files is mounted at boot (fstab); the mounted root inode is owned by
# root, so chowning the bare mountpoint dir is hidden by the mount. Chown the
# mounted root after it comes up. Root has ClientRootAccess, so it can chown
# the root inode, and the new owner persists in S3 Files metadata across
# remounts (idempotent). Skip gracefully if the mount is absent (nofail).
cat > /etc/systemd/system/s3files-postgres-owner.service << 'UNIT'
[Unit]
Description=Make S3 Files mount (/mnt/s3files) writable by postgres
After=mnt-s3files.mount

[Service]
Type=oneshot
ConditionPathIsMountPoint=/mnt/s3files
ExecStart=/bin/chown postgres:postgres /mnt/s3files
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
UNIT
systemctl enable s3files-postgres-owner.service

# Tiko identity + storage paths. tiko.env is the single source of truth,
# sourced by .bash_profile (login shells) and the PG scripts (via tiko_env.sh).
# start_vm.sh rewrites it per-VM (host side) so each VM is a distinct project.
# These are the VM-0 defaults for the base image.
cat > /var/lib/postgresql/tiko.env << 'TIKO_ENV'
TIKO_ORG_ID=12
TIKO_DB_ID=34
TIKO_PROJECT_ID=56
TIKO_STORAGE_ROOT=/mnt/s3files/tiko_root
TIKO_LOCAL_PATH=/var/lib/postgresql/tiko_local
TIKO_ENV

cat > /var/lib/postgresql/.bash_profile << 'BASH_PROFILE'
[ -f ~/tiko.env ] && set -a && . ~/tiko.env && set +a
BASH_PROFILE

chown postgres:postgres /var/lib/postgresql/tiko.env /var/lib/postgresql/.bash_profile

EOF

# Unmount in reverse order after chroot exits
sudo umount "$ROOTFS/dev/pts"
sudo umount "$ROOTFS/dev"
sudo umount "$ROOTFS/sys"
sudo umount "$ROOTFS/proc"

echo ">>> Installing Postgres..."
sudo cp -r $PG_INSTALL_DIR/* "$PG_TGT_DIR/"
sudo cp "$SCRIPT_DIR/start_pg.sh" "$SCRIPT_DIR/init_pg.sh" "$SCRIPT_DIR/tiko_env.sh" "$PG_HOME_DIR"
sudo chmod +x "$PG_HOME_DIR/start_pg.sh" "$PG_HOME_DIR/init_pg.sh" "$PG_HOME_DIR/tiko_env.sh"
sudo cp "$SCRIPT_DIR/../../postgresql.tiko.conf" "$PG_HOME_DIR"

echo ">>> Installing tikoguest guest agent..."
# Build the control agent (release) and bake it into the image. tikod reaches
# it over the guest IP at :9000 (see tikod/src/guestcontrol.rs).
( cd "$SCRIPT_DIR/../.." && cargo build --release -p tikoguest )
sudo install -m755 "$SCRIPT_DIR/../../target/release/tikoguest" "$ROOTFS/usr/local/bin/tikoguest"
sudo install -m644 "$SCRIPT_DIR/tikoguest.service" "$ROOTFS/etc/systemd/system/tikoguest.service"
# Enable at boot by creating the wants symlink (equivalent to `systemctl enable`
# from inside the chroot, done host-side since the chroot phase already ran).
sudo mkdir -p "$ROOTFS/etc/systemd/system/multi-user.target.wants"
sudo ln -sf /etc/systemd/system/tikoguest.service \
    "$ROOTFS/etc/systemd/system/multi-user.target.wants/tikoguest.service"

echo ">>> Installing CLI tools..."
( cd "$SCRIPT_DIR/../.." && cargo build --release -p cli )
sudo mkdir -p "$ROOTFS/usr/local/libexec"
for bin in tiko_tlseg_viewer; do
    sudo install -m755 "$SCRIPT_DIR/../../target/release/$bin" "$ROOTFS/usr/local/bin/$bin"
done
# tiko_pitr / tiko_branch / tiko_restore: real binary in libexec, wrapper at
# /usr/local/bin sources tiko_env.sh so identity/storage/PGDATA are set up
# automatically.
for bin in tiko_pitr tiko_branch tiko_restore; do
    sudo install -m755 "$SCRIPT_DIR/../../target/release/$bin" "$ROOTFS/usr/local/libexec/$bin"
    sudo install -m755 "$SCRIPT_DIR/$bin.sh" "$ROOTFS/usr/local/bin/$bin"
done

echo ">>> Configuring S3 Files auto-mount..."

# Credentials: prefer env vars, else a gitignored assets/s3files-creds.env
# (format: S3FILES_AWS_ACCESS_KEY_ID=... / S3FILES_AWS_SECRET_ACCESS_KEY=...).
S3FILES_CREDS_ENV="$ASSETS_DIR/s3files-creds.env"
if [ -z "${S3FILES_AWS_ACCESS_KEY_ID:-}" ] && [ -f "$S3FILES_CREDS_ENV" ]; then
    . "$S3FILES_CREDS_ENV"
fi
if [ -n "${S3FILES_AWS_ACCESS_KEY_ID:-}" ] && [ -n "${S3FILES_AWS_SECRET_ACCESS_KEY:-}" ]; then
    sudo tee "$ROOTFS/root/.aws/credentials" > /dev/null <<CREDS
[default]
aws_access_key_id = ${S3FILES_AWS_ACCESS_KEY_ID}
aws_secret_access_key = ${S3FILES_AWS_SECRET_ACCESS_KEY}
CREDS
    sudo chmod 600 "$ROOTFS/root/.aws/credentials"
    echo "    credentials baked in (IAM user tiko-s3files-vm)."
else
    echo "    WARNING: S3FILES_AWS_* not set and no assets/s3files-creds.env." >&2
    echo "    /root/.aws/credentials left empty; S3 Files will NOT auto-mount." >&2
fi

# fstab: auto-mount at boot. _netdev waits for networking; nofail => no hang.
S3FILES_FS_ID="${S3FILES_FS_ID:-fs-02b6905b6653757b6}"
S3FILES_MT_IP="${S3FILES_MOUNT_TARGET_IP:-172.31.38.90}"   # ap-southeast-2a / apse2-az3
sudo tee -a "$ROOTFS/etc/fstab" > /dev/null <<FSTAB_S3
${S3FILES_FS_ID}:/ /mnt/s3files s3files _netdev,nofail,mounttargetip=${S3FILES_MT_IP},tls,iam 0 0
FSTAB_S3

# Ship the manual mount helper into the guest for ad-hoc use.
sudo cp "$SCRIPT_DIR/mount_s3files_vm.sh" "$ROOTFS/usr/local/sbin/mount-s3files"
sudo chmod +x "$ROOTFS/usr/local/sbin/mount-s3files"

echo ">>> Verifying image..."
sudo umount "$ROOTFS"
# -y: auto-answer yes so non-interactive builds don't abort on minor dirt.
e2fsck -fy "$IMAGE"

echo ">>> Done"
