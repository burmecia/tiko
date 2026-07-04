#!/bin/bash
#
# Build the Tiko initramfs (assets/tiko-initramfs.cpio.gz).
#
# The initramfs contains just busybox + an /init that sets up an overlayfs
# root across the two guest block devices:
#
#   /dev/vda  RO  shared immutable base image  (overlayfs lower)
#   /dev/vdb  RW  per-VM writable image        (overlayfs upper + work)
#
# After assembling the overlay it switch_roots into systemd on the base image.
# This is what lets every VM share one read-only base image while keeping its
# own writable state, so a root-fs upgrade is just swapping the base image.
#
# Run on a Linux host (the guest kernel is x86_64). Produces a gzipped newc
# cpio, the format Firecracker consumes via boot-source.initrd_path.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ASSETS_DIR="$SCRIPT_DIR/../assets"
INITRAMFS="$ASSETS_DIR/tiko-initramfs.cpio.gz"
INIT_SRC="$SCRIPT_DIR/initramfs_init.sh"

echo ">>> Ensuring busybox-static + cpio are installed..."
if ! command -v busybox >/dev/null 2>&1; then
    sudo apt update -qq && sudo apt install -y busybox-static cpio >/dev/null 2>&1
fi
command -v cpio >/dev/null 2>&1 || sudo apt install -y cpio >/dev/null 2>&1

# busybox-static is a fully-statically-linked binary, so it runs inside the
# initramfs with no shared libraries from the host.
BUSYBOX="$(command -v busybox)"
if ! file "$BUSYBOX" | grep -qi 'statically linked'; then
    echo "WARNING: $BUSYBOX is not statically linked; the initramfs may fail to boot." >&2
    echo "         Install busybox-static (apt install busybox-static)." >&2
fi

WORK="$(mktemp -d)"
trap 'sudo rm -rf "$WORK"' EXIT

echo ">>> Assembling initramfs tree..."
mkdir -p "$WORK"/{bin,proc,sys,dev,lower,upper-disk,sysroot}

# /init — the overlay setup script.
install -m 0755 "$INIT_SRC" "$WORK/init"

# busybox + its applet symlinks (installed at /init runtime, but ship the
# binary itself).
cp "$BUSYBOX" "$WORK/bin/busybox"
chmod 0755 "$WORK/bin/busybox"

# Minimal static device nodes so the kernel can wire up the console before
# /init mounts devtmpfs. (mknod needs root.)
sudo mknod -m 0600 "$WORK/dev/console" c 5 1
sudo mknod -m 0666 "$WORK/dev/null"    c 1 3
sudo mknod -m 0666 "$WORK/dev/zero"    c 1 5

echo ">>> Packing cpio.gz -> $INITRAMFS..."
# newc format + gzip; owned by root so the guest sees clean ownership.
( cd "$WORK" \
    && sudo find . -print0 \
    | sudo cpio --null --create --format=newc --owner=root:root 2>/dev/null \
    | gzip -9 ) > "$INITRAMFS"

echo ">>> Done: $(ls -lh "$INITRAMFS" | awk '{print $5}')  $INITRAMFS"
