#!/bin/busybox sh
#
# Tiko initramfs /init — sets up an overlayfs root from two block devices.
#
#   /dev/vda  → shared, READ-ONLY base image (immutable OS + pre-baked files)
#   /dev/vdb  → per-VM, READ-WRITE image (overlayfs upper + work dirs)
#
# The overlay makes the entire base filesystem appear writable: every write
# transparently lands on /dev/vdb, so the base image (/dev/vda) stays pristine
# and is shared unchanged across all VMs. The per-VM identity/network files
# live in /dev/vdb's `upper/` tree (seeded at image-creation time) and shadow
# the base's versions via overlayfs.
#
# After the overlay is mounted at /sysroot we move the kernel VFSes into it
# and switch_root into systemd. From the guest's perspective it boots from a
# normal writable ext4 root.

set -eu

export PATH=/bin

# Drop to an interactive shell on failure (reachable over the serial console).
rescue_shell() {
    echo "tiko-init: dropping to rescue shell"
    exec /bin/busybox sh
}

# Populate /bin with busybox applet symlinks (mount, switch_root, mkdir, …).
/bin/busybox --install -s /bin 2>/dev/null || true

# Kernel VFSes. devtmpfs gives us /dev/vda, /dev/vdb, /dev/console, …
mount -t proc     none /proc 2>/dev/null || true
mount -t sysfs    none /sys  2>/dev/null || true
mount -t devtmpfs none /dev  2>/dev/null || true

# Wait (briefly) for the two virtio-blk devices to be probed.
i=0
while [ "$i" -lt 100 ]; do
    [ -b /dev/vda ] && [ -b /dev/vdb ] && break
    i=$((i + 1))
    sleep 0.1
done
[ -b /dev/vda ] || { echo "tiko-init: /dev/vda (RO base) missing"; rescue_shell; }
[ -b /dev/vdb ] || { echo "tiko-init: /dev/vdb (RW overlay) missing"; rescue_shell; }

mkdir -p /lower /upper-disk /sysroot

# RO base = overlayfs lower layer.
mount -t ext4 -o ro /dev/vda /lower || { echo "tiko-init: mount /dev/vda ro failed"; rescue_shell; }

# RW per-VM image = backing store for overlayfs upper + work.
mount -t ext4 /dev/vdb /upper-disk || { echo "tiko-init: mount /dev/vdb rw failed"; rescue_shell; }

# upper/ holds file-level changes (created/seeded at image build, or grown at
# runtime); work/ is overlayfs scratch (must exist, must be empty on first use).
mkdir -p /upper-disk/upper /upper-disk/work

# Assemble the overlay root.
mount -t overlay overlay \
    -o "lowerdir=/lower,upperdir=/upper-disk/upper,workdir=/upper-disk/work" \
    /sysroot || { echo "tiko-init: overlay mount failed"; rescue_shell; }

# Hand the kernel VFSes to the real root before we switch into it.
mount --move /proc /sysroot/proc 2>/dev/null || true
mount --move /sys  /sysroot/sys  2>/dev/null || true
mount --move /dev  /sysroot/dev  2>/dev/null || true

echo "tiko-init: switch_root -> /sysroot /sbin/init"
# switch_root deletes the initramfs and execs /sbin/init in the new root.
exec switch_root /sysroot /sbin/init

# (unreachable: switch_root execs and never returns)
