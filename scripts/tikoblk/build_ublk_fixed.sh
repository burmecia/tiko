#!/usr/bin/env bash
# build_ublk_fixed.sh — build/install a FIXED mainline ublk_drv.ko for the
# running kernel (Ubuntu 6.17.0-10xx), out-of-tree against its headers.
#
# Background (target/tmp/ublk-spike/NOTES.md, "Mainline oops
# investigation"): Ubuntu's 6.17.0-10xx cloud kernels backported the 6.18
# NUMA patch (529d4d632788) without its prerequisite call-order change, so
# EVERY ADD_DEV NULL-derefs. This script fetches the exact Ubuntu source of
# drivers/block/ublk_drv.c for the running kernel (Launchpad cgit, linux-aws
# noble), applies scripts/tikoblk/ublk-fix-adddev-order.patch (mirrors the
# upstream 6.18 order), builds ublk_drv.ko, and (as root) installs it to
# /lib/modules/<krel>/updates/ + stamps /var/lib/tikoblk/module-src/ for
# offline rebuilds.
#
# Modes:
#   (default)          build + install (fetch source if needed)
#   --check            exit 0 if a fixed module for $(uname -r) is installed
#                      and matches our stamp; exit 1 if a rebuild is needed
#   --boot             non-interactive: no-op when --check passes, otherwise
#                      build + install (uses the offline cached source when
#                      the network is unavailable) + modprobe if unloaded
#   --quiet            less chatter
set -euo pipefail

MODE=build
QUIET=0
for a in "$@"; do
    case $a in
        --check) MODE=check ;;
        --boot) MODE=boot ;;
        --quiet) QUIET=1 ;;
        *) echo "usage: build_ublk_fixed.sh [--check|--boot] [--quiet]" >&2; exit 2 ;;
    esac
done

log()  { [[ $QUIET == 1 ]] || echo "[build_ublk_fixed] $*"; }
die()  { echo "[build_ublk_fixed] ERROR: $*" >&2; exit 1; }

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
if [[ -f $SCRIPT_DIR/ublk-fix-adddev-order.patch ]]; then
    # Installed location (/usr/local/lib/tikoblk): patch sits next to the
    # script; scratch build area is host-local.
    PATCH_FILE=$SCRIPT_DIR/ublk-fix-adddev-order.patch
    WORK=${TIKOBLK_BUILD_WORK:-/var/lib/tikoblk/build}
else
    # Repo checkout: patch + scratch under the repo.
    REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)
    PATCH_FILE=$REPO_ROOT/scripts/tikoblk/ublk-fix-adddev-order.patch
    WORK=${TIKOBLK_BUILD_WORK:-$REPO_ROOT/target/tmp/ublk-fixed}
fi
KREL=$(uname -r)
UPDATES_DIR=/lib/modules/$KREL/updates
KO=$UPDATES_DIR/ublk_drv.ko
SRC_CACHE=/var/lib/tikoblk/module-src
STAMP=$SRC_CACHE/ublk_drv-$KREL.sha256
PATCHED_SRC=$SRC_CACHE/ublk_drv-patched-$KREL.c

# --- --check ----------------------------------------------------------------
check_fixed() {
    [[ -f $KO && -f $STAMP ]] || return 1
    (cd / && sha256sum -c "$STAMP" >/dev/null 2>&1)
}

if [[ $MODE == check ]]; then
    if check_fixed; then
        exit 0
    else
        exit 1
    fi
fi

if [[ $MODE == boot ]] && check_fixed; then
    log "fixed ublk_drv.ko already installed for $KREL"
    exit 0
fi

# --- source (network fetch, with offline fallback) ---------------------------
KVER=$(apt-cache policy "linux-image-$KREL" 2>/dev/null | awk '/Installed:/ {print $2}') \
    || KVER=""
[[ -n $KVER ]] || die "linux-image-$KREL version unknown (apt-cache policy failed)"
BASE=$(echo "$KREL" | sed -E 's/^([0-9]+\.[0-9]+)\..*/\1/')
ABI=$(echo "$KVER" | sed -E 's/^([0-9]+\.[0-9]+\.[0-9]+-[0-9]+)\..*/\1/')
SUB=$(echo "$KVER" | sed -E 's/^[0-9]+\.[0-9]+\.[0-9]+-[0-9]+\.([0-9]+).*/\1/')
SUFFIX=$(echo "$KVER" | sed -E 's/.*(24\.04\.[0-9]+).*/\1/')
TAG="Ubuntu-aws-$BASE-$ABI.$SUB"_"$SUFFIX"
PLAIN="https://git.launchpad.net/~canonical-kernel/ubuntu/+source/linux-aws/+git/noble/plain/drivers/block/ublk_drv.c?h=$TAG"

[[ -f $PATCH_FILE ]] || die "missing patch $PATCH_FILE"
mkdir -p "$WORK"
SRC=$WORK/ublk_drv-$KREL.c
TAGFILE=$WORK/.tag-$KREL

fetch_source() {
    if [[ -f $SRC && -f $TAGFILE ]] && [[ $(cat "$TAGFILE") == "$TAG" ]]; then
        log "using cached source ($TAG)"
        return 0
    fi
    log "fetching $PLAIN"
    if curl -sfL --max-time 120 -o "$SRC.tmp" "$PLAIN" && head -1 "$SRC.tmp" | grep -q SPDX; then
        mv "$SRC.tmp" "$SRC"
        echo "$TAG" > "$TAGFILE"
        return 0
    fi
    rm -f "$SRC.tmp"
    return 1
}

if ! fetch_source; then
    if [[ -f $PATCHED_SRC ]]; then
        log "network fetch failed; using offline cached patched source"
        cp "$PATCHED_SRC" "$WORK/ublk_drv-patched-$KREL.c"
    else
        die "source fetch failed and no offline cache at $PATCHED_SRC"
    fi
fi

BUILD=$WORK/build-$KREL
rm -rf "$BUILD"
mkdir -p "$BUILD"

if [[ -f $WORK/ublk_drv-patched-$KREL.c ]]; then
    log "using pre-patched source"
    cp "$WORK/ublk_drv-patched-$KREL.c" "$BUILD/ublk_drv.c"
else
    cp "$SRC" "$BUILD/ublk_drv.c"
    (cd "$BUILD" && patch -p3 --no-backup-if-mismatch < "$PATCH_FILE") \
        || die "patch failed — source layout changed?"
    log "applied ublk-fix-adddev-order.patch"
fi

# Sanity: in the patched add_dev, the add_tag_set CALL must precede the
# init_queues CALL (the whole point of the fix).
order_ok=$(awk '
    /ublk_align_max_io_size\(ub\);/ {in_region=1}
    in_region && /ret = ublk_add_tag_set\(ub\);/ && !ts {ts=NR}
    in_region && /ret = ublk_init_queues\(ub\);/ && !iq {iq=NR}
    END {print (ts && iq && ts < iq) ? "yes" : "no"}' "$BUILD/ublk_drv.c")
[[ $order_ok == yes ]] || die "sanity check: add_tag_set does not precede init_queues in build source"

cat > "$BUILD/Makefile" <<'EOF'
obj-m := ublk_drv.o
EOF

log "building against /lib/modules/$KREL/build"
make -C "/lib/modules/$KREL/build" M="$BUILD" modules >/dev/null \
    || die "kernel module build failed"
[[ -f $BUILD/ublk_drv.ko ]] || die "build produced no ublk_drv.ko"

# --- install (root only) ------------------------------------------------------
if [[ $(id -u) -ne 0 ]]; then
    log "not root — built but not installed: $BUILD/ublk_drv.ko"
    exit 0
fi

log "installing -> $KO (+ depmod)"
mkdir -p "$UPDATES_DIR" "$SRC_CACHE"
install -m 0644 "$BUILD/ublk_drv.ko" "$KO"
depmod -a
# Offline-rebuild cache + stamp for --check.
cp "$BUILD/ublk_drv.c" "$PATCHED_SRC"
sha256sum "$KO" > "$STAMP.tmp" && mv "$STAMP.tmp" "$STAMP"
rm -f "$WORK/ublk_drv-patched-$KREL.c"  # cache lives in $SRC_CACHE now

if [[ $MODE == boot ]] && ! lsmod | grep -q '^ublk_drv'; then
    log "modprobe ublk_drv"
    modprobe ublk_drv || die "modprobe ublk_drv failed"
fi

log "OK: $KO"
