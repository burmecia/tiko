#!/usr/bin/env bash
# setup_host.sh — prepare a FRESH host for tikoblkd. Idempotent.
#
#   sudo scripts/tikoblk/setup_host.sh
#
# What it does:
#   1. installs linux-modules-extra-$(uname -r) (provides mainline ublk_drv
#      for kernels where it works — see warning below)
#   2. ensures a WORKING ublk driver: on Ubuntu 6.17.0-10xx cloud kernels
#      mainline ublk_drv NULL-derefs on EVERY ADD_DEV (Ubuntu backported the
#      6.18 NUMA patch without the prerequisite call-order change; see
#      target/tmp/ublk-spike/NOTES.md "Mainline oops investigation"). This
#      script builds the FIXED mainline driver via
#      scripts/tikoblk/build_ublk_fixed.sh (exact Ubuntu source + the
#      call-order patch) and installs it to /lib/modules/$(uname -r)/updates/
#      (depmod's highest-priority dir on Ubuntu) so modprobe/modules-load
#      pick it over the broken in-tree one. TIKOBLK_UBLK2_KO=<path> remains
#      an override to install a supplied module instead (e.g. a saved
#      ublk2_drv.ko).
#   3. creates /var/lib/tikoblk (+ backing/) and /run/tikoblk
#   4. builds + installs tikoblkd to /usr/local/bin (if cargo is available)
#   5. installs + enables the tikoblkd systemd unit (starts it if ublk is up)
set -euo pipefail

log()  { echo "[setup_host] $*"; }
warn() { echo "[setup_host] WARNING: $*" >&2; }
die()  { echo "[setup_host] ERROR: $*" >&2; exit 1; }

[[ $(id -u) -eq 0 ]] || die "must run as root (sudo $0)"

KREL=$(uname -r)
UPDATES_DIR=/lib/modules/$KREL/updates
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)

# --- 1. distro ublk package (mainline; only useful on fixed kernels) -------
if command -v apt-get >/dev/null 2>&1; then
    if dpkg -s "linux-modules-extra-$KREL" >/dev/null 2>&1; then
        log "linux-modules-extra-$KREL already installed"
    else
        log "installing linux-modules-extra-$KREL"
        apt-get install -y "linux-modules-extra-$KREL" || \
            warn "could not install linux-modules-extra-$KREL (continuing)"
    fi
fi

# --- 2. working ublk driver --------------------------------------------------
# Already-fixed module installed (ours, or an override module)?
fixed_installed() {
    [[ -f $UPDATES_DIR/ublk_drv.ko ]] || [[ -f $UPDATES_DIR/ublk2_drv.ko ]]
}

if [[ -n "${TIKOBLK_UBLK2_KO:-}" ]]; then
    [[ -f $TIKOBLK_UBLK2_KO ]] || die "TIKOBLK_UBLK2_KO=$TIKOBLK_UBLK2_KO not found"
    log "installing override module $TIKOBLK_UBLK2_KO -> $UPDATES_DIR/"
    mkdir -p "$UPDATES_DIR"
    install -m 0644 "$TIKOBLK_UBLK2_KO" "$UPDATES_DIR/"
    depmod -a
else
    log "building + installing fixed mainline ublk_drv (build_ublk_fixed.sh)"
    bash "$SCRIPT_DIR/build_ublk_fixed.sh" \
        || die "fixed module build failed"
fi

# Boot-time safety net: rebuild the fixed module after kernel upgrades.
log "installing tikoblk-module.service (boot-time module rebuild)"
install -D -m 0755 "$SCRIPT_DIR/build_ublk_fixed.sh" /usr/local/lib/tikoblk/build_ublk_fixed.sh
install -m 0644 "$SCRIPT_DIR/ublk-fix-adddev-order.patch" \
    /usr/local/lib/tikoblk/ublk-fix-adddev-order.patch
install -m 0644 "$REPO_ROOT/tikoblk/scripts/tikoblk-module.service" \
    /etc/systemd/system/tikoblk-module.service

# modules-load.d: load ublk at boot via modprobe (updates/ wins over the
# broken in-tree module after depmod).
if [[ ! -f /etc/modules-load.d/tikoblk.conf ]]; then
    log "writing /etc/modules-load.d/tikoblk.conf"
    echo ublk_drv > /etc/modules-load.d/tikoblk.conf
else
    log "/etc/modules-load.d/tikoblk.conf already present"
fi

# Load it now if no ublk driver is present (never unload a loaded one).
if ! lsmod | grep -qE '^(ublk_drv|ublk2_drv)'; then
    log "modprobe ublk_drv"
    modprobe ublk_drv || warn "modprobe ublk_drv failed"
else
    log "an ublk driver is already loaded — leaving it as-is"
fi

# Ensure /dev/ublk-control exists host-wide for admin tooling (the daemon
# itself also fixes this up inside its own mount namespace). Prefer ublk2's
# control node; never mask an existing node.
if [[ ! -e /dev/ublk-control ]] && [[ -e /dev/ublk2-control ]]; then
    minor=$(awk '$2 == "ublk2-control" {print $1}' /proc/misc)
    if [[ -n ${minor:-} ]]; then
        log "creating /dev/ublk-control (char 10:$minor -> ublk2)"
        mknod /dev/ublk-control c 10 "$minor"
    fi
fi

# udev rule: ublk block nodes default to 0660 root:disk, and udev applies
# that AFTER any application-level chmod (winning the race every attach).
# Unprivileged consumers (tikovm-hostd/Firecracker) need the drive nodes
# world-accessible on this dev host — set the mode at uevent time instead.
# (A production deployment should scope this to a dedicated group.)
UDEV_RULE=/etc/udev/rules.d/99-tikoblk.rules
if [[ ! -f $UDEV_RULE ]]; then
    log "writing $UDEV_RULE"
    cat > "$UDEV_RULE" <<'EOF'
KERNEL=="ublk2b*", MODE="0666"
KERNEL=="ublkb*", MODE="0666"
EOF
    udevadm control --reload >/dev/null 2>&1 || true
else
    log "$UDEV_RULE already present"
fi

# --- 3. directories ---------------------------------------------------------
log "creating /var/lib/tikoblk and /run/tikoblk"
mkdir -p /var/lib/tikoblk/backing /run/tikoblk
chmod 0755 /var/lib/tikoblk /var/lib/tikoblk/backing /run/tikoblk

# --- 4. build + install tikoblkd -------------------------------------------
if command -v cargo >/dev/null 2>&1; then
    avail_kb=$(df --output=avail / | tail -1)
    if (( avail_kb < 4 * 1024 * 1024 )); then
        die "less than 4 GB free on / — refusing to build"
    fi
    log "building tikoblkd (release)"
    (cd "$REPO_ROOT" && cargo build --release -p tikoblk)
    log "installing tikoblkd -> /usr/local/bin"
    install -m 0755 "$REPO_ROOT/target/release/tikoblkd" /usr/local/bin/tikoblkd
elif [[ -x /usr/local/bin/tikoblkd ]]; then
    log "cargo not found; using existing /usr/local/bin/tikoblkd"
else
    die "cargo not found and no /usr/local/bin/tikoblkd — build tikoblkd first"
fi

# --- 5. systemd unit ---------------------------------------------------------
log "installing systemd unit"
install -m 0644 "$SCRIPT_DIR/../../tikoblk/scripts/tikoblkd.service" \
    /etc/systemd/system/tikoblkd.service
systemctl daemon-reload
systemctl enable tikoblk-module.service tikoblkd.service

if lsmod | grep -qE '^(ublk_drv|ublk2_drv)' || [[ -e /dev/ublk-control ]]; then
    log "starting tikoblkd"
    systemctl restart tikoblkd.service
    systemctl --no-pager --full status tikoblkd.service | head -5 || true
else
    warn "no ublk control device visible; tikoblkd enabled but not started"
fi

log "done"
