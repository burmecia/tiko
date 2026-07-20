#!/bin/bash
# =============================================================================
# tikovm end-to-end test (real KVM + Firecracker).
#
# Exercises the full platform against real microVMs:
#   provision -> reach via proxy -> scale-to-zero (idle->suspend->wake)
#   -> lifecycle (pause/resume) -> crash recovery -> metrics -> cleanup.
#
# Each step prints [PASS]/[FAIL]; exits non-zero on any failure.
#
# Prerequisites (the script checks these):
#   * Linux + KVM (/dev/kvm), passwordless sudo (for TAP/iptables/mount)
#   * Firecracker binary on PATH or via $FIRECRACKER_BIN
#   * Assets: tikod/assets/{vmlinux-6.1, ubuntu-24.04-rootfs.ext4,
#             tiko-initramfs.cpio.gz}
#   * The echo rootfs is (re)built by scripts/tikovm/build_echo_rootfs.sh
# =============================================================================
set -uo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
ASSETS="$REPO/tikod/assets"
export FIRECRACKER_BIN="${FIRECRACKER_BIN:-/usr/local/bin/firecracker}"
export OVERLAY_SIZE_MB=512
API=http://127.0.0.1:9000
PROXY=http://127.0.0.1:8080
DD=/tmp/tikovm-e2e
HOSTD_LOG="$DD/hostd.log"
HOSTD_PG=""
PROV_JSON="$DD/provision.json"
PROV_TEMPLATE="$REPO/scripts/tikovm/provision.json"
# remote_slow backing under test: s3files_image (default) or ublk.
BACKING="${BACKING:-s3files_image}"
GUEST_IP=172.16.1.2

# ssh into the guest (key baked by build_echo_rootfs.sh).
SSH_KEY=""
for f in "$HOME/.ssh/id_ed25519" "$HOME/.ssh/id_rsa"; do
  [ -f "$f" ] && SSH_KEY="$f" && break
done
ssh_g() {
  local keyarg=()
  [ -n "$SSH_KEY" ] && keyarg=(-i "$SSH_KEY")
  ssh "${keyarg[@]}" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
      -o ConnectTimeout=5 -o LogLevel=ERROR "root@$GUEST_IP" "$@"
}

PASS=0; FAIL=0
ok()   { echo "  [PASS] $1"; PASS=$((PASS+1)); }
bad()  { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
step() { echo; echo "=== $1 ==="; }
die()  { echo "FATAL: $1" >&2; exit 1; }

cleanup() {
  bash "$REPO/scripts/tikovm/cleanup.sh" "$HOSTD_PG" "$DD"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# 0. Pre-flight
# ---------------------------------------------------------------------------
step "0. pre-flight checks"
[ -w /dev/kvm ] || sudo -n chmod 666 /dev/kvm 2>/dev/null
[ -w /dev/kvm ] && ok "/dev/kvm accessible" || die "/dev/kvm not accessible"
[ -x "$FIRECRACKER_BIN" ] && ok "firecracker: $FIRECRACKER_BIN" || die "firecracker not found at $FIRECRACKER_BIN (set FIRECRACKER_BIN)"
sudo -n true 2>/dev/null && ok "passwordless sudo" || die "need passwordless sudo (TAP/iptables/mount)"
for f in vmlinux-6.1 ubuntu-24.04-rootfs.ext4 tiko-initramfs.cpio.gz; do
  [ -f "$ASSETS/$f" ] && ok "asset $f" || die "missing asset $ASSETS/$f"
done

# ---------------------------------------------------------------------------
# 0b. Build binaries + echo rootfs
# ---------------------------------------------------------------------------
step "0b. build binaries + echo rootfs"
( cd "$REPO" && cargo build -p tikovm-host -p tikovm-guest --example echo-server ) \
  || die "cargo build failed"
HOSTD="$REPO/target/debug/tikovm-hostd"
[ -x "$HOSTD" ] || die "tikovm-hostd not built"
# (re)build the echo rootfs (idempotent: re-injects fresh binaries + manifest)
bash "$REPO/scripts/tikovm/build_echo_rootfs.sh" >/dev/null || die "build_echo_rootfs.sh failed"
ok "binaries + echo rootfs ready"

bash "$REPO/scripts/tikovm/cleanup.sh" "" "$DD"
mkdir -p "$DD"

# The s3files_image backing writes <source>/vm-1/ as the (unprivileged)
# hostd user; pre-create it with our ownership on the root-owned mount.
if [ "$BACKING" = "s3files_image" ] && mountpoint -q /mnt/s3files; then
  sudo -n mkdir -p /mnt/s3files/tikoblk/vm-1 || die "cannot create image dir on /mnt/s3files"
  sudo -n chown "$(id -u):$(id -g)" /mnt/s3files/tikoblk/vm-1 || die "cannot chown image dir"
fi

ASSETS="$ASSETS" envsubst < "$PROV_TEMPLATE" > "$PROV_JSON" || die "envsubst failed"

# ---------------------------------------------------------------------------
# Start hostd (real Firecracker backend, proxy, metrics)
# ---------------------------------------------------------------------------
step "1. start tikovm-hostd (remote_slow backing: $BACKING)"
setsid "$HOSTD" --data-dir "$DD" --api-listen 0.0.0.0:9000 \
  --proxy-listen 0.0.0.0:8080 --proxy-default-vm vm-1 --proxy-default-port 8080 \
  --remote-slow-backing "$BACKING" \
  >"$HOSTD_LOG" 2>&1 &
HOSTD_PG=$!
for i in $(seq 1 30); do curl -sf -m 1 $API/health >/dev/null 2>&1 && break; sleep 0.5; done
curl -sf -m 2 $API/health >/dev/null && ok "hostd up" || die "hostd did not start"

# ---------------------------------------------------------------------------
# 2. Provision + reach via proxy
# ---------------------------------------------------------------------------
step "2. provision + reach workload via proxy"
curl -s -m 60 -X POST $API/vms/provision -H 'Content-Type: application/json' -d @"$PROV_JSON" >/dev/null || die "provision failed"
for i in $(seq 1 40); do curl -sf -m 2 $PROXY/health >/dev/null 2>&1 && break; sleep 2; done
H=$(curl -s -m 5 $PROXY/health)
[ "$H" = '{"status":"ok"}' ] && ok "echo reachable via proxy ($H)" || bad "echo not reachable ($H)"

# ---------------------------------------------------------------------------
# 3. Scale-to-zero: idle -> suspend -> wake-on-connect
# ---------------------------------------------------------------------------
step "3. scale-to-zero (idle -> suspend -> wake)"
echo "  (waiting up to ~60s for the guest idle evaluator to signal suspend...)"
SUSPENDED=0
for i in $(seq 1 30); do
  st=$(curl -s -m 2 $API/vms/vm-1 | grep -o '"state":"[a-z]*"' | cut -d'"' -f4)
  if [ "$st" = "suspended" ]; then SUSPENDED=1; break; fi
  sleep 2
done
[ "$SUSPENDED" = 1 ] && ok "VM suspended after idle" || bad "VM did not suspend (state=$st)"
W=$(curl -s -m 30 $PROXY/woken)
echo "$W" | grep -q '"path":"/woken"' && ok "proxy woke the VM: $W" || bad "wake failed ($W)"

# ---------------------------------------------------------------------------
# 3b. Volume checks: LABEL mounts + remote_slow persistence over suspend
# ---------------------------------------------------------------------------
step "3b. volume checks ($BACKING)"
ssh_g true 2>/dev/null && ok "ssh into guest" || bad "ssh into guest failed"
ssh_g 'findmnt /mnt/data' >/dev/null 2>&1 \
  && ok "local_fast mounted: $(ssh_g 'findmnt -n -o SOURCE /mnt/data' 2>/dev/null)" \
  || bad "local_fast /mnt/data not mounted"
ssh_g 'findmnt /mnt/archive' >/dev/null 2>&1 \
  && ok "remote_slow mounted: $(ssh_g 'findmnt -n -o SOURCE /mnt/archive' 2>/dev/null)" \
  || bad "remote_slow /mnt/archive not mounted"
ssh_g 'dd if=/dev/urandom of=/mnt/archive/e2e.bin bs=1M count=4 status=none && sha256sum /mnt/archive/e2e.bin > /mnt/archive/e2e.sha256 && sync' \
  && ok "wrote 4 MiB checksummed file to /mnt/archive" || bad "could not write /mnt/archive"
ssh_g 'cd /mnt/archive && sha256sum -c e2e.sha256' >/dev/null 2>&1 \
  && ok "archive checksum OK after suspend/restore" || bad "archive checksum FAILED after suspend/restore"

# ---------------------------------------------------------------------------
# 4. Lifecycle: pause -> resume
# ---------------------------------------------------------------------------
step "4. lifecycle (pause/resume)"
curl -s -m 30 -X POST $API/vms/vm-1/pause | grep -q '"state":"paused"' && ok "pause" || bad "pause"
curl -s -m 30 -X POST $API/vms/vm-1/resume | grep -q '"state":"started"' && ok "resume" || bad "resume"

# ---------------------------------------------------------------------------
# 5. Crash recovery: suspend -> kill -9 hostd -> restart -> reconcile
# ---------------------------------------------------------------------------
step "5. crash recovery (kill -9 hostd -> restart)"
curl -s -m 30 -X POST $API/vms/vm-1/pause >/dev/null
curl -s -m 60 -X POST $API/vms/vm-1/suspend | grep -q suspended || bad "could not suspend before crash"
kill -TERM -$HOSTD_PG 2>/dev/null; kill -9 -- -"$HOSTD_PG" 2>/dev/null
HOSTD_PG=""; sleep 1
# keep the same data-dir (SQLite state) -> reconcile recovers the VM
setsid "$HOSTD" --data-dir "$DD" --api-listen 0.0.0.0:9000 \
  --proxy-listen 0.0.0.0:8080 --proxy-default-vm vm-1 --proxy-default-port 8080 \
  --remote-slow-backing "$BACKING" \
  >>"$HOSTD_LOG" 2>&1 &
HOSTD_PG=$!
for i in $(seq 1 30); do curl -sf -m 1 $API/health >/dev/null 2>&1 && break; sleep 0.5; done
REC=$(curl -s -m 5 $API/vms/vm-1)
echo "$REC" | grep -q '"state":"suspended"' && ok "VM recovered from SQLite: $REC" || bad "recovery failed ($REC)"
# wake the recovered VM
curl -s -m 30 $PROXY/after-crash | grep -q '"path":"/after-crash"' && ok "wake after crash" || bad "wake after crash failed"

# ---------------------------------------------------------------------------
# 6. Metrics
# ---------------------------------------------------------------------------
step "6. metrics scrape"
M=$(curl -s -m 3 $API/metrics)
echo "$M" | grep -q 'tikovm_vm_total{state="started"}' && ok "metrics: vm_total present" || bad "metrics missing vm_total"
echo "$M" | grep -q 'tikovm_restores_total' && ok "metrics: restore counter present" || bad "metrics missing restores"

# ---------------------------------------------------------------------------
# 7. Cleanup VMs + remote_slow persistence across destroy
# ---------------------------------------------------------------------------
step "7. cleanup vms + remote_slow persistence across destroy ($BACKING)"
curl -s -m 30 -X DELETE $API/vms/vm-1 -o /dev/null -w "destroy: HTTP %{http_code}\n"

# Re-provision the same VM: the archive volume must still hold our file.
curl -s -m 60 -X POST $API/vms/provision -H 'Content-Type: application/json' -d @"$PROV_JSON" >/dev/null \
  || die "re-provision failed"
for i in $(seq 1 40); do curl -sf -m 2 $PROXY/health >/dev/null 2>&1 && break; sleep 2; done
ssh_g 'cd /mnt/archive && sha256sum -c e2e.sha256' >/dev/null 2>&1 \
  && ok "archive file persisted across destroy ($BACKING)" || bad "archive file LOST across destroy ($BACKING)"
curl -s -m 30 -X DELETE $API/vms/vm-1 -o /dev/null -w "re-destroy: HTTP %{http_code}\n"

# Leave the store clean.
if [ "$BACKING" = "ublk" ]; then
  D=$(sudo -n curl -s --unix-socket /run/tikoblk/daemon.sock -X DELETE http://localhost/volumes/vm-1-archive -w '%{http_code}' -o /dev/null)
  [ "$D" = 200 ] && ok "tikoblk volume vm-1-archive deleted" || bad "tikoblk volume delete: HTTP $D"
else
  sudo -n rm -f /mnt/s3files/tikoblk/vm-1/archive.ext4 && sudo -n rmdir /mnt/s3files/tikoblk/vm-1 2>/dev/null \
    && ok "s3files image removed" || bad "s3files image cleanup failed"
fi

echo
echo "============================================="
echo "  E2E result: $PASS passed, $FAIL failed"
echo "============================================="
[ "$FAIL" = 0 ]
