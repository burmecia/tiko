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

PASS=0; FAIL=0
ok()   { echo "  [PASS] $1"; PASS=$((PASS+1)); }
bad()  { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
step() { echo; echo "=== $1 ==="; }
die()  { echo "FATAL: $1" >&2; exit 1; }

cleanup() {
  [ -n "$HOSTD_PG" ] && kill -TERM -$HOSTD_PG 2>/dev/null
  wait 2>/dev/null
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

pkill -f tikovm-hostd 2>/dev/null; sleep 1
rm -rf "$DD"; mkdir -p "$DD"

cat > "$PROV_JSON" <<EOF
{"vm_id":"vm-1","rootfs":{"path":"$ASSETS/echo-rootfs.ext4","read_only_base":true},"resources":{"memory_mb":1024,"vcpus":2},"kernel":{"kernel_path":"$ASSETS/vmlinux-6.1","kernel_cmdline":"console=ttyS0 reboot=k panic=1 pci=off systemd.unified_cgroup_hierarchy=0","initrd_path":"$ASSETS/tiko-initramfs.cpio.gz"},"network":{},"manifest":{"version":1,"workload":"echo","health":{"kind":"none"}}}
EOF

# ---------------------------------------------------------------------------
# Start hostd (real Firecracker backend, proxy, metrics)
# ---------------------------------------------------------------------------
step "1. start tikovm-hostd"
setsid "$HOSTD" --data-dir "$DD" --api-listen 0.0.0.0:9000 \
  --proxy-listen 0.0.0.0:8080 --proxy-default-vm vm-1 --proxy-default-port 8080 \
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
# 7. Cleanup
# ---------------------------------------------------------------------------
step "7. cleanup"
curl -s -m 30 -X DELETE $API/vms/vm-1 -o /dev/null -w "destroy: HTTP %{http_code}\n"

echo
echo "============================================="
echo "  E2E result: $PASS passed, $FAIL failed"
echo "============================================="
[ "$FAIL" = 0 ]
