#!/bin/bash
# =============================================================================
# tikovm scheduled-job (cron) e2e test (real KVM + Firecracker).
#
# Validates the 3rd tikovm workload kind — scheduled jobs (design §13) —
# end-to-end. A VM running a "hello world" echo job (a /bin/sh loop that
# prints to the serial console every 2s) is provisioned with a schedule.
# The test verifies the full periodic-run loop:
#
#   provision + start
#     ->  guest idle evaluator auto-suspends the VM (no HTTP traffic)
#     ->  host scheduler wakes the VM on the configured interval
#     ->  job resumes printing (new output in the serial log)
#     ->  repeat for a second scheduled wake
#     ->  destroy.
#
# Host-driven scheduling (§13) + guest-driven idle (§8) = the periodic-job
# pattern without any workload-specific scheduler code. No proxy needed
# (the job has no HTTP server); verification is via API state transitions +
# SSH into the guest to check /mnt/data/cron-runs.log (the serial console
# is unreliable across Firecracker snapshot/restore).
#
# Each step prints [PASS]/[FAIL]; exits non-zero on any failure.
#
# Prerequisites (same as run_e2e.sh / run_lang_e2e.sh).
# =============================================================================
set -uo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
ASSETS="$REPO/tikod/assets"
export FIRECRACKER_BIN="${FIRECRACKER_BIN:-/usr/local/bin/firecracker}"
export OVERLAY_SIZE_MB=512
API=http://127.0.0.1:9000
DD=/tmp/tikovm-cron-e2e
HOSTD_LOG="$DD/hostd.log"
HOSTD_PG=""
PROV_TEMPLATE="$REPO/scripts/tikovm/provision-cron.json"
ROOTFS="$ASSETS/cron-rootfs.ext4"
GUEST_IP=172.16.1.2

# Scheduler interval (seconds). The scheduler ticks every 1s; the VM
# auto-suspends ~6s after each wake (idle_secs in the rootfs manifest).
# Default 12s gives a comfortable suspend-then-wake cadence; two full
# scheduled wakes complete in well under 2 minutes.
CRON_INTERVAL_SECS="${CRON_INTERVAL_SECS:-12}"

PASS=0; FAIL=0
ok()   { echo "  [PASS] $1"; PASS=$((PASS+1)); }
bad()  { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
step() { echo; echo "=== $1 ==="; }
die()  { echo "FATAL: $1" >&2; exit 1; }

cleanup() {
  bash "$REPO/scripts/tikovm/cleanup.sh" "$HOSTD_PG" "$DD"
}
trap cleanup EXIT

# SSH into the guest (key baked by build_base_rootfs.sh, same as run_e2e.sh).
ssh_g() {
  local keyarg=() key=""
  for f in "$HOME/.ssh/id_ed25519" "$HOME/.ssh/id_rsa"; do
    [ -f "$f" ] && key="$f" && break
  done
  [ -n "$key" ] && keyarg=(-i "$key")
  ssh "${keyarg[@]}" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
      -o ConnectTimeout=4 -o LogLevel=ERROR "root@$GUEST_IP" "$@"
}

# Wait for vm-1 to reach a target state. Args: $1 = state, $2 = max seconds.
wait_state() {
  local want="$1" max="$2"
  for _ in $(seq 1 "$max"); do
    local st
    st=$(curl -s -m 2 $API/vms/vm-1 2>/dev/null | grep -o '"state":"[a-z]*"' | cut -d'"' -f4)
    [ "$st" = "$want" ] && return 0
    sleep 1
  done
  return 1
}

# Read the cron-runs.log line count from the guest (via SSH). Returns 0 if
# the file doesn't exist yet or SSH is unreachable. The job appends one line
# per ~2s; the test compares counts before/after a scheduler wake.
log_lines() {
  ssh_g 'wc -l < /mnt/data/cron-runs.log 2>/dev/null || echo 0' 2>/dev/null | tr -dc '0-9'
  [ -n "${REPLY:-}" ] || true
}

# ---------------------------------------------------------------------------
# 0. Pre-flight
# ---------------------------------------------------------------------------
step "0. pre-flight checks"
[ -w /dev/kvm ] || sudo -n chmod 666 /dev/kvm 2>/dev/null
[ -w /dev/kvm ] && ok "/dev/kvm accessible" || die "/dev/kvm not accessible"
[ -x "$FIRECRACKER_BIN" ] && ok "firecracker: $FIRECRACKER_BIN" || die "firecracker not found at $FIRECRACKER_BIN (set FIRECRACKER_BIN)"
sudo -n true 2>/dev/null && ok "passwordless sudo" || die "need passwordless sudo (TAP/iptables/mount)"
command -v envsubst >/dev/null && ok "envsubst on PATH" || die "envsubst not found (apt install gettext-base)"
for f in vmlinux-6.1 tikovm-base-rootfs.ext4 tiko-initramfs.cpio.gz; do
  [ -f "$ASSETS/$f" ] && ok "asset $f" || die "missing asset $ASSETS/$f (build with scripts/tikovm/build_base_rootfs.sh)"
done

# ---------------------------------------------------------------------------
# 0b. Build guestd + hostd + cron rootfs
# ---------------------------------------------------------------------------
step "0b. build guestd + hostd + cron rootfs"
( cd "$REPO" && cargo build -p tikovm-guest -p tikovm-host ) \
  || die "cargo build failed"
HOSTD="$REPO/target/debug/tikovm-hostd"
[ -x "$HOSTD" ] || die "tikovm-hostd not built"

if [ "${SKIP_BUILD:-0}" != "1" ]; then
  bash "$REPO/scripts/tikovm/build_cron_rootfs.sh" >/dev/null \
    || die "build_cron_rootfs.sh failed"
fi
[ -f "$ROOTFS" ] || die "cron rootfs missing: $ROOTFS"
ok "cron rootfs: $ROOTFS"

bash "$REPO/scripts/tikovm/cleanup.sh" "" "$DD"
mkdir -p "$DD"

# ---------------------------------------------------------------------------
# 1. Start hostd (with scheduler — auto-spawned, no special flags needed).
#    No proxy: the cron job has no HTTP server.
# ---------------------------------------------------------------------------
step "1. start tikovm-hostd (scheduler auto-spawned)"
setsid "$HOSTD" --data-dir "$DD" --api-listen 0.0.0.0:9000 \
  >"$HOSTD_LOG" 2>&1 &
HOSTD_PG=$!
for _ in $(seq 1 30); do curl -sf -m 1 $API/health >/dev/null 2>&1 && break; sleep 0.5; done
curl -sf -m 2 $API/health >/dev/null && ok "hostd up (log: $HOSTD_LOG)" \
  || die "hostd did not start"

# ---------------------------------------------------------------------------
# 2. Provision scheduled-job VM
# ---------------------------------------------------------------------------
step "2. provision scheduled-job VM (interval=${CRON_INTERVAL_SECS}s, keep_warm=true)"
PROV_JSON="$DD/provision-cron.json"
ASSETS="$ASSETS" ROOTFS="$ROOTFS" CRON_INTERVAL_SECS="$CRON_INTERVAL_SECS" \
  envsubst < "$PROV_TEMPLATE" > "$PROV_JSON" || die "envsubst failed"
curl -s -m 60 -X POST $API/vms/provision \
     -H 'Content-Type: application/json' -d @"$PROV_JSON" >/dev/null \
  || die "provision failed"
ok "VM provisioned + started"

# ---------------------------------------------------------------------------
# 3. Wait for initial job output (proves boot + supervisor + job started)
# ---------------------------------------------------------------------------
step "3. initial job run"
echo "  waiting for job to write to /mnt/data/cron-runs.log (via SSH) ..."
INITIAL=0
for _ in $(seq 1 40); do
  INITIAL=$(log_lines)
  [ "${INITIAL:-0}" -gt 0 ] && break
  sleep 2
done
if [ "${INITIAL:-0}" -gt 0 ]; then
  ok "job is running: $INITIAL lines in cron-runs.log"
else
  bad "job produced no output within 80s (boot or SSH failure)"
  die "cannot continue without initial job output"
fi

# ---------------------------------------------------------------------------
# 4. Wait for guest idle evaluator to auto-suspend the VM
# ---------------------------------------------------------------------------
step "4. wait for idle-driven auto-suspend"
if wait_state "suspended" 30; then
  ok "VM auto-suspended (guest idle evaluator fired)"
else
  st=$(curl -s -m 2 $API/vms/vm-1 | grep -o '"state":"[a-z]*"' | cut -d'"' -f4)
  bad "VM did not auto-suspend within 30s (state=$st)"
fi

# ---------------------------------------------------------------------------
# 5. Scheduled wake #1 (host scheduler restore)
# ---------------------------------------------------------------------------
step "5. wait for scheduled wake #1 (scheduler restore)"
LINES_BEFORE_WAKE1=$(log_lines 2>/dev/null || echo 0)
echo "  cron-runs.log lines before wake: $LINES_BEFORE_WAKE1"
MAX_WAKE_WAIT=$(( CRON_INTERVAL_SECS * 2 + 30 ))
if wait_state "started" "$MAX_WAKE_WAIT"; then
  ok "scheduler woke the VM (Suspended -> Started)"
else
  bad "scheduler did not wake the VM within ${MAX_WAKE_WAIT}s"
fi
# Wait briefly for the job to append new lines, then verify via SSH.
# idle_secs=6 + tick_secs=2 => ~6s of uptime per wake; 4s wait leaves margin.
sleep 4
LINES_AFTER_WAKE1=$(log_lines 2>/dev/null || echo 0)
if [ "${LINES_AFTER_WAKE1:-0}" -gt "${LINES_BEFORE_WAKE1:-0}" ]; then
  ok "wake #1: job ran ($LINES_BEFORE_WAKE1 -> $LINES_AFTER_WAKE1 lines in cron-runs.log)"
else
  bad "wake #1: no new job output ($LINES_BEFORE_WAKE1 -> $LINES_AFTER_WAKE1 lines)"
fi

# ---------------------------------------------------------------------------
# 6. Auto-suspend again + scheduled wake #2 (repeatability)
# ---------------------------------------------------------------------------
step "6. auto-suspend + scheduled wake #2 (repeatability)"
if wait_state "suspended" 30; then
  ok "VM auto-suspended again after wake #1"
else
  bad "VM did not auto-suspend after wake #1"
fi

LINES_BEFORE_WAKE2=$(log_lines 2>/dev/null || echo 0)
echo "  cron-runs.log lines before wake #2: $LINES_BEFORE_WAKE2"
if wait_state "started" "$MAX_WAKE_WAIT"; then
  ok "scheduler woke the VM again (second scheduled run)"
else
  bad "scheduler did not wake the VM a second time"
fi
sleep 4
LINES_AFTER_WAKE2=$(log_lines 2>/dev/null || echo 0)
if [ "${LINES_AFTER_WAKE2:-0}" -gt "${LINES_BEFORE_WAKE2:-0}" ]; then
  ok "wake #2: job ran ($LINES_BEFORE_WAKE2 -> $LINES_AFTER_WAKE2 lines in cron-runs.log)"
else
  bad "wake #2: no new job output ($LINES_BEFORE_WAKE2 -> $LINES_AFTER_WAKE2 lines)"
fi

# ---------------------------------------------------------------------------
# 7. Metrics sanity (optional): the scheduler should have incremented restore
#    counters. Don't fail the test if metrics are incomplete — this is a bonus.
# ---------------------------------------------------------------------------
step "7. metrics sanity (bonus)"
M=$(curl -s -m 3 $API/metrics 2>/dev/null || true)
if echo "$M" | grep -q 'tikovm_restores_total'; then
  ok "metrics: restore counter present"
else
  echo "  (skip) metrics endpoint did not report restores counter"
fi

# ---------------------------------------------------------------------------
# 8. Destroy + cleanup
# ---------------------------------------------------------------------------
step "8. destroy + cleanup"
# Retry: the scheduler may be mid-restore (transitional state) when we try
# to destroy, which returns 409. Wait + retry until the VM reaches a stable
# state and destroy succeeds.
for i in $(seq 1 6); do
  code=$(curl -s -m 30 -X DELETE $API/vms/vm-1 -o /dev/null -w '%{http_code}')
  if [ "$code" = "204" ]; then
    echo "  destroy: HTTP $code"
    break
  fi
  echo "  destroy attempt $i: HTTP $code (VM may be in transitional state), retrying..."
  sleep 3
done
[ "$code" = "204" ] && ok "VM destroyed" || bad "destroy failed (HTTP $code)"

echo
echo "============================================="
echo "  Cron scheduled-job E2E result: $PASS passed, $FAIL failed"
echo "============================================="
[ "$FAIL" = 0 ]
