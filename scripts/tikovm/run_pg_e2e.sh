#!/bin/bash
# =============================================================================
# tikovm Postgres serverless e2e test (real KVM + Firecracker).
#
# Validates the 4th tikovm workload kind — scale-to-zero Postgres — with the
# Lambda-like compute-storage separation:
#
#   Phase 1: SEED (cold start)
#     provision vm-0  ->  pg-supervisor runs initdb on local_fast  ->
#     PG starts  ->  psql: CREATE TABLE + INSERT  ->  verify SELECT.
#
#   Phase 2: SCALE-TO-ZERO (the core serverless demo)
#     close psql  ->  idle evaluator fires (pg-idle-check + host_network)  ->
#     VM suspends  ->  psql reconnect  ->  proxy wake-on-connect  ->  PG
#     restores from snapshot  ->  verify data intact.
#
#   Phase 3: WARM PAUSE/RESUME (bonus lifecycle)
#     API pause  ->  API resume  ->  verify data intact.
#
#   Phase 4: DESTROY + RE-PROVISION (local_fast persists across destroy)
#     destroy vm-0  ->  re-provision with the same persist_key  ->
#     pg-supervisor reuses the existing PGDATA (no initdb)  ->
#     verify data intact.
#
# PGDATA lives on local_fast (/mnt/data/pgdata) — fast I/O, survives
# suspend/restore AND destroy (the volume carries a persist_key). Bulk data
# (chunks, WAL) goes through the smgr to remote_slow
# (/mnt/archive/tiko_root) — durable, survives destroy.
#
# The proxy on port 15432 forwards raw TCP to the guest's PG on 5432, with
# wake-on-connect when the VM is suspended (the proxy detects non-HTTP traffic
# and falls through to the configured default target).
#
# Prerequisites (checked at start):
#   * Linux + KVM (/dev/kvm), passwordless sudo
#   * Firecracker binary, envsubst, psql client on the host
#   * Postgres built (target/pg-install/ exists — run build_postgres.sh)
#   * Assets: tikod/assets/{vmlinux-6.1, tikovm-base-rootfs.ext4, tiko-initramfs.cpio.gz}
# =============================================================================
set -uo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
ASSETS="$REPO/tikod/assets"
export FIRECRACKER_BIN="${FIRECRACKER_BIN:-/usr/local/bin/firecracker}"
export OVERLAY_SIZE_MB=1024
API=http://127.0.0.1:9000
PROXY_PORT="${PROXY_PORT:-15432}"   # psql connects here; proxy forwards to guest:5432
DD=/tmp/tikovm-pg-e2e
HOSTD_LOG="$DD/hostd.log"
HOSTD_PG=""
PROV_TEMPLATE="$REPO/scripts/tikovm/provision-pg.json"
ROOTFS="$ASSETS/pg-rootfs.ext4"
VM_ID="vm-0"
REMOTE_SLOW_SOURCE="${REMOTE_SLOW_SOURCE:-/mnt/s3files/tikoblk}"

PASS=0; FAIL=0
ok()   { echo "  [PASS] $1"; PASS=$((PASS+1)); }
bad()  { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
step() { echo; echo "=== $1 ==="; }
die()  { echo "FATAL: $1" >&2; exit 1; }

cleanup() {
  bash "$REPO/scripts/tikovm/cleanup.sh" "$HOSTD_PG" "$DD"
}
trap cleanup EXIT

# Run a psql command via the proxy (wake-on-connect if suspended).
# (env -u PGSERVICE: an EMPTY PGSERVICE is an error in psql 18 —
# "definition of service \"\" not found" — so unset it instead.)
psql_g() {
  env -u PGSERVICE PGUSER=postgres psql -h 127.0.0.1 -p "$PROXY_PORT" -U postgres \
      -t -A --set ON_ERROR_STOP=1 "$@" 2>/dev/null
}

# Wait for vm-0 to reach a target state. Args: $1 = state, $2 = max seconds.
wait_state() {
  local want="$1" max="$2"
  for _ in $(seq 1 "$max"); do
    local st
    st=$(curl -s -m 2 $API/vms/$VM_ID 2>/dev/null | grep -o '"state":"[a-z]*"' | cut -d'"' -f4)
    [ "$st" = "$want" ] && return 0
    sleep 1
  done
  return 1
}

# Wait for PG to accept connections (via proxy). Args: $1 = max seconds.
wait_pg() {
  local max="$1"
  for _ in $(seq 1 "$max"); do
    if psql_g -c "SELECT 1" >/dev/null 2>&1; then return 0; fi
    sleep 2
  done
  return 1
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
command -v psql >/dev/null && ok "psql on PATH ($(psql --version | head -1))" || die "psql not found (apt install postgresql-client)"
[ -d "$REPO/target/pg-install" ] && ok "PG build present (target/pg-install)" \
  || die "PG not built — run ./scripts/build_postgres.sh first"
for f in vmlinux-6.1 tikovm-base-rootfs.ext4 tiko-initramfs.cpio.gz; do
  [ -f "$ASSETS/$f" ] && ok "asset $f" || die "missing asset $ASSETS/$f (build with scripts/tikovm/build_base_rootfs.sh)"
done

# ---------------------------------------------------------------------------
# 0b. Build tikovm-guestd + tikovm-hostd + PG rootfs
# ---------------------------------------------------------------------------
step "0b. build guestd + hostd + PG rootfs"
( cd "$REPO" && cargo build -p tikovm-guest -p tikovm-host ) \
  || die "cargo build failed"
HOSTD="$REPO/target/debug/tikovm-hostd"
[ -x "$HOSTD" ] || die "tikovm-hostd not built"

if [ "${SKIP_BUILD:-0}" != "1" ]; then
  bash "$REPO/scripts/tikovm/build_pg_rootfs.sh" || die "build_pg_rootfs.sh failed"
fi
[ -f "$ROOTFS" ] || die "PG rootfs missing: $ROOTFS"
ok "PG rootfs: $ROOTFS"

bash "$REPO/scripts/tikovm/cleanup.sh" "" "$DD"
mkdir -p "$DD"

# Pre-create remote_slow source dir with our ownership (same as run_e2e.sh).
# Also drop any stale volume image from a previous failed run — the end-of-run
# cleanup is skipped when the script dies early, and a stale image breaks the
# next run's initdb (tiko_create: relfork already exists).
if mountpoint -q /mnt/s3files 2>/dev/null; then
  sudo -n mkdir -p "$REMOTE_SLOW_SOURCE/$VM_ID" 2>/dev/null || true
  sudo -n chown "$(id -u):$(id -g)" "$REMOTE_SLOW_SOURCE/$VM_ID" 2>/dev/null || true
else
  # Local fallback: use a dir under the test data dir.
  REMOTE_SLOW_SOURCE="$DD/remote-slow"
  mkdir -p "$REMOTE_SLOW_SOURCE/$VM_ID"
fi
rm -f "$REMOTE_SLOW_SOURCE/$VM_ID/archive.ext4" 2>/dev/null || true

# ---------------------------------------------------------------------------
# 1. Start hostd (API + proxy on $PROXY_PORT forwarding to guest:5432)
# ---------------------------------------------------------------------------
step "1. start tikovm-hostd (proxy on :$PROXY_PORT -> guest:5432)"
setsid "$HOSTD" --data-dir "$DD" --api-listen 0.0.0.0:9000 \
  --proxy-listen "0.0.0.0:$PROXY_PORT" --proxy-default-vm "$VM_ID" --proxy-default-port 5432 \
  >"$HOSTD_LOG" 2>&1 &
HOSTD_PG=$!
for _ in $(seq 1 30); do curl -sf -m 1 $API/health >/dev/null 2>&1 && break; sleep 0.5; done
curl -sf -m 2 $API/health >/dev/null && ok "hostd up (log: $HOSTD_LOG)" \
  || die "hostd did not start"

# ---------------------------------------------------------------------------
# Phase 1: SEED (cold start → PG initializes → verify SQL)
# ---------------------------------------------------------------------------
step "Phase 1: SEED (cold start → initdb → PG starts)"

PROV_JSON="$DD/provision-pg.json"
ASSETS="$ASSETS" ROOTFS="$ROOTFS" REMOTE_SLOW_SOURCE="$REMOTE_SLOW_SOURCE" \
  envsubst < "$PROV_TEMPLATE" > "$PROV_JSON" || die "envsubst failed"

echo "  provisioning $VM_ID (cold start) ..."
curl -s -m 120 -X POST $API/vms/provision \
     -H 'Content-Type: application/json' -d @"$PROV_JSON" >/dev/null \
  || die "provision failed"

echo "  waiting for PG to accept connections (initdb + start, may take ~3 min) ..."
if wait_pg 150; then
  ok "PG is up and accepting connections"
else
  bad "PG did not become reachable within 300s (see $HOSTD_LOG)"
  # Diagnostic: SSH into the guest and check what happened.
  GUEST_IP=172.16.0.2
  SSH_KEY=""
  for f in "$HOME/.ssh/id_ed25519" "$HOME/.ssh/id_rsa"; do
    [ -f "$f" ] && SSH_KEY="$f" && break
  done
  SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5 -o LogLevel=ERROR"
  [ -n "$SSH_KEY" ] && SSH_OPTS="$SSH_OPTS -i $SSH_KEY"
  echo "  --- diagnostic: guest state ---"
  curl -s -m 2 $API/vms/$VM_ID 2>/dev/null | python3 -m json.tool 2>/dev/null || true
  echo "  --- diagnostic: hostd log (last 30 lines) ---"
  tail -30 "$HOSTD_LOG" 2>/dev/null || true
  echo "  --- diagnostic: serial log (last 40 lines) ---"
  tail -40 "$DD/snapshots/runtime/$VM_ID.console.log" 2>/dev/null || echo "  (serial log not found)"
  echo "  --- diagnostic: SSH into guest ---"
  ssh $SSH_OPTS "root@$GUEST_IP" \
    "systemctl status tikovm-guestd --no-pager -l 2>/dev/null; echo '---'; journalctl -u tikovm-guestd --no-pager -n 30 2>/dev/null; echo '---'; ls -la /mnt/data/pgdata/ 2>/dev/null || echo 'pgdata not found'; echo '---'; which initdb su 2>/dev/null; echo '---'; cat /mnt/data/pgdata/postmaster.pid 2>/dev/null || echo 'PG not running'" \
    2>/dev/null || echo "  (SSH failed — VM may not be reachable)"
  die "cannot continue without PG"
fi

# Create a table + insert data.
echo "  creating seed table + inserting data ..."
psql_g -c "CREATE TABLE IF NOT EXISTS tt(a int)" || bad "CREATE TABLE failed"
psql_g -c "INSERT INTO tt VALUES (123)" || bad "INSERT failed"
RESULT=$(psql_g -c "SELECT a FROM tt" || echo "ERROR")
if [ "$RESULT" = "123" ]; then
  ok "seed data verified: SELECT a FROM tt -> $RESULT"
else
  bad "seed data mismatch: expected 123, got '$RESULT'"
  die "seed verification failed"
fi

# ---------------------------------------------------------------------------
# Phase 2: SCALE-TO-ZERO (idle → suspend → wake-on-connect)
# ---------------------------------------------------------------------------
step "Phase 2: SCALE-TO-ZERO (idle → suspend → wake-on-connect)"

echo "  waiting for idle evaluator to suspend the VM (~30-50s) ..."
if wait_state "suspended" 90; then
  ok "VM auto-suspended (idle evaluator fired)"
else
  st=$(curl -s -m 2 $API/vms/$VM_ID | grep -o '"state":"[a-z]*"' | cut -d'"' -f4)
  bad "VM did not auto-suspend within 90s (state=$st)"
fi

echo "  connecting via proxy (wake-on-connect) ..."
# psql triggers the proxy's wake-on-connect: proxy sees the TCP connection,
# calls ensure_running (restore from snapshot), then forwards to guest:5432.
WAKE_RESULT=$(psql_g -c "SELECT a FROM tt" 2>/dev/null || echo "ERROR")
if [ "$WAKE_RESULT" = "123" ]; then
  ok "wake-on-connect: PG restored, data intact (SELECT a FROM tt -> $WAKE_RESULT)"
else
  bad "wake-on-connect: data mismatch after restore (got '$WAKE_RESULT')"
fi

# ---------------------------------------------------------------------------
# Phase 3: WARM PAUSE/RESUME (bonus lifecycle)
# ---------------------------------------------------------------------------
step "Phase 3: WARM PAUSE/RESUME (bonus lifecycle)"

curl -s -m 30 -X POST $API/vms/$VM_ID/pause | grep -q '"state":"paused"' \
  && ok "pause: VM paused" \
  || bad "pause failed"
curl -s -m 30 -X POST $API/vms/$VM_ID/resume | grep -q '"state":"started"' \
  && ok "resume: VM resumed" \
  || bad "resume failed"

PAUSE_RESULT=$(psql_g -c "SELECT a FROM tt" 2>/dev/null || echo "ERROR")
if [ "$PAUSE_RESULT" = "123" ]; then
  ok "warm pause/resume: data intact (SELECT a FROM tt -> $PAUSE_RESULT)"
else
  bad "warm pause/resume: data mismatch (got '$PAUSE_RESULT')"
fi

# ---------------------------------------------------------------------------
# Phase 4: DESTROY + RE-PROVISION (local_fast persists across destroy)
# ---------------------------------------------------------------------------
step "Phase 4: DESTROY + RE-PROVISION (persist_key keeps PGDATA)"

echo "  destroying $VM_ID ..."
DESTROYED=""
for i in $(seq 1 6); do
  code=$(curl -s -m 30 -X DELETE $API/vms/$VM_ID -o /dev/null -w '%{http_code}')
  if [ "$code" = "204" ]; then
    DESTROYED=1
    echo "  destroy: HTTP $code"
    break
  fi
  echo "  destroy attempt $i: HTTP $code, retrying..."
  sleep 3
done
[ -n "$DESTROYED" ] && ok "VM destroyed" || die "destroy failed (HTTP $code)"

echo "  re-provisioning $VM_ID with the same persist_key ..."
curl -s -m 120 -X POST $API/vms/provision \
     -H 'Content-Type: application/json' -d @"$PROV_JSON" >/dev/null \
  || die "re-provision failed"

echo "  waiting for PG (existing PGDATA reused — no initdb, fast start) ..."
if wait_pg 120; then
  ok "PG is up after re-provision"
else
  bad "PG did not come back within 240s after re-provision (see $HOSTD_LOG)"
  die "cannot continue without PG"
fi

REPROV_RESULT=$(psql_g -c "SELECT a FROM tt" 2>/dev/null || echo "ERROR")
if [ "$REPROV_RESULT" = "123" ]; then
  ok "local_fast persisted across destroy: data intact (SELECT a FROM tt -> $REPROV_RESULT)"
else
  bad "local_fast persistence: data mismatch after re-provision (got '$REPROV_RESULT')"
fi

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
step "Cleanup"
for i in $(seq 1 6); do
  code=$(curl -s -m 30 -X DELETE $API/vms/$VM_ID -o /dev/null -w '%{http_code}')
  if [ "$code" = "204" ]; then
    echo "  destroy: HTTP $code"
    break
  fi
  echo "  destroy attempt $i: HTTP $code, retrying..."
  sleep 3
done
[ "$code" = "204" ] && ok "VM destroyed" || bad "destroy failed (HTTP $code)"

# Clean remote_slow artifacts.
if [ -f "$REMOTE_SLOW_SOURCE/$VM_ID/archive.ext4" ]; then
  rm -f "$REMOTE_SLOW_SOURCE/$VM_ID/archive.ext4" 2>/dev/null \
    || sudo -n rm -f "$REMOTE_SLOW_SOURCE/$VM_ID/archive.ext4" 2>/dev/null || true
  rmdir "$REMOTE_SLOW_SOURCE/$VM_ID" 2>/dev/null \
    || sudo -n rmdir "$REMOTE_SLOW_SOURCE/$VM_ID" 2>/dev/null || true
  ok "cleaned remote_slow volume image"
else
  ok "skipped remote_slow cleanup (image not found)"
fi

echo
echo "============================================="
echo "  PG serverless E2E result: $PASS passed, $FAIL failed"
echo "============================================="
[ "$FAIL" = 0 ]
