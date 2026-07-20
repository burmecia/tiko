#!/bin/bash
# =============================================================================
# tikovm language-runtime Lambda-style e2e test (real KVM + Firecracker).
#
# Validates the 2nd tikovm workload kind — a lambda-like language-runtime
# serverless worker — with two request-driven lifecycle paths per runtime
# (Node.js 22 LTS, then Python 3.12):
#
#   cold-start path: provision VM from scratch  ->  wait for workload reachable
#                    ->  curl / and /health  ->  destroy the VM.  (pays full
#                    boot cost; one Lambda cold invocation per runtime)
#
#   warm-start path: provision VM from scratch  ->  wait for workload reachable
#                    ->  issue N back-to-back requests (only the first pays any
#                    runtime-level warmup; the rest are steady-state warm)
#                    ->  destroy the VM.  Reports per-request + avg warm latency
#                    so the cold-vs-warm delta is visible — the Lambda warm-start
#                    benefit.
#
# The VM is provisioned on demand for each path and destroyed once the response
# is served. No scale-to-zero suspend/wake (that path is covered by run_e2e.sh
# against the echo rootfs). Each step prints [PASS]/[FAIL]; exits non-zero on
# any failure.
#
# Prerequisites (checked at start):
#   * Linux + KVM (/dev/kvm), passwordless sudo (for TAP/iptables/mount)
#   * Firecracker binary on PATH or via $FIRECRACKER_BIN
#   * `envsubst` on PATH (gettext-base)
#   * Assets: tikod/assets/{vmlinux-6.1, tikovm-base-rootfs.ext4,
#             tiko-initramfs.cpio.gz}  (the lang rootfs is (re)built below)
# =============================================================================
set -uo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
ASSETS="$REPO/tikod/assets"
export FIRECRACKER_BIN="${FIRECRACKER_BIN:-/usr/local/bin/firecracker}"
export OVERLAY_SIZE_MB=512
API=http://127.0.0.1:9000
PROXY=http://127.0.0.1:8080
DD=/tmp/tikovm-lang-e2e
HOSTD_LOG="$DD/hostd.log"
HOSTD_PG=""
PROV_TEMPLATE="$REPO/scripts/tikovm/provision-lang.json"
NODE_ROOTFS="$ASSETS/lang-rootfs.ext4"
PYTHON_ROOTFS="$ASSETS/lang-python-rootfs.ext4"
# How many back-to-back requests the warm-start path issues per runtime.
WARM_REQ_COUNT="${WARM_REQ_COUNT:-3}"

PASS=0; FAIL=0
ok()   { echo "  [PASS] $1"; PASS=$((PASS+1)); }
bad()  { echo "  [FAIL] $1"; FAIL=$((FAIL+1)); }
step() { echo; echo "=== $1 ==="; }
die()  { echo "FATAL: $1" >&2; exit 1; }

cleanup() {
  bash "$REPO/scripts/tikovm/cleanup.sh" "$HOSTD_PG" "$DD"
}
trap cleanup EXIT

# -----------------------------------------------------------------------------
# Lambda cycle: cold-start provision -> serve one request -> destroy.
#   $1 = runtime label (node | python)
#   $2 = rootfs path
#   $3 = substring expected in the GET / response body
# Each cycle is a full, independent Lambda-style invocation.
# -----------------------------------------------------------------------------
lambda_cycle() {
  local runtime="$1" rootfs="$2" needle="$3"
  step "Lambda cycle: $runtime  (cold-start -> serve -> destroy)"

  local PROV_JSON="$DD/provision-$runtime.json"
  ASSETS="$ASSETS" ROOTFS="$rootfs" envsubst < "$PROV_TEMPLATE" > "$PROV_JSON" \
    || { bad "$runtime: envsubst failed"; return; }

  local t0 t1 cold_ms
  t0=$(date +%s%3N)
  echo "  provisioning vm-1 from $rootfs ..."
  curl -s -m 60 -X POST $API/vms/provision \
       -H 'Content-Type: application/json' -d @"$PROV_JSON" >/dev/null \
    || { bad "$runtime: provision request failed"; return; }

  echo "  waiting for workload reachable via proxy (cold start) ..."
  local ready=0
  for _ in $(seq 1 60); do
    if curl -sf -m 2 $PROXY/health >/dev/null 2>&1; then ready=1; break; fi
    sleep 2
  done
  t1=$(date +%s%3N)
  cold_ms=$((t1 - t0))
  if [ "$ready" = 1 ]; then
    ok "$runtime: cold start -> ready in ${cold_ms} ms"
  else
    bad "$runtime: workload not ready after ${cold_ms} ms (see $HOSTD_LOG)"
    curl -s -m 30 -X DELETE $API/vms/vm-1 -o /dev/null
    return
  fi

  local body
  body=$(curl -s -m 5 $PROXY/ | tr -d '\n')
  if echo "$body" | grep -q "$needle"; then
    ok "$runtime: GET / -> $body"
  else
    bad "$runtime: GET / mismatched (got: $body, want needle: $needle)"
  fi

  local h
  h=$(curl -s -m 5 $PROXY/health | tr -d '\n')
  case "$h" in
    *'"ok":true'*) ok "$runtime: GET /health -> $h" ;;
    *)             bad "$runtime: GET /health mismatched ($h)" ;;
  esac

  echo "  destroying vm-1 ..."
  curl -s -m 30 -X DELETE $API/vms/vm-1 \
       -o /dev/null -w "  destroy: HTTP %{http_code}\n"
  ok "$runtime: Lambda cycle complete (VM destroyed)"
}

# -----------------------------------------------------------------------------
# Warm-start cycle: provision -> N back-to-back warm requests -> destroy.
#   $1 = runtime label (node | python)
#   $2 = rootfs path
#   $3 = substring expected in the GET / response body
#
# The provision + wait-for-ready is the cold-start cost (reported for context);
# the per-request latencies are the warm-start measurements. Request 1 may
# include runtime-level warmup (V8 JIT, Python module init); requests 2..N are
# steady-state warm. The cold-vs-warm delta is the Lambda warm-start benefit.
# -----------------------------------------------------------------------------
warm_cycle() {
  local runtime="$1" rootfs="$2" needle="$3"
  step "Warm-start cycle: $runtime  (provision -> ${WARM_REQ_COUNT} warm requests -> destroy)"

  local PROV_JSON="$DD/provision-warm-$runtime.json"
  ASSETS="$ASSETS" ROOTFS="$rootfs" envsubst < "$PROV_TEMPLATE" > "$PROV_JSON" \
    || { bad "$runtime warm: envsubst failed"; return; }

  local t0 t1 cold_ms
  t0=$(date +%s%3N)
  echo "  provisioning vm-1 from $rootfs ..."
  curl -s -m 60 -X POST $API/vms/provision \
       -H 'Content-Type: application/json' -d @"$PROV_JSON" >/dev/null \
    || { bad "$runtime warm: provision request failed"; return; }

  echo "  waiting for workload reachable (cold start, not counted as warm) ..."
  local ready=0
  for _ in $(seq 1 60); do
    if curl -sf -m 2 $PROXY/health >/dev/null 2>&1; then ready=1; break; fi
    sleep 2
  done
  t1=$(date +%s%3N)
  cold_ms=$((t1 - t0))
  if [ "$ready" = 1 ]; then
    ok "$runtime warm: VM warm and ready (cold-start was ${cold_ms} ms)"
  else
    bad "$runtime warm: workload not ready after ${cold_ms} ms (see $HOSTD_LOG)"
    curl -s -m 30 -X DELETE $API/vms/vm-1 -o /dev/null
    return
  fi

  # Issue WARM_REQ_COUNT back-to-back requests. The VM is already up; each
  # request latency is the warm-start cost (proxy -> guest -> workload -> back).
  local i wt0 wt1 req_ms body warm_total=0 warm_ok=0
  echo "  issuing $WARM_REQ_COUNT warm requests ..."
  for i in $(seq 1 "$WARM_REQ_COUNT"); do
    wt0=$(date +%s%3N)
    body=$(curl -s -m 5 $PROXY/ | tr -d '\n')
    wt1=$(date +%s%3N)
    req_ms=$((wt1 - wt0))
    warm_total=$((warm_total + req_ms))
    if echo "$body" | grep -q "$needle"; then
      warm_ok=$((warm_ok + 1))
      ok "$runtime warm: request $i -> $body (${req_ms} ms)"
    else
      bad "$runtime warm: request $i mismatched (got: $body, want needle: $needle)"
    fi
  done

  if [ "$warm_ok" -gt 0 ]; then
    local avg_ms=$((warm_total / warm_ok))
    ok "$runtime warm: avg warm latency ${avg_ms} ms over $warm_ok/$WARM_REQ_COUNT ok (${cold_ms} ms cold = $(( cold_ms / (avg_ms > 0 ? avg_ms : 1) ))x)"
  else
    bad "$runtime warm: 0/$WARM_REQ_COUNT warm requests succeeded"
  fi

  echo "  destroying vm-1 ..."
  curl -s -m 30 -X DELETE $API/vms/vm-1 \
       -o /dev/null -w "  destroy: HTTP %{http_code}\n"
  ok "$runtime warm: cycle complete (VM destroyed)"
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
# 0b. Build tikovm-guestd + tikovm-hostd + lang rootfs (node) + python variant
# ---------------------------------------------------------------------------
step "0b. build guestd + hostd + lang rootfs (node) + python variant"
( cd "$REPO" && cargo build -p tikovm-guest -p tikovm-host ) \
  || die "cargo build failed"
HOSTD="$REPO/target/debug/tikovm-hostd"
[ -x "$HOSTD" ] || die "tikovm-hostd not built"

if [ "${SKIP_BUILD:-0}" != "1" ]; then
  bash "$REPO/scripts/tikovm/build_lang_rootfs.sh" >/dev/null \
    || die "build_lang_rootfs.sh failed"
fi
[ -f "$NODE_ROOTFS" ] || die "node rootfs missing: $NODE_ROOTFS"
ok "node rootfs: $NODE_ROOTFS"

# Python variant: sparse-copy the lang rootfs and rewrite
# /etc/tikovm/workload.toml to point [process] at python3 + echo-python.py.
# Demonstrates that a single base rootfs serves both runtimes via a one-file
# manifest swap (the platform is workload-agnostic; a different runtime is
# just a different rootfs).
echo "  building python variant (sparse copy + manifest swap) ..."
# If a previous run was killed mid-mount, the file may still be attached to a
# loop device. Best-effort detach before we replace it.
sudo -n losetup -j "$PYTHON_ROOTFS" 2>/dev/null | awk -F: '{print $1}' | \
  while read -r ld; do sudo -n losetup -d "$ld" 2>/dev/null || true; done
[ -f "$PYTHON_ROOTFS" ] && rm -f "$PYTHON_ROOTFS"
cp --sparse=always "$NODE_ROOTFS" "$PYTHON_ROOTFS"
PMNT=$(mktemp -d)
sudo mount -o loop "$PYTHON_ROOTFS" "$PMNT"
sudo tee "$PMNT/etc/tikovm/workload.toml" >/dev/null <<'TOML'
version = 1
workload = "lang-echo-python"

# Same shape as the node manifest; only [process] differs.
[process]
cmd = "/usr/bin/python3"
args = ["/usr/local/lib/tikovm/echo-python.py", "--port", "8080"]

[health]
kind = "http"
path = "/health"
port = 8080
interval_secs = 5

[expose]
http_port = 8080

[[volumes]]
name = "data"
tier = "local_fast"
mount_path = "/mnt/data"
size_mb = 64

[[volumes]]
name = "archive"
tier = "remote_slow"
mount_path = "/mnt/archive"
size_mb = 64
TOML
sync
sudo umount "$PMNT"
rmdir "$PMNT" 2>/dev/null || true
ok "python rootfs: $PYTHON_ROOTFS"

bash "$REPO/scripts/tikovm/cleanup.sh" "" "$DD"
mkdir -p "$DD"

# The s3files_image backing writes <source>/vm-1/ as the (unprivileged) hostd
# user; pre-create it with our ownership on the root-owned mount (same trick
# as run_e2e.sh). No-op if /mnt/s3files isn't mounted.
if mountpoint -q /mnt/s3files 2>/dev/null; then
  sudo -n mkdir -p /mnt/s3files/tikoblk/vm-1 2>/dev/null || true
  sudo -n chown "$(id -u):$(id -g)" /mnt/s3files/tikoblk/vm-1 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# 1. Start hostd (real Firecracker backend, proxy)
# ---------------------------------------------------------------------------
step "1. start tikovm-hostd"
setsid "$HOSTD" --data-dir "$DD" --api-listen 0.0.0.0:9000 \
  --proxy-listen 0.0.0.0:8080 --proxy-default-vm vm-1 --proxy-default-port 8080 \
  >"$HOSTD_LOG" 2>&1 &
HOSTD_PG=$!
for _ in $(seq 1 30); do curl -sf -m 1 $API/health >/dev/null 2>&1 && break; sleep 0.5; done
curl -sf -m 2 $API/health >/dev/null && ok "hostd up (log: $HOSTD_LOG)" \
  || die "hostd did not start"

# ---------------------------------------------------------------------------
# 2. Lambda cycles per runtime: cold-start path, then warm-start path
# ---------------------------------------------------------------------------
# Node.js 22 LTS
lambda_cycle "node" "$NODE_ROOTFS"   "hello world from node"
warm_cycle   "node" "$NODE_ROOTFS"   "hello world from node"
# Python 3.12
lambda_cycle "python" "$PYTHON_ROOTFS" "hello world from python"
warm_cycle   "python" "$PYTHON_ROOTFS" "hello world from python"

# ---------------------------------------------------------------------------
# 3. Cleanup remote_slow artifacts left behind by destroy
# ---------------------------------------------------------------------------
step "3. cleanup remote_slow artifacts"
if mountpoint -q /mnt/s3files 2>/dev/null; then
  sudo -n rm -f /mnt/s3files/tikoblk/vm-1/archive.ext4 2>/dev/null || true
  sudo -n rmdir /mnt/s3files/tikoblk/vm-1 2>/dev/null || true
  ok "cleaned s3files image dir"
else
  ok "skipped remote_slow cleanup (no /mnt/s3files)"
fi

echo
echo "============================================="
echo "  Lang Lambda E2E result: $PASS passed, $FAIL failed"
echo "============================================="
[ "$FAIL" = 0 ]
