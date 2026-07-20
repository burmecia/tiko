#!/bin/bash
# =============================================================================
# tikovm language-runtime Lambda-style e2e test (real KVM + Firecracker).
#
# Validates the 2nd tikovm workload kind — a lambda-like language-runtime
# serverless worker — with the Lambda compute-vs-storage separation:
#
#   - The rootfs (lang-rootfs.ext4) contains ONLY the language RUNTIMES
#     (Node.js + Python) and a generic bootstrap loader. NO application code.
#   - The application CODE (echo-node.js, echo-python.py) lives on the
#     remote_slow volume, deployed by this test before provisioning.
#   - At cold start, the bootstrap loader reads the code from /mnt/archive/,
#     selects the runtime via a .runtime marker file, and exec's the handler.
#
# Two request-driven lifecycle paths per runtime (Node.js 22 LTS, Python 3.12):
#
#   cold-start path: seed code -> provision VM -> wait for workload reachable
#                    -> curl / and /health -> destroy the VM.
#
#   warm-start path: provision VM -> wait for workload reachable -> issue N
#                    back-to-back requests -> destroy. Reports per-request +
#                    avg warm latency so the cold-vs-warm delta is visible.
#
# Switching runtimes is a STORAGE-SIDE operation: call seed_volume to change
# the .runtime marker on the remote_slow volume. The rootfs is unchanged —
# the same ephemeral compute image serves both runtimes.
#
# Each step prints [PASS]/[FAIL]; exits non-zero on any failure.
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
LANG_ROOTFS="$ASSETS/lang-rootfs.ext4"
LANG_CODE_DIR="$REPO/scripts/tikovm/lang-code"
# Where the remote_slow volume image is placed on the host. Defaults to the
# S3 Files mount; override with REMOTE_SLOW_SOURCE for a local fallback.
REMOTE_SLOW_SOURCE="${REMOTE_SLOW_SOURCE:-/mnt/s3files/tikoblk}"
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
# Deploy "function code" to the remote_slow volume. Creates the ext4 image
# (labeled "archive") if it doesn't exist, then writes the handler source +
# a .runtime marker. The provisioner is idempotent (skips image creation if
# it already exists), so a pre-seeded image is used as-is at provision time.
#
# This is the Lambda "deploy code" step — done BEFORE provisioning the compute
# VM, and persisted ACROSS destroy/provision cycles. Switching runtimes is
# just re-calling this with a different marker value.
#
# Args: $1 = runtime ("node" or "python")
# -----------------------------------------------------------------------------
seed_volume() {
  local runtime="$1"
  local img_dir="$REMOTE_SLOW_SOURCE/vm-1"
  local img="$img_dir/archive.ext4"

  mkdir -p "$img_dir" 2>/dev/null || sudo -n mkdir -p "$img_dir"
  if [ ! -w "$img_dir" ]; then
    sudo -n chown "$(id -u):$(id -g)" "$img_dir" 2>/dev/null || true
  fi

  if [ ! -f "$img" ]; then
    truncate -s 64M "$img"
    mkfs.ext4 -q -L archive "$img"
  fi

  local mnt
  mnt=$(mktemp -d)
  sudo mount -o loop "$img" "$mnt"
  sudo mkdir -p "$mnt/code"
  sudo install -m644 "$LANG_CODE_DIR/echo-node.js"   "$mnt/code/echo-node.js"
  sudo install -m644 "$LANG_CODE_DIR/echo-python.py" "$mnt/code/echo-python.py"
  echo "$runtime" | sudo tee "$mnt/code/.runtime" >/dev/null
  sync
  sudo umount "$mnt"
  rmdir "$mnt" 2>/dev/null || true
  ok "deployed function code to remote_slow: $img (runtime=$runtime)"
}

# -----------------------------------------------------------------------------
# Lambda cycle: cold-start provision -> serve one request -> destroy.
#   $1 = runtime label (node | python)
#   $2 = substring expected in the GET / response body
# Uses the single LANG_ROOTFS for both runtimes; runtime selection is via
# the .runtime marker on the remote_slow volume (seed_volume).
# -----------------------------------------------------------------------------
lambda_cycle() {
  local runtime="$1" needle="$2"
  step "Lambda cycle: $runtime  (cold-start -> serve -> destroy)"

  local PROV_JSON="$DD/provision-$runtime.json"
  ASSETS="$ASSETS" ROOTFS="$LANG_ROOTFS" REMOTE_SLOW_SOURCE="$REMOTE_SLOW_SOURCE" \
    envsubst < "$PROV_TEMPLATE" > "$PROV_JSON" \
    || { bad "$runtime: envsubst failed"; return; }

  local t0 t1 cold_ms
  t0=$(date +%s%3N)
  echo "  provisioning vm-1 (rootfs=$LANG_ROOTFS, code from remote_slow) ..."
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
  # Match both Node's {"ok":true} and Python's {"ok": true} (json.dumps adds
  # a space after the colon; JSON.stringify does not).
  case "$h" in
    *'"ok"'*true*) ok "$runtime: GET /health -> $h" ;;
    *)             bad "$runtime: GET /health mismatched ($h)" ;;
  esac

  echo "  destroying vm-1 ..."
  curl -s -m 30 -X DELETE $API/vms/vm-1 \
       -o /dev/null -w "  destroy: HTTP %{http_code}\n"
  ok "$runtime: Lambda cycle complete (VM destroyed, code persists on remote_slow)"
}

# -----------------------------------------------------------------------------
# Warm-start cycle: provision -> N back-to-back warm requests -> destroy.
#   $1 = runtime label (node | python)
#   $2 = substring expected in the GET / response body
# -----------------------------------------------------------------------------
warm_cycle() {
  local runtime="$1" needle="$2"
  step "Warm-start cycle: $runtime  (provision -> ${WARM_REQ_COUNT} warm requests -> destroy)"

  local PROV_JSON="$DD/provision-warm-$runtime.json"
  ASSETS="$ASSETS" ROOTFS="$LANG_ROOTFS" REMOTE_SLOW_SOURCE="$REMOTE_SLOW_SOURCE" \
    envsubst < "$PROV_TEMPLATE" > "$PROV_JSON" \
    || { bad "$runtime warm: envsubst failed"; return; }

  local t0 t1 cold_ms
  t0=$(date +%s%3N)
  echo "  provisioning vm-1 ..."
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
[ -f "$LANG_CODE_DIR/echo-node.js" ] && ok "handler: echo-node.js" || die "missing $LANG_CODE_DIR/echo-node.js"
[ -f "$LANG_CODE_DIR/echo-python.py" ] && ok "handler: echo-python.py" || die "missing $LANG_CODE_DIR/echo-python.py"

# ---------------------------------------------------------------------------
# 0b. Build tikovm-guestd + tikovm-hostd + lang rootfs (runtimes + bootstrap)
# ---------------------------------------------------------------------------
step "0b. build guestd + hostd + lang rootfs (runtimes only, no app code)"
( cd "$REPO" && cargo build -p tikovm-guest -p tikovm-host ) \
  || die "cargo build failed"
HOSTD="$REPO/target/debug/tikovm-hostd"
[ -x "$HOSTD" ] || die "tikovm-hostd not built"

if [ "${SKIP_BUILD:-0}" != "1" ]; then
  bash "$REPO/scripts/tikovm/build_lang_rootfs.sh" >/dev/null \
    || die "build_lang_rootfs.sh failed"
fi
[ -f "$LANG_ROOTFS" ] || die "lang rootfs missing: $LANG_ROOTFS"
ok "lang rootfs: $LANG_ROOTFS (runtimes + bootstrap, code is on remote_slow)"

bash "$REPO/scripts/tikovm/cleanup.sh" "" "$DD"
mkdir -p "$DD"

# ---------------------------------------------------------------------------
# 0c. Deploy function code to remote_slow volume (the Lambda "deploy" step)
# ---------------------------------------------------------------------------
step "0c. deploy function code to remote_slow (runtime: node)"
seed_volume "node"

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
# 2. Node.js Lambda cycles: cold-start, then warm-start
# ---------------------------------------------------------------------------
lambda_cycle "node" "hello world from node"
warm_cycle   "node" "hello world from node"

# ---------------------------------------------------------------------------
# 3. Switch to Python: redeploy code on remote_slow (change .runtime marker)
#    The rootfs is UNCHANGED — same ephemeral compute image. This is the
#    Lambda "deploy new function" operation: pure storage-side, no rebuild.
# ---------------------------------------------------------------------------
step "3. redeploy function code to remote_slow (runtime: python)"
seed_volume "python"

# ---------------------------------------------------------------------------
# 4. Python Lambda cycles: cold-start, then warm-start
# ---------------------------------------------------------------------------
lambda_cycle "python" "hello world from python"
warm_cycle   "python" "hello world from python"

# ---------------------------------------------------------------------------
# 5. Cleanup remote_slow artifacts left behind by destroy
# ---------------------------------------------------------------------------
step "5. cleanup remote_slow artifacts"
if [ -f "$REMOTE_SLOW_SOURCE/vm-1/archive.ext4" ]; then
  rm -f "$REMOTE_SLOW_SOURCE/vm-1/archive.ext4" 2>/dev/null \
    || sudo -n rm -f "$REMOTE_SLOW_SOURCE/vm-1/archive.ext4" 2>/dev/null || true
  rmdir "$REMOTE_SLOW_SOURCE/vm-1" 2>/dev/null \
    || sudo -n rmdir "$REMOTE_SLOW_SOURCE/vm-1" 2>/dev/null || true
  ok "cleaned remote_slow volume image"
else
  ok "skipped remote_slow cleanup (image not found)"
fi

echo
echo "============================================="
echo "  Lang Lambda E2E result: $PASS passed, $FAIL failed"
echo "============================================="
[ "$FAIL" = 0 ]
