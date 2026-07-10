#!/bin/bash
#
# stress_create_dbs.sh — sequentially create N databases via the tikod
# control-plane API to measure how many concurrent VMs the host can sustain.
#
# Assumes tikod is already running and healthy, the bootstrap pack is in
# place, and the base VM assets exist. This script only issues POST /dbs
# requests and reports timing + success/failure counts.
#
# Usage:
#   ./stress_create_dbs.sh [COUNT]
#
# Env:
#   TIKOD_API        API base URL           (default http://127.0.0.1:9000)
#   STRESS_DB_COUNT  number of DBs to create (default 40, overridden by arg)
#   PER_CALL_TIMEOUT max seconds per create  (default 180)
#   CLEANUP          "1" to DELETE all VMs at the end (default 0)
#
# Examples:
#   ./stress_create_dbs.sh                    # create 40 DBs
#   ./stress_create_dbs.sh 10                 # create 10 DBs
#   CLEANUP=1 ./stress_create_dbs.sh 40       # create 40, then tear down

set -euo pipefail

TIKOD_API="${TIKOD_API:-http://127.0.0.1:9000}"
STRESS_DB_COUNT="${STRESS_DB_COUNT:-40}"
PER_CALL_TIMEOUT="${PER_CALL_TIMEOUT:-180}"
CLEANUP="${CLEANUP:-0}"

# positional arg overrides env
[ $# -ge 1 ] && STRESS_DB_COUNT="$1"

RE='^[0-9]+$'
if ! [[ "$STRESS_DB_COUNT" =~ $RE ]] || [ "$STRESS_DB_COUNT" -lt 1 ]; then
    echo "COUNT must be a positive integer (got '$STRESS_DB_COUNT')" >&2
    exit 1
fi

OK=0
FAIL=0
START_EPOCH=$(date +%s)

# ── Pre-flight: tikod must be up ───────────────────────────────────────────
echo ">>> pre-flight: checking tikod health at ${TIKOD_API}/health ..."
if ! curl -sf --max-time 10 "${TIKOD_API}/health" >/dev/null 2>&1; then
    echo "!!! tikod is not reachable at ${TIKOD_API} (GET /health failed)" >&2
    exit 1
fi
# ── Determine the starting db id ───────────────────────────────────────────
# Query the current max trailing integer across existing vm ids so we never
# reuse a db id that a previous run (still live or left behind) already owns.
MAX_DB_ID=$(curl -sf --max-time 10 "${TIKOD_API}/vms" 2>/dev/null \
    | python3 -c '
import sys, json, re
try:
    vms = json.load(sys.stdin).get("vms", [])
except Exception:
    vms = []
mx = 0
for v in vms:
    m = re.search(r"vm-(\d+)$", v.get("vm_id","") or "")
    if m:
        mx = max(mx, int(m.group(1)))
print(mx)
' 2>/dev/null || echo 0)

START_DB_ID=$((MAX_DB_ID + 1))
END_DB_ID=$((MAX_DB_ID + STRESS_DB_COUNT))

echo ">>> tikod is healthy. creating ${STRESS_DB_COUNT} databases (db_id ${START_DB_ID}..${END_DB_ID})."
echo ">>> per-call timeout: ${PER_CALL_TIMEOUT}s   cleanup at end: $([ "$CLEANUP" = "1" ] && echo yes || echo no)"
echo

# ── Create loop ────────────────────────────────────────────────────────────
n=0
for db_id in $(seq "$START_DB_ID" "$END_DB_ID"); do
    n=$((n + 1))
    t0=$(date +%s)
    printf '[%3d/%d] POST /dbs (vm-%d) ... ' "$n" "$STRESS_DB_COUNT" "$db_id"

    HTTP_CODE=$(curl -s -o /tmp/stress_resp.$$ -w '%{http_code}' \
        --max-time "$PER_CALL_TIMEOUT" \
        -X POST "${TIKOD_API}/dbs" \
        -H 'Content-Type: application/json' \
        -d "{\"vm_id\":\"vm-${db_id}\"}" 2>/dev/null) || HTTP_CODE="000"

    t1=$(date +%s)
    elapsed=$((t1 - t0))
    elapsed_total=$((t1 - START_EPOCH))

    BODY="$(cat /tmp/stress_resp.$$ 2>/dev/null || true)"
    rm -f /tmp/stress_resp.$$

    if [ "$HTTP_CODE" = "200" ]; then
        OK=$((OK + 1))
        printf 'OK  (%2ds, total %4ds)  %s\n' "$elapsed" "$elapsed_total" "$BODY"
    else
        FAIL=$((FAIL + 1))
        printf 'FAIL http=%s (%2ds)  %s\n' "$HTTP_CODE" "$elapsed" "$BODY"
    fi
done

END_EPOCH=$(date +%s)
WALL=$((END_EPOCH - START_EPOCH))

# ── Summary ────────────────────────────────────────────────────────────────
echo
echo "========================================"
echo "  Stress test complete"
echo " Requested : ${STRESS_DB_COUNT}"
echo " OK        : ${OK}"
echo " Failed    : ${FAIL}"
echo " Wall time : ${WALL}s  (avg $(( WALL / STRESS_DB_COUNT ))s/db)"
echo "========================================"

# ── Verify with GET /vms ──────────────────────────────────────────────────
echo
echo ">>> current VM list (GET /vms):"
curl -sf --max-time 10 "${TIKOD_API}/vms" 2>/dev/null \
    | python3 -c 'import sys,json; vms=json.load(sys.stdin).get("vms",[]); print(f"  {len(vms)} VM(s):", ", ".join(m.get("vm_id","?") for m in vms))' \
    2>/dev/null || echo "  (could not parse /vms response)"

# ── Optional cleanup ──────────────────────────────────────────────────────
if [ "$CLEANUP" = "1" ]; then
    echo
    echo ">>> CLEANUP: destroying all VMs ..."
    VM_IDS="$(curl -sf --max-time 10 "${TIKOD_API}/vms" 2>/dev/null \
        | python3 -c 'import sys,json; [print(m["vm_id"]) for m in json.load(sys.stdin).get("vms",[])]' \
        2>/dev/null || true)"
    for vid in $VM_IDS; do
        printf '  DELETE /vms/%s ... ' "$vid"
        code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 30 \
            -X DELETE "${TIKOD_API}/vms/${vid}" 2>/dev/null || echo "000")
        if [ "$code" = "204" ] || [ "$code" = "200" ]; then
            echo "ok"
        else
            echo "failed (http=$code)"
        fi
    done
    echo ">>> cleanup done."
fi

[ "$FAIL" -gt 0 ]