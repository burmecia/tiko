#!/bin/bash
#
# cleanup.sh — tear down everything left behind by a stress test run.
#
#   1. Kill the tikod server process
#   2. Remove the tikod tmp directory
#   3. Delete all leftover tiko tap interfaces
#
# Usage:
#   ./cleanup.sh
#
# Env:
#   TIKOD_PID   kill a specific PID instead of pkill    (default: empty)

set -euo pipefail

# ── 1. Kill tikod ──────────────────────────────────────────────────────────
if [ -n "${TIKOD_PID:-}" ]; then
    echo ">>> killing tikod (pid ${TIKOD_PID}) ..."
    kill "$TIKOD_PID" 2>/dev/null || echo "  (pid ${TIKOD_PID} not running)"
else
    echo ">>> killing tikod processes (pkill -f \"target.*tikod\") ..."
    pkill -f "target.*tikod" 2>/dev/null && echo "  sent SIGTERM" || echo "  (no tikod processes found)"
fi

# give it a moment to exit cleanly
sleep 1

# ── 2. Remove tikod tmp directory ─────────────────────────────────────────
TIKOD_TMP_DIR="${TIKOD_TMP_DIR:-/tmp/tikod}"
echo ">>> removing ${TIKOD_TMP_DIR} ..."
rm -rf "$TIKOD_TMP_DIR" && echo "  done" || echo "  (nothing to remove)"

# ── 3. Delete leftover tiko tap interfaces ────────────────────────────────
echo ">>> removing tiko tap interfaces ..."
LEFTOVER_TAPS="$(ip -o link show 2>/dev/null | awk -F': ' '{print $2}' | grep '^tiko-tap' || true)"
if [ -z "$LEFTOVER_TAPS" ]; then
    echo "  (no tiko-tap-* interfaces found)"
else
    for tap in $LEFTOVER_TAPS; do
        printf '  deleting %s ... ' "$tap"
        sudo ip link delete "$tap" 2>/dev/null && echo "ok" || echo "failed"
    done
fi

echo
echo ">>> cleanup complete."
