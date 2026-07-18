#!/bin/bash
# =============================================================================
# Cleanup helper for the tikovm e2e test.
#
# Usage: cleanup.sh [HOSTD_PG] [DD]
#   HOSTD_PG - process-group ID of the running tikovm-hostd (empty = skip)
#   DD       - data dir to remove (empty = skip)
#
# Kills any running tikovm-hostd (the tracked process group first, then a
# broad pkill fallback) and removes the data dir.
# =============================================================================
set -uo pipefail

HOSTD_PG="${1:-}"
DD="${2:-}"

if [ -n "$HOSTD_PG" ]; then
  kill -TERM -$HOSTD_PG 2>/dev/null
fi

pkill -f tikovm-hostd 2>/dev/null
sleep 1

if [ -n "$DD" ]; then
  rm -rf "$DD"
fi