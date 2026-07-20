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

# Stale TAP devices + their iptables rules from killed hostd runs.
for t in $(ip -o link show 2>/dev/null | awk -F': ' '{print $2}' | grep '^tikovm-tap'); do
  sudo -n ip link del "$t" 2>/dev/null
done
sudo -n iptables -S FORWARD 2>/dev/null | grep tikovm-tap | sed 's/^-A /-D /' | \
  while read -r r; do sudo -n iptables $r 2>/dev/null; done
sudo -n iptables -t nat -S POSTROUTING 2>/dev/null | grep '172\.16\.' | sed 's/^-A /-D /' | \
  while read -r r; do sudo -n iptables -t nat $r 2>/dev/null; done

if [ -n "$DD" ]; then
  rm -rf "$DD"
fi