#!/bin/bash
# Fake pg_ctl used by pgctl/tests/http_api.rs. Simulates the real binary using a
# marker file (<STATE_DIR>/running) so start/stop/status behave like a real
# postmaster. Every argv line is appended to calls.log for assertions.
# @STATE_DIR@ is substituted with an absolute path per test fixture.
set -uo pipefail

STATE_DIR="@STATE_DIR@"
mkdir -p "$STATE_DIR"
echo "$*" >> "$STATE_DIR/calls.log"
# Capture the TIKO_* env vars we were invoked with (for pass-through tests).
env | grep '^TIKO_' | sort >> "$STATE_DIR/pg_ctl_env.log"

# The command is the last argument.
CMD="${!#}"

case "$CMD" in
  status)
    if [ -f "$STATE_DIR/running" ]; then
      echo "pg_ctl: server is running (PID: $(cat "$STATE_DIR/running"))"
      exit 0
    else
      echo "pg_ctl: no server running" >&2
      exit 4
    fi
    ;;
  start)
    echo $$ > "$STATE_DIR/running"
    exit 0
    ;;
  stop)
    rm -f "$STATE_DIR/running"
    exit 0
    ;;
  restart)
    echo $$ > "$STATE_DIR/running"
    exit 0
    ;;
  reload)
    exit 0
    ;;
  *)
    echo "fake-pg_ctl: unknown command '$CMD'" >&2
    exit 1
    ;;
esac
