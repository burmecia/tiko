#!/bin/bash
# Fake initdb used by tikoguest/tests/http_api.rs. Writes a minimal cluster layout
# (PG_VERSION + empty postgresql.conf + empty pg_hba.conf) into the -D <dir>
# argument so the agent's post-init wiring (append to those files) succeeds.
# @STATE_DIR@ is substituted with an absolute path per test fixture.
set -uo pipefail

STATE_DIR="@STATE_DIR@"
mkdir -p "$STATE_DIR"
echo "$*" >> "$STATE_DIR/initdb_calls.log"
# Capture the TIKO_* env vars we were invoked with (for pass-through tests).
env | grep '^TIKO_' | sort >> "$STATE_DIR/initdb_env.log"

# Parse -D <dir>.
DIR=""
prev=""
for a in "$@"; do
    if [ "$prev" = "-D" ]; then DIR="$a"; fi
    prev="$a"
done
if [ -z "$DIR" ]; then
    echo "fake-initdb: missing -D <dir>" >&2
    exit 1
fi

mkdir -p "$DIR"
echo "17.0" > "$DIR/PG_VERSION"
: > "$DIR/postgresql.conf"
: > "$DIR/pg_hba.conf"
exit 0
