#!/bin/bash
#
# Start PostgreSQL inside the guest. Assumes the data dir was initialized by
# init_pg.sh. Safe to re-run: a no-op if the server is already running.
#
# Usage: /var/lib/postgresql/start_pg.sh

set -euo pipefail

. /var/lib/postgresql/tiko_env.sh

cd "$PGHOME"
if [ ! -d "$DB" ]; then
    echo "data dir $PGHOME/$DB not found — run init_pg.sh first" >&2
    exit 1
fi
if pg_ctl -D "$DB" status >/dev/null 2>&1; then
    exit 0
fi
pg_ctl -D "$DB" -l log.log start
