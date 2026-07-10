#!/bin/bash
#
# Initialize (create) the PostgreSQL data directory inside the guest.
# Run once on a fresh rootfs / first boot. Re-runnable: it wipes and re-inits
# the data dir each invocation, so don't run it if you want to keep data.
#
# Usage: /var/lib/postgresql/init_pg.sh

set -euo pipefail

. /var/lib/postgresql/tiko_env.sh

cd "$PGHOME"
rm -rf "$DB" log.log

initdb -D "$DB"
cp postgresql.tiko.conf "$DB"
echo "include_if_exists='postgresql.tiko.conf'" >> "$DB/postgresql.conf"
# Trust connections from any per-VM subnet (172.16.0.0/16 covers 172.16.N.0/24).
echo "host all all 172.16.0.0/16 trust" >> "$DB/pg_hba.conf"
