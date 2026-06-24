#!/bin/bash

set -e  # Exit on any error

# Clean up orphaned System V shmem segments from previously killed postgres runs.
# macOS default limit is 32 (kern.sysv.shmmni); each unclean exit leaks one segment.
ipcs -m | awk "/$(whoami)/"'{print $2}' | xargs ipcrm -m 2>/dev/null || true

# Set environment variables for Tiko configuration
unset TIKO_ROOT_PATH
export TIKO_ORG_ID="12"
export TIKO_DB_ID="34"
export TIKO_PROJECT_ID="56"
#export TIKO_PITR_INTERVAL_SECS="300"

rm -rf tt log.log
./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/initdb -D tt
cp ./postgresql.conf.sample tt/postgresql.conf

./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/pg_ctl -D tt -l log.log start

./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/psql -d postgres -f ./load_data.sql

./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/pg_ctl -D tt -l log.log stop