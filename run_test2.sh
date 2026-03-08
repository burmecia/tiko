#!/bin/bash

set -e  # Exit on any error

# Clean up orphaned System V shmem segments from previously killed postgres runs.
# macOS default limit is 32 (kern.sysv.shmmni); each unclean exit leaks one segment.
ipcs -m | awk "/$(whoami)/"'{print $2}' | xargs ipcrm -m 2>/dev/null || true

# Set environment variables for Tiko configuration
TIKO_ROOT_PATH="/Users/bolu/supabase/tiko/tt"
TIKO_ORG_ID="123"
TIKO_PROJECT_ID="0"
TIKO_BRANCH_ID="0"
TIKO_PITR_INTERVAL_SECS="300"

rm -rf tt log.log
./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/initdb -D tt
cp ../tmp/postgresql.conf tt/

./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/pg_ctl -D tt -l log.log start

./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/psql -d postgres -f ./load_data.sql