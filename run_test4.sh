#!/bin/bash

set -e  # Exit on any error

# Clean up orphaned System V shmem segments from previously killed postgres runs.
# macOS default limit is 32 (kern.sysv.shmmni); each unclean exit leaks one segment.
ipcs -m | awk "/$(whoami)/"'{print $2}' | xargs ipcrm -m 2>/dev/null || true

# Set environment variables for Tiko configuration
unset TIKO_ROOT_PATH
TIKO_ORG_ID="12"
TIKO_DB_ID="34"
TIKO_PROJECT_ID="56"

PG_BIN_DIR="./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin"

rm -rf tt log.log
$PG_BIN_DIR/initdb -D tt
cp ./postgresql.conf.sample tt/postgresql.conf

$PG_BIN_DIR/pg_ctl -D tt -l log.log start

$PG_BIN_DIR/psql -d postgres -c "create temp table tt(a int);create index idx_tt on tt(a);insert into tt values(123);select * from tt;"

$PG_BIN_DIR/pg_ctl -D tt -l log.log stop