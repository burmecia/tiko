#!/bin/bash

set -e  # Exit on any error

# Clean up orphaned System V shmem segments from previously killed postgres runs.
# macOS default limit is 32 (kern.sysv.shmmni); each unclean exit leaks one segment.
ipcs -m | awk "/$(whoami)/"'{print $2}' | xargs ipcrm -m 2>/dev/null || true

# Set environment variables for Tiko configuration
TIKO_STORAGE_ROOT="/Users/bolu/.tiko"
TIKO_ORG_ID="456"
TIKO_PROJECT_ID="42"
TIKO_BRANCH_ID="1"
#TIKO_PITR_INTERVAL_SECS="300"

PG_BIN_DIR="./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin"

# Function: test_row_count
# Description: Tests if a table's row count equals expected value
test_row_count() {
    local table_name="${1:-tt}"
    local expected_count="${2:-2}"
    local count

    # Execute query and capture output
    count=$($PG_BIN_DIR/psql -t -A -c "SELECT COUNT(*) FROM $table_name;" -d "postgres" 2>&1)
    local psql_status=$?

    # Check if psql command succeeded
    if [ $psql_status -ne 0 ]; then
        echo "✗ psql command failed: $count"
        return 2
    fi

    # Trim whitespace
    count=$(echo "$count" | tr -d '[:space:]')

    # Compare count
    if [ "$count" -eq "$expected_count" ]; then
        echo "✓ Row count is $expected_count"
        return 0
    else
        echo "✗ Row count is $count (expected $expected_count)"
        return 1
    fi
}

start_db() {
    $PG_BIN_DIR/pg_ctl -D tt -l log.log start > /dev/null 2>&1
}

stop_db() {
    $PG_BIN_DIR/pg_ctl -D tt -l log.log stop > /dev/null 2>&1
}

restart_db() {
    stop_db
    start_db
}

create_branch() {
    rm -rf tt2 && rm -rf ~/.tiko/sim/standard/456/chunks/2 && rm -rf ~/.tiko/sim/standard/456/metadata/43
    rm -rf ~/.tiko/sim/standard/456/pitr/43 && truncate -s 0 log.log
    cargo run -p cli --bin tiko_ctl -- --tiko-root ~/.tiko create-branch \
        --org 456 --project 43 --branch 2 \
        --parent-project 42 --parent-branch 1 \
        --parent-pgdata ./tt \
        --template template-180001.tar.gz \
        --pg-data ./tt2
}
create_branch
exit 0

rm -rf tt && rm -rf ~/.tiko/* && truncate -s 0 log.log
cargo run -p cli --bin tiko_ctl -- --tiko-root ~/.tiko make-template --pg-bindir $PG_BIN_DIR
cargo run -p cli --bin tiko_ctl -- --tiko-root ~/.tiko create-org --org 456 --template template-180001.tar.gz
cargo run -p cli --bin tiko_ctl -- --tiko-root ~/.tiko create-branch --org 456 --project 42 --branch 1 --parent-project 0 --parent-branch 0  --template template-180001.tar.gz --pg-data ./tt

start_db
$PG_BIN_DIR/psql -d postgres -c "create table tt(a int);insert into tt values(123);insert into tt values(456);checkpoint;"

exit 0

restart_db
test_row_count "tt" 2

$PG_BIN_DIR/psql -d postgres -c "insert into tt values (789);" 
restart_db
test_row_count "tt" 3

restart_db
$PG_BIN_DIR/psql -d postgres -c "delete from tt where a = 789;"
restart_db
test_row_count "tt" 2
restart_db
test_row_count "tt" 2

$PG_BIN_DIR/psql -d postgres -c "drop table tt;"
restart_db
# Table no longer exists — psql should error; verify by expecting failure.
if $PG_BIN_DIR/psql -t -A -c "SELECT COUNT(*) FROM tt;" -d postgres > /dev/null 2>&1; then
    echo "✗ Table tt still exists after DROP"
else
    echo "✓ Table tt correctly absent after DROP and restart"
fi
stop_db