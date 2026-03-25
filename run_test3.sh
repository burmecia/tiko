#!/bin/bash

set -e  # Exit on any error

# Clean up orphaned System V shmem segments from previously killed postgres runs.
# macOS default limit is 32 (kern.sysv.shmmni); each unclean exit leaks one segment.
ipcs -m | awk "/$(whoami)/"'{print $2}' | xargs ipcrm -m 2>/dev/null || true

# Set environment variables for Tiko configuration
TIKO_ROOT_PATH="/Users/bolu/.tiko"
TIKO_ORG_ID="456"
TIKO_PROJECT_ID="42"
TIKO_BRANCH_ID="1"
TIKO_PITR_INTERVAL_SECS="300"


rm -rf tt && rm -rf ~/.tiko/* && truncate -s 0 log.log
cargo run -p cli --bin tiko_ctl -- --tiko-root ~/.tiko make-template --pg-bindir ./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin
cargo run -p cli --bin tiko_ctl -- --tiko-root ~/.tiko create-org --org 456 --template template-180001.tar.gz 
cargo run -p cli --bin tiko_ctl -- --tiko-root ~/.tiko create-branch --org 456 --project 42 --branch 1 --parent-project 0 --parent-branch 0  --template template-180001.tar.gz --lsn 000000000201F770 --pg-data ./tt
./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/pg_ctl -D tt -l log.log start
./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/psql -d postgres -c "create table tt(a int);insert into tt values(123);insert into tt values(456);"
./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/pg_ctl -D tt -l log.log stop 
./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/pg_ctl -D tt -l log.log start
./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/psql -d postgres -c "select count(*) from tt;" 

./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/psql -d postgres -c "insert into tt values (789);" 
./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/pg_ctl -D tt -l log.log stop 
./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/pg_ctl -D tt -l log.log start
./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/psql -d postgres -c "select count(*) from tt;"
./postgres/tmp_install/Users/bolu/supabase/tiko/target/pg-install/bin/pg_ctl -D tt -l log.log stop 