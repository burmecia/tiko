#!/bin/bash

set -euo pipefail

PGHOME=/var/lib/postgresql

export TIKO_ORG_ID="12"
export TIKO_DB_ID="34"
export TIKO_PROJECT_ID="56"
export TIKO_STORAGE_ROOT="$PGHOME/tiko_root"
export TIKO_LOCAL_PATH="$PGHOME/toko_local"

cd $PGHOME
rm -rf tt log.log

initdb -D tt

echo >> tt/postgresql.conf << 'EOF'
# Add settings for extensions here
log_min_messages=debug1

shared_preload_libraries=libtikoworker
shared_buffers=256kB
log_statement=all

# WAL is streamed in real-time by the tikoworker background task.
# archive_mode / archive_command are not used with Tiko.
wal_level             = replica
max_wal_senders       = 4      # ≥ 1 for the streaming task; +1 spare
max_replication_slots = 4      # ≥ 1 for tiko_wal_stream; +1 spare

# Safety valve: bounds pg_wal disk usage if streaming falls behind.
# When hit, the slot is invalidated and a WAL gap results — size accordingly.
max_slot_wal_keep_size = 1GB

EOF

pg_ctl -D tt -l log.log start
