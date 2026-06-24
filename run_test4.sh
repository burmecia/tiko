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

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${BASE_DIR}/target"
POSTGRES_INSTALL="${TARGET_DIR}/pg-install"
PG_BIN_DIR="${POSTGRES_INSTALL}/bin"
PG_LIB_DIR="${POSTGRES_INSTALL}/lib/postgresql"

echo "Building Tiko smgr..."
if ! (cargo build -p smgr) >/dev/null; then
  echo "Tiko smgr build failed" >&2
  exit 1
fi

echo "Building PostgreSQL..."
rm -f postgres/src/backend/postgres
if ! (cd postgres && make -j4 && make install) >/dev/null; then
  echo "Postgres build/install failed" >&2
  exit 1
fi

echo "Building Tiko Worker..."
if ! (cargo build -p worker) >/dev/null; then
  echo "Tiko Worker build failed" >&2
  exit 1
fi

# Copy the compiled worker library to destination
if [ -f "${TARGET_DIR}/debug/libtikoworker.dylib" ]; then
    echo "Copying Tiko Worker extension files ..."
    cp "${TARGET_DIR}/debug/libtikoworker.dylib" "${PG_LIB_DIR}"
fi

rm -rf tt log.log
$PG_BIN_DIR/initdb -D tt
cp ./postgresql.conf.sample tt/postgresql.conf

$PG_BIN_DIR/pg_ctl -D tt -l log.log start

$PG_BIN_DIR/psql -d postgres -c "create temp table tt(a int);create index idx_tt on tt(a);insert into tt values(123);select * from tt;"

sleep 2

$PG_BIN_DIR/pg_ctl -D tt -l log.log stop