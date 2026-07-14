#!/bin/bash

set -e  # Exit on any error

# Clean up orphaned System V shmem segments from previously killed postgres runs.
# macOS default limit is 32 (kern.sysv.shmmni); each unclean exit leaks one segment.
ipcs -m | awk "/$(whoami)/"'{print $2}' | xargs ipcrm -m 2>/dev/null || true

# Set environment variables for Tiko configuration
unset TIKO_STORAGE_ROOT TIKO_LOCAL_PATH
export TIKO_ORG_ID="12"
export TIKO_DB_ID="34"
export TIKO_PROJECT_ID="56"

# Pin the macOS deployment target to the SDK's major version (e.g. "26.0").
# Without this, the Rust `cc` crate (zstd-sys and other C deps compiled into
# libtikosmgr.a) defaults to the full SDK version (e.g. 26.5) while clang
# floors to the major (26.0), triggering ld warnings like "object file ... was
# built for newer 'macOS' version (26.5) than being linked (26.0)" when
# libtikosmgr.a is linked into postgres.
if [ "$(uname)" = "Darwin" ]; then
    export MACOSX_DEPLOYMENT_TARGET="$(xcrun --show-sdk-version | cut -d. -f1).0"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET_DIR="${BASE_DIR}/target"
POSTGRES_INSTALL="${TARGET_DIR}/pg-install"
PG_BIN_DIR="${POSTGRES_INSTALL}/bin"
PG_LIB_DIR="${POSTGRES_INSTALL}/lib/postgresql"

echo "Building Tiko smgr..."
if ! (cargo build --manifest-path "${BASE_DIR}/Cargo.toml" -p smgr) >/dev/null; then
  echo "Tiko smgr build failed" >&2
  exit 1
fi

echo "Building PostgreSQL..."
rm -f "${BASE_DIR}/postgres/src/backend/postgres"
if ! (cd "${BASE_DIR}/postgres" && make -j4 && make install) >/dev/null; then
  echo "Postgres build/install failed" >&2
  exit 1
fi

echo "Building Tiko Worker..."
if ! (cargo build --manifest-path "${BASE_DIR}/Cargo.toml" -p worker) >/dev/null; then
  echo "Tiko Worker build failed" >&2
  exit 1
fi

# Copy the compiled worker library to destination
if [ -f "${TARGET_DIR}/debug/libtikoworker.dylib" ]; then
    echo "Copying Tiko Worker extension files ..."
    cp "${TARGET_DIR}/debug/libtikoworker.dylib" "${PG_LIB_DIR}"
fi
if [ -f "${TARGET_DIR}/debug/libtikoworker.so" ]; then
    echo "Copying Tiko Worker extension files ..."
    cp "${TARGET_DIR}/debug/libtikoworker.so" "${PG_LIB_DIR}"
fi

TEST_DIR="${BASE_DIR}/tt"
rm -rf "${TEST_DIR}" "${BASE_DIR}/log.log"
$PG_BIN_DIR/initdb -D "${TEST_DIR}"
cp "${SCRIPT_DIR}/postgresql.tiko.conf" "${TEST_DIR}/postgresql.tiko.conf"
echo "include_if_exists='postgresql.tiko.conf'" >> "${TEST_DIR}/postgresql.conf"

$PG_BIN_DIR/pg_ctl -D "${TEST_DIR}" -l "${BASE_DIR}/log.log" -w start

$PG_BIN_DIR/psql -d postgres -c "create temp table tt(a int);create index idx_tt on tt(a);insert into tt values(123);select * from tt;"

sleep 2

$PG_BIN_DIR/pg_ctl -D "${TEST_DIR}" -l "${BASE_DIR}/log.log" -w stop

echo
echo "Test run completed. 🎉"
echo
