#!/bin/bash

# Build script for PostgreSQL with Rust S3 storage manager

set -e  # Exit on any error

# Clean up orphaned System V shmem segments from previously killed postgres runs.
# macOS default limit is 32 (kern.sysv.shmmni); each unclean exit leaks one segment.
ipcs -m | awk "/$(whoami)/"'{print $2}' | xargs ipcrm -m 2>/dev/null || true

# Set environment variables for Tiko configuration
unset TIKO_STORAGE_ROOT TIKO_LOCAL_PATH
export TIKO_ORG_ID="12"
export TIKO_DB_ID="34"
export TIKO_PROJECT_ID="56"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET_DIR="${BASE_DIR}/target"
TEST_DIR="${BASE_DIR}/postgres/src/test/modules/test_tiko"
POSTGRES_INSTALL="${TARGET_DIR}/pg-install"
EXTENSION_DIR="${POSTGRES_INSTALL}/share/postgresql/extension"

echo "Building Tiko smgr..."
if ! (cargo build --manifest-path "${BASE_DIR}/Cargo.toml" -p smgr) >/dev/null 2>&1; then
  echo "Tiko smgr build failed" >&2
  exit 1
fi

echo "Verifying Rust library exists..."
if [ ! -f "${TARGET_DIR}/debug/libtikosmgr.a" ]; then
    echo "ERROR: Rust library libtikosmgr.a not found!"
    exit 1
fi

echo "Building PostgreSQL..."
rm -f "${BASE_DIR}/postgres/src/backend/postgres"
if ! (cd "${BASE_DIR}/postgres" && make -j4 && make install) >/dev/null 2>&1; then
  echo "Postgres build/install failed" >&2
  exit 1
fi

echo "Building Tiko Worker..."
if ! (cargo build --manifest-path "${BASE_DIR}/Cargo.toml" -p worker) >/dev/null 2>&1; then
  echo "Tiko Worker build failed" >&2
  exit 1
fi

# Copy the compiled library to the test directory for use in tests
if [ -f "${TARGET_DIR}/debug/libtikoworker.dylib" ]; then
    echo "Copying Tiko Worker extension files ..."
    cp "${TARGET_DIR}/debug/libtikoworker.dylib" "${TEST_DIR}/worker"
fi

echo "Running tests..."
if ! (cd "${BASE_DIR}/postgres" && EXTRA_INSTALL=src/test/modules/test_tiko/worker make check TESTS="create_index" PG_TEST_INITDB_EXTRA_OPTS='-c log_min_messages=debug1 -c shared_preload_libraries=libtikoworker'); then
  echo "PG test failed" >&2
  exit 1
fi

echo
echo "All PG tests passed. 🎉"
echo
