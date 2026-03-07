#!/bin/bash

# Build script for PostgreSQL with Rust S3 storage manager

set -e  # Exit on any error

# Clean up orphaned System V shmem segments from previously killed postgres runs.
# macOS default limit is 32 (kern.sysv.shmmni); each unclean exit leaks one segment.
ipcs -m | awk "/$(whoami)/"'{print $2}' | xargs ipcrm -m 2>/dev/null || true

# Set environment variables for Tiko configuration
TIKO_ORG_ID="123"
TIKO_PROJECT_ID="0"
TIKO_BRANCH_ID="0"

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${BASE_DIR}/target"
TEST_DIR="${BASE_DIR}/postgres/src/test/modules/test_tiko"
POSTGRES_INSTALL="${TARGET_DIR}/pg-install"
EXTENSION_DIR="${POSTGRES_INSTALL}/share/postgresql/extension"

echo "Building S3smgr..."
if ! (cd s3smgr && cargo build --release) >/dev/null; then
  echo "S3smgr build failed" >&2
  exit 1
fi

echo "Verifying Rust library exists..."
if [ ! -f "${TARGET_DIR}/release/libs3smgr.a" ]; then
    echo "ERROR: Rust library libs3smgr.a not found!"
    exit 1
fi

echo "Building PostgreSQL..."
rm -f postgres/src/backend/postgres
#if ! (cd postgres && make && make install) >/dev/null; then
#  echo "Postgres build/install failed" >&2
#  exit 1
#fi

echo "Building S3 Worker..."
if ! (cd s3worker && cargo build --release) >/dev/null; then
  echo "S3 Worker build failed" >&2
  exit 1
fi

# Copy the compiled library to the test directory for use in tests
if [ -f "${TARGET_DIR}/release/libs3worker.dylib" ]; then
    echo "Copying S3 Worker extension files ..."
    cp "${TARGET_DIR}/release/libs3worker.dylib" "${TEST_DIR}/s3worker"
fi

echo "Running tests..."
if ! (cd postgres/src/test/modules/test_tiko && make check PG_TEST_INITDB_EXTRA_OPTS='-c log_min_messages=debug1 -c shared_preload_libraries=libs3worker -c shared_buffers=256kB') >/dev/null; then
  echo "Test Tiko failed" >&2
  exit 1
fi

echo "All tests passed. 🎉"
