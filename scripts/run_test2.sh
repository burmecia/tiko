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
#export TIKO_PITR_INTERVAL_SECS="300"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET_DIR="${BASE_DIR}/target"
POSTGRES_INSTALL="${TARGET_DIR}/pg-install"
PG_BIN_DIR="${POSTGRES_INSTALL}/bin"
TEST_DIR="${BASE_DIR}/tt"

# How many times to duplicate the sample dataset (199 rows each).
# Default: 50x = ~9950 rows. Override with TIKO_TEST_DATA_DUPLICATES env var.
DUPLICATES="${TIKO_TEST_DATA_DUPLICATES:-50}"

# Build a large CSV from the committed small sample by duplicating data rows.
# The sample file has a header + data rows; we keep one header and repeat the
# data rows DUPLICATES times, regenerating order_id to avoid PK conflicts if
# needed downstream.
SAMPLE_CSV="${SCRIPT_DIR}/test_data/ecommerce_dataset_small.csv"
LARGE_CSV="${BASE_DIR}/target/ecommerce_dataset_large.csv"

echo "Generating large dataset (${DUPLICATES}x sample rows)..."
mkdir -p "${BASE_DIR}/target"
# Write header
head -1 "${SAMPLE_CSV}" > "${LARGE_CSV}"
# Write data rows DUPLICATES times
for ((i = 0; i < DUPLICATES; i++)); do
    tail -n +2 "${SAMPLE_CSV}" >> "${LARGE_CSV}"
done
ROW_COUNT=$(($(wc -l < "${LARGE_CSV}") - 1))
echo "Generated ${LARGE_CSV} with ${ROW_COUNT} data rows"

rm -rf "${TEST_DIR}" "${BASE_DIR}/log.log"
$PG_BIN_DIR/initdb -D "${TEST_DIR}"
cp "${SCRIPT_DIR}/postgresql.tiko.conf" "${TEST_DIR}/postgresql.tiko.conf"
echo "include_if_exists='postgresql.tiko.conf'" >> "${TEST_DIR}/postgresql.conf"

$PG_BIN_DIR/pg_ctl -D "${TEST_DIR}" -l "${BASE_DIR}/log.log" start

$PG_BIN_DIR/psql -d postgres -v csvfile="'${LARGE_CSV}'" -f "${SCRIPT_DIR}/load_data.sql"

$PG_BIN_DIR/pg_ctl -D "${TEST_DIR}" -l "${BASE_DIR}/log.log" stop
