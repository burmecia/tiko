#!/bin/bash

# Branch end-to-end test: create a copy-on-write branch of a RUNNING parent,
# then verify:
#   - the branch reads the parent's data through COW (shared storage),
#   - the branch accepts writes,
#   - the parent is unaffected (the two diverge).

set -e  # Exit on any error

# Clean up orphaned System V shmem segments from previously killed postgres runs.
ipcs -m | awk "/$(whoami)/"'{print $2}' | xargs ipcrm -m 2>/dev/null || true

# Parent identity (the branch shares the org, differs on db_id).
unset TIKO_LOCAL_PATH
export TIKO_ORG_ID="12"
export TIKO_DB_ID="34"
export TIKO_PROJECT_ID="56"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET_DIR="${BASE_DIR}/target"
PG_BIN_DIR="${TARGET_DIR}/pg-install/bin"
PG_LIB_DIR="${TARGET_DIR}/pg-install/lib/postgresql"
TIKO_BIN_DIR="${TARGET_DIR}/debug"
TEST_DIR="${BASE_DIR}/tt"
BRANCH_DIR="${BASE_DIR}/tt_branch"

export PATH="${PG_BIN_DIR}":$PATH

# Shared storage root OUTSIDE both PGDATAs so parent + branch share one tree
# (required for copy-on-write: the branch reads the parent's chunks in place).
export TIKO_STORAGE_ROOT="${BASE_DIR}/tiko_root"

echo "Building Tiko smgr + worker + cli..."
if ! (cargo build --manifest-path "${BASE_DIR}/Cargo.toml" -p smgr -p worker -p cli) >/dev/null; then
  echo "Build failed" >&2; exit 1
fi

echo "Building PostgreSQL..."
rm -f "${BASE_DIR}/postgres/src/backend/postgres"
if ! (cd "${BASE_DIR}/postgres" && make -j4 && make install) >/dev/null; then
  echo "Postgres build/install failed" >&2; exit 1
fi

if [ -f "${TARGET_DIR}/debug/libtikoworker.dylib" ]; then
    cp "${TARGET_DIR}/debug/libtikoworker.dylib" "${PG_LIB_DIR}"
fi

# Fresh parent cluster + shared storage root.
TIKO_PACK="${BASE_DIR}/tt_branch_pack.tar.zst"
rm -rf "${TEST_DIR}" "${BRANCH_DIR}" "${TIKO_PACK}" "${TIKO_STORAGE_ROOT}" "${BASE_DIR}/parent.log"
$PG_BIN_DIR/initdb -D "${TEST_DIR}" --auth=trust --no-instructions
cp "${SCRIPT_DIR}/postgresql.tiko.conf" "${TEST_DIR}/postgresql.tiko.conf"
echo "include_if_exists='postgresql.tiko.conf'" >> "${TEST_DIR}/postgresql.conf"

$PG_BIN_DIR/pg_ctl -D "${TEST_DIR}" -l "${BASE_DIR}/parent.log" start -w

# 1. Seed the parent with data spanning several pages, then checkpoint.
$PG_BIN_DIR/psql -p 5432 -d postgres -c \
  "create table branch_test(id int, data text); insert into branch_test select g, 'orig' from generate_series(1,200) g; checkpoint;"
sleep 2

# 2. Create the branch (db_id=35, port 5433) WHILE the parent keeps running.
#    pg_basebackup -X stream makes the backup self-contained (backup_label +
#    WAL), so the branch recovers to consistency and promotes with no
#    recovery.signal/target. Split into three steps:
#      a) `backup`  — pg_basebackup + pack to a tar.zst file (no Tiko storage).
#      b) `restore` — unpack the pack, seed the branch namespace with the
#         parent's base manifest (ChunkRef.db_id=parent → COW), run recovery and
#         promote, then STOP the branch (run `restart` to bring it back up).
#      c) `restart` — start the stopped branch PostgreSQL (final step).
echo "--- tiko_branch backup ---"
"${TIKO_BIN_DIR}/tiko_branch" backup \
  --pack "${TIKO_PACK}" \
  --pg-basebackup "${PG_BIN_DIR}/pg_basebackup"

echo "--- tiko_branch restore ---"
"${TIKO_BIN_DIR}/tiko_branch" restore \
  --pack "${TIKO_PACK}" \
  --parent-db-id 34 \
  --db-id 35 \
  --pgdata "${BRANCH_DIR}" --branch-port 5433 \
  --pg-ctl "${PG_BIN_DIR}/pg_ctl"

echo "--- tiko_branch restart ---"
"${TIKO_BIN_DIR}/tiko_branch" restart \
  --db-id 35 \
  --pgdata "${BRANCH_DIR}" --branch-port 5433 \
  --pg-ctl "${PG_BIN_DIR}/pg_ctl"

# 3. Verify the branch reads the parent's data via copy-on-write.
echo "--- verify branch (COW read of parent data) ---"
BRANCH_COUNT=$($PG_BIN_DIR/psql -p 5433 -d postgres -Atqc "select count(*) from branch_test")
echo "branch row count: ${BRANCH_COUNT} (expect 200)"
if [ "${BRANCH_COUNT}" != "200" ]; then
  echo "BRANCH FAILED: expected 200 rows from parent via COW, got ${BRANCH_COUNT}" >&2
  exit 1
fi

# 4. Write on the branch; verify the parent is unaffected (divergence).
$PG_BIN_DIR/psql -p 5433 -d postgres -c \
  "insert into branch_test values(999,'branch_only'); checkpoint;"
sleep 1
PARENT_COUNT=$($PG_BIN_DIR/psql -p 5432 -d postgres -Atqc "select count(*) from branch_test")
BRANCH_COUNT2=$($PG_BIN_DIR/psql -p 5433 -d postgres -Atqc "select count(*) from branch_test")
echo "after branch insert: parent=${PARENT_COUNT} (expect 200), branch=${BRANCH_COUNT2} (expect 201)"
if [ "${PARENT_COUNT}" != "200" ]; then
  echo "BRANCH FAILED: parent changed to ${PARENT_COUNT} (should be unaffected by the branch write)" >&2
  exit 1
fi
if [ "${BRANCH_COUNT2}" != "201" ]; then
  echo "BRANCH FAILED: branch has ${BRANCH_COUNT2} rows (expected 201)" >&2
  exit 1
fi

# Cleanup.
$PG_BIN_DIR/pg_ctl -D "${BRANCH_DIR}" stop -m fast -w
$PG_BIN_DIR/pg_ctl -D "${TEST_DIR}" stop -m fast -w
rm -f "${TIKO_PACK}"

echo
echo "Branch test passed. ✅"
echo
