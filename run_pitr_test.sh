#!/bin/bash

# PITR end-to-end test: take a pg_basebackup, mutate, recover to a target LSN,
# and verify the recovered state (base-backup rows via the smgr cache-miss path
# + WAL-replayed rows). Exercises the full tiko_pitr backup/recover flow.

set -e  # Exit on any error

# Clean up orphaned System V shmem segments from previously killed postgres runs.
ipcs -m | awk "/$(whoami)/"'{print $2}' | xargs ipcrm -m 2>/dev/null || true

# Set environment variables for Tiko configuration
unset TIKO_STORAGE_ROOT TIKO_LOCAL_PATH
export TIKO_ORG_ID="12"
export TIKO_DB_ID="34"
export TIKO_PROJECT_ID="56"
# Keep the Tiko storage root OUTSIDE PGDATA so it survives PGDATA wipe/restore.
# Per-db local cache (base_manifest.tikm etc.) defaults to $PGDATA/tiko.
export TIKO_STORAGE_ROOT="${PWD}/tiko_root"

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${BASE_DIR}/target"
POSTGRES_INSTALL="${TARGET_DIR}/pg-install"
PG_BIN_DIR="${POSTGRES_INSTALL}/bin"
PG_LIB_DIR="${POSTGRES_INSTALL}/lib/postgresql"
TIKO_BIN_DIR="${TARGET_DIR}/debug"

echo "Building Tiko smgr..."
if ! (cargo build -p smgr) >/dev/null; then
  echo "Tiko smgr build failed" >&2; exit 1
fi

echo "Building PostgreSQL..."
rm -f postgres/src/backend/postgres
if ! (cd postgres && make -j4 && make install) >/dev/null; then
  echo "Postgres build/install failed" >&2; exit 1
fi

echo "Building Tiko CLI + Worker..."
if ! (cargo build -p cli -p worker) >/dev/null; then
  echo "Tiko CLI/Worker build failed" >&2; exit 1
fi

if [ -f "${TARGET_DIR}/debug/libtikoworker.dylib" ]; then
    cp "${TARGET_DIR}/debug/libtikoworker.dylib" "${PG_LIB_DIR}"
fi

# Fresh cluster + fresh Tiko storage root.
rm -rf tt "${TIKO_STORAGE_ROOT}" log.log recovery.out
$PG_BIN_DIR/initdb -D tt --auth=trust --no-instructions
cp ./postgresql.conf.sample tt/postgresql.conf

# Stop any postgres that might still own port 5432 / the tt data dir.
$PG_BIN_DIR/pg_ctl -D tt -l log.log stop -m fast -w 2>/dev/null || true

$PG_BIN_DIR/pg_ctl -D tt -l log.log start -w

# 1. Seed the table BEFORE the backup with enough rows to span several pages,
#    then checkpoint so they land in a segment.
$PG_BIN_DIR/psql -d postgres -c \
  "create table pitr_test(id int, data text); insert into pitr_test select g, 'orig' from generate_series(1,200) g; checkpoint;"
sleep 2

# 2. Take a base backup. pg_basebackup connects over the local socket as the
#    current user; the CHECKPOINT_CAUSE_BASEBACKUP checkpoint forms a base
#    manifest AT the backup LSN.
echo "--- tiko_pitr backup ---"
"${TIKO_BIN_DIR}/tiko_pitr" backup

# 3. Insert a row AFTER the backup (must replay in), checkpoint, and capture a
#    target LSN safely mid-window.
$PG_BIN_DIR/psql -d postgres -c \
  "insert into pitr_test values(201,'after_backup'); checkpoint;"
TARGET_LSN=$($PG_BIN_DIR/psql -d postgres -Atqc "select pg_current_wal_lsn()")
echo "recover target (after first mutation): ${TARGET_LSN}"

# 3b. AFTER the target, UPDATE an existing row (id=50) whose page existed at
#     the backup and is NOT touched by the replayed insert above — so the page
#     is never brought into buffers during recovery. Post-promote, reading
#     id=50 must return the backup value ('orig'), NOT 'FUTURE'. With the old
#     hydration bug the smgr would resolve the post-target (future) chunk
#     version instead of the base-manifest one. pg_switch_wal() + checkpoint
#     force the WAL (and the dirty page) to archive/flush deterministically.
$PG_BIN_DIR/psql -d postgres -c \
  "update pitr_test set data='FUTURE' where id=50; checkpoint;"
$PG_BIN_DIR/psql -d postgres -Atqc "select pg_switch_wal()" >/dev/null

# 4. Wait for the wal_receiver to archive enough WAL that the recoverable
#    window is available (the backup's checkpoint WAL is archived). Poll the
#    list output instead of a fixed sleep so the test is robust to archiving
#    lag.
echo "--- waiting for recoverable window ---"
window_ready=0
for _ in $(seq 1 60); do
  # `tiko_pitr list` emits JSON; the recoverable window is populated once the
  # "latest_lsn" field appears (it's omitted while WAL coverage is incomplete).
  if "${TIKO_BIN_DIR}/tiko_pitr" list 2>/dev/null | grep -q '"latest_lsn"'; then
    window_ready=1
    break
  fi
  sleep 1
done
if [ "$window_ready" != "1" ]; then
  echo "PITR FAILED: recoverable window never became available" >&2
  "${TIKO_BIN_DIR}/tiko_pitr" list >&2
  $PG_BIN_DIR/pg_ctl -D tt -l log.log stop -m fast -w 2>/dev/null || true
  exit 1
fi

# 4b. Sanity: print the recoverable window (target must be within it).
echo "--- tiko_pitr list ---"
"${TIKO_BIN_DIR}/tiko_pitr" list

# 5. Recover to the target LSN. This stops PG, restores the latest backup at/before
#    the target, installs its base manifest, deletes the pre-recovery segments,
#    replays WAL, promotes, and then STOPS the db. `restart` brings it back up
#    so the verification queries below can connect.
echo "--- tiko_pitr recover --lsn ${TARGET_LSN} ---"
"${TIKO_BIN_DIR}/tiko_pitr" recover --pgdata tt --pg-ctl "${PG_BIN_DIR}/pg_ctl" --log-file log.log --lsn "${TARGET_LSN}"

echo "--- tiko_pitr restart ---"
"${TIKO_BIN_DIR}/tiko_pitr" restart --pgdata tt --pg-ctl "${PG_BIN_DIR}/pg_ctl" --log-file log.log

# 6. Verify the recovered state.
#    - count = 201: the 200 pre-backup rows (read via the base manifest) + the
#      one replayed 'after_backup' row.
#    - id=50 data = 'orig' (NOT 'FUTURE'): the post-target UPDATE was not
#      replayed, and the smgr must resolve id=50's page from the base manifest
#      at the backup LSN — the cross-LSN read that the hydration/skip fix + the
#      baseback-compaction-through + segment deletion make correct.
echo "--- verify recovered data ---"
COUNT=$($PG_BIN_DIR/psql -d postgres -Atqc "select count(*) from pitr_test")
ID50=$($PG_BIN_DIR/psql -d postgres -Atqc "select data from pitr_test where id=50")
echo "row count: ${COUNT}; id=50 data: '${ID50}'"
if [ "${COUNT}" != "201" ]; then
  echo "PITR FAILED: expected 201 rows, got ${COUNT}" >&2
  $PG_BIN_DIR/pg_ctl -D tt -l log.log stop -m fast -w 2>/dev/null || true
  exit 1
fi
if [ "${ID50}" != "orig" ]; then
  echo "PITR FAILED: id=50 should be 'orig' (post-target UPDATE must not be visible), got '${ID50}'" >&2
  $PG_BIN_DIR/pg_ctl -D tt -l log.log stop -m fast -w 2>/dev/null || true
  exit 1
fi

# Cleanup.
$PG_BIN_DIR/pg_ctl -D tt -l log.log stop -m fast -w
echo "PITR test passed. ✅"
