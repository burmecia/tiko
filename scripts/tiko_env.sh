#!/bin/bash
#
# Common Tiko environment for guest PostgreSQL scripts. Sourced (not executed
# directly) by init_pg.sh and start_pg.sh.
#
# Tiko identity (org/db/project) comes from /var/lib/postgresql/tiko.env, which
# is written per-VM by start_vm.sh on the host. If absent (base-image /
# single-VM case), defaults below are used. Inherited env vars win over the
# file, so `TIKO_DB_ID=7 ./start_pg.sh` overrides.

PGHOME=/var/lib/postgresql
S3FILES=/mnt/s3files
DB="tt"

# Per-VM identity from tiko.env (single source of truth, also sourced by
# .bash_profile for login shells).
if [ -f "$PGHOME/tiko.env" ]; then
    set -a
    . "$PGHOME/tiko.env"
    set +a
fi

export TIKO_ORG_ID="${TIKO_ORG_ID:-12}"
export TIKO_DB_ID="${TIKO_DB_ID:-34}"
export TIKO_PROJECT_ID="${TIKO_PROJECT_ID:-56}"
export TIKO_STORAGE_ROOT="${TIKO_STORAGE_ROOT:-$S3FILES/tiko_root}"
export TIKO_LOCAL_PATH="${TIKO_LOCAL_PATH:-$PGHOME/tiko_local}"
