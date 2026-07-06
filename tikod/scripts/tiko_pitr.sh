#!/bin/bash
#
# Wrapper for tiko_pitr that sources the Tiko environment (identity, storage
# paths, PGDATA) before exec'ing the real binary. Installed at
# /usr/local/bin/tiko_pitr; the Rust binary lives in /usr/local/libexec/.
#
# Usage: tiko_pitr <subcommand> [args]   (see `tiko_pitr --help`)

set -euo pipefail

. /var/lib/postgresql/tiko_env.sh

export PGDATA="${PGDATA:-$PGHOME/$DB}"

exec /usr/local/libexec/tiko_pitr "$@"
