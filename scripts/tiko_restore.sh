#!/bin/bash
#
# Wrapper for tiko_restore that sources the Tiko environment (identity, storage
# paths) before exec'ing the real binary. Installed at /usr/local/bin/tiko_restore;
# the Rust binary lives in /usr/local/libexec/.
#
# Exit codes are passed through unchanged, preserving the restore_command
# contract (0 = restored, nonzero = not found / error).
#
# Usage: tiko_restore <wal_filename> <dest_path>

set -euo pipefail

. /var/lib/postgresql/tiko_env.sh

exec /usr/local/libexec/tiko_restore "$@"
