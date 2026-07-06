#!/bin/bash
#
# Wrapper for tiko_branch that sources the Tiko environment (parent identity,
# storage paths) before exec'ing the real binary. Installed at
# /usr/local/bin/tiko_branch; the Rust binary lives in /usr/local/libexec/.
#
# Usage: tiko_branch <args>   (see `tiko_branch --help`)

set -euo pipefail

. /var/lib/postgresql/tiko_env.sh

exec /usr/local/libexec/tiko_branch "$@"
