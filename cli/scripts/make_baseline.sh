#!/usr/bin/env bash
# scripts/make_baseline.sh
# Usage: ./make_baseline.sh <pg_bindir> <output_tarball>
#   e.g. ./make_baseline.sh pg-install/bin baseline-18.tar.gz
#
# The baseline must be regenerated when:
#
# - PostgreSQL major version bumps (PG_VERSION changes → postmaster refuses to start if mismatched)
# - You change the template postgresql.conf defaults
# - Any of the fixed system catalog OIDs change (extremely rare; only on major PG version)

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <pg_bindir> <output_tarball>"
    echo ""
    echo "  pg_bindir       directory containing the PostgreSQL binaries (initdb, postgres)"
    echo "  output_tarball  path for the generated baseline tarball (e.g. baseline-18.tar.gz)"
    echo ""
    echo "Example:"
    echo "  $0 pg-install/bin baseline-18.tar.gz"
    exit 1
fi

PG_BINDIR="$1"
OUTPUT="$2"

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

SKEL="$WORK/baseline"
"$PG_BINDIR/initdb" -D "$SKEL" --data-checksums

# Strip pg_control (will be provided by pg_state.tar.zst at restore)
rm "$SKEL/global/pg_control"

# Strip WAL contents (keep the directory)
rm -rf "$SKEL/pg_wal"/*

# Strip transactional state contents (come from pg_state.tar.zst)
rm -rf "$SKEL/pg_xact"/*
rm -rf "$SKEL/pg_commit_ts"/*
rm -rf "$SKEL/pg_multixact/members"/* "$SKEL/pg_multixact/offsets"/*

# Strip relation files — keep ONLY pg_filenode.map and pg_internal.init
find "$SKEL/global" -maxdepth 1 -type f \
    ! -name 'pg_filenode.map' ! -name 'pg_internal.init' -delete
find "$SKEL/base" -mindepth 2 -maxdepth 2 -type f \
    ! -name 'pg_filenode.map' ! -name 'pg_internal.init' -delete

tar -czf "$OUTPUT" -C "$WORK" baseline
echo "Created $OUTPUT ($(du -sh "$OUTPUT" | cut -f1))"
