#!/bin/bash

set -euo pipefail

PGHOME=/var/lib/postgresql
S3FILES=/mnt/s3files
DB="tt"

export TIKO_ORG_ID="12"
export TIKO_DB_ID="34"
export TIKO_PROJECT_ID="56"
export TIKO_STORAGE_ROOT="$S3FILES/tiko_root"
export TIKO_LOCAL_PATH="$PGHOME/tiko_local"

cd $PGHOME
rm -rf $DB log.log

initdb -D $DB
cp postgresql.tiko.conf $DB
echo "include_if_exists='postgresql.tiko.conf'" >> $DB/postgresql.conf
echo "host all all 172.16.0.0/24 trust" >> $DB/pg_hba.conf

pg_ctl -D $DB -l log.log start
