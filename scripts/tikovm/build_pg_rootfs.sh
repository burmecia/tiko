#!/bin/bash
# =============================================================================
# Build a Tiko Postgres rootfs for the tikovm platform.
#
# Derivative of tikovm-base-rootfs.ext4. Installs PostgreSQL 18 + Tiko storage
# extensions (tikosmgr + tikoworker), the tikovm-guestd agent, CLI tools, and
# a Lambda-style pg-supervisor entrypoint.
#
# COMPUTE-STORAGE SEPARATION (Lambda model):
#   - PGDATA (catalogs, WAL, control files) on local_fast (/mnt/data/pgdata).
#     Hot data on fast local storage; persists across suspend/restore AND
#     across destroy when the provision request sets a persist_key on the
#     volume (see scripts/tikovm/provision-pg.json).
#   - Chunk cache (TIKO_LOCAL_PATH) on local_fast (/mnt/data/tiko_local).
#   - Bulk data (TIKO_STORAGE_ROOT — chunks, WAL, manifests) on remote_slow
#     (/mnt/archive/tiko_root). Durable; persists across destroy.
#
# The pg-supervisor creates a symlink /var/lib/postgresql/tt -> /mnt/data/pgdata
# so all existing PG scripts and CLI tools (which use DB="tt") work unchanged.
# On first seed, initdb creates PGDATA on local_fast. On subsequent boots
# (after suspend/restore, or re-provision onto a persistent local_fast
# volume), the existing PGDATA is reused — fast start/stop.
#
# Prerequisites: build_postgres.sh must have been run (target/pg-install/).
#
# Output: tikod/assets/pg-rootfs.ext4
# =============================================================================
set -euo pipefail

REPO=/home/ubuntu/tiko
BASE=$REPO/tikod/assets/tikovm-base-rootfs.ext4
OUT=$REPO/tikod/assets/pg-rootfs.ext4
GUESTD=$REPO/target/debug/tikovm-guestd
PG_INSTALL_DIR="$REPO/target/pg-install"

[ -f "$GUESTD" ]      || { echo "build guestd first: cargo build -p tikovm-guest"; exit 1; }
[ -f "$BASE" ]        || { echo "build the tikovm base first: bash scripts/tikovm/build_base_rootfs.sh"; exit 1; }
[ -d "$PG_INSTALL_DIR" ] || { echo "build Postgres first: ./scripts/build_postgres.sh"; exit 1; }

echo ">>> Copy + resize base rootfs to 4 GB ..."
rm -f "$OUT"
cp --sparse=always "$BASE" "$OUT"
truncate -s 4096M "$OUT"
e2fsck -fy "$OUT" >/dev/null 2>&1 || true
resize2fs "$OUT" >/dev/null

MNT=$(mktemp -d)
cleanup() {
  sudo umount "$MNT/dev"  2>/dev/null || true
  sudo umount "$MNT/sys"  2>/dev/null || true
  sudo umount "$MNT/proc" 2>/dev/null || true
  sudo umount "$MNT"      2>/dev/null || true
  rmdir "$MNT" 2>/dev/null || true
}
trap cleanup EXIT

echo ">>> Mounting $OUT ..."
sudo mount -o loop "$OUT" "$MNT"

# ---- Install PG binaries (from build_postgres.sh output) --------------------
echo ">>> Installing PostgreSQL + Tiko extensions ..."
sudo cp -r "$PG_INSTALL_DIR"/* "$MNT/usr/local/"

# ---- Create postgres user + install psql (for idle checks) ------------------
echo ">>> Creating postgres user + installing postgresql-client ..."
sudo mount --bind /proc "$MNT/proc"
sudo mount --bind /sys  "$MNT/sys"
sudo mount --bind /dev  "$MNT/dev"
sudo chroot "$MNT" /bin/bash <<'EOF'
export DEBIAN_FRONTEND=noninteractive
# Create postgres user (system user with home + bash shell)
if ! id postgres >/dev/null 2>&1; then
    useradd --system --create-home --home-dir /var/lib/postgresql --shell /bin/bash postgres
fi
apt-get update -qq
apt-get install -y --no-install-recommends postgresql-client >/dev/null
apt-get clean
rm -rf /var/lib/apt/lists/*
EOF
sudo umount "$MNT/dev"
sudo umount "$MNT/sys"
sudo umount "$MNT/proc"

# ---- Install PG scripts + config --------------------------------------------
echo ">>> Installing PG scripts + config ..."
sudo install -d -m755 "$MNT/var/lib/postgresql"
sudo install -m755 "$REPO/scripts/tiko_env.sh" "$MNT/var/lib/postgresql/tiko_env.sh"
sudo install -m644 "$REPO/scripts/postgresql.tiko.conf" "$MNT/var/lib/postgresql/postgresql.tiko.conf"

# ---- Build + install CLI tools ----------------------------------------------
echo ">>> Building + installing CLI tools ..."
( cd "$REPO" && cargo build --release -p cli ) || { echo "cargo build -p cli failed"; exit 1; }
sudo install -d -m755 "$MNT/usr/local/libexec"
sudo install -m755 "$REPO/target/release/tiko_tlseg_viewer" "$MNT/usr/local/bin/tiko_tlseg_viewer"
for bin in tiko_pitr tiko_branch tiko_restore; do
    sudo install -m755 "$REPO/target/release/$bin" "$MNT/usr/local/libexec/$bin"
    sudo install -m755 "$REPO/scripts/$bin.sh" "$MNT/usr/local/bin/$bin"
done

# ---- Install tikovm-guestd --------------------------------------------------
echo ">>> Installing tikovm-guestd ..."
sudo install -m755 "$GUESTD" "$MNT/usr/local/bin/tikovm-guestd"

# ---- pg-supervisor: the Lambda-style entrypoint -----------------------------
echo ">>> Installing pg-supervisor + pg-idle-check ..."
sudo tee "$MNT/usr/local/bin/pg-supervisor" >/dev/null <<'SCRIPT'
#!/bin/bash
# Lambda-style PG supervisor for tikovm.
#
# Runs as root (for volume setup + initdb), then execs postgres as the postgres
# user. The tikovm-guestd supervisor tracks this PID; on suspend/restore the
# entire VM is frozen/thawed (no process-level signals needed).
#
# PGDATA lives on local_fast (/mnt/data/pgdata) — fast I/O, persists across
# suspend/restore, and across destroy when the volume carries a persist_key
# (provision-side). Bulk data goes through the smgr to
# remote_slow (/mnt/archive/tiko_root) — durable, persists across destroy.
set -ex

export PATH="/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin"

PGHOME=/var/lib/postgresql
PGDATA_VOL=/mnt/data/pgdata

# Storage paths on tikovm volumes (mounted by guestd before this starts)
export TIKO_STORAGE_ROOT=/mnt/archive/tiko_root
export TIKO_LOCAL_PATH=/mnt/data/tiko_local
export TIKO_ORG_ID="${TIKO_ORG_ID:-1}"
export TIKO_DB_ID="${TIKO_DB_ID:-1}"
export TIKO_PROJECT_ID="${TIKO_PROJECT_ID:-1}"

# Ensure volume dirs exist and are writable by postgres
mkdir -p "$PGDATA_VOL" "$TIKO_STORAGE_ROOT" "$TIKO_LOCAL_PATH" "$PGHOME"
chown -R postgres:postgres /mnt/data /mnt/archive "$PGHOME"

# Symlink: $PGHOME/tt -> PGDATA on local_fast. Preserves the DB="tt" convention
# from tiko_env.sh so all existing scripts and CLI tools work unchanged.
ln -sfn "$PGDATA_VOL" "$PGHOME/tt"

# Write tiko.env (sourced by tiko_env.sh; overrides the /mnt/s3files defaults)
cat > "$PGHOME/tiko.env" << ENV
TIKO_ORG_ID=$TIKO_ORG_ID
TIKO_DB_ID=$TIKO_DB_ID
TIKO_PROJECT_ID=$TIKO_PROJECT_ID
TIKO_STORAGE_ROOT=$TIKO_STORAGE_ROOT
TIKO_LOCAL_PATH=$TIKO_LOCAL_PATH
ENV
chown postgres:postgres "$PGHOME/tiko.env"

# Initialize PGDATA on first seed only. On subsequent boots (after
# suspend/restore, or re-provision onto a persistent local_fast volume), the
# existing PGDATA on local_fast is reused — fast start.
if [ ! -f "$PGDATA_VOL/PG_VERSION" ]; then
    echo "pg-supervisor: initializing PGDATA at $PGDATA_VOL (first seed)..."
    su postgres -c "export PATH=/usr/local/bin:/usr/bin:/bin; initdb -D $PGDATA_VOL"
    cp "$PGHOME/postgresql.tiko.conf" "$PGDATA_VOL/"
    echo "include_if_exists='postgresql.tiko.conf'" >> "$PGDATA_VOL/postgresql.conf"
    # Trust all connections from the per-VM TAP subnet (172.16.N.0/24)
    echo "host all all 172.16.0.0/16 trust" >> "$PGDATA_VOL/pg_hba.conf"
    echo "pg-supervisor: initdb complete"
fi

# Start PG in foreground. su -c with inner exec replaces su's child shell with
# postgres. The guestd supervisor tracks su's PID; on suspend/restore the
# entire VM is frozen/thawed at the hardware level (no process signals needed).
echo "pg-supervisor: starting PG (PGDATA=$PGDATA_VOL via $PGHOME/tt)..."
exec su postgres -c ". $PGHOME/tiko_env.sh && exec postgres -D $PGHOME/tt"
SCRIPT
sudo chmod 755 "$MNT/usr/local/bin/pg-supervisor"

# ---- pg-idle-check: exec probe for the idle evaluator -----------------------
sudo tee "$MNT/usr/local/bin/pg-idle-check" >/dev/null <<'SCRIPT'
#!/bin/sh
# pg-idle-check: exec idle probe for tikovm-guestd's idle evaluator.
# Exit 0 (idle) if PG has no active CLIENT backends (excluding this probe's
# own connection); exit 1 (busy) otherwise or if PG is unreachable.
#
# Two filters:
# - pid != pg_backend_pid() excludes this probe's own connection (its own
#   SELECT would otherwise count as an "active" backend).
# - backend_type = 'client backend' excludes non-session backends — crucially
#   the tikoworker WAL-streaming walsender (START_REPLICATION SLOT
#   tiko_wal_stream), which sits at state='active' forever and would block
#   scale-to-zero permanently; autovacuum workers are likewise ignored.
ACTIVE=$(psql -h 127.0.0.1 -p 5432 -U postgres -t -A -c \
    "SELECT count(*) FROM pg_stat_activity WHERE state != 'idle' AND pid != pg_backend_pid() AND backend_type = 'client backend'" \
    2>/dev/null) || ACTIVE=1
[ "${ACTIVE:-1}" -eq 0 ]
SCRIPT
sudo chmod 755 "$MNT/usr/local/bin/pg-idle-check"

# ---- workload manifest ------------------------------------------------------
echo ">>> Injecting workload manifest ..."
sudo mkdir -p "$MNT/etc/tikovm"
sudo tee "$MNT/etc/tikovm/workload.toml" >/dev/null <<'TOML'
version = 1
workload = "postgres"

# Lambda-style PG supervisor: sets up volumes, initializes PGDATA on first
# seed, then execs postgres in foreground. PGDATA on local_fast (fast I/O);
# bulk data through the smgr to remote_slow.
[process]
cmd = "/usr/local/bin/pg-supervisor"

[health]
kind = "tcp"
port = 5432
interval_secs = 5

# Scale-to-zero: suspend the VM after 30s of no PG activity AND no network
# connections. The exec probe checks pg_stat_activity (excluding its own
# connection); the host_network probe checks for external traffic.
[idle]
tick_secs = 5
idle_secs = 30
[[idle.probes]]
kind = "host_network"
[[idle.probes]]
kind = "exec"
cmd = "/usr/local/bin/pg-idle-check"

# local_fast: PGDATA + chunk cache. Fast local storage; persists across
# suspend/restore; persists across destroy when the provision request sets a
# persist_key on this volume (see provision-pg.json).
[[volumes]]
name = "data"
tier = "local_fast"
mount_path = "/mnt/data"
size_mb = 768

# remote_slow: TIKO_STORAGE_ROOT (chunks, WAL, manifests). Durable storage;
# persists across destroy. The smgr reads/writes bulk data here.
[[volumes]]
name = "archive"
tier = "remote_slow"
mount_path = "/mnt/archive"
size_mb = 512

[suspend]
pre_suspend_cmd = "echo tikovm: pre-suspend hook ran (postgres)"
post_restore_cmd = "echo tikovm: post-restore hook ran (postgres)"
TOML

# ---- systemd unit -----------------------------------------------------------
echo ">>> Installing systemd unit ..."
sudo tee "$MNT/etc/systemd/system/tikovm-guestd.service" >/dev/null <<'UNIT'
[Unit]
Description=tikovm guest agent (Postgres workload)
After=network-online.target systemd-networkd.service

[Service]
ExecStart=/usr/local/bin/tikovm-guestd
Restart=on-failure
RestartSec=2
StandardOutput=journal+console
StandardError=journal+console

[Install]
WantedBy=multi-user.target
UNIT

sudo mkdir -p "$MNT/etc/systemd/system/multi-user.target.wants"
sudo ln -sf /etc/systemd/system/tikovm-guestd.service \
            "$MNT/etc/systemd/system/multi-user.target.wants/tikovm-guestd.service"

# Defensive: mask the legacy tikoguest agent.
sudo ln -sf /dev/null "$MNT/etc/systemd/system/tikoguest.service"

sync
sudo umount "$MNT"
echo ">>> Built: $OUT"
echo "    PGDATA on local_fast (/mnt/data/pgdata) — fast start/stop"
echo "    Storage on remote_slow (/mnt/archive/tiko_root) — durable"
