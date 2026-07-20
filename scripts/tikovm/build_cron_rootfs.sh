#!/bin/bash
# =============================================================================
# Build a scheduled-job ("cron") rootfs as a derivative of the tikovm base
# rootfs. The workload is a "hello world" echo job: a shell script that
# prints a timestamped hello message in a loop to stdout (captured by the
# serial console). Combined with the manifest's [idle] policy (auto-suspend
# after a few seconds of no HTTP traffic) and the provision request's
# [schedule] (host-driven restore), this produces a periodic "hello world
# from scheduled job" burst in the serial log each time the scheduler wakes
# the VM — the 3rd tikovm workload kind: scheduled jobs (design §13).
#
# No language runtime, no HTTP server, no SSH key management beyond what the
# base already bakes in. The entire job is a /bin/sh script.
#
# Output: tikod/assets/cron-rootfs.ext4
# =============================================================================
set -euo pipefail

REPO=/home/ubuntu/tiko
# tikovm-family base (scripts/tikovm/build_base_rootfs.sh).
BASE=$REPO/tikod/assets/tikovm-base-rootfs.ext4
OUT=$REPO/tikod/assets/cron-rootfs.ext4
GUESTD=$REPO/target/debug/tikovm-guestd

[ -f "$GUESTD" ] || { echo "build guestd first: cargo build -p tikovm-guest"; exit 1; }
[ -f "$BASE" ]   || { echo "build the tikovm base first: bash scripts/tikovm/build_base_rootfs.sh"; exit 1; }

if [ ! -f "$OUT" ]; then
  echo "sparse-copying base rootfs -> $OUT (one-time)"
  cp --sparse=always "$BASE" "$OUT"
fi

MNT=$(mktemp -d)
cleanup() { sudo umount "$MNT" 2>/dev/null || true; rmdir "$MNT" 2>/dev/null || true; }
trap cleanup EXIT

echo "mounting $OUT at $MNT"
sudo mount -o loop "$OUT" "$MNT"

echo "injecting guestd + cron-echo job + manifest + systemd unit"
sudo install -m755 "$GUESTD" "$MNT/usr/local/bin/tikovm-guestd"

# The "hello world" job: a shell loop that prints a timestamped message every
# 2 seconds. The supervisor (Always) keeps it alive across suspend/restore;
# on each scheduler-driven restore the process resumes from its snapshotted
# state and prints again within ~2s, surfacing new output in the serial log.
sudo install -d -m755 "$MNT/usr/local/lib/tikovm"
sudo tee "$MNT/usr/local/lib/tikovm/cron-echo.sh" >/dev/null <<'SH'
#!/bin/sh
# Minimal "hello world" scheduled job for the tikovm cron rootfs.
#
# Prints a timestamped hello message to stdout (captured by the serial
# console) every 2 seconds AND appends a line to /mnt/data/cron-runs.log
# so the e2e test can verify (via SSH) that the job actually executed
# after each scheduler-driven wake — the serial log is unreliable across
# Firecracker snapshot/restore.
#
# Combined with the manifest's [idle] policy (auto-suspend after a few
# seconds of no HTTP traffic) and the provision request's [schedule]
# (host-driven restore), this produces a periodic "hello world from
# scheduled job" burst each time the scheduler wakes the VM — the
# scheduled-job / cron workload pattern (design §13).
set -eu

RUNS_LOG=/mnt/data/cron-runs.log

while true; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  epoch=$(date +%s)
  echo "hello world from scheduled job at $ts"
  echo "$epoch $ts" >> "$RUNS_LOG"
  sleep 2
done
SH
sudo chmod 755 "$MNT/usr/local/lib/tikovm/cron-echo.sh"

sudo mkdir -p "$MNT/etc/tikovm"
sudo tee "$MNT/etc/tikovm/workload.toml" >/dev/null <<'TOML'
version = 1
workload = "cron-echo"

# The "job": a shell loop that prints hello world every 2s. The supervisor
# (Always) keeps it alive across suspend/restore, so it resumes printing
# shortly after each scheduler-driven wake.
[process]
cmd = "/bin/sh"
args = ["/usr/local/lib/tikovm/cron-echo.sh"]

[restart]
policy = "always"
backoff_secs = 2

# No HTTP server => the host_network probe always reports idle (no traffic).
# idle_secs thus acts as a "suspend shortly after each scheduled wake" timer.
# The scheduler (host-side, reading [schedule] from the provision request)
# restores the VM on the configured interval. Together they produce the
# periodic-run pattern without any workload-specific scheduler code.
[idle]
tick_secs = 2
idle_secs = 6
[[idle.probes]]
kind = "host_network"

[suspend]
pre_suspend_cmd = "echo tikovm: pre-suspend hook ran (cron-echo)"
post_restore_cmd = "echo tikovm: post-restore hook ran (cron-echo)"

# A local_fast volume: available as scratch space for the job. (Not required
# for the hello-world demo, but declared for parity with the platform's
# 2-tier storage model.)
[[volumes]]
name = "data"
tier = "local_fast"
mount_path = "/mnt/data"
size_mb = 64
TOML

sudo tee "$MNT/etc/systemd/system/tikovm-guestd.service" >/dev/null <<'UNIT'
[Unit]
Description=tikovm guest agent (scheduled-job workload)
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

# Defensive: mask the legacy Tiko agent from the tikod platform. The tikovm
# base already does this, but keep it so a future base swap can't resurrect it.
sudo ln -sf /dev/null "$MNT/etc/systemd/system/tikoguest.service"

sync
sudo umount "$MNT"
echo "built $OUT"
echo "the job writes to the serial console; verify with:"
echo "  grep 'hello world from scheduled job' \$DD/snapshots/runtime/vm-1.console.log"
